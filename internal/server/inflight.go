package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mostlygeek/llama-swap/internal/chain"
	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/event"
	"github.com/mostlygeek/llama-swap/internal/router"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

const inflightUpdateInterval = 250 * time.Millisecond
const inflightOutboxSize = 128

// stageUpdateInterval is how often the stage updater polls the scheduler queue
// snapshot. It is deliberately larger than inflightUpdateInterval so the
// 250ms SSE throttle absorbs most queue -> serving transitions without an
// extra flush.
const stageUpdateInterval = 500 * time.Millisecond

const (
	inflightOperationSnapshot = "snapshot"
	inflightOperationUpsert   = "upsert"
	inflightOperationRemove   = "remove"
)

type inflightStartContextKey struct{}

func markInflightStart(r *http.Request) *http.Request {
	if _, ok := r.Context().Value(inflightStartContextKey{}).(time.Time); ok {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), inflightStartContextKey{}, time.Now()))
}

func inflightStart(r *http.Request) time.Time {
	if started, ok := r.Context().Value(inflightStartContextKey{}).(time.Time); ok {
		return started
	}
	return time.Now()
}

// inflightTracker tracks in-flight model-dispatched requests and their
// cancellable contexts.
type inflightTracker struct {
	nextID atomic.Uint64

	// mu serializes state mutations and their corresponding outbox writes so
	// request updates keep the same order in which they were applied.
	mu       sync.RWMutex
	requests map[string]*inflightRequest

	updates          chan swaputil.InFlightRequestsEvent
	needsSnapshot    atomic.Bool
	publisherRunning atomic.Bool
	publish          func(swaputil.InFlightRequestsEvent)

	// queueSnapshot, when non-nil, returns the router scheduler queue's
	// current contents. The stage updater polls it to decide each tracked
	// request's "queued" / "serving" stage. Requests routed to a scheduler
	// without a queue (or peer-direct requests) never appear in it and keep
	// stage "serving".
	queueSnapshot       func() []router.QueueInfo
	stageUpdaterRunning atomic.Bool
}

type inflightRequest struct {
	entry       swaputil.InflightRequestEntry
	cancel      context.CancelFunc
	lastEmitted time.Time
	timer       *time.Timer
}

func newInflightTracker() *inflightTracker {
	return newInflightTrackerWithPublisher(inflightOutboxSize, func(update swaputil.InFlightRequestsEvent) {
		event.Emit(update)
	})
}

// newInflightTrackerWithQueueSnapshot wires the scheduler queue poller that
// marks tracked requests as queued or serving.
func newInflightTrackerWithQueueSnapshot(snapshot func() []router.QueueInfo) *inflightTracker {
	t := newInflightTrackerWithPublisher(inflightOutboxSize, func(update swaputil.InFlightRequestsEvent) {
		event.Emit(update)
	})
	t.queueSnapshot = snapshot
	return t
}

func newInflightTrackerWithPublisher(size int, publish func(swaputil.InFlightRequestsEvent)) *inflightTracker {
	t := &inflightTracker{
		requests: make(map[string]*inflightRequest),
		updates:  make(chan swaputil.InFlightRequestsEvent, size),
		publish:  publish,
	}
	return t
}

func (t *inflightTracker) Add(r *http.Request, cancel context.CancelFunc) string {
	id := strconv.FormatUint(t.nextID.Add(1), 10)
	entry := swaputil.InflightRequestEntry{
		ID:          id,
		Timestamp:   inflightStart(r),
		ReqPath:     r.URL.Path,
		Method:      r.Method,
		ReqHeaders:  headerMap(r.Header),
		RemoteIP:    clientIP(r),
		RespHeaders: map[string]string{},
		// Every tracked request starts out serving; the stage updater flips
		// it to "queued" once the scheduler reports it in the queue.
		Stage: "serving",
	}
	redactHeaders(entry.ReqHeaders)
	if data, ok := swaputil.ReadContext(r.Context()); ok {
		entry.Model = data.ModelID
		entry.Metadata = copyMetadata(data.Metadata)
	}

	t.mu.Lock()
	req := &inflightRequest{entry: entry, cancel: cancel, lastEmitted: time.Now()}
	t.requests[id] = req
	t.enqueueLocked(upsertInflightEvent(req.entry))
	t.mu.Unlock()
	t.startStageUpdater()
	return id
}

func (t *inflightTracker) Remove(id string) {
	t.mu.Lock()
	req, ok := t.requests[id]
	if ok {
		delete(t.requests, id)
		if req.timer != nil {
			req.timer.Stop()
		}
		t.enqueueLocked(swaputil.InFlightRequestsEvent{Operation: inflightOperationRemove, ID: id})
	}
	t.mu.Unlock()
}

// startStageUpdater launches the periodic stage-polling goroutine. Like
// startPublisher it uses an idle-exit lifecycle: it starts when the first
// request is tracked (and only when a queue snapshot source is wired —
// otherwise there is nothing to poll) and the goroutine exits once requests
// disappear, so a tracker that is never used does not tick forever.
func (t *inflightTracker) startStageUpdater() {
	if t.queueSnapshot == nil {
		return
	}
	if t.stageUpdaterRunning.CompareAndSwap(false, true) {
		go t.runStageUpdater()
	}
}

func (t *inflightTracker) runStageUpdater() {
	// reclaimStageUpdater runs on exit (idle or error) and re-claims the
	// running slot when a request raced the transition to idle.
	defer t.reclaimStageUpdater()
	ticker := time.NewTicker(stageUpdateInterval)
	defer ticker.Stop()
	for range ticker.C {
		// updateStages returns false to signal "nothing left to poll".
		if !t.updateStages() {
			return
		}
	}
}

// reclaimStageUpdater is the stage updater's idle-exit defense. It mirrors the
// pattern publishUpdates uses for the equivalent race: Store(false) releases
// the "running" flag first, so an Add on the other side of the exit window can
// win the startStageUpdater CAS and launch its own poller; the re-check below
// then notices a request that landed during the window, re-claims the slot via
// CAS, and restarts the poller. Without this, such a request would keep the
// "serving" stage forever even while it sits in the queue.
func (t *inflightTracker) reclaimStageUpdater() {
	t.stageUpdaterRunning.Store(false)
	// Do not restart without a snapshot source: startStageUpdater refuses to
	// launch in that case, so a detached poller could only churn one tick.
	if t.queueSnapshot == nil {
		return
	}
	t.mu.Lock()
	more := len(t.requests) > 0
	t.mu.Unlock()
	if more && t.stageUpdaterRunning.CompareAndSwap(false, true) {
		go t.runStageUpdater()
	}
}

// updateStages polls the scheduler queue snapshot and flips every tracked
// request whose stage or queue position changed, pushing one upsert event per
// change through the existing outbox (250ms SSE throttle applies as usual).
//
// Stage semantics are approximate (v1): only requests present in the queue
// snapshot are marked "queued"; fast-path admitted requests and requests
// joined to an in-progress swap (join waiters) are not queued and keep the
// default "serving" stage.
//
// It returns false when there is no snapshot source or no tracked request,
// which tells runStageUpdater to exit. Called directly in tests; lock-free
// aside from the requests map mutex, which it takes itself.
func (t *inflightTracker) updateStages() bool {
	snapshot := t.queueSnapshot
	if snapshot == nil {
		return false
	}

	// Build the position map outside the lock: QueueSnapshot round-trips
	// through the router's run-loop goroutine and may block on scheduler
	// work.
	positions := make(map[string]int)
	for _, q := range snapshot() {
		if q.RequestID != "" {
			positions[q.RequestID] = q.QueuePosition
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.requests) == 0 {
		return false
	}

	for id, req := range t.requests {
		stage, pos := "serving", 0
		if qp, ok := positions[id]; ok {
			stage, pos = "queued", qp
		}
		if req.entry.Stage != stage || req.entry.QueuePosition != pos {
			req.entry.Stage = stage
			req.entry.QueuePosition = pos
			t.enqueueLocked(upsertInflightEvent(req.entry))
		}
	}
	return true
}

func (t *inflightTracker) SetResponseHeaders(id string, headers http.Header) {
	values := headerMap(headers)
	redactHeaders(values)

	t.mu.Lock()
	req, ok := t.requests[id]
	if ok {
		req.entry.RespHeaders = values
		req.lastEmitted = time.Now()
		t.enqueueLocked(upsertInflightEvent(req.entry))
	}
	t.mu.Unlock()
}

func (t *inflightTracker) AddResponseBytes(id string, total int) {
	if total <= 0 {
		return
	}

	t.mu.Lock()
	req, ok := t.requests[id]
	if !ok {
		t.mu.Unlock()
		return
	}
	req.entry.RespBytes += int64(total)

	now := time.Now()
	remaining := inflightUpdateInterval - now.Sub(req.lastEmitted)
	if remaining <= 0 && req.timer == nil {
		req.lastEmitted = now
		t.enqueueLocked(upsertInflightEvent(req.entry))
		t.mu.Unlock()
		return
	}
	if req.timer == nil {
		if remaining < 0 {
			remaining = 0
		}
		req.timer = time.AfterFunc(remaining, func() { t.emitPending(id) })
	}
	t.mu.Unlock()
}

func (t *inflightTracker) emitPending(id string) {
	t.mu.Lock()
	req, ok := t.requests[id]
	if !ok {
		t.mu.Unlock()
		return
	}
	req.timer = nil
	req.lastEmitted = time.Now()
	t.enqueueLocked(upsertInflightEvent(req.entry))
	t.mu.Unlock()
}

// enqueueLocked adds an update without allowing event-bus backpressure to
// block the request path. On overflow, a later snapshot replaces any dropped
// incremental updates with the tracker's authoritative state.
func (t *inflightTracker) enqueueLocked(update swaputil.InFlightRequestsEvent) {
	select {
	case t.updates <- update:
	default:
		t.needsSnapshot.Store(true)
	}
	t.startPublisher()
}

func (t *inflightTracker) startPublisher() {
	if t.publisherRunning.CompareAndSwap(false, true) {
		go t.publishUpdates()
	}
}

func (t *inflightTracker) publishUpdates() {
	for {
		select {
		case update := <-t.updates:
			t.publish(refreshInflightElapsed(update))
			t.publishRecoverySnapshots()
		default:
			t.publisherRunning.Store(false)
			// An enqueue racing with the transition to idle either starts a new
			// publisher or leaves work here for this publisher to reclaim.
			if len(t.updates) > 0 && t.publisherRunning.CompareAndSwap(false, true) {
				continue
			}
			return
		}
	}
}

func (t *inflightTracker) publishRecoverySnapshots() {
	for t.needsSnapshot.Swap(false) {
		t.discardQueuedUpdates()
		t.publish(t.Current())
	}
}

func (t *inflightTracker) discardQueuedUpdates() {
	for {
		select {
		case <-t.updates:
		default:
			return
		}
	}
}

func (t *inflightTracker) Cancel(id string) bool {
	t.mu.RLock()
	req, ok := t.requests[id]
	t.mu.RUnlock()
	if !ok {
		return false
	}
	req.cancel()
	return true
}

func (t *inflightTracker) Current() swaputil.InFlightRequestsEvent {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return swaputil.InFlightRequestsEvent{
		Operation: inflightOperationSnapshot,
		Requests:  t.snapshotLocked(),
	}
}

func (t *inflightTracker) snapshotLocked() []swaputil.InflightRequestEntry {
	requests := make([]swaputil.InflightRequestEntry, 0, len(t.requests))
	for _, req := range t.requests {
		requests = append(requests, copyInflightEntry(req.entry))
	}
	sort.Slice(requests, func(i, j int) bool {
		if requests[i].Timestamp.Equal(requests[j].Timestamp) {
			return requests[i].ID < requests[j].ID
		}
		return requests[i].Timestamp.Before(requests[j].Timestamp)
	})
	return requests
}

func upsertInflightEvent(entry swaputil.InflightRequestEntry) swaputil.InFlightRequestsEvent {
	entry = copyInflightEntry(entry)
	return swaputil.InFlightRequestsEvent{Operation: inflightOperationUpsert, Request: &entry}
}

func copyInflightEntry(entry swaputil.InflightRequestEntry) swaputil.InflightRequestEntry {
	entry.Metadata = copyMetadata(entry.Metadata)
	entry.ReqHeaders = copyStringMap(entry.ReqHeaders)
	entry.RespHeaders = copyStringMap(entry.RespHeaders)
	setInflightElapsed(&entry)
	return entry
}

func refreshInflightElapsed(update swaputil.InFlightRequestsEvent) swaputil.InFlightRequestsEvent {
	if update.Request != nil {
		entry := *update.Request
		setInflightElapsed(&entry)
		update.Request = &entry
	}
	for i := range update.Requests {
		setInflightElapsed(&update.Requests[i])
	}
	return update
}

func setInflightElapsed(entry *swaputil.InflightRequestEntry) {
	elapsed := time.Since(entry.Timestamp)
	if elapsed < 0 {
		elapsed = 0
	}
	entry.ElapsedMs = elapsed.Milliseconds()
}

func copyStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func copyMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for k, v := range metadata {
		out[k] = v
	}
	return out
}

type inflightResponseWriter struct {
	http.ResponseWriter
	tracker     *inflightTracker
	id          string
	wroteHeader bool
}

func (w *inflightResponseWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.tracker.SetResponseHeaders(w.id, w.Header())
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *inflightResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.tracker.AddResponseBytes(w.id, n)
	return n, err
}

// MarkStatus forwards a recorded-only status to the wrapped writer. The
// tracker records bytes and headers rather than a status code, so there is
// nothing to update here.
func (w *inflightResponseWriter) MarkStatus(code int) {
	if marker, ok := w.ResponseWriter.(swaputil.StatusMarker); ok {
		marker.MarkStatus(code)
	}
}

// WroteHeader reports whether a response status reached the client.
func (w *inflightResponseWriter) WroteHeader() bool { return w.wroteHeader }

func (w *inflightResponseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *inflightResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	return nil, nil, fmt.Errorf("underlying ResponseWriter does not support hijacking")
}

// CreateInflightMiddleware returns middleware that tracks model-dispatched
// requests until downstream handling completes.
func CreateInflightMiddleware(t *inflightTracker, cfg config.Config) chain.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if swaputil.ShouldIgnoreWebsocket(r, cfg) {
				next.ServeHTTP(w, r)
				return
			}

			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()

			r = r.WithContext(ctx)
			id := t.Add(r, cancel)
			defer t.Remove(id)

			// Carry the tracker's request ID in the context so the router's
			// scheduler queue snapshot can match this request back to its
			// tracked entry. Deliberately outside ReqContextData.Metadata,
			// which is user-facing and lands in activity logs.
			r = r.WithContext(swaputil.WithInflightID(r.Context(), id))

			next.ServeHTTP(&inflightResponseWriter{ResponseWriter: w, tracker: t, id: id}, r)
		})
	}
}

// CreateUpstreamInflightMiddleware tracks /upstream/<model>/<path> requests
// only when the stripped upstream path is one of the model-dispatched
// inference endpoints.
func CreateUpstreamInflightMiddleware(t *inflightTracker, cfg config.Config) chain.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(r.URL.Path, "/upstream/") {
				next.ServeHTTP(w, r)
				return
			}
			r = markInflightStart(r)

			_, _, remainingPath, found := swaputil.FindModelInPath(cfg, strings.TrimPrefix(r.URL.Path, "/upstream"))
			if !found || !isModelDispatchedRequest(r.Method, remainingPath) {
				next.ServeHTTP(w, r)
				return
			}

			if _, err := swaputil.FetchContext(r, cfg); err != nil {
				next.ServeHTTP(w, r)
				return
			}
			if swaputil.ShouldIgnoreWebsocket(r, cfg) {
				next.ServeHTTP(w, r)
				return
			}

			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()

			r = r.WithContext(ctx)
			tracked := r.Clone(ctx)
			tracked.URL.Path = remainingPath
			id := t.Add(tracked, cancel)
			defer t.Remove(id)

			next.ServeHTTP(&inflightResponseWriter{ResponseWriter: w, tracker: t, id: id}, r)
		})
	}
}

func isModelDispatchedRequest(method, path string) bool {
	switch method {
	case http.MethodPost:
		for _, p := range modelPostJSONRoutes {
			if p == path {
				return true
			}
		}
		for _, p := range modelPostFormRoutes {
			if p == path {
				return true
			}
		}
	case http.MethodGet:
		for _, p := range modelGetRoutes {
			if p == path {
				return true
			}
		}
	}
	return false
}
