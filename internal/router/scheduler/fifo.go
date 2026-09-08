package scheduler

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// defaultConcurrencyLimit caps simultaneous in-flight requests per model when
// the model config leaves concurrencyLimit unset.
const defaultConcurrencyLimit = 10

// PATCH(v255): queueing for over-limit requests
// defaultQueueDepth is the default number of over-limit requests that may wait
// in the queue per model before admission falls back to a 429 rejection.
const defaultQueueDepth = 10

// PATCH(v255): queueing for over-limit requests
// defaultQueueTimeout bounds how long an over-limit request waits in the queue
// before it is rejected with 429 + Retry-After (see FifoConfig.QueueTimeout).
const defaultQueueTimeout = 60 * time.Second

// activeSwap tracks one in-flight swap and the callers waiting on it.
type activeSwap struct {
	modelID string
	evict   []string
	waiters []HandlerReq
}

// PATCH(v255): queueing for over-limit requests
// queuedItem is one slot in the scheduler queue: the request plus the deadline
// after which a queued waiter is rejected with 429 (zero value = never expires).
// The deadline is set at enqueue time and can never outlive the item, so no
// timer or sweeper is needed.
type queuedItem struct {
	Req      HandlerReq
	Deadline time.Time
}

// FIFO is the default scheduler. Requests are handled in a first-in, first-out order.
// To reduce swapping requests for a model that is already running will be handled
// immediately by the running process.
//
// Requests into this schedule are handled like this:
//
// A B C A B C --> A A B B C C
//
// The strategy is simple and reduces the number of swaps required.
type FIFO struct {
	name    string
	logger  *logmon.Monitor
	planner Swapper
	cfg     config.FifoConfig
	effects Effects

	limits   map[string]int
	active   map[string]*activeSwap
	reserved map[string]int
	inFlight map[string]int
	queued   []queuedItem

	// PATCH(v255): queueing for over-limit requests
	queueDepth   int           // 0 = disabled (legacy 429 behavior)
	queueTimeout time.Duration // 0 = wait indefinitely
}

// QueuedInfo describes one request currently waiting in the scheduler queue.
// QueuePosition is 1-indexed and reflects the queue's service order (priority
// desc, stable FIFO within equal priority).
type QueuedInfo struct {
	RequestID     string
	Model         string
	QueuePosition int
}

// Queued returns a read-only snapshot of the queue in service order. It is
// safe to call only from the router's single run-loop goroutine, like every
// other FIFO method — the queue is not synchronized for concurrent access.
//
// The snapshot covers only requests sitting in f.queued. Requests joined to an
// in-progress swap (join waiters) and requests admitted on the fast path are
// not queued and never appear in it; callers display those as "serving"
// (approximate v1 stage semantics).
//
// RequestID is the inflight tracker ID carried in each request's context via
// swaputil.WithInflightID; requests without one (unit-test requests, requests
// that never passed the inflight middleware) report an empty ID.
func (s *FIFO) Queued() []QueuedInfo {
	if len(s.queued) == 0 {
		return nil
	}
	out := make([]QueuedInfo, 0, len(s.queued))
	for i, item := range s.queued {
		info := QueuedInfo{Model: item.Req.Model, QueuePosition: i + 1}
		if id, ok := swaputil.InflightID(item.Req.Ctx); ok {
			info.RequestID = id
		}
		out = append(out, info)
	}
	return out
}

// NewFIFO builds a FIFO scheduler. Per-model concurrency limits are derived
// from models: each model's ConcurrencyLimit overrides defaultConcurrencyLimit
// when set to a value greater than zero.
func NewFIFO(name string, logger *logmon.Monitor, planner Swapper, cfg config.FifoConfig, models map[string]config.ModelConfig, eff Effects) *FIFO {
	limits := make(map[string]int, len(models))
	for id, mc := range models {
		limit := defaultConcurrencyLimit
		if mc.ConcurrencyLimit > 0 {
			limit = mc.ConcurrencyLimit
		}
		limits[id] = limit
	}

	// PATCH(v255): queueing for over-limit requests
	qDepth := defaultQueueDepth
	if cfg.QueueDepth != nil {
		qDepth = *cfg.QueueDepth
	}
	qTimeout := defaultQueueTimeout
	if cfg.QueueTimeout != nil {
		qTimeout = time.Duration(*cfg.QueueTimeout) * time.Second
	}

	return &FIFO{
		name:     name,
		logger:   logger,
		planner:  planner,
		cfg:      cfg,
		effects:  eff,
		limits:   limits,
		active:   make(map[string]*activeSwap),
		reserved: make(map[string]int),
		inFlight: make(map[string]int),
		// PATCH(v255): queueing for over-limit requests
		queueDepth:   qDepth,
		queueTimeout: qTimeout,
	}
}

// OnRequest decides what to do with one incoming ServeHTTP request. It never
// blocks indefinitely: any work that has to wait (starting a process, stopping
// siblings, waiting for ready) is deferred to a swap goroutine and reported back
// via OnSwapDone.
//
// The decision tree, in order:
//
//  1. Unknown model — respond with ErrModelNotFound and move on.
//  2. A swap to the same model is already in flight — attach this waiter so
//     one swap serves all callers that asked for the same model.
//  3. Fast path — the target process is already ready, the planner sees
//     nothing to evict, and no in-flight swap is evicting it. Hand back its
//     ServeHTTP immediately.
//  4. Would collide with an in-flight swap (we'd stop their target, or they're
//     stopping us) — park in the queue for OnSwapDone to drain.
//  5. Would evict a process that is still handling requests — park in the
//     queue. OnServeDone will retry when the busy process drains.
//  6. Otherwise — start a new swap. This may run in parallel with other active
//     swaps when their evict sets don't intersect.
func (s *FIFO) OnRequest(req HandlerReq) {
	// (1) Unknown model.
	state, ok := s.effects.ModelState(req.Model)
	if !ok {
		s.logger.Debugf("%s: model %s not handled by this router", s.name, req.Model)
		s.rejectAdmission(req, ErrModelNotFound)
		return
	}

	if !s.admit(req) {
		return
	}

	// PATCH(v255): queueing for over-limit requests
	// Capacity branch: the model has no free serving slot left, so park the
	// request instead of rejecting it at admission. This must run before the
	// in-flight-swap join so swap waiters can never exceed the limit.
	if s.queueingEnabled() && s.reserved[req.Model] > s.limit(req.Model) {
		s.logger.Debugf("%s: queueing over-limit request for model %s (reserved=%d limit=%d)", s.name, req.Model, s.reserved[req.Model], s.limit(req.Model))
		s.enqueue(req, s.deadline())
		return
	}

	// (2) Join an in-flight swap for the same model.
	if sw, ok := s.active[req.Model]; ok {
		s.logger.Debugf("%s: joining in-flight swap for model %s (%d waiters)", s.name, req.Model, len(sw.waiters)+1)
		sw.waiters = append(sw.waiters, req)
		return
	}

	running := s.runningSet(req.Model)
	evict := s.planner.EvictionFor(req.Model, running)

	// (3) Fast path: ready, nothing to evict, and nobody is evicting us.
	if state == process.StateReady && len(evict) == 0 && !collidesWith(req.Model, evict, s.active) {
		s.logger.Debugf("%s: fast-path serving model %s (already ready)", s.name, req.Model)
		s.grantHandler(req, req.Model)
		return
	}

	// (4) Collision with an in-flight swap — queue.
	if collidesWith(req.Model, evict, s.active) {
		s.logger.Debugf("%s: queuing request for model %s (collides with in-flight swap)", s.name, req.Model)
		s.enqueue(req, s.deadline())
		return
	}

	// (5) Would evict a busy process — queue until it drains.
	if conflictsWithInFlight(evict, s.inFlight) {
		s.logger.Debugf("%s: queuing request for model %s (would evict in-flight process)", s.name, req.Model)
		s.enqueue(req, s.deadline())
		return
	}

	// (6) Start a new (possibly parallel) swap.
	s.logger.Debugf("%s: starting swap for model %s, evicting %v", s.name, req.Model, evict)
	s.startSwap(req, evict, running)
}

// OnCancel removes a request whose client has disconnected from the queue and
// from every in-flight swap's waiters. If the request was the sole waiter of an
// active swap, the swap goroutine is left to complete on its own — OnSwapDone
// will find no waiters and simply clean up. This prevents drainQueue from ever
// starting a model load for a caller that is no longer there.
func (s *FIFO) OnCancel(req HandlerReq) {
	removed := false

	// Prune from the queue.
	if len(s.queued) > 0 {
		kept := s.queued[:0]
		for _, q := range s.queued {
			if q.Req.Respond == req.Respond {
				removed = true
				s.release(q.Req.Model)
				continue
			}
			kept = append(kept, q)
		}
		s.queued = kept
	}

	// Prune from any active swap's waiters.
	for _, sw := range s.active {
		filtered := sw.waiters[:0]
		for _, w := range sw.waiters {
			if w.Respond == req.Respond {
				removed = true
				s.release(w.Model)
				continue
			}
			filtered = append(filtered, w)
		}
		sw.waiters = filtered
	}

	if removed {
		s.logger.Debugf("%s: cancelled request for model %s pruned from scheduler", s.name, req.Model)
		broadcastQueuePositions(s.queued)
	}
}

// OnSwapDone fans the result out to every waiter that joined this swap, removes
// the swap from the active map, then walks the queue once, promoting any items
// that no longer collide with the remaining active set. FIFO order is preserved:
// items still blocked stay in place.
func (s *FIFO) OnSwapDone(ev SwapDone) {
	sw, ok := s.active[ev.ModelID]
	if !ok {
		return
	}
	delete(s.active, ev.ModelID)

	for _, w := range sw.waiters {
		if ev.Err != nil {
			s.grantError(w, ev.Err)
		} else {
			s.grantHandler(w, ev.ModelID)
		}
	}

	s.drainQueue()
}

// OnServeDone decrements the per-model in-flight count and, when that drops to
// zero, retries the queue: requests whose swap was deferred because they would
// have evicted this (now-idle) process can now proceed.
func (s *FIFO) OnServeDone(ev ServeDoneEvent) {
	s.inFlight[ev.ModelID]--
	s.release(ev.ModelID)
	// PATCH(v255): queueing for over-limit requests
	// Drain on every serve completion while capacity waiters exist: a freed
	// serving slot must go to the next queued request even when the model still
	// has other requests in flight. Without capacity waiters keep the upstream
	// "inFlight == 0" gate so swap-related queueing behaves exactly as before.
	if s.hasCapacityWaiters() {
		s.drainQueue()
		return
	}
	if s.inFlight[ev.ModelID] <= 0 {
		delete(s.inFlight, ev.ModelID)
		s.drainQueue()
	}
}

// OnUnload reconciles router-owned state with the impending Stop, performs the
// Stop (synchronously, via Effects) so callers of Unload remain blocked until
// each targeted process has exited, then drains the queue.
func (s *FIFO) OnUnload(targets []string, timeout time.Duration) {
	unloadErr := fmt.Errorf("%s: model unloaded", s.name)

	targetSet := make(map[string]bool, len(targets))
	for _, id := range targets {
		targetSet[id] = true
	}

	// Release waiters of any in-flight swap whose target is being unloaded.
	// The swap goroutine itself is left to finish on its own; when its
	// SwapDone arrives, OnSwapDone will find no entry in active and drop it.
	for id := range targetSet {
		sw, ok := s.active[id]
		if !ok {
			continue
		}
		for _, w := range sw.waiters {
			s.grantError(w, unloadErr)
		}
		delete(s.active, id)
	}

	// Drop queued requests addressed to unloaded models. Requests for other
	// models stay queued and may benefit from drainQueue at the end.
	if len(s.queued) > 0 {
		kept := s.queued[:0]
		for _, w := range s.queued {
			if targetSet[w.Req.Model] {
				s.grantError(w.Req, unloadErr)
				continue
			}
			kept = append(kept, w)
		}
		s.queued = kept
	}

	// Stop the targeted processes. Done synchronously so Unload's caller can
	// rely on "after Unload returns, the process is stopped". inFlight is
	// intentionally NOT cleared here: each dying handler will fire its tracked
	// serve and reach OnServeDone in the normal way.
	s.effects.StopProcesses(timeout, targets)

	// Removing entries from active above may have unblocked queued requests
	// that previously collided with the now-cancelled swaps.
	s.drainQueue()
}

// OnShutdown grants err to every waiter still held by the scheduler.
func (s *FIFO) OnShutdown(err error) {
	for _, sw := range s.active {
		for _, w := range sw.waiters {
			s.grantError(w, err)
		}
	}
	for _, w := range s.queued {
		s.grantError(w.Req, err)
	}
}

// grantHandler hands the caller a tracked handler for modelID and, only if the
// caller was still there to receive it, bumps the in-flight count. Incrementing
// when the grant failed would strand the counter and block future evictions.
// Concurrency-limit rejection happens earlier in admit, before a request can
// start the loading stream.
func (s *FIFO) grantHandler(req HandlerReq, modelID string) {
	if err := swaputil.SetReqData(req.Ctx, "fifo_priority", strconv.Itoa(req.Priority)); err != nil {
		s.logger.Debugf("failed to set fifo_priority metadata: %v", err)
	}

	if s.effects.GrantServe(req, modelID) {
		s.inFlight[modelID]++
	} else {
		s.release(modelID)
	}
}

// grantError reports a post-admission error to the caller and releases the
// request's reserved concurrency slot.
func (s *FIFO) grantError(req HandlerReq, err error) {
	s.release(req.Model)
	s.effects.GrantError(req, err)
}

// admit performs the pre-stream admission handshake. Accepted requests reserve
// one future serving slot until they serve, cancel while waiting, or receive a
// post-admission error.
func (s *FIFO) admit(req HandlerReq) bool {
	// PATCH(v255): queueing for over-limit requests
	capacity := s.limit(req.Model)
	if s.queueingEnabled() {
		capacity += s.queueDepth
	}
	if s.reserved[req.Model] >= capacity {
		s.rejectAdmission(req, swaputil.ConcurrencyLimitError{})
		return false
	}
	if !sendAdmission(req, nil) {
		return false
	}
	s.reserved[req.Model]++
	return true
}

func (s *FIFO) rejectAdmission(req HandlerReq, err error) {
	sendAdmission(req, err)
}

func sendAdmission(req HandlerReq, err error) bool {
	if req.Admit == nil {
		return true
	}
	done := reqDone(req)
	select {
	case <-done:
		return false
	default:
	}
	select {
	case req.Admit <- err:
		return true
	case <-done:
		return false
	}
}

func reqDone(req HandlerReq) <-chan struct{} {
	if req.Ctx == nil {
		return nil
	}
	return req.Ctx.Done()
}

func (s *FIFO) release(modelID string) {
	if s.reserved[modelID] <= 0 {
		panic(fmt.Sprintf("%s: release without reservation for model %s", s.name, modelID))
	}
	s.reserved[modelID]--
	if s.reserved[modelID] == 0 {
		delete(s.reserved, modelID)
	}
}

// limit returns the per-model concurrency cap, defaulting to
// defaultConcurrencyLimit when the model has no explicit entry.
func (s *FIFO) limit(modelID string) int {
	if l, ok := s.limits[modelID]; ok {
		return l
	}
	return defaultConcurrencyLimit
}

// PATCH(v255): queueing for over-limit requests
// queueingEnabled reports whether over-limit requests queue instead of being
// rejected at admission. queueDepth 0 disables queueing and restores the
// legacy 429 behavior.
func (s *FIFO) queueingEnabled() bool { return s.queueDepth > 0 }

// PATCH(v255): queueing for over-limit requests
// deadline returns the queue deadline for a newly enqueued request, or the zero
// time when queueTimeout is not configured (wait indefinitely).
func (s *FIFO) deadline() time.Time {
	if s.queueTimeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(s.queueTimeout)
}

// PATCH(v255): queueing for over-limit requests
// hasCapacityWaiters reports whether any queued request is waiting on a model
// at its concurrency limit. While one exists, OnServeDone drains on every
// completion so a freed serving slot is handed over immediately.
func (s *FIFO) hasCapacityWaiters() bool {
	for _, item := range s.queued {
		if s.queueingEnabled() && s.reserved[item.Req.Model] >= s.limit(item.Req.Model) {
			return true
		}
	}
	return false
}

// startSwap records the swap as active and launches it via Effects. running is
// the set EvictionFor saw, forwarded to OnSwapStart so the planner logs against
// the same picture it decided on.
func (s *FIFO) startSwap(initial HandlerReq, evict, running []string) {
	s.active[initial.Model] = &activeSwap{
		modelID: initial.Model,
		evict:   evict,
		waiters: []HandlerReq{initial},
	}
	s.planner.OnSwapStart(initial.Model, running)
	s.effects.StartSwap(initial.Model, evict)
}

// enqueue inserts req into the queue in priority order: it goes just before the
// first queued item whose priority is strictly lower, so higher-priority
// requests are serviced first while equal-priority requests keep their arrival
// (FIFO) order. Priority is the request's effective request-level priority set
// at ingress; it is always > 0 (0 is normalized away before the scheduler is
// reached, see docs/design/request-priority.md §7).
func (s *FIFO) enqueue(req HandlerReq, deadline time.Time) {
	p := req.Priority
	i := len(s.queued)
	for j, q := range s.queued {
		if q.Req.Priority < p {
			i = j
			break
		}
	}
	s.queued = append(s.queued, queuedItem{})
	copy(s.queued[i+1:], s.queued[i:])
	s.queued[i] = queuedItem{Req: req, Deadline: deadline} // zero Deadline => never expires
	broadcastQueuePositions(s.queued)
}

// drainQueue walks the queued requests in order, re-running the OnRequest
// decision tree against the (now smaller) active set. Items that can now start
// or join become satisfied; items still blocked remain queued in original order
// so they get another chance on the next swap completion.
func (s *FIFO) drainQueue() {
	if len(s.queued) == 0 {
		return
	}
	pending := s.queued
	var remaining []queuedItem
	for _, item := range pending {
		req := item.Req

		// PATCH(v255): queueing for over-limit requests
		// Lazy timeout: prune expired waiters first so their reservations are
		// released and become visible to later items in this same drain.
		if !item.Deadline.IsZero() && time.Now().After(item.Deadline) {
			s.logger.Debugf("%s: over-limit queued request for model %s timed out", s.name, req.Model)
			s.grantError(req, swaputil.ConcurrencyLimitError{RetryAfter: 1})
			continue // grantError releases the reservation
		}
		// PATCH(v255): queueing for over-limit requests
		// Capacity gate: no free serving slot for this model yet, stay queued.
		// The gate counts in-flight, not reserved, because every queued item
		// holds a reservation: a reserved-based gate would strand waiters
		// behind their own reservations (a model at its limit with only queued
		// waiters would never drain).
		if s.queueingEnabled() && s.inFlight[req.Model] >= s.limit(req.Model) {
			remaining = append(remaining, item)
			continue
		}

		state, ok := s.effects.ModelState(req.Model)
		if !ok {
			s.grantError(req, ErrModelNotFound)
			continue
		}
		if sw, ok := s.active[req.Model]; ok {
			// PATCH(v255): queueing for over-limit requests
			// Joining hands the request a serving slot at swap completion, so
			// never let a drain-time join push the post-completion in-flight
			// count over the model's limit: count current waiters AND the
			// model's still-serving requests (a swap can be in flight while the
			// same model keeps serving — e.g. it only evicts a sibling).
			if s.queueingEnabled() &&
				len(sw.waiters)+s.inFlight[req.Model] >= s.limit(req.Model) {
				remaining = append(remaining, item)
				continue
			}
			s.logger.Debugf("%s: queued request for model %s now joining in-flight swap", s.name, req.Model)
			sw.waiters = append(sw.waiters, req)
			continue
		}
		running := s.runningSet(req.Model)
		evict := s.planner.EvictionFor(req.Model, running)
		if state == process.StateReady && len(evict) == 0 && !collidesWith(req.Model, evict, s.active) {
			s.logger.Debugf("%s: queued request for model %s now served fast-path", s.name, req.Model)
			s.grantHandler(req, req.Model)
			continue
		}
		if collidesWith(req.Model, evict, s.active) {
			remaining = append(remaining, item)
			continue
		}
		if conflictsWithInFlight(evict, s.inFlight) {
			remaining = append(remaining, item)
			continue
		}
		s.logger.Debugf("%s: queued request for model %s now starting swap, evicting %v", s.name, req.Model, evict)
		s.startSwap(req, evict, running)
	}
	s.queued = remaining
	broadcastQueuePositions(s.queued)
}

// runningSet is the live model set handed to the Swapper: every process the
// baseRouter reports as running, unioned with the targets of in-flight swaps
// (excluding excludeActive, the model whose own swap is being decided — its
// in-flight entry must not count as "already running"). The result is sorted so
// eviction decisions derived from it are deterministic.
func (s *FIFO) runningSet(excludeActive string) []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(id string) {
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for id := range s.effects.RunningModels() {
		add(id)
	}
	for _, id := range activeTargets(s.active, excludeActive) {
		add(id)
	}
	sort.Strings(out)
	return out
}

// activeTargets returns the IDs of every in-flight swap target except exclude.
// The planner uses this to account for models committed to but not yet reflected
// in process state.
func activeTargets(active map[string]*activeSwap, exclude string) []string {
	if len(active) == 0 {
		return nil
	}
	out := make([]string, 0, len(active))
	for id := range active {
		if id == exclude {
			continue
		}
		out = append(out, id)
	}
	return out
}

// collidesWith reports whether a new swap with this target and evict set can
// safely run alongside the currently active swaps. Same-target callers should
// JOIN (handled before this) — they do not collide with themselves.
func collidesWith(target string, evict []string, active map[string]*activeSwap) bool {
	for id, sw := range active {
		if id == target {
			continue
		}
		if containsString(evict, id) {
			return true
		}
		if containsString(sw.evict, target) {
			return true
		}
		if slicesOverlap(evict, sw.evict) {
			return true
		}
	}
	return false
}

// slicesOverlap reports whether xs and ys share any common element.
func slicesOverlap(xs, ys []string) bool {
	for _, x := range xs {
		if containsString(ys, x) {
			return true
		}
	}
	return false
}

// conflictsWithInFlight reports whether any model in evict is still handling
// requests. Stopping a busy process would cancel its callers' connections, so
// the scheduler defers the swap until those callers finish.
func conflictsWithInFlight(evict []string, inFlight map[string]int) bool {
	for _, m := range evict {
		if inFlight[m] > 0 {
			return true
		}
	}
	return false
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// broadcastQueuePositions sends each queued request its current 1-indexed
// position. Sends are non-blocking: if the channel is full, the old value is
// drained first so the consumer always sees the latest position.
func broadcastQueuePositions(queued []queuedItem) {
	for i, item := range queued {
		req := item.Req
		pos := i + 1
		select {
		case req.PositionCh <- pos:
		default:
			select {
			case <-req.PositionCh:
			default:
			}
			select {
			case req.PositionCh <- pos:
			default:
			}
		}
	}
}
