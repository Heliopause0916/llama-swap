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

// promotedPriority is the internal top tier (D17), strictly above every band.
const promotedPriority = 2147483647 // math.MaxInt32

// activeSwap tracks one in-flight swap and the callers waiting on it.
type activeSwap struct {
	modelID    string
	generation uint64
	cancel     func()
	evict      []string
	waiters    []HandlerReq
}

// queuedItem is one entry in the global queue. Deadline is the queueTimeout
// bound (zero when queueTimeout is infinite); EnqueuedAt is the promotion
// clock (D16) written once at enqueue and never reset (D19).
type queuedItem struct {
	Req        HandlerReq
	Deadline   time.Time
	EnqueuedAt time.Time
	// InflightID ties this queued request to the server's inflight entry, so
	// QueueSnapshot can report its stage/queue_position. Empty when the request
	// never passed through the inflight middleware (e.g. tests).
	InflightID string
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

	nextGeneration uint64

	queueDepth    int                  // 解析后的队列容量（0 = 关闭排队）
	queueTimeout  time.Duration        // 解析后的超时（0 = 无限）
	promoteAfter  time.Duration        // 解析后的老化阈值（0 = 关闭）
	now           func() time.Time     // 可注入时钟（D23），默认 time.Now
	fuzzy         groupFuzzy           // 构造期 fuzzy 配置快照
	lastServeDone map[string]time.Time // groupID -> 最后一次服务完成时刻；缺失 = 从未服务
}

// NewFIFO builds a FIFO scheduler. Per-model concurrency limits are derived
// from models: each model's ConcurrencyLimit overrides defaultConcurrencyLimit
// when set to a value greater than zero. fuzzy is the construction-time snapshot
// of fuzzy-enabled swap groups (empty/inert when no fuzzy groups are configured).
func NewFIFO(name string, logger *logmon.Monitor, planner Swapper, cfg config.FifoConfig, models map[string]config.ModelConfig, eff Effects, fuzzy groupFuzzy) *FIFO {
	limits := make(map[string]int, len(models))
	for id, mc := range models {
		limit := defaultConcurrencyLimit
		if mc.ConcurrencyLimit > 0 {
			limit = mc.ConcurrencyLimit
		}
		limits[id] = limit
	}

	qd := 10 // nil default
	if cfg.QueueDepth != nil {
		qd = *cfg.QueueDepth
	}
	qt := 60 * time.Second // nil default
	if cfg.QueueTimeout != nil {
		if *cfg.QueueTimeout == 0 {
			qt = 0
		} else {
			qt = time.Duration(*cfg.QueueTimeout) * time.Second
		}
	}
	pa := time.Duration(0)
	if cfg.PromoteAfter != nil && *cfg.PromoteAfter > 0 {
		pa = time.Duration(*cfg.PromoteAfter) * time.Second
	}

	return &FIFO{
		name:          name,
		logger:        logger,
		planner:       planner,
		cfg:           cfg,
		effects:       eff,
		limits:        limits,
		active:        make(map[string]*activeSwap),
		reserved:      make(map[string]int),
		inFlight:      make(map[string]int),
		queueDepth:    qd,
		queueTimeout:  qt,
		promoteAfter:  pa,
		now:           time.Now,
		fuzzy:         fuzzy,
		lastServeDone: make(map[string]time.Time),
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
	// (0) Fuzzy rewrite — before ALL bookkeeping (admit/reserved/inFlight/queued/active).
	req = s.fuzzySubstitute(req)

	// (0b) Maintenance interception — never start/route to a maintenance model.
	req, ok := s.maintenanceSubstitute(req)
	if !ok {
		s.rejectAdmission(req, swaputil.ErrMaintenance)
		return
	}

	// (1) Unknown model.
	state, ok := s.effects.ModelState(req.Model)
	if !ok {
		s.logger.Debugf("%s: model %s not handled by this router", s.name, req.Model)
		s.rejectAdmission(req, ErrModelNotFound)
		return
	}

	// Compute the eviction set once, before admit, so a PIN-protection rewrite
	// reserves concurrency for the model it actually serves.
	running := s.runningSet(req.Model)
	evict := s.planner.EvictionFor(req.Model, running)

	// (1a) PIN protection — if serving req.Model would evict a pinned model,
	// force-fuzzy (same group) or reject. Runs before admit.
	if rewritten, err := s.protectPinned(&req, running, evict); err != nil {
		s.rejectAdmission(req, err)
		return
	} else if rewritten {
		state, ok = s.effects.ModelState(req.Model)
		if !ok {
			s.rejectAdmission(req, ErrModelNotFound)
			return
		}
		running = s.runningSet(req.Model)
		evict = s.planner.EvictionFor(req.Model, running)
	}

	if !s.admit(req) {
		return
	}

	// (1b) Serving-capacity gate: if the model is already serving at its limit,
	// enqueue rather than serve/swap (queueDepth enabled path).
	if s.inFlight[req.Model] >= s.limit(req.Model) {
		s.logger.Debugf("%s: model %s at concurrency limit, enqueueing", s.name, req.Model)
		s.enqueue(req)
		return
	}

	// (2) Join an in-flight swap for the same model.
	if sw, ok := s.active[req.Model]; ok {
		s.logger.Debugf("%s: joining in-flight swap for model %s (%d waiters)", s.name, req.Model, len(sw.waiters)+1)
		sw.waiters = append(sw.waiters, req)
		return
	}

	// (3) Fast path: ready, nothing to evict, and nobody is evicting us.
	if state == process.StateReady && len(evict) == 0 && !collidesWith(req.Model, evict, s.active) {
		s.logger.Debugf("%s: fast-path serving model %s (already ready)", s.name, req.Model)
		s.grantHandler(req, req.Model)
		return
	}

	// (4) Collision with an in-flight swap — queue.
	if collidesWith(req.Model, evict, s.active) {
		s.logger.Debugf("%s: queuing request for model %s (collides with in-flight swap)", s.name, req.Model)
		s.enqueue(req)
		return
	}

	// (5) Would evict a busy process — queue until it drains.
	if conflictsWithInFlight(evict, s.inFlight) {
		s.logger.Debugf("%s: queuing request for model %s (would evict in-flight process)", s.name, req.Model)
		s.enqueue(req)
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
	if !ok || sw.generation != ev.Generation {
		return
	}
	sw.cancel()
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

// OnServeDone decrements the per-model in-flight count and, when a serving slot
// frees up (inFlight < limit), retries the queue: requests parked by the
// capacity gate (inFlight >= limit) can now proceed, as can requests whose swap
// was deferred because they would have evicted a busy process. Draining on every
// slot release (not only when the model goes fully idle) prevents starvation and
// priority inversion when limit > 1.
func (s *FIFO) OnServeDone(ev ServeDoneEvent) {
	if gid := s.memberOf(ev.ModelID); gid != "" {
		s.lastServeDone[gid] = s.now()
	}
	s.inFlight[ev.ModelID]--
	s.release(ev.ModelID)
	if s.inFlight[ev.ModelID] <= 0 {
		delete(s.inFlight, ev.ModelID)
	}
	// After delete, the missing-key read yields 0, which is < any limit, so a
	// fully-idle model still drains. A partial release (limit-1) also drains.
	if s.inFlight[ev.ModelID] < s.limit(ev.ModelID) {
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

	// Cancel and join swaps before stopping their targets: otherwise a swap
	// still evicting siblings could restart its target after the unload.
	for id := range targetSet {
		sw, ok := s.active[id]
		if !ok {
			continue
		}
		sw.cancel()
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
		sw.cancel()
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
	if req.RequestedModel != "" && req.RequestedModel != modelID {
		if err := swaputil.SetReqData(req.Ctx, "served_model", modelID); err != nil {
			s.logger.Debugf("failed to set served_model metadata: %v", err)
		}
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

// admit reserves one future serving slot. When the model is at its serving
// limit and queueing is enabled (queueDepth > 0), the request is admitted and
// will be enqueued by OnRequest/drainQueue; when the queue is full
// (reserved >= limit + queueDepth) or queueing is off, it is rejected 429.
func (s *FIFO) admit(req HandlerReq) bool {
	if s.reserved[req.Model] >= s.limit(req.Model)+s.queueDepth {
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

// startSwap records the swap as active and launches it via Effects. running is
// the set EvictionFor saw, forwarded to OnSwapStart so the planner logs against
// the same picture it decided on.
func (s *FIFO) startSwap(initial HandlerReq, evict, running []string) {
	s.nextGeneration++
	sw := &activeSwap{
		modelID:    initial.Model,
		generation: s.nextGeneration,
		evict:      evict,
		waiters:    []HandlerReq{initial},
	}
	s.active[initial.Model] = sw
	s.planner.OnSwapStart(initial.Model, running)
	sw.cancel = s.effects.StartSwap(initial.Model, sw.generation, evict)
}

// enqueue inserts req into the queue in priority order: it goes just before the
// first queued item whose priority is strictly lower, so higher-priority models
// are serviced first while equal-priority requests keep their arrival (FIFO)
// order. Priority is the resolved request band (req.Priority); it is a
// scheduling key only, never a model-config lookup.
func (s *FIFO) enqueue(req HandlerReq) {
	now := s.now()
	var deadline time.Time
	if s.queueTimeout > 0 {
		deadline = now.Add(s.queueTimeout)
	}
	item := queuedItem{Req: req, Deadline: deadline, EnqueuedAt: now}
	if id, ok := swaputil.InflightID(req.Ctx); ok {
		item.InflightID = id
	}
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
	s.queued[i] = item
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
	// Capture the clock once so the re-rank comparator and the timeout prune use
	// a single, consistent timestamp. Calling s.now() per item against a real
	// clock could make the comparator non-transitive at a threshold boundary.
	now := s.now()
	// L1.5: stable re-rank by effective priority (desc), zero cost when off.
	if s.promoteAfter > 0 {
		sort.SliceStable(s.queued, func(i, j int) bool {
			return s.effective(s.queued[i], now) > s.effective(s.queued[j], now)
		})
	}
	pending := s.queued
	var remaining []queuedItem
	for _, item := range pending {
		req := item.Req
		// Lazy queueTimeout prune: past deadline -> 429 + Retry-After: 1.
		if s.queueTimeout > 0 && now.After(item.Deadline) {
			s.grantError(req, swaputil.ConcurrencyLimitError{})
			continue
		}
		state, ok := s.effects.ModelState(req.Model)
		if !ok {
			s.grantError(req, ErrModelNotFound)
			continue
		}
		// Maintenance: a maintenance model is never started, so a queued
		// request for it is rejected rather than held.
		if s.effects.IsMaintenance(req.Model) {
			s.grantError(req, swaputil.ErrMaintenance)
			continue
		}
		// In-flight capacity gate (inFlight >= limit): keep queued (D19 keeps
		// EnqueuedAt). This is the deadlock-free form.
		if s.inFlight[req.Model] >= s.limit(req.Model) {
			remaining = append(remaining, item)
			continue
		}
		if sw, ok := s.active[req.Model]; ok {
			s.logger.Debugf("%s: queued request for model %s now joining in-flight swap", s.name, req.Model)
			sw.waiters = append(sw.waiters, req)
			continue
		}
		running := s.runningSet(req.Model)
		evict := s.planner.EvictionFor(req.Model, running)
		// PIN protection (reject only, never rewrite): a queued request's
		// concurrency slot is bound to its target model, so rewriting here
		// would misalign reserved/inFlight. Reject and let the client retry.
		if s.evictsPinned(evict) {
			s.grantError(req, swaputil.ErrPinnedProtected)
			continue
		}
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

// QueueSnapshot returns the current queue in effective service order. Position
// is the 1-indexed physical position (i+1) in s.queued, matching the loading
// stream's "Queue position: #N"; items without an InflightID are skipped. Runs
// on the run-loop goroutine. nil when the queue is empty.
func (s *FIFO) QueueSnapshot() []QueueEntry {
	if len(s.queued) == 0 {
		return nil
	}
	out := make([]QueueEntry, 0, len(s.queued))
	for i, item := range s.queued {
		if item.InflightID == "" {
			continue
		}
		out = append(out, QueueEntry{InflightID: item.InflightID, Position: i + 1})
	}
	return out
}

// effective returns the aging-adjusted scheduling priority (D18). now is the
// drain-time clock snapshot, captured once so the sort comparator stays
// transitive. Pinned items ride the exclusive top tier; promotion is
// scheduler-internal only (D22).
func (s *FIFO) effective(item queuedItem, now time.Time) int {
	if s.promoteAfter > 0 && now.Sub(item.EnqueuedAt) >= s.promoteAfter {
		return promotedPriority
	}
	return item.Req.Priority
}

// fuzzySubstitute rewrites a fuzzy-group request onto the group's online model
// when fuzzy applies. Returns the (possibly rewritten) req.
func (s *FIFO) fuzzySubstitute(req HandlerReq) HandlerReq {
	// An explicit /upstream/<model>/ target must be served by the named model,
	// never fuzzy-substituted onto another member of the group.
	if req.Explicit {
		return req
	}
	gid, ok := s.fuzzy.groupOf[req.Model]
	if !ok {
		return req
	}
	served := s.fuzzyOnline(gid)
	if served == "" || served == req.Model {
		return req
	}
	if !s.fuzzyApplies(gid) {
		return req
	}
	s.logger.Debugf("%s: fuzzy serving %s with online model %s (group %s)", s.name, req.Model, served, gid)
	req.RequestedModel = req.Model
	req.Model = served
	return req
}

// maintenanceSubstitute routes a request for a maintenance model onto an online
// member of the same fuzzy group, or rejects it (ok=false) when the group has
// no online fallback. Bypasses the fuzzy idle window: a maintenance model is
// never started, so the group must keep serving if it can.
func (s *FIFO) maintenanceSubstitute(req HandlerReq) (HandlerReq, bool) {
	if !s.effects.IsMaintenance(req.Model) {
		return req, true
	}
	// An explicit /upstream/<model>/ target is never routed onto another model;
	// a maintenance model cannot be started, so reject it here rather than
	// starting a swap that is guaranteed to fail in doSwap.
	if req.Explicit {
		return req, false
	}
	gid, ok := s.fuzzy.groupOf[req.Model]
	if !ok {
		return req, false
	}
	served := s.fuzzyOnline(gid)
	if served == "" || served == req.Model {
		return req, false
	}
	s.logger.Debugf("%s: serving maintenance model %s with online model %s (group %s)",
		s.name, req.Model, served, gid)
	req.RequestedModel = req.Model
	req.Model = served
	return req, true
}

// protectPinned guards pinned models from being evicted. If evict contains a
// pinned model and the request's model is in the same fuzzy group, it force-
// rewrites req.Model to that pinned model (ignoring the fuzzy idle window).
// Otherwise it returns ErrPinnedProtected. rewritten reports whether req.Model
// was rewritten. The reservation for the (possibly rewritten) model is made by
// the caller's subsequent admit(), so this must run before admit().
func (s *FIFO) protectPinned(req *HandlerReq, running, evict []string) (rewritten bool, err error) {
	for _, m := range evict {
		if !s.effects.IsPinned(m) {
			continue
		}
		// An explicit /upstream/<model>/ target is never rewritten onto the
		// pinned model; reject so the client learns the target cannot be served.
		if req.Explicit {
			s.logger.Debugf("%s: rejecting explicit request for %s: would evict pinned model %s",
				s.name, req.Model, m)
			return false, swaputil.ErrPinnedProtected
		}
		if gid := s.fuzzy.groupOf[req.Model]; gid != "" && s.memberOf(m) == gid {
			s.logger.Debugf("%s: protecting pinned model %s, serving it instead of %s (group %s)",
				s.name, m, req.Model, gid)
			req.RequestedModel = req.Model
			req.Model = m
			return true, nil
		}
		s.logger.Debugf("%s: rejecting request for %s: would evict pinned model %s",
			s.name, req.Model, m)
		return false, swaputil.ErrPinnedProtected
	}
	return false, nil
}

// evictsPinned reports whether any model in evict is pinned. drainQueue uses it
// to reject (never rewrite) queued requests whose serving would evict a pinned
// model, since a queued request's concurrency slot is bound to its target model.
func (s *FIFO) evictsPinned(evict []string) bool {
	for _, m := range evict {
		if s.effects.IsPinned(m) {
			return true
		}
	}
	return false
}

// fuzzyOnline returns the model an incoming request would be fuzzed onto: the
// in-flight swap target first (the direction the group is already switching),
// else a running group member — preferring a pinned Ready one, then Ready, then
// lexicographically first for determinism. Maintenance models are excluded.
// "" when the group has neither.
func (s *FIFO) fuzzyOnline(gid string) string {
	// 1) in-flight swap targets belonging to gid, skipping maintenance targets.
	var targets []string
	for target := range s.active {
		if s.memberOf(target) == gid && !s.effects.IsMaintenance(target) {
			targets = append(targets, target)
		}
	}
	if len(targets) > 0 {
		sort.Strings(targets)
		return targets[0]
	}
	// 2) running members: pinned Ready first, then Ready, then others, all
	//    excluding maintenance targets.
	var pinnedReady, ready, others []string
	for m, st := range s.effects.RunningModels() {
		if s.memberOf(m) != gid || s.effects.IsMaintenance(m) {
			continue
		}
		switch st {
		case process.StateReady:
			if s.effects.IsPinned(m) {
				pinnedReady = append(pinnedReady, m)
			} else {
				ready = append(ready, m)
			}
		default:
			others = append(others, m)
		}
	}
	if len(pinnedReady) > 0 {
		sort.Strings(pinnedReady)
		return pinnedReady[0]
	}
	if len(ready) > 0 {
		sort.Strings(ready)
		return ready[0]
	}
	if len(others) > 0 {
		sort.Strings(others)
		return others[0]
	}
	return ""
}

// fuzzyApplies: busy -> yes; idle -> only within the idle window of a group
// that has served at least once (never-served means a real switch).
func (s *FIFO) fuzzyApplies(gid string) bool {
	if s.groupBusy(gid) {
		return true
	}
	t, ok := s.lastServeDone[gid]
	if !ok {
		return false
	}
	win := s.fuzzy.idleTimeout[gid]
	if win == 0 {
		return false
	}
	return s.now().Sub(t) < win
}

// groupBusy: any group member serving, queued, or targeted/evicted by an
// in-flight swap.
func (s *FIFO) groupBusy(gid string) bool {
	for m, n := range s.inFlight {
		if n > 0 && s.memberOf(m) == gid {
			return true
		}
	}
	for _, it := range s.queued {
		if s.memberOf(it.Req.Model) == gid {
			return true
		}
	}
	for target, sw := range s.active {
		if s.memberOf(target) == gid {
			return true
		}
		for _, e := range sw.evict {
			if s.memberOf(e) == gid {
				return true
			}
		}
	}
	return false
}

func (s *FIFO) memberOf(m string) string { return s.fuzzy.groupOf[m] }

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
