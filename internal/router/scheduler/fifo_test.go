package scheduler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// FIFO methods all run on the router's single run-loop goroutine, so these
// tests drive them directly and synchronously. A swap is "completed" by calling
// OnSwapDone, a served request "finishes" by calling OnServeDone — exactly the
// events the run loop would deliver. fakeEffects records every side-effect and
// stubPlanner supplies a fixed eviction set per target.

// stubPlanner returns a fixed eviction list per target.
type stubPlanner struct {
	evict map[string][]string
}

func (s *stubPlanner) EvictionFor(target string, _ []string) []string {
	if s.evict == nil {
		return nil
	}
	return s.evict[target]
}

func (s *stubPlanner) OnSwapStart(string, []string) {}

// evictIfRunningPlanner returns an eviction list only while the eviction
// candidate is in the running set — modelling a shared-GPU setup where loading
// a target stops a sibling only once that sibling is actually running.
type evictIfRunningPlanner struct {
	evict map[string]string // target -> candidate to evict when running
}

func (p *evictIfRunningPlanner) EvictionFor(target string, running []string) []string {
	cand, ok := p.evict[target]
	if !ok {
		return nil
	}
	for _, r := range running {
		if r == cand {
			return []string{cand}
		}
	}
	return nil
}

func (p *evictIfRunningPlanner) OnSwapStart(string, []string) {}

// grantRec is one GrantError / GrantServe call. err!=nil marks an error grant;
// otherwise it is a serve grant and serve reports whether the caller received it.
type grantRec struct {
	model string
	err   error
	serve bool
}

type startRec struct {
	model string
	evict []string
}

type stopRec struct {
	timeout time.Duration
	ids     []string
}

// fakeEffects is an in-memory scheduler.Effects. Tests program process states
// and GrantServe outcomes, then assert on the recorded calls.
type fakeEffects struct {
	states       map[string]process.ProcessState // model -> state; missing => not handled
	serveResult  map[string]bool                 // GrantServe return per model (default true)
	lastServeReq HandlerReq

	starts []startRec
	grants []grantRec
	stops  []stopRec
}

func newFakeEffects() *fakeEffects {
	return &fakeEffects{
		states:      map[string]process.ProcessState{},
		serveResult: map[string]bool{},
	}
}

func (f *fakeEffects) ModelState(modelID string) (process.ProcessState, bool) {
	st, ok := f.states[modelID]
	return st, ok
}

func (f *fakeEffects) RunningModels() map[string]process.ProcessState {
	out := make(map[string]process.ProcessState)
	for id, st := range f.states {
		if st == process.StateStopped || st == process.StateShutdown {
			continue
		}
		out[id] = st
	}
	return out
}

func (f *fakeEffects) StartSwap(modelID string, evict []string) {
	f.starts = append(f.starts, startRec{model: modelID, evict: evict})
}

func (f *fakeEffects) GrantError(req HandlerReq, err error) {
	f.grants = append(f.grants, grantRec{model: req.Model, err: err})
}

func (f *fakeEffects) GrantServe(req HandlerReq, modelID string) bool {
	ok := true
	if v, set := f.serveResult[modelID]; set {
		ok = v
	}
	f.lastServeReq = req
	f.grants = append(f.grants, grantRec{model: modelID, serve: ok})
	return ok
}

func (f *fakeEffects) StopProcesses(timeout time.Duration, ids []string) {
	f.stops = append(f.stops, stopRec{timeout: timeout, ids: ids})
}

// served counts grants that handed modelID a handler and were received.
func (f *fakeEffects) served(modelID string) int {
	n := 0
	for _, g := range f.grants {
		if g.err == nil && g.serve && g.model == modelID {
			n++
		}
	}
	return n
}

// errored counts error grants, optionally filtered by model ("" = any).
func (f *fakeEffects) errored(model string) int {
	n := 0
	for _, g := range f.grants {
		if g.err != nil && (model == "" || g.model == model) {
			n++
		}
	}
	return n
}

// startsFor counts StartSwap calls for modelID.
func (f *fakeEffects) startsFor(modelID string) int {
	n := 0
	for _, s := range f.starts {
		if s.model == modelID {
			n++
		}
	}
	return n
}

func newFIFO(planner Swapper, eff Effects) *FIFO {
	return NewFIFO("test", logmon.NewWriter(io.Discard), planner, config.FifoConfig{}, nil, eff)
}

func req(model string) HandlerReq {
	return HandlerReq{
		Model: model,
		Ctx:   context.Background(),
		Admit: make(chan error, 1),
	}
}

// reqCh creates a HandlerReq with a unique Respond channel so OnCancel can
// identify it among queued requests and swap waiters.
func reqCh(model string) HandlerReq {
	r := req(model)
	r.Respond = make(chan HandlerResp, 1)
	return r
}

// reqPos creates a HandlerReq that also listens for queue-position broadcasts.
func reqPos(model string) HandlerReq {
	r := reqCh(model)
	r.PositionCh = make(chan int, 1)
	return r
}

// queueDepthOff returns a FifoConfig with over-limit queueing disabled
// (queueDepth=0), so a FIFO built from it keeps the legacy behavior: requests
// over the concurrency limit are rejected at admission with 429.
func queueDepthOff() config.FifoConfig {
	zero := 0
	return config.FifoConfig{QueueDepth: &zero}
}

// queueTimeoutInfinite returns a pointer to 0, an explicitly configured
// infinite queue timeout. Queueing tests that do not exercise the timeout use
// it so they stay independent of the default 60s queueTimeout.
func queueTimeoutInfinite() *int {
	zero := 0
	return &zero
}

// queuePosition reads the latest broadcast queue position, failing the test if
// none was sent.
func queuePosition(t *testing.T, req HandlerReq) int {
	t.Helper()
	select {
	case pos := <-req.PositionCh:
		return pos
	default:
		t.Fatal("no queue position broadcast")
		return -1
	}
}

func admitErr(t *testing.T, req HandlerReq) error {
	t.Helper()
	select {
	case err := <-req.Admit:
		return err
	default:
		t.Fatal("admission result not sent")
		return nil
	}
}

func assertAdmitted(t *testing.T, req HandlerReq) {
	t.Helper()
	if err := admitErr(t, req); err != nil {
		t.Fatalf("admission err=%v want nil", err)
	}
}

func assertAdmission429(t *testing.T, req HandlerReq) {
	t.Helper()
	var httpErr swaputil.HTTPError
	err := admitErr(t, req)
	if !errors.As(err, &httpErr) {
		t.Fatalf("admission err=%v want HTTPError", err)
	}
	if httpErr.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("StatusCode()=%d want 429", httpErr.StatusCode())
	}
	if httpErr.Header().Get("Retry-After") == "" {
		t.Fatal("missing Retry-After header")
	}
}

func TestFIFO_SendAdmission_CancelledContextWins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := HandlerReq{
		Ctx:   ctx,
		Admit: make(chan error, 1),
	}

	if sendAdmission(r, nil) {
		t.Fatal("sendAdmission returned true for cancelled request")
	}
	select {
	case err := <-r.Admit:
		t.Fatalf("admission sent after cancellation: %v", err)
	default:
	}
}

func TestFIFO_SendAdmission_NilAdmitPreservesExistingBehavior(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if !sendAdmission(HandlerReq{Ctx: ctx}, nil) {
		t.Fatal("nil Admit should preserve existing accepted behavior")
	}
}

func TestFIFO_ReleaseWithoutReservationPanics(t *testing.T) {
	s := newFIFO(&stubPlanner{}, newFakeEffects())

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("release without reservation did not panic")
		}
	}()
	s.release("a")
}

func TestFIFO_FastPath(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	s := newFIFO(&stubPlanner{}, eff)

	s.OnRequest(req("a"))

	if got := eff.startsFor("a"); got != 0 {
		t.Errorf("StartSwap calls=%d want 0 (fast path should not swap)", got)
	}
	if got := eff.served("a"); got != 1 {
		t.Errorf("served(a)=%d want 1", got)
	}
}

func TestFIFO_GrantSetsPriorityMetadata(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	cfg := config.FifoConfig{Priority: map[string]int{"a": 7}}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, cfg, nil, eff)

	ctx := swaputil.SetContext(context.Background(), swaputil.ReqContextData{ModelID: "a", Metadata: make(map[string]string)})
	s.OnRequest(HandlerReq{Model: "a", Ctx: ctx})

	if got := eff.served("a"); got != 1 {
		t.Fatalf("served(a)=%d want 1", got)
	}
	data, ok := swaputil.ReadContext(eff.lastServeReq.Ctx)
	if !ok {
		t.Fatal("context data missing from granted request")
	}
	if data.Metadata["fifo_priority"] != "7" {
		t.Errorf("fifo_priority = %q, want 7", data.Metadata["fifo_priority"])
	}
}

func TestFIFO_ModelNotFound(t *testing.T) {
	eff := newFakeEffects() // no states => model unknown
	s := newFIFO(&stubPlanner{}, eff)

	r := req("ghost")
	s.OnRequest(r)

	if got := len(eff.starts); got != 0 {
		t.Errorf("StartSwap calls=%d want 0", got)
	}
	if got := eff.errored("ghost"); got != 0 {
		t.Fatalf("error grants=%d want 0 for admission rejection", got)
	}
	if err := admitErr(t, r); !errors.Is(err, ErrModelNotFound) {
		t.Errorf("admission err=%v want ErrModelNotFound", err)
	}
}

func TestFIFO_OnDemandStartThenServe(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	s := newFIFO(&stubPlanner{}, eff)

	s.OnRequest(req("a"))
	if got := eff.startsFor("a"); got != 1 {
		t.Fatalf("StartSwap(a)=%d want 1", got)
	}
	if got := eff.served("a"); got != 0 {
		t.Errorf("served(a)=%d want 0 before swap completes", got)
	}

	// Swap finishes, model is now ready.
	eff.states["a"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "a"})

	if got := eff.served("a"); got != 1 {
		t.Errorf("served(a)=%d want 1 after swap done", got)
	}
}

func TestFIFO_JoinInFlightSwap(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	s := newFIFO(&stubPlanner{}, eff)

	s.OnRequest(req("a")) // starts swap
	s.OnRequest(req("a")) // joins
	s.OnRequest(req("a")) // joins

	if got := eff.startsFor("a"); got != 1 {
		t.Fatalf("StartSwap(a)=%d want 1 (all three share one swap)", got)
	}

	eff.states["a"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "a"})

	if got := eff.served("a"); got != 3 {
		t.Errorf("served(a)=%d want 3 (one swap serves all waiters)", got)
	}
}

func TestFIFO_SwapDoneError_FailsAllWaiters(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	s := newFIFO(&stubPlanner{}, eff)

	s.OnRequest(req("a"))
	s.OnRequest(req("a"))

	s.OnSwapDone(SwapDone{ModelID: "a", Err: errors.New("boom")})

	if eff.served("a") != 0 {
		t.Errorf("served(a)=%d want 0 on swap error", eff.served("a"))
	}
	if eff.errored("a") != 2 {
		t.Errorf("errored(a)=%d want 2 (both waiters fail)", eff.errored("a"))
	}
}

// TestFIFO_QueueOnEvictionCollision covers a request whose target evicts the
// model currently being swapped: it must queue until that swap finishes AND its
// served request drains, because starting it would stop a busy process.
func TestFIFO_QueueOnEvictionCollision(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	// Loading b evicts a.
	s := newFIFO(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff)

	s.OnRequest(req("a")) // StartSwap(a)
	s.OnRequest(req("b")) // collides with a's in-flight swap -> queue
	if got := eff.startsFor("b"); got != 0 {
		t.Fatalf("b started early: StartSwap(b)=%d want 0", got)
	}

	// a becomes ready and is granted (now serving, inFlight[a]=1).
	eff.states["a"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "a"})
	if got := eff.startsFor("b"); got != 0 {
		t.Fatalf("b started while a is serving: StartSwap(b)=%d want 0", got)
	}

	// a's request finishes -> a no longer in-flight -> b may now swap.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if got := eff.startsFor("b"); got != 1 {
		t.Fatalf("StartSwap(b)=%d want 1 after a drained", got)
	}
	if got := eff.starts[len(eff.starts)-1].evict; len(got) != 1 || got[0] != "a" {
		t.Errorf("b swap evict=%v want [a]", got)
	}
}

// TestFIFO_DisjointSwapsRunInParallel verifies two requests with
// non-conflicting evict sets both start without waiting for each other.
func TestFIFO_DisjointSwapsRunInParallel(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	s := newFIFO(&stubPlanner{}, eff) // empty evicts

	s.OnRequest(req("a"))
	s.OnRequest(req("b"))

	if eff.startsFor("a") != 1 || eff.startsFor("b") != 1 {
		t.Fatalf("StartSwap a=%d b=%d want 1 each (parallel)", eff.startsFor("a"), eff.startsFor("b"))
	}
}

// TestFIFO_OverlappingEvictSetsDoNotRunInParallel verifies two swaps with
// different targets that evict the *same* model do not run concurrently: the
// second must queue rather than double-evict the shared model. Neither target is
// in the other's evict set, so this is only caught by the evict-set overlap
// check in collidesWith.
func TestFIFO_OverlappingEvictSetsDoNotRunInParallel(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	eff.states["x"] = process.StateReady // shared eviction target, running
	// Loading a or b both require evicting x.
	s := newFIFO(&stubPlanner{evict: map[string][]string{"a": {"x"}, "b": {"x"}}}, eff)

	s.OnRequest(req("a")) // StartSwap(a, [x])
	s.OnRequest(req("b")) // overlaps a's evict set ([x]) -> queue
	if eff.startsFor("a") != 1 {
		t.Fatalf("StartSwap(a)=%d want 1", eff.startsFor("a"))
	}
	if got := eff.startsFor("b"); got != 0 {
		t.Fatalf("b started in parallel while a evicts x: StartSwap(b)=%d want 0", got)
	}

	// a's swap completes and x is gone; b can now evict nothing and start.
	eff.states["a"] = process.StateReady
	eff.states["x"] = process.StateStopped
	s.OnSwapDone(SwapDone{ModelID: "a"})
	if got := eff.startsFor("b"); got != 1 {
		t.Fatalf("StartSwap(b)=%d want 1 after a's swap drained", got)
	}
}

// TestFIFO_QueueDrainPromotesMultiple verifies completing one swap unblocks
// every queued request that no longer collides — they all start together.
func TestFIFO_QueueDrainPromotesMultiple(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	eff.states["c"] = process.StateStopped
	// a's swap evicts both b and c; b and c evict nothing.
	s := newFIFO(&stubPlanner{evict: map[string][]string{"a": {"b", "c"}}}, eff)

	s.OnRequest(req("a")) // StartSwap(a, [b,c])
	s.OnRequest(req("b")) // collides (in a's evict set) -> queue
	s.OnRequest(req("c")) // collides -> queue
	if eff.startsFor("b") != 0 || eff.startsFor("c") != 0 {
		t.Fatalf("b/c started early")
	}

	eff.states["a"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "a"})

	// b and c have empty evict sets and don't evict a, so both start now.
	if eff.startsFor("b") != 1 || eff.startsFor("c") != 1 {
		t.Fatalf("StartSwap b=%d c=%d want 1 each after a done", eff.startsFor("b"), eff.startsFor("c"))
	}
	if eff.served("a") != 1 {
		t.Errorf("served(a)=%d want 1", eff.served("a"))
	}
}

// TestFIFO_QueueCollation verifies duplicate requests collapse into one swap
// per model: the second request for each model joins the active swap (at arrival
// or at drain time) rather than triggering its own swap.
func TestFIFO_QueueCollation(t *testing.T) {
	eff := newFakeEffects()
	for _, id := range []string{"a", "b", "c"} {
		eff.states[id] = process.StateStopped
	}
	// Each model evicts the other two: all swaps are mutually exclusive.
	s := newFIFO(&stubPlanner{evict: map[string][]string{
		"a": {"b", "c"},
		"b": {"a", "c"},
		"c": {"a", "b"},
	}}, eff)

	for _, id := range []string{"a", "b", "c", "a", "b", "c"} {
		s.OnRequest(req(id))
	}

	// Drain a, then its served requests, which promotes b; repeat for b -> c.
	drain := func(model string, waiters int) {
		eff.states[model] = process.StateReady
		s.OnSwapDone(SwapDone{ModelID: model})
		for i := 0; i < waiters; i++ {
			s.OnServeDone(ServeDoneEvent{ModelID: model})
		}
	}
	drain("a", 2)
	drain("b", 2)
	drain("c", 2)

	for _, id := range []string{"a", "b", "c"} {
		if got := eff.startsFor(id); got != 1 {
			t.Errorf("StartSwap(%s)=%d want 1 (collation)", id, got)
		}
		if got := eff.served(id); got != 2 {
			t.Errorf("served(%s)=%d want 2", id, got)
		}
	}
}

// TestFIFO_NoSwapWhileServing verifies a model still handling requests is not
// evicted: the evicting request waits until every in-flight request drains.
func TestFIFO_NoSwapWhileServing(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	s := newFIFO(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff)

	s.OnRequest(req("a")) // fast path, inFlight[a]=1
	s.OnRequest(req("a")) // fast path, inFlight[a]=2
	s.OnRequest(req("b")) // would evict busy a -> queue
	if eff.startsFor("b") != 0 {
		t.Fatalf("b started while a serving")
	}

	s.OnServeDone(ServeDoneEvent{ModelID: "a"}) // inFlight[a]=1
	if eff.startsFor("b") != 0 {
		t.Fatalf("b started while a still serving one request")
	}

	s.OnServeDone(ServeDoneEvent{ModelID: "a"}) // inFlight[a]=0
	if eff.startsFor("b") != 1 {
		t.Fatalf("StartSwap(b)=%d want 1 after a fully drained", eff.startsFor("b"))
	}
}

// TestFIFO_GrantServeFalseDoesNotLeakInFlight verifies that when a caller has
// walked away (GrantServe returns false) the in-flight count is not bumped, so a
// later evicting request is not blocked forever.
func TestFIFO_GrantServeFalseDoesNotLeakInFlight(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	eff.serveResult["a"] = false // a's waiter is gone by grant time
	s := newFIFO(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff)

	s.OnRequest(req("a"))
	eff.states["a"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "a"}) // grant fails, inFlight[a] stays 0

	// b evicts a; since a is not in-flight, b should start immediately.
	s.OnRequest(req("b"))
	if eff.startsFor("b") != 1 {
		t.Fatalf("StartSwap(b)=%d want 1 (no leaked in-flight on a)", eff.startsFor("b"))
	}
}

// TestFIFO_OnShutdown_FailsAllWaiters verifies shutdown errors every waiter the
// scheduler holds: active-swap waiters and queued requests alike.
func TestFIFO_OnShutdown_FailsAllWaiters(t *testing.T) {
	eff := newFakeEffects()
	for _, id := range []string{"a", "b", "c"} {
		eff.states[id] = process.StateStopped
	}
	// a and b load in parallel; c collides with both and queues.
	s := newFIFO(&stubPlanner{evict: map[string][]string{"c": {"a", "b"}}}, eff)

	s.OnRequest(req("a")) // StartSwap(a)
	s.OnRequest(req("a")) // join a
	s.OnRequest(req("b")) // StartSwap(b)
	s.OnRequest(req("b")) // join b
	s.OnRequest(req("c")) // queued

	s.OnShutdown(errors.New("shutting down"))

	if got := eff.errored(""); got != 5 {
		t.Errorf("error grants=%d want 5 (2 a + 2 b + 1 c)", got)
	}
}

func TestFIFO_OnUnload_ReleasesActiveWaiters(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	s := newFIFO(&stubPlanner{}, eff)

	s.OnRequest(req("a")) // active swap a with one waiter
	s.OnRequest(req("a")) // join

	s.OnUnload([]string{"a"}, time.Second)

	if got := eff.errored("a"); got != 2 {
		t.Errorf("errored(a)=%d want 2 (active swap waiters released)", got)
	}
	if len(eff.stops) != 1 || len(eff.stops[0].ids) != 1 || eff.stops[0].ids[0] != "a" {
		t.Errorf("StopProcesses=%+v want one call stopping [a]", eff.stops)
	}
	if eff.stops[0].timeout != time.Second {
		t.Errorf("StopProcesses timeout=%v want 1s", eff.stops[0].timeout)
	}
}

func TestFIFO_OnUnload_DropsQueuedRequests(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	// b evicts a, so a request for b queues while a is loading.
	s := newFIFO(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff)

	s.OnRequest(req("a")) // StartSwap(a)
	s.OnRequest(req("b")) // queued

	s.OnUnload([]string{"b"}, time.Second)

	if got := eff.errored("b"); got != 1 {
		t.Errorf("errored(b)=%d want 1 (queued request dropped)", got)
	}
	if got := eff.startsFor("b"); got != 0 {
		t.Errorf("StartSwap(b)=%d want 0 (b should never start)", got)
	}
	// a's swap is untouched: its waiter is neither served nor errored yet.
	if eff.served("a") != 0 || eff.errored("a") != 0 {
		t.Errorf("a swap should be untouched: served=%d errored=%d", eff.served("a"), eff.errored("a"))
	}
}

// TestFIFO_PriorityQueueOrder verifies queued requests are ordered by descending
// priority, with arrival (FIFO) order preserved among equal-priority models.
func TestFIFO_PriorityQueueOrder(t *testing.T) {
	eff := newFakeEffects()
	for _, m := range []string{"z", "A", "B", "C", "D"} {
		eff.states[m] = process.StateStopped
	}
	// z's swap evicts every other model, so any request that arrives while z is
	// loading collides with z's in-flight swap and parks in the queue.
	planner := &stubPlanner{evict: map[string][]string{"z": {"A", "B", "C", "D"}}}
	cfg := config.FifoConfig{Priority: map[string]int{"A": 10, "B": 5, "C": 5, "D": 1}}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), planner, cfg, nil, eff)

	s.OnRequest(req("z")) // StartSwap(z, [A,B,C,D])

	// Arrive out of priority order; B before C exercises FIFO tie-breaking.
	for _, m := range []string{"B", "D", "C", "A"} {
		s.OnRequest(req(m))
	}

	got := make([]string, len(s.queued))
	for i, q := range s.queued {
		got[i] = q.Req.Model
	}
	want := []string{"A", "B", "C", "D"}
	if len(got) != len(want) {
		t.Fatalf("queue=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("queue=%v want %v", got, want)
		}
	}
}

// TestFIFO_OnCancel_QueuedRequest verifies that cancelling a queued request
// prevents drainQueue from ever starting a model load for it. Without OnCancel
// the dead request would sit in the queue until a drain triggers a wasted swap.
func TestFIFO_OnCancel_QueuedRequest(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	// b evicts a, so a request for b queues while a is loading.
	s := newFIFO(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff)

	s.OnRequest(req("a")) // StartSwap(a)

	cancelledReq := reqCh("b")
	s.OnRequest(cancelledReq) // queued (collides with a's in-flight swap)
	if len(s.queued) != 1 {
		t.Fatalf("queue len=%d want 1 before cancel", len(s.queued))
	}

	// Client disconnects.
	s.OnCancel(cancelledReq)

	if len(s.queued) != 0 {
		t.Fatalf("queue len=%d want 0 after cancel", len(s.queued))
	}

	// a's swap finishes; drainQueue runs but b is gone — no swap for b.
	eff.states["a"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "a"})

	if got := eff.startsFor("b"); got != 0 {
		t.Errorf("StartSwap(b)=%d want 0 (cancelled request should not trigger a load)", got)
	}
}

// TestFIFO_OnCancel_SwapWaiter verifies that cancelling a request that joined an
// in-flight swap removes it from the waiter list. When the swap completes, the
// cancelled waiter receives no grant and does not bump the in-flight count.
func TestFIFO_OnCancel_SwapWaiter(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	s := newFIFO(&stubPlanner{}, eff)

	liveReq := reqCh("a")
	cancelledReq := reqCh("a")
	s.OnRequest(liveReq)      // starts swap
	s.OnRequest(cancelledReq) // joins

	if sw := s.active["a"]; len(sw.waiters) != 2 {
		t.Fatalf("waiters=%d want 2", len(sw.waiters))
	}

	s.OnCancel(cancelledReq)

	if sw := s.active["a"]; len(sw.waiters) != 1 {
		t.Fatalf("waiters=%d want 1 after cancel", len(sw.waiters))
	}

	// Swap finishes: only the live waiter is granted.
	eff.states["a"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "a"})

	if got := eff.served("a"); got != 1 {
		t.Errorf("served(a)=%d want 1 (only the non-cancelled waiter)", got)
	}
}

// TestFIFO_OnCancel_NotPresent is a no-op: cancelling a request that was already
// granted (and is no longer queued or waiting) must not affect anything.
func TestFIFO_OnCancel_NotPresent(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	s := newFIFO(&stubPlanner{}, eff)

	r := reqCh("a")
	s.OnRequest(r) // fast-path served immediately

	// Cancel after grant — should be a harmless no-op.
	s.OnCancel(r)

	if got := eff.served("a"); got != 1 {
		t.Errorf("served(a)=%d want 1 (cancel of granted request is a no-op)", got)
	}
	if len(s.queued) != 0 {
		t.Errorf("queue should be empty, len=%d", len(s.queued))
	}
}

// newFIFOWithLimit builds a FIFO whose single model has the given concurrency
// limit, already in StateReady so every request exercises the fast path.
func newFIFOWithLimit(t *testing.T, model string, limit int) (*FIFO, *fakeEffects) {
	t.Helper()
	eff := newFakeEffects()
	eff.states[model] = process.StateReady
	models := map[string]config.ModelConfig{
		model: {ConcurrencyLimit: limit},
	}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueTimeout: queueTimeoutInfinite()}, models, eff)
	return s, eff
}

// TestFIFO_ConcurrencyLimit_RejectsOverLimit verifies that with queueing
// explicitly disabled (queueDepth=0) a request arriving while the model is at
// capacity is rejected during admission, before it can be queued or served, and
// that a new request succeeds once capacity returns. This is the legacy
// upstream regression path.
func TestFIFO_ConcurrencyLimit_RejectsOverLimit(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 1},
	}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, queueDepthOff(), models, eff)

	// First request: served (inFlight 0 → 1).
	r1 := req("a")
	s.OnRequest(r1)
	assertAdmitted(t, r1)
	if got := eff.served("a"); got != 1 {
		t.Fatalf("served(a)=%d want 1", got)
	}

	// Second request while slot is occupied: rejected at admission with 429.
	r2 := req("a")
	s.OnRequest(r2)
	assertAdmission429(t, r2)
	if got := eff.errored("a"); got != 0 {
		t.Fatalf("errored(a)=%d want 0 (over-limit rejects before grant)", got)
	}

	// After the in-flight request finishes, a new request succeeds.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	r3 := req("a")
	s.OnRequest(r3)
	assertAdmitted(t, r3)
	if got := eff.served("a"); got != 2 {
		t.Fatalf("served(a)=%d want 2 after drain", got)
	}
}

// TestFIFO_ConcurrencyLimit_DefaultIsTen verifies that a model without an
// explicit ConcurrencyLimit gets the default cap of 10, and that with queueing
// enabled (the default queueDepth=10) requests 11-20 wait in the queue while
// the 21st overflows limit+queueDepth and is rejected.
func TestFIFO_ConcurrencyLimit_DefaultIsTen(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	// nil models → every model gets defaultConcurrencyLimit (10).
	s := newFIFO(&stubPlanner{}, eff)

	for i := 0; i < 10; i++ {
		r := req("a")
		s.OnRequest(r)
		assertAdmitted(t, r)
	}
	if got := eff.served("a"); got != 10 {
		t.Fatalf("served(a)=%d want 10 (default limit)", got)
	}

	// With queueing enabled, the next 10 requests are admitted and queue
	// instead of being rejected.
	for i := 0; i < 10; i++ {
		r := req("a")
		s.OnRequest(r)
		assertAdmitted(t, r)
	}
	if got := len(s.queued); got != 10 {
		t.Fatalf("queue len=%d want 10 (default queue depth)", got)
	}

	// 21st request overflows limit + queueDepth: rejected at admission.
	r := req("a")
	s.OnRequest(r)
	assertAdmission429(t, r)
	if got := eff.errored("a"); got != 0 {
		t.Fatalf("errored(a)=%d want 0 (over limit+queueDepth rejects before grant)", got)
	}
}

// TestFIFO_ConcurrencyLimit_CustomLimit verifies that with queueing enabled a
// ConcurrencyLimit greater than zero overrides the default, and an over-limit
// request queues and is served as soon as a slot frees.
func TestFIFO_ConcurrencyLimit_CustomLimit(t *testing.T) {
	s, eff := newFIFOWithLimit(t, "a", 2)

	r1 := req("a")
	r2 := req("a")
	r3 := req("a")
	s.OnRequest(r1)
	s.OnRequest(r2)
	s.OnRequest(r3)
	assertAdmitted(t, r1)
	assertAdmitted(t, r2)
	assertAdmitted(t, r3)

	if got := eff.served("a"); got != 2 {
		t.Fatalf("served(a)=%d want 2 (custom limit)", got)
	}
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1 (over-limit request queues)", got)
	}

	// One completion frees a serving slot; the queued request is served
	// immediately even though the other in-flight request continues.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if got := eff.served("a"); got != 3 {
		t.Fatalf("served(a)=%d want 3 after a slot frees", got)
	}
	if got := len(s.queued); got != 0 {
		t.Fatalf("queue len=%d want 0", got)
	}
}

// TestFIFO_ConcurrencyLimit_SwapWaiters verifies that with queueing disabled
// (queueDepth=0) more swap waiters than the concurrency limit are rejected
// during admission rather than joining the in-flight swap or being queued.
func TestFIFO_ConcurrencyLimit_SwapWaiters(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 2},
	}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, queueDepthOff(), models, eff)

	// Three requests arrive while model is loading: one starts swap, two join.
	r1 := req("a")
	r2 := req("a")
	r3 := req("a")
	s.OnRequest(r1)
	s.OnRequest(r2)
	s.OnRequest(r3)
	assertAdmitted(t, r1)
	assertAdmitted(t, r2)
	assertAdmission429(t, r3)

	if got := eff.startsFor("a"); got != 1 {
		t.Fatalf("StartSwap(a)=%d want 1", got)
	}
	if sw := s.active["a"]; len(sw.waiters) != 2 {
		t.Fatalf("waiters=%d want 2 (third request must not join)", len(sw.waiters))
	}

	// Swap completes: only the two admitted requests are served.
	eff.states["a"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "a"})

	if got := eff.served("a"); got != 2 {
		t.Fatalf("served(a)=%d want 2", got)
	}
	if got := eff.errored("a"); got != 0 {
		t.Fatalf("errored(a)=%d want 0 (excess waiter rejected at admission)", got)
	}
}

func TestFIFO_ConcurrencyLimit_QueuedWaitersReserveCapacity(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 2},
		"b": {},
	}
	// Queueing disabled so an over-reserved waiter is rejected up front
	// instead of queueing, keeping this test focused on swap reservation.
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{evict: map[string][]string{"a": {"b"}}}, queueDepthOff(), models, eff)

	bReq := req("b")
	aReq1 := req("a")
	aReq2 := req("a")
	aReq3 := req("a")

	s.OnRequest(bReq)  // StartSwap(b)
	s.OnRequest(aReq1) // queued behind b
	s.OnRequest(aReq2) // queued behind b
	s.OnRequest(aReq3) // rejected before queueing

	assertAdmitted(t, bReq)
	assertAdmitted(t, aReq1)
	assertAdmitted(t, aReq2)
	assertAdmission429(t, aReq3)

	if got := len(s.queued); got != 2 {
		t.Fatalf("queue len=%d want 2", got)
	}
	if got := eff.startsFor("a"); got != 0 {
		t.Fatalf("StartSwap(a)=%d want 0 while b is loading", got)
	}

	eff.states["b"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "b"})
	s.OnServeDone(ServeDoneEvent{ModelID: "b"})
	if got := eff.startsFor("a"); got != 1 {
		t.Fatalf("StartSwap(a)=%d want 1 after b drains", got)
	}

	eff.states["a"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "a"})
	if got := eff.served("a"); got != 2 {
		t.Fatalf("served(a)=%d want 2", got)
	}
}

func TestFIFO_ConcurrencyLimit_CancelledQueuedWaiterReleasesReservation(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 1},
		"b": {},
	}
	// Queueing disabled so the rejected waiter is a pure admission rejection,
	// exercising the legacy reservation-release path on cancel.
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{evict: map[string][]string{"a": {"b"}}}, queueDepthOff(), models, eff)

	bReq := req("b")
	cancelledReq := reqCh("a")
	rejectedReq := req("a")
	retryReq := req("a")

	s.OnRequest(bReq)
	s.OnRequest(cancelledReq)
	s.OnRequest(rejectedReq)
	assertAdmitted(t, bReq)
	assertAdmitted(t, cancelledReq)
	assertAdmission429(t, rejectedReq)

	s.OnCancel(cancelledReq)
	s.OnRequest(retryReq)
	assertAdmitted(t, retryReq)

	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1 after cancel and retry", got)
	}
}

// PATCH(v255) queueing for over-limit requests: the tests below cover the new
// capacity-queueing behavior in the default-on mode.

// TestFIFO_Queueing_OverLimitQueuesAndServesInOrder verifies that over-limit
// requests queue (with queue positions) instead of being rejected, and are
// served FIFO as serving slots free up one by one.
func TestFIFO_Queueing_OverLimitQueuesAndServesInOrder(t *testing.T) {
	s, eff := newFIFOWithLimit(t, "a", 1)

	r1 := req("a")
	s.OnRequest(r1)
	assertAdmitted(t, r1)
	if got := eff.served("a"); got != 1 {
		t.Fatalf("served(a)=%d want 1", got)
	}

	// Over-limit requests queue instead of being rejected.
	r2 := reqPos("a")
	r3 := reqPos("a")
	s.OnRequest(r2)
	s.OnRequest(r3)
	assertAdmitted(t, r2)
	assertAdmitted(t, r3)
	if got := len(s.queued); got != 2 {
		t.Fatalf("queue len=%d want 2", got)
	}
	if got := eff.errored("a"); got != 0 {
		t.Fatalf("errored(a)=%d want 0 (queued, not rejected)", got)
	}

	// Positions were broadcast: r2 is #1, r3 is #2.
	if pos := queuePosition(t, r2); pos != 1 {
		t.Errorf("r2 position=%d want 1", pos)
	}
	if pos := queuePosition(t, r3); pos != 2 {
		t.Errorf("r3 position=%d want 2", pos)
	}

	// Each serve completion frees one slot for the next waiter, in FIFO order.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if got := eff.served("a"); got != 2 {
		t.Fatalf("served(a)=%d want 2 after first completion", got)
	}
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1 (r3 still waiting)", got)
	}
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if got := eff.served("a"); got != 3 {
		t.Fatalf("served(a)=%d want 3 after second completion", got)
	}
	if got := len(s.queued); got != 0 {
		t.Fatalf("queue len=%d want 0", got)
	}
}

// TestFIFO_Queueing_QueueFullRejects429 verifies the hard cap: once
// limit + queueDepth reservations are taken, the next request is rejected at
// admission with 429.
func TestFIFO_Queueing_QueueFullRejects429(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	models := map[string]config.ModelConfig{"a": {ConcurrencyLimit: 1}}
	one := 1
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueDepth: &one, QueueTimeout: queueTimeoutInfinite()}, models, eff)

	r1 := req("a")
	s.OnRequest(r1)
	assertAdmitted(t, r1) // served, reserved=1

	r2 := req("a")
	s.OnRequest(r2)
	assertAdmitted(t, r2) // queued, reserved=2 = limit+queueDepth
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1", got)
	}

	r3 := req("a")
	s.OnRequest(r3)
	assertAdmission429(t, r3)
	if got := eff.errored("a"); got != 0 {
		t.Fatalf("errored(a)=%d want 0 (rejected before grant)", got)
	}
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1 (r3 must not queue)", got)
	}
}

// TestFIFO_Queueing_TimeoutRejects429 verifies the lazy-queue-timeout: a queued
// waiter is pruned with a 429 ConcurrencyLimitError once its deadline passes,
// releasing its reservation. Pruning only happens at a drain trigger.
func TestFIFO_Queueing_TimeoutRejects429(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 1},
		"b": {},
	}
	one := 1
	cfg := config.FifoConfig{QueueDepth: &one, QueueTimeout: &one}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, cfg, models, eff)

	// b's swap completion is the drain trigger that happens to run without
	// freeing a's serving slot.
	bReq := req("b")
	s.OnRequest(bReq) // StartSwap(b)

	r1 := req("a")
	s.OnRequest(r1) // fast-path, inFlight[a]=1, reserved[a]=1
	assertAdmitted(t, r1)

	r2 := reqCh("a")
	s.OnRequest(r2) // over limit -> queued with a 1s deadline
	assertAdmitted(t, r2)
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1", got)
	}

	// A drain before the deadline must not prune the waiter.
	s.drainQueue()
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1 before deadline", got)
	}

	// Let the deadline pass, then trigger the lazy drain via b's swap done.
	time.Sleep(1100 * time.Millisecond)
	eff.states["b"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "b"})

	if got := len(s.queued); got != 0 {
		t.Fatalf("queue len=%d want 0 after timeout drain", got)
	}
	if got := eff.served("a"); got != 1 {
		t.Fatalf("served(a)=%d want 1 (r2 must not be served)", got)
	}
	var httpErr swaputil.HTTPError
	if !errors.As(eff.grants[len(eff.grants)-1].err, &httpErr) {
		t.Fatalf("timeout error=%v want HTTPError", eff.grants[len(eff.grants)-1].err)
	}
	if httpErr.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("StatusCode()=%d want 429", httpErr.StatusCode())
	}
	if httpErr.Header().Get("Retry-After") == "" {
		t.Fatal("missing Retry-After header")
	}
	// r2's reservation was released: only r1's remains.
	if got := s.reserved["a"]; got != 1 {
		t.Fatalf("reserved[a]=%d want 1 after timeout", got)
	}
}

// TestFIFO_Queueing_CancelReleasesSlot verifies OnCancel prunes a queued
// capacity waiter and returns its reservation, so later requests can be served.
func TestFIFO_Queueing_CancelReleasesSlot(t *testing.T) {
	s, eff := newFIFOWithLimit(t, "a", 1)

	r1 := req("a")
	s.OnRequest(r1) // fast-path, inFlight[a]=1, reserved[a]=1
	assertAdmitted(t, r1)

	r2 := reqCh("a")
	s.OnRequest(r2) // over limit -> queued, reserved[a]=2
	assertAdmitted(t, r2)
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1", got)
	}

	s.OnCancel(r2)
	if got := len(s.queued); got != 0 {
		t.Fatalf("queue len=%d want 0 after cancel", got)
	}
	if got := s.reserved["a"]; got != 1 {
		t.Fatalf("reserved[a]=%d want 1 (only r1's slot remains)", got)
	}

	// r1 finishes; a new request is then served directly.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	r3 := req("a")
	s.OnRequest(r3)
	assertAdmitted(t, r3)
	if got := eff.served("a"); got != 2 {
		t.Fatalf("served(a)=%d want 2", got)
	}
	if got := len(s.queued); got != 0 {
		t.Fatalf("queue len=%d want 0", got)
	}
}

// TestFIFO_Queueing_OnServeDoneDrainsWhileInFlight verifies the serve-done
// path: with capacity waiters queued, every serve completion drains the queue
// even when the model still has other requests in flight.
func TestFIFO_Queueing_OnServeDoneDrainsWhileInFlight(t *testing.T) {
	s, eff := newFIFOWithLimit(t, "a", 2)

	r1 := req("a")
	r2 := req("a")
	s.OnRequest(r1)
	s.OnRequest(r2)
	assertAdmitted(t, r1)
	assertAdmitted(t, r2)
	if got := eff.served("a"); got != 2 {
		t.Fatalf("served(a)=%d want 2", got)
	}

	r3 := req("a")
	s.OnRequest(r3) // over limit -> queued
	assertAdmitted(t, r3)

	// One completion frees a serving slot while r2 is still in flight; the
	// queued waiter is served immediately instead of waiting for inFlight==0.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if got := eff.served("a"); got != 3 {
		t.Fatalf("served(a)=%d want 3 while one request is still in flight", got)
	}
	if got := len(s.queued); got != 0 {
		t.Fatalf("queue len=%d want 0 after drain", got)
	}

	// Same for the next waiter.
	r4 := req("a")
	s.OnRequest(r4)
	assertAdmitted(t, r4)
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if got := eff.served("a"); got != 4 {
		t.Fatalf("served(a)=%d want 4", got)
	}
	if got := len(s.queued); got != 0 {
		t.Fatalf("queue len=%d want 0", got)
	}
}

// TestFIFO_Queueing_OverLimitWaiterDoesNotJoinSwap verifies the capacity branch
// runs before the in-flight-swap join: a request arriving after the limit is
// reached queues instead of joining, so swap completion can never grant more
// waiters than the model's concurrency limit (in-flight stays <= limit).
func TestFIFO_Queueing_OverLimitWaiterDoesNotJoinSwap(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	models := map[string]config.ModelConfig{"a": {ConcurrencyLimit: 2}}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueTimeout: queueTimeoutInfinite()}, models, eff)

	r1 := req("a")
	r2 := req("a")
	r3 := req("a")
	s.OnRequest(r1)
	s.OnRequest(r2)
	s.OnRequest(r3)
	assertAdmitted(t, r1)
	assertAdmitted(t, r2)
	assertAdmitted(t, r3)
	if sw := s.active["a"]; len(sw.waiters) != 2 {
		t.Fatalf("waiters=%d want 2 (r3 must not join the swap)", len(sw.waiters))
	}
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1 (r3 queued as capacity waiter)", got)
	}

	// Swap completes: only r1 and r2 are granted (inFlight=2=limit).
	eff.states["a"] = process.StateReady
	s.OnSwapDone(SwapDone{ModelID: "a"})
	if got := eff.served("a"); got != 2 {
		t.Fatalf("served(a)=%d want 2", got)
	}
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1 (r3 still queued at capacity)", got)
	}

	// One waiter finishes: r3 is served while the other is still in flight.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if got := eff.served("a"); got != 3 {
		t.Fatalf("served(a)=%d want 3 after one completion", got)
	}
	if got := len(s.queued); got != 0 {
		t.Fatalf("queue len=%d want 0", got)
	}
}

// TestFIFO_Queueing_JoinGateCountsInFlight is the H1 regression: a swap can be
// in flight for a model that is STILL serving requests (the swap only stops a
// sibling). During that window the drain-time join gate must count both the
// existing waiters and the in-flight serving, otherwise OnSwapDone grants every
// waiter at once and in-flight exceeds the concurrency limit.
func TestFIFO_Queueing_JoinGateCountsInFlight(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 2},
		"b": {},
	}
	// Loading a evicts b only once b is running; start with b stopped so r1
	// serves a on the fast path.
	planner := &evictIfRunningPlanner{evict: map[string]string{"a": "b"}}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), planner, config.FifoConfig{QueueTimeout: queueTimeoutInfinite()}, models, eff)

	// r1 is served while b is stopped and stays in flight.
	r1 := req("a")
	s.OnRequest(r1)
	assertAdmitted(t, r1)
	if got := eff.served("a"); got != 1 {
		t.Fatalf("served(a)=%d want 1", got)
	}

	// b starts running; r2 now swaps a (evicting b) with r1 still serving, so
	// reserved=2=limit and the swap is in flight with inFlight[a]=1.
	eff.states["b"] = process.StateReady
	r2 := req("a")
	s.OnRequest(r2)
	assertAdmitted(t, r2)
	if got := eff.startsFor("a"); got != 1 {
		t.Fatalf("StartSwap(a)=%d want 1", got)
	}
	if sw := s.active["a"]; sw == nil || len(sw.waiters) != 1 {
		t.Fatalf("active swap waiters=%v want [r2]", sw)
	}

	// r3 arrives during the swap: a capacity waiter (reserved[a]=3 > limit=2).
	r3 := reqCh("a")
	s.OnRequest(r3)
	assertAdmitted(t, r3)
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1", got)
	}

	// A drain during the swap must NOT let r3 join: joining would push the
	// post-completion in-flight to 1 (r1) + 2 (waiters) = 3 > limit=2.
	s.drainQueue()
	if sw := s.active["a"]; len(sw.waiters) != 1 {
		t.Fatalf("waiters=%d want 1 (r3 must not join while r1 is in flight)", len(sw.waiters))
	}
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1 (r3 stays queued)", got)
	}

	// Swap completes: only r2 is granted; in-flight stays at the limit.
	s.OnSwapDone(SwapDone{ModelID: "a"})
	if got := eff.served("a"); got != 2 {
		t.Fatalf("served(a)=%d want 2", got)
	}
	if got := s.inFlight["a"]; got != 2 {
		t.Fatalf("inFlight[a]=%d want 2 (must not exceed limit)", got)
	}

	// r1 finishes; r3 is eventually servable — it swaps in (a still evicts b)
	// and is granted on completion.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if got := eff.startsFor("a"); got != 2 {
		t.Fatalf("StartSwap(a)=%d want 2 (r3 swaps in once capacity frees)", got)
	}
	s.OnSwapDone(SwapDone{ModelID: "a"})
	if got := eff.served("a"); got != 3 {
		t.Fatalf("served(a)=%d want 3 (r3 eventually served)", got)
	}
	if got := len(s.queued); got != 0 {
		t.Fatalf("queue len=%d want 0", got)
	}
}

// TestFIFO_Queueing_QueueTimeoutNormalization verifies the queueTimeout
// three-state semantics: nil defaults to 60s, explicit 0 waits indefinitely,
// and a positive value is honored in seconds.
func TestFIFO_Queueing_QueueTimeoutNormalization(t *testing.T) {
	// nil -> default 60s: enqueuing an over-limit request stamps a deadline
	// ~60s in the future rather than none.
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	models := map[string]config.ModelConfig{"a": {ConcurrencyLimit: 1}}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{}, models, eff)
	if got := s.queueTimeout; got != 60*time.Second {
		t.Fatalf("queueTimeout=%v want default 60s", got)
	}

	r1 := req("a")
	s.OnRequest(r1)
	assertAdmitted(t, r1)
	r2 := req("a")
	s.OnRequest(r2)
	assertAdmitted(t, r2)
	if got := len(s.queued); got != 1 {
		t.Fatalf("queue len=%d want 1", got)
	}
	if d := s.queued[0].Deadline; d.IsZero() {
		t.Fatal("queued deadline is zero, want default 60s from now")
	} else if wait := time.Until(d); wait < 59*time.Second || wait > 61*time.Second {
		t.Fatalf("deadline in %v, want ~60s", wait)
	}

	// explicit 0 -> infinite (no deadline anywhere).
	s = NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueTimeout: queueTimeoutInfinite()}, nil, newFakeEffects())
	if got := s.queueTimeout; got != 0 {
		t.Fatalf("queueTimeout=%v want 0 (infinite)", got)
	}

	// positive value honored in seconds.
	five := 5
	s = NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueTimeout: &five}, nil, newFakeEffects())
	if got := s.queueTimeout; got != 5*time.Second {
		t.Fatalf("queueTimeout=%v want 5s", got)
	}
}
