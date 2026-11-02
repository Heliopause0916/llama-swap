package scheduler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
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

// grantRec is one GrantError / GrantServe call. err!=nil marks an error grant;
// otherwise it is a serve grant and serve reports whether the caller received it.
type grantRec struct {
	model string
	err   error
	serve bool
}

type startRec struct {
	model      string
	generation uint64
	evict      []string
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
	pinned       map[string]bool                 // model -> pinned (default false)
	maintenance  map[string]bool                 // model -> under maintenance (default false)
	lastServeReq HandlerReq

	starts    []startRec
	grants    []grantRec
	stops     []stopRec
	cancelled map[uint64]bool
}

func newFakeEffects() *fakeEffects {
	return &fakeEffects{
		states:      map[string]process.ProcessState{},
		serveResult: map[string]bool{},
		pinned:      map[string]bool{},
		maintenance: map[string]bool{},
		cancelled:   map[uint64]bool{},
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

func (f *fakeEffects) StartSwap(modelID string, generation uint64, evict []string) func() {
	f.starts = append(f.starts, startRec{model: modelID, generation: generation, evict: evict})
	return func() { f.cancelled[generation] = true }
}

// swapDone returns a completion for the most recently started swap of modelID.
func (f *fakeEffects) swapDone(modelID string, err error) SwapDone {
	for i := len(f.starts) - 1; i >= 0; i-- {
		if start := f.starts[i]; start.model == modelID {
			return SwapDone{ModelID: modelID, Generation: start.generation, Err: err}
		}
	}
	panic("no swap started for " + modelID)
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

func (f *fakeEffects) IsPinned(modelID string) bool { return f.pinned[modelID] }

func (f *fakeEffects) IsMaintenance(modelID string) bool { return f.maintenance[modelID] }

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
	return NewFIFO("test", logmon.NewWriter(io.Discard), planner, config.FifoConfig{}, nil, eff, groupFuzzy{})
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

// reqP creates a HandlerReq carrying a resolved priority band.
func reqP(model string, priority int) HandlerReq {
	r := req(model)
	r.Priority = priority
	return r
}

// reqInflight creates a HandlerReq whose context carries the server's inflight
// ID, so QueueSnapshot can tie it to an inflight row.
func reqInflight(model, id string) HandlerReq {
	r := req(model)
	r.Ctx = swaputil.WithInflightID(context.Background(), id)
	return r
}

// reqMeta creates a HandlerReq whose context carries a request metadata map, so
// tests can assert on the metadata keys the scheduler writes at grant time.
func reqMeta(model string) HandlerReq {
	r := req(model)
	r.Ctx = swaputil.SetContext(context.Background(), swaputil.ReqContextData{ModelID: model, Metadata: make(map[string]string)})
	return r
}

// fakeClock is a controllable clock used to drive aging/fuzzy idle-window tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newFIFOWithClock builds a FIFO from the given config/models and injects the
// supplied clock. Passing a nil clock leaves time.Now in place.
func newFIFOWithClock(planner Swapper, eff Effects, cfg config.FifoConfig, models map[string]config.ModelConfig, clock *fakeClock) *FIFO {
	s := NewFIFO("test", logmon.NewWriter(io.Discard), planner, cfg, models, eff, groupFuzzy{})
	if clock != nil {
		s.now = clock.now
	}
	return s
}

// newFIFOAging builds a FIFO (no fuzzy groups) with an injectable clock, for
// L1.5 aging tests.
func newFIFOAging(planner Swapper, eff Effects, cfg config.FifoConfig, clock *fakeClock) *FIFO {
	return newFIFOWithClock(planner, eff, cfg, nil, clock)
}

// newFIFOFuzzy builds a FIFO with a fuzzy group snapshot and an injectable
// clock, for theme-A fuzzy tests.
func newFIFOFuzzy(planner Swapper, eff Effects, gf groupFuzzy, clock *fakeClock) *FIFO {
	s := NewFIFO("test", logmon.NewWriter(io.Discard), planner, config.FifoConfig{}, nil, eff, gf)
	if clock != nil {
		s.now = clock.now
	}
	return s
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
	s := newFIFO(&stubPlanner{}, eff)

	ctx := swaputil.SetContext(context.Background(), swaputil.ReqContextData{ModelID: "a", Metadata: make(map[string]string)})
	s.OnRequest(HandlerReq{Model: "a", Ctx: ctx, Priority: 7})

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
	s.OnSwapDone(eff.swapDone("a", nil))

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
	s.OnSwapDone(eff.swapDone("a", nil))

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

	s.OnSwapDone(eff.swapDone("a", errors.New("boom")))

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
	s.OnSwapDone(eff.swapDone("a", nil))
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
	s.OnSwapDone(eff.swapDone("a", nil))
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
	s.OnSwapDone(eff.swapDone("a", nil))

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
		s.OnSwapDone(eff.swapDone(model, nil))
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
	s.OnSwapDone(eff.swapDone("a", nil)) // grant fails, inFlight[a] stays 0

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

	if !eff.cancelled[eff.starts[0].generation] {
		t.Error("unload did not cancel the active swap")
	}
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

// A completion from an unloaded swap must not finish a later swap for the same
// model. Drive the event ordering directly so the regression needs no sleeps.
func TestFIFO_OnUnload_IgnoresStaleSwapCompletion(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "error", err: errors.New("old swap failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eff := newFakeEffects()
			eff.states["a"] = process.StateStopped
			s := newFIFO(&stubPlanner{}, eff)

			s.OnRequest(reqCh("a"))
			oldCompletion := eff.swapDone("a", tc.err)
			s.OnUnload([]string{"a"}, time.Second)
			if got := eff.errored("a"); got != 1 {
				t.Fatalf("errored(a)=%d want 1 (unloaded request released)", got)
			}

			newRequest := reqCh("a")
			s.OnRequest(newRequest)
			if got := eff.startsFor("a"); got != 2 {
				t.Fatalf("StartSwap(a)=%d want 2 (new request starts a new swap)", got)
			}
			newSwap := s.active["a"]
			grantsBefore := len(eff.grants)

			// The first swap reports its result while the second is still loading.
			s.OnSwapDone(oldCompletion)

			if s.active["a"] != newSwap {
				t.Error("stale completion removed the newer active swap")
			}
			if len(eff.grants) != grantsBefore {
				t.Error("stale completion responded to the newer request before its swap completed")
			}

			s.OnSwapDone(eff.swapDone("a", nil))
			if eff.served("a") != 1 || eff.lastServeReq.Respond != newRequest.Respond {
				t.Error("new swap completion did not serve the newer request")
			}
		})
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

// TestFIFO_Queue_EnqueueOrderByPriority verifies queued requests are ordered by
// descending priority, with arrival (FIFO) order preserved among equal-priority
// models.
func TestFIFO_Queue_EnqueueOrderByPriority(t *testing.T) {
	eff := newFakeEffects()
	for _, m := range []string{"z", "A", "B", "C", "D"} {
		eff.states[m] = process.StateStopped
	}
	// z's swap evicts every other model, so any request that arrives while z is
	// loading collides with z's in-flight swap and parks in the queue.
	planner := &stubPlanner{evict: map[string][]string{"z": {"A", "B", "C", "D"}}}
	s := newFIFO(planner, eff)

	s.OnRequest(req("z")) // StartSwap(z, [A,B,C,D])

	// Arrive out of priority order; B before C exercises FIFO tie-breaking.
	for _, r := range []HandlerReq{
		reqP("B", 5), reqP("D", 1), reqP("C", 5), reqP("A", 10),
	} {
		s.OnRequest(r)
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
	s.OnSwapDone(eff.swapDone("a", nil))

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
	s.OnSwapDone(eff.swapDone("a", nil))

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
// limit, already in StateReady so every request exercises the fast path. It
// disables queueing (queueDepth=0) so over-limit requests are rejected at
// admission, isolating the concurrency-limit semantics under test.
func newFIFOWithLimit(t *testing.T, model string, limit int) (*FIFO, *fakeEffects) {
	t.Helper()
	eff := newFakeEffects()
	eff.states[model] = process.StateReady
	models := map[string]config.ModelConfig{
		model: {ConcurrencyLimit: limit},
	}
	qd := 0
	cfg := config.FifoConfig{QueueDepth: &qd}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, cfg, models, eff, groupFuzzy{})
	return s, eff
}

// TestFIFO_ConcurrencyLimit_RejectsOverLimit verifies that a request arriving
// while the model is at capacity gets rejected during admission, before it can
// be queued or served, and that a new request succeeds once capacity returns.
func TestFIFO_ConcurrencyLimit_RejectsOverLimit(t *testing.T) {
	s, eff := newFIFOWithLimit(t, "a", 1)

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
// explicit ConcurrencyLimit gets the default cap of 10.
func TestFIFO_ConcurrencyLimit_DefaultIsTen(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	// nil models → every model gets defaultConcurrencyLimit (10).
	// queueDepth=0 keeps over-limit rejection at admission so the cap is observable.
	qd := 0
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueDepth: &qd}, nil, eff, groupFuzzy{})

	for i := 0; i < 10; i++ {
		r := req("a")
		s.OnRequest(r)
		assertAdmitted(t, r)
	}
	if got := eff.served("a"); got != 10 {
		t.Fatalf("served(a)=%d want 10 (default limit)", got)
	}

	// 11th request is rejected.
	r := req("a")
	s.OnRequest(r)
	assertAdmission429(t, r)
	if got := eff.errored("a"); got != 0 {
		t.Fatalf("errored(a)=%d want 0 (over default limit rejects before grant)", got)
	}
}

// TestFIFO_ConcurrencyLimit_CustomLimit verifies a ConcurrencyLimit greater
// than zero overrides the default.
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
	assertAdmission429(t, r3)

	if got := eff.served("a"); got != 2 {
		t.Fatalf("served(a)=%d want 2 (custom limit)", got)
	}
	if got := eff.errored("a"); got != 0 {
		t.Fatalf("errored(a)=%d want 0 (over custom limit rejects before grant)", got)
	}
}

// TestFIFO_ConcurrencyLimit_SwapWaiters verifies that when more swap waiters
// exist than the concurrency limit, excess waiters are rejected during
// admission rather than after the loading stream has started.
func TestFIFO_ConcurrencyLimit_SwapWaiters(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 2},
	}
	qd := 0
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueDepth: &qd}, models, eff, groupFuzzy{})

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
	s.OnSwapDone(eff.swapDone("a", nil))

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
	qd := 0
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{evict: map[string][]string{"a": {"b"}}}, config.FifoConfig{QueueDepth: &qd}, models, eff, groupFuzzy{})

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
	s.OnSwapDone(eff.swapDone("b", nil))
	s.OnServeDone(ServeDoneEvent{ModelID: "b"})
	if got := eff.startsFor("a"); got != 1 {
		t.Fatalf("StartSwap(a)=%d want 1 after b drains", got)
	}

	eff.states["a"] = process.StateReady
	s.OnSwapDone(eff.swapDone("a", nil))
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
	qd := 0
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{evict: map[string][]string{"a": {"b"}}}, config.FifoConfig{QueueDepth: &qd}, models, eff, groupFuzzy{})

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

// TestFIFO_Priority_HeaderResolved verifies the header→band→default chain via
// ResolvePriority, and that a granted request carries the resolved band in the
// fifo_priority metadata key.
func TestFIFO_Priority_HeaderResolved(t *testing.T) {
	cfg := config.FifoConfig{
		PriorityHeader:  "X-Request-Priority",
		RequestPriority: map[string]int{"high": 100, "low": 30},
		DefaultPriority: 60,
	}
	if p, on := cfg.ResolvePriority("high"); !on || p != 100 {
		t.Errorf("ResolvePriority(high)=%d,%v want 100,true", p, on)
	}
	if p, on := cfg.ResolvePriority("bogus"); !on || p != 60 {
		t.Errorf("ResolvePriority(bogus)=%d,%v want 60,true (unknown band -> default)", p, on)
	}
	if p, on := cfg.ResolvePriority(""); !on || p != 60 {
		t.Errorf("ResolvePriority('')=%d,%v want 60,true (absent header -> default)", p, on)
	}

	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	s := newFIFO(&stubPlanner{}, eff)
	ctx := swaputil.SetContext(context.Background(), swaputil.ReqContextData{ModelID: "a", Metadata: make(map[string]string)})
	p, _ := cfg.ResolvePriority("high")
	s.OnRequest(HandlerReq{Model: "a", Ctx: ctx, Priority: p})
	data, ok := swaputil.ReadContext(eff.lastServeReq.Ctx)
	if !ok {
		t.Fatal("context data missing from granted request")
	}
	if data.Metadata["fifo_priority"] != "100" {
		t.Errorf("fifo_priority=%q want 100", data.Metadata["fifo_priority"])
	}
}

// TestFIFO_Priority_FeatureOff verifies that with RequestPriority empty the
// feature is off: ResolvePriority reports off and a granted request carries
// fifo_priority=0.
func TestFIFO_Priority_FeatureOff(t *testing.T) {
	cfg := config.FifoConfig{}
	if p, on := cfg.ResolvePriority("high"); on || p != 0 {
		t.Errorf("ResolvePriority(high)=%d,%v want 0,false when feature off", p, on)
	}

	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	s := newFIFO(&stubPlanner{}, eff)
	ctx := swaputil.SetContext(context.Background(), swaputil.ReqContextData{ModelID: "a", Metadata: make(map[string]string)})
	s.OnRequest(HandlerReq{Model: "a", Ctx: ctx}) // Priority defaults to 0
	data, ok := swaputil.ReadContext(eff.lastServeReq.Ctx)
	if !ok {
		t.Fatal("context data missing from granted request")
	}
	if data.Metadata["fifo_priority"] != "0" {
		t.Errorf("fifo_priority=%q want 0", data.Metadata["fifo_priority"])
	}
}

// TestFIFO_Priority_EmptyHeaderDisables verifies that a non-empty RequestPriority
// with an empty PriorityHeader keeps the feature off (pure FIFO): every request
// would otherwise read the empty header and silently fall to defaultPriority,
// contradicting "empty header = pure FIFO".
func TestFIFO_Priority_EmptyHeaderDisables(t *testing.T) {
	cfg := config.FifoConfig{
		RequestPriority: map[string]int{"high": 100},
		DefaultPriority: 60,
	}
	if p, on := cfg.ResolvePriority("high"); on || p != 0 {
		t.Errorf("ResolvePriority(high)=%d,%v want 0,false with empty PriorityHeader", p, on)
	}
	if p, on := cfg.ResolvePriority(""); on || p != 0 {
		t.Errorf("ResolvePriority('')=%d,%v want 0,false with empty PriorityHeader", p, on)
	}
}

// TestFIFO_Queue_AdmitQueueDepth verifies the admission bound: with queueDepth=n
// a request is rejected only when reserved >= limit+n, and with queueDepth=0 it
// is rejected as soon as reserved >= limit.
func TestFIFO_Queue_AdmitQueueDepth(t *testing.T) {
	// queueDepth=3: limit 2 + 3 = 5 reserved allowed; the 6th is rejected.
	{
		eff := newFakeEffects()
		eff.states["a"] = process.StateReady
		qd := 3
		models := map[string]config.ModelConfig{"a": {ConcurrencyLimit: 2}}
		s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueDepth: &qd}, models, eff, groupFuzzy{})
		for i := 0; i < 5; i++ {
			r := req("a")
			s.OnRequest(r)
			assertAdmitted(t, r)
		}
		r := req("a")
		s.OnRequest(r)
		assertAdmission429(t, r)
		if got := eff.errored("a"); got != 0 {
			t.Fatalf("errored(a)=%d want 0 (over queue depth rejects before grant)", got)
		}
	}
	// queueDepth=0: limit 2, the 3rd is rejected at admission.
	{
		eff := newFakeEffects()
		eff.states["a"] = process.StateReady
		qd := 0
		models := map[string]config.ModelConfig{"a": {ConcurrencyLimit: 2}}
		s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueDepth: &qd}, models, eff, groupFuzzy{})
		s.OnRequest(req("a"))
		s.OnRequest(req("a"))
		r := req("a")
		s.OnRequest(r)
		assertAdmission429(t, r)
	}
}

// TestFIFO_Queue_CapacityGateInFlight verifies drainQueue does not grant a
// queued request whose model is already at its serving limit, and preserves the
// item's EnqueuedAt (D19 continuous aging).
func TestFIFO_Queue_CapacityGateInFlight(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	qd := 10
	models := map[string]config.ModelConfig{"a": {ConcurrencyLimit: 1}}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueDepth: &qd}, models, eff, groupFuzzy{})

	r1 := req("a")
	s.OnRequest(r1)
	assertAdmitted(t, r1)
	if eff.served("a") != 1 {
		t.Fatalf("served(a)=%d want 1", eff.served("a"))
	}

	r2 := req("a")
	s.OnRequest(r2)
	assertAdmitted(t, r2)
	if len(s.queued) != 1 {
		t.Fatalf("queue len=%d want 1 (capacity gate)", len(s.queued))
	}
	before := s.queued[0].EnqueuedAt

	// drain while a is still in flight: r2 must stay queued, not granted.
	s.drainQueue()
	if len(s.queued) != 1 {
		t.Fatalf("queue len=%d want 1 after drain while in-flight", len(s.queued))
	}
	if s.queued[0].EnqueuedAt != before {
		t.Errorf("EnqueuedAt changed: %v -> %v", before, s.queued[0].EnqueuedAt)
	}
	if eff.served("a") != 1 {
		t.Errorf("served(a)=%d want 1 (queued request not granted)", eff.served("a"))
	}
}

// TestFIFO_Queue_CapacityGateSlotRelease verifies that releasing a single
// serving slot (limit>1) drains the queue: a request parked by the capacity
// gate is granted as soon as inFlight drops below the limit, without waiting for
// the model to become fully idle. With limit=1 the old drain-on-idle trigger
// masked this, but with limit=2 a partial release would starve the queued item.
func TestFIFO_Queue_CapacityGateSlotRelease(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	qd := 10
	models := map[string]config.ModelConfig{"a": {ConcurrencyLimit: 2}}
	s := NewFIFO("test", logmon.NewWriter(io.Discard), &stubPlanner{}, config.FifoConfig{QueueDepth: &qd}, models, eff, groupFuzzy{})

	r1 := req("a")
	s.OnRequest(r1)
	assertAdmitted(t, r1)
	r2 := req("a")
	s.OnRequest(r2)
	assertAdmitted(t, r2)
	if eff.served("a") != 2 {
		t.Fatalf("served(a)=%d want 2 (two slots filled)", eff.served("a"))
	}

	// Third request is parked by the capacity gate (inFlight[a]=2 == limit).
	r3 := req("a")
	s.OnRequest(r3)
	assertAdmitted(t, r3)
	if len(s.queued) != 1 {
		t.Fatalf("queue len=%d want 1 (capacity gate at limit 2)", len(s.queued))
	}

	// Release one slot: inFlight drops 2->1 (< limit), so the queued request
	// must be granted immediately rather than waiting for the model to go idle.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if eff.served("a") != 3 {
		t.Fatalf("served(a)=%d want 3 after slot release drains queue", eff.served("a"))
	}
	if len(s.queued) != 0 {
		t.Fatalf("queue len=%d want 0 after slot release", len(s.queued))
	}
}

// TestFIFO_Queue_TimeoutPrune verifies an item past its queueTimeout deadline
// is dropped with a 429 on drain.
func TestFIFO_Queue_TimeoutPrune(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	qd := 10
	qt := 5
	models := map[string]config.ModelConfig{"a": {ConcurrencyLimit: 1}}
	cfg := config.FifoConfig{QueueDepth: &qd, QueueTimeout: &qt}
	s := newFIFOWithClock(&stubPlanner{}, eff, cfg, models, clock)

	s.OnRequest(req("a")) // served -> inFlight[a]=1
	r2 := req("a")
	s.OnRequest(r2) // queued via capacity gate at t0
	assertAdmitted(t, r2)
	if len(s.queued) != 1 {
		t.Fatalf("queue len=%d want 1", len(s.queued))
	}
	deadline := s.queued[0].Deadline

	clock.t = deadline.Add(time.Second)
	s.drainQueue()

	if eff.errored("a") != 1 {
		t.Fatalf("errored(a)=%d want 1 (expired item dropped with 429)", eff.errored("a"))
	}
	if len(s.queued) != 0 {
		t.Fatalf("queue len=%d want 0 after prune", len(s.queued))
	}
}

// TestFIFO_Aging_PromotesAfterThreshold verifies an item older than promoteAfter
// is pinned to the internal top tier at drain and jumps ahead of a younger
// higher-priority item.
func TestFIFO_Aging_PromotesAfterThreshold(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateReady
	pa := 10
	cfg := config.FifoConfig{PromoteAfter: &pa}
	s := newFIFOAging(&stubPlanner{}, eff, cfg, clock)

	a := reqP("a", 1)
	b := reqP("b", 100)
	s.enqueue(a) // at t0, age will be 20s
	clock.advance(15 * time.Second)
	s.enqueue(b)                   // at t0+15s, age 5s
	clock.advance(5 * time.Second) // t0+20s

	if s.queued[0].Req.Model != "b" {
		t.Fatalf("queue[0]=%s want b before drain", s.queued[0].Req.Model)
	}

	s.drainQueue()

	if len(eff.grants) < 2 || eff.grants[0].model != "a" || !eff.grants[0].serve {
		t.Fatalf("first grant=%+v want serve a (promoted)", eff.grants[0])
	}
	if eff.grants[1].model != "b" || !eff.grants[1].serve {
		t.Fatalf("second grant=%+v want serve b", eff.grants[1])
	}
}

// TestFIFO_Aging_FeatureOffNil verifies that with promoteAfter unset, drainQueue
// performs no re-rank (baseline L1 behavior).
func TestFIFO_Aging_FeatureOffNil(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateReady
	s := newFIFOAging(&stubPlanner{}, eff, config.FifoConfig{}, clock)

	s.enqueue(reqP("a", 1)) // t0
	clock.advance(15 * time.Second)
	s.enqueue(reqP("b", 100))      // t0+15
	clock.advance(5 * time.Second) // t0+20

	if s.queued[0].Req.Model != "b" {
		t.Fatalf("queue[0]=%s want b", s.queued[0].Req.Model)
	}
	s.drainQueue()
	if len(eff.grants) < 2 || eff.grants[0].model != "b" {
		t.Fatalf("first grant=%+v want serve b (no aging)", eff.grants[0])
	}
}

// TestFIFO_Aging_FeatureOffZero verifies that promoteAfter=0 behaves like the
// feature being off (no re-rank).
func TestFIFO_Aging_FeatureOffZero(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateReady
	pa := 0
	cfg := config.FifoConfig{PromoteAfter: &pa}
	s := newFIFOAging(&stubPlanner{}, eff, cfg, clock)

	s.enqueue(reqP("a", 1)) // t0
	clock.advance(15 * time.Second)
	s.enqueue(reqP("b", 100))      // t0+15
	clock.advance(5 * time.Second) // t0+20

	if s.queued[0].Req.Model != "b" {
		t.Fatalf("queue[0]=%s want b", s.queued[0].Req.Model)
	}
	s.drainQueue()
	if len(eff.grants) < 2 || eff.grants[0].model != "b" {
		t.Fatalf("first grant=%+v want serve b (promoteAfter=0)", eff.grants[0])
	}
}

// TestFIFO_Aging_ClockBoundary verifies the promotion threshold is inclusive:
// age exactly equal to promoteAfter promotes, slightly less does not.
func TestFIFO_Aging_ClockBoundary(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	pa := 10
	cfg := config.FifoConfig{PromoteAfter: &pa}
	s := newFIFOAging(&stubPlanner{}, newFakeEffects(), cfg, clock)

	item := queuedItem{Req: reqP("a", 1), EnqueuedAt: time.Unix(0, 0)}

	clock.t = time.Unix(0, 0).Add(10 * time.Second)
	if got := s.effective(item, clock.t); got != promotedPriority {
		t.Errorf("effective at age==threshold=%d want promotedPriority", got)
	}
	clock.t = time.Unix(0, 0).Add(9 * time.Second)
	if got := s.effective(item, clock.t); got != 1 {
		t.Errorf("effective at age<threshold=%d want 1", got)
	}
}

// TestFIFO_Fuzzy_DisabledByDefault verifies that with no fuzzy groups configured
// (empty groupFuzzy) requests are never rewritten.
func TestFIFO_Fuzzy_DisabledByDefault(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	s := newFIFOFuzzy(&stubPlanner{}, eff, groupFuzzy{}, nil)

	r := reqMeta("a")
	s.OnRequest(r)
	if eff.lastServeReq.Model != "a" {
		t.Errorf("served model=%q want a (no fuzzy)", eff.lastServeReq.Model)
	}
	if eff.lastServeReq.RequestedModel != "" {
		t.Errorf("RequestedModel=%q want empty", eff.lastServeReq.RequestedModel)
	}
}

// TestFIFO_Fuzzy_BusyServes verifies that a request for a member is rewritten to
// the online model while the group is busy, and that served_model metadata is
// written on grant.
func TestFIFO_Fuzzy_BusyServes(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, nil)

	s.OnRequest(reqMeta("a")) // a serving -> group busy
	r := reqMeta("b")
	s.OnRequest(r)

	if eff.lastServeReq.Model != "a" {
		t.Errorf("served model=%q want a (fuzzed)", eff.lastServeReq.Model)
	}
	if eff.lastServeReq.RequestedModel != "b" {
		t.Errorf("RequestedModel=%q want b", eff.lastServeReq.RequestedModel)
	}
	data, _ := swaputil.ReadContext(eff.lastServeReq.Ctx)
	if data.Metadata["served_model"] != "a" {
		t.Errorf("served_model=%q want a", data.Metadata["served_model"])
	}
}

// TestFIFO_Fuzzy_ActiveSwapTargetWins verifies fuzzyOnline prefers the in-flight
// swap target of the group over running members.
func TestFIFO_Fuzzy_ActiveSwapTargetWins(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, nil)

	s.OnRequest(req("b")) // startSwap(b) -> active swap target b
	r := reqMeta("a")
	s.OnRequest(r) // a fuzzed to b and joins b's swap

	if eff.startsFor("b") != 1 {
		t.Fatalf("StartSwap(b)=%d want 1", eff.startsFor("b"))
	}
	sw := s.active["b"]
	if sw == nil || len(sw.waiters) != 2 {
		t.Fatalf("active[b] waiters=%d want 2 (b's own request + fuzzed a)", len(sw.waiters))
	}
	joined := false
	for _, w := range sw.waiters {
		if w.RequestedModel == "a" {
			joined = true
		}
	}
	if !joined {
		t.Error("a did not join b's swap as a fuzzed waiter")
	}
}

// TestFIFO_Fuzzy_IdleWithinWindow verifies that an idle group that has served
// before fuzzes a request for a different member within the idle window.
func TestFIFO_Fuzzy_IdleWithinWindow(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, clock)

	s.OnRequest(reqMeta("a"))
	s.OnServeDone(ServeDoneEvent{ModelID: "a"}) // idle, lastServeDone[g]=t0
	clock.advance(100 * time.Second)            // within 300s window

	r := reqMeta("b")
	s.OnRequest(r)
	if eff.lastServeReq.Model != "a" {
		t.Errorf("served model=%q want a (idle within window)", eff.lastServeReq.Model)
	}
}

// TestFIFO_Fuzzy_NeverServed verifies an idle group that has never served does
// not fuzz (real switch).
func TestFIFO_Fuzzy_NeverServed(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, clock)

	r := reqMeta("b")
	s.OnRequest(r)
	if eff.startsFor("b") != 1 {
		t.Fatalf("StartSwap(b)=%d want 1 (no fuzzy, real switch)", eff.startsFor("b"))
	}
	if s.active["b"] == nil {
		t.Fatal("no active swap for b")
	}
}

// TestFIFO_Fuzzy_ZeroTimeoutOnlyWhenBusy verifies that an idleTimeout of 0
// disables fuzzy when the group is idle but keeps it while busy.
func TestFIFO_Fuzzy_ZeroTimeoutOnlyWhenBusy(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g"}, idleTimeout: map[string]time.Duration{"g": 0}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, clock)

	// Busy: a serving -> b fuzzed to a.
	s.OnRequest(reqMeta("a"))
	r := reqMeta("b")
	s.OnRequest(r)
	if eff.lastServeReq.Model != "a" {
		t.Errorf("busy: served model=%q want a", eff.lastServeReq.Model)
	}

	// Idle: a finishes (both a's own and the fuzzed-b serves drain); timeout 0
	// -> b not fuzzed (real switch).
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	r2 := reqMeta("b")
	s.OnRequest(r2)
	if eff.startsFor("b") != 1 {
		t.Fatalf("idle: StartSwap(b)=%d want 1 (timeout 0 => no fuzzy when idle)", eff.startsFor("b"))
	}
}

// TestFIFO_Fuzzy_ServeDoneRefreshesWindow verifies OnServeDone refreshes the
// group's lastServeDone timestamp.
func TestFIFO_Fuzzy_ServeDoneRefreshesWindow(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, clock)

	s.OnRequest(reqMeta("a"))
	s.OnServeDone(ServeDoneEvent{ModelID: "a"}) // lastServeDone[g]=t0
	clock.advance(100 * time.Second)
	s.OnRequest(reqMeta("a"))
	s.OnServeDone(ServeDoneEvent{ModelID: "a"}) // lastServeDone[g]=t0+100

	if s.lastServeDone["g"] != clock.t {
		t.Errorf("lastServeDone[g]=%v want %v (refreshed)", s.lastServeDone["g"], clock.t)
	}
}

// TestFIFO_Fuzzy_CancelDoesNotRefresh verifies cancelling a queued request does
// not update the group's lastServeDone window.
func TestFIFO_Fuzzy_CancelDoesNotRefresh(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["z"] = process.StateStopped
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	planner := &stubPlanner{evict: map[string][]string{"z": {"a"}}}
	s := newFIFOFuzzy(planner, eff, gf, clock)

	s.OnRequest(reqMeta("a"))
	s.OnServeDone(ServeDoneEvent{ModelID: "a"}) // idle within window
	s.OnRequest(req("z"))                       // startSwap(z), evicts a

	br := reqCh("b") // fuzzed to a, collides with z's swap -> queued (admitted)
	s.OnRequest(br)
	if len(s.queued) != 1 {
		t.Fatalf("queue len=%d want 1", len(s.queued))
	}
	got := s.lastServeDone["g"]

	s.OnCancel(br)

	if s.lastServeDone["g"] != got {
		t.Errorf("lastServeDone[g]=%v want %v (cancel must not refresh)", s.lastServeDone["g"], got)
	}
}

// TestFIFO_Fuzzy_BookkeepingKeysUseRewrittenModel verifies the scheduler keys
// (reserved/inFlight) use the rewritten model, not the original requested one.
func TestFIFO_Fuzzy_BookkeepingKeysUseRewrittenModel(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, nil)

	s.OnRequest(reqMeta("a")) // a serving
	r := reqMeta("b")
	s.OnRequest(r) // b fuzzed to a

	if s.inFlight["a"] != 2 {
		t.Errorf("inFlight[a]=%d want 2", s.inFlight["a"])
	}
	if _, ok := s.inFlight["b"]; ok {
		t.Error("inFlight has key b (should be a)")
	}
	if s.reserved["a"] != 2 || s.reserved["b"] != 0 {
		t.Errorf("reserved a=%d b=%d want 2/0", s.reserved["a"], s.reserved["b"])
	}
}

// TestFIFO_Fuzzy_MultipleRunningPrefersReady verifies fuzzyOnline prefers a
// Ready group member over a running-but-not-ready one.
func TestFIFO_Fuzzy_MultipleRunningPrefersReady(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStarting // running but not ready
	eff.states["b"] = process.StateReady
	eff.states["c"] = process.StateStopped
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g", "c": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, nil)

	s.OnRequest(reqMeta("b")) // b serving -> group busy
	r := reqMeta("c")
	s.OnRequest(r)

	if eff.lastServeReq.Model != "b" {
		t.Errorf("served model=%q want b (Ready preferred over starting a)", eff.lastServeReq.Model)
	}
}

// TestFIFO_Fuzzy_NonMemberUnaffected verifies a model outside every fuzzy group
// is never rewritten.
func TestFIFO_Fuzzy_NonMemberUnaffected(t *testing.T) {
	eff := newFakeEffects()
	eff.states["x"] = process.StateReady
	gf := groupFuzzy{groupOf: map[string]string{"a": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, nil)

	r := reqMeta("x")
	s.OnRequest(r)
	if eff.lastServeReq.Model != "x" {
		t.Errorf("served model=%q want x (not a member)", eff.lastServeReq.Model)
	}
	if eff.lastServeReq.RequestedModel != "" {
		t.Errorf("RequestedModel=%q want empty", eff.lastServeReq.RequestedModel)
	}
}

// TestFIFO_Fuzzy_ExplicitBypasses verifies an explicit /upstream/<model>/
// request (HandlerReq.Explicit) is never fuzzy-substituted onto another group
// member, even when the group is busy and fuzzy would otherwise apply.
func TestFIFO_Fuzzy_ExplicitBypasses(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, nil)

	s.OnRequest(reqMeta("a")) // group busy (a serving)

	// Non-explicit b is fuzzed onto the online member a.
	if got := s.fuzzySubstitute(reqMeta("b")); got.Model != "a" {
		t.Errorf("non-explicit fuzzySubstitute(b) model=%q want a", got.Model)
	}

	// Explicit b is never rewritten.
	explicit := reqMeta("b")
	explicit.Explicit = true
	got := s.fuzzySubstitute(explicit)
	if got.Model != "b" {
		t.Errorf("explicit fuzzySubstitute(b) model=%q want b (no rewrite)", got.Model)
	}
	if got.RequestedModel != "" {
		t.Errorf("explicit fuzzySubstitute(b) RequestedModel=%q want empty", got.RequestedModel)
	}
}

// TestFIFO_Fuzzy_GrantErrorNoMetadata verifies the grantError path (queued
// request released on shutdown) writes no served_model/fifo_priority metadata.
func TestFIFO_Fuzzy_GrantErrorNoMetadata(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["z"] = process.StateStopped
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "b": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	planner := &stubPlanner{evict: map[string][]string{"z": {"a"}}}
	s := newFIFOFuzzy(planner, eff, gf, clock)

	s.OnRequest(reqMeta("a"))
	s.OnServeDone(ServeDoneEvent{ModelID: "a"}) // within window, idle
	s.OnRequest(req("z"))                       // startSwap(z), evicts a

	r := reqMeta("b") // fuzzed to a, collides with z's swap -> queued
	s.OnRequest(r)
	if len(s.queued) != 1 {
		t.Fatalf("queue len=%d want 1", len(s.queued))
	}

	s.OnShutdown(errors.New("shutting down"))

	data, ok := swaputil.ReadContext(r.Ctx)
	if !ok {
		t.Fatal("no context data")
	}
	if _, has := data.Metadata["served_model"]; has {
		t.Error("served_model should not be written on grantError")
	}
	if _, has := data.Metadata["fifo_priority"]; has {
		t.Error("fifo_priority should not be written on grantError")
	}
}

// TestFIFO_QueueSnapshot_EmptyWhenNoQueue verifies an empty queue yields nil.
func TestFIFO_QueueSnapshot_EmptyWhenNoQueue(t *testing.T) {
	s := newFIFO(&stubPlanner{}, newFakeEffects())
	if got := s.QueueSnapshot(); got != nil {
		t.Fatalf("QueueSnapshot() = %v, want nil", got)
	}
}

// TestFIFO_QueueSnapshot_ReturnsPositions verifies the snapshot lists each
// queued request's inflight ID and 1-indexed position in enqueue order.
func TestFIFO_QueueSnapshot_ReturnsPositions(t *testing.T) {
	s := newFIFO(&stubPlanner{}, newFakeEffects())
	s.enqueue(reqInflight("a", "id1"))
	s.enqueue(reqInflight("b", "id2"))

	got := s.QueueSnapshot()
	want := []QueueEntry{{InflightID: "id1", Position: 1}, {InflightID: "id2", Position: 2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("QueueSnapshot() = %v, want %v", got, want)
	}
}

// TestFIFO_QueueSnapshot_SkipsNoInflightID verifies queued requests that never
// passed through the inflight middleware are omitted, while Position still
// reflects their physical index (i+1) in the queue.
func TestFIFO_QueueSnapshot_SkipsNoInflightID(t *testing.T) {
	s := newFIFO(&stubPlanner{}, newFakeEffects())
	s.enqueue(req("a"))               // no InflightID -> skipped
	s.enqueue(reqInflight("b", "id")) // physical position 2

	got := s.QueueSnapshot()
	want := []QueueEntry{{InflightID: "id", Position: 2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("QueueSnapshot() = %v, want %v", got, want)
	}
}

// TestFIFO_QueueSnapshot_ReflectsDrainOrder verifies the positions reported
// after drainQueue reflect the aging re-ranked effective service order.
func TestFIFO_QueueSnapshot_ReflectsDrainOrder(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	qd := 10
	pa := 10
	models := map[string]config.ModelConfig{"a": {ConcurrencyLimit: 1}}
	cfg := config.FifoConfig{QueueDepth: &qd, PromoteAfter: &pa}
	s := newFIFOWithClock(&stubPlanner{}, eff, cfg, models, clock)

	// Occupy the single serving slot so the queue never drains.
	s.OnRequest(req("a"))
	if eff.served("a") != 1 {
		t.Fatalf("served(a)=%d want 1", eff.served("a"))
	}

	// Older lower-priority and younger higher-priority requests, both queued.
	s.enqueue(reqInflight("a", "old"))
	clock.advance(15 * time.Second)
	s.enqueue(reqInflight("a", "young"))
	clock.advance(5 * time.Second) // "old" is now 20s old (>= promoteAfter)

	// drainQueue re-ranks by effective priority: "old" is promoted first.
	s.drainQueue()

	got := s.QueueSnapshot()
	want := []QueueEntry{{InflightID: "old", Position: 1}, {InflightID: "young", Position: 2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("QueueSnapshot() = %v, want %v", got, want)
	}
}

// grantErrorFor returns the error recorded by GrantError for model, or nil when
// no error grant addressed that model.
func grantErrorFor(eff *fakeEffects, model string) error {
	for _, g := range eff.grants {
		if g.model == model && g.err != nil {
			return g.err
		}
	}
	return nil
}

// TestFIFO_MaintenanceNoGroupRejects verifies a request for a maintenance model
// with no fuzzy group is rejected at admission.
func TestFIFO_MaintenanceNoGroupRejects(t *testing.T) {
	eff := newFakeEffects()
	eff.states["z"] = process.StateStopped
	eff.maintenance["z"] = true
	s := newFIFO(&stubPlanner{}, eff)

	r := req("z")
	s.OnRequest(r)
	if err := admitErr(t, r); !errors.Is(err, swaputil.ErrMaintenance) {
		t.Fatalf("admit err=%v want ErrMaintenance", err)
	}
	if eff.startsFor("z") != 0 {
		t.Errorf("startSwap(z) called %d times, want 0", eff.startsFor("z"))
	}
}

// TestFIFO_MaintenanceSubstituteToOnline verifies a request for a maintenance
// model in a fuzzy group with an online member is served by that member.
func TestFIFO_MaintenanceSubstituteToOnline(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["z"] = process.StateStopped
	eff.maintenance["z"] = true
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "z": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, nil)

	r := reqMeta("z")
	s.OnRequest(r)
	if err := admitErr(t, r); err != nil {
		t.Fatalf("admit err=%v want nil", err)
	}
	if eff.lastServeReq.Model != "a" {
		t.Errorf("served model=%q want a (maintenance z served by online a)", eff.lastServeReq.Model)
	}
}

// TestFIFO_MaintenanceSubstituteNoOnlineRejects verifies a request for a
// maintenance model in a fuzzy group with no online member is rejected.
func TestFIFO_MaintenanceSubstituteNoOnlineRejects(t *testing.T) {
	eff := newFakeEffects()
	eff.states["z"] = process.StateStopped
	eff.maintenance["z"] = true
	gf := groupFuzzy{groupOf: map[string]string{"z": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, nil)

	r := req("z")
	s.OnRequest(r)
	if err := admitErr(t, r); !errors.Is(err, swaputil.ErrMaintenance) {
		t.Fatalf("admit err=%v want ErrMaintenance", err)
	}
}

// TestFIFO_PinProtectionReject verifies a request that would evict a pinned
// model outside its fuzzy group is rejected at admission.
func TestFIFO_PinProtectionReject(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["z"] = process.StateStopped
	eff.pinned["a"] = true
	planner := &stubPlanner{evict: map[string][]string{"z": {"a"}}}
	s := newFIFO(planner, eff)

	r := req("z")
	s.OnRequest(r)
	if err := admitErr(t, r); !errors.Is(err, swaputil.ErrPinnedProtected) {
		t.Fatalf("admit err=%v want ErrPinnedProtected", err)
	}
	if eff.startsFor("z") != 0 {
		t.Errorf("startSwap(z) called %d times, want 0", eff.startsFor("z"))
	}
}

// TestFIFO_PinProtectionRewriteSameGroup verifies a request that would evict a
// pinned model in the same fuzzy group is rewritten to serve that pinned model.
func TestFIFO_PinProtectionRewriteSameGroup(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["z"] = process.StateStopped
	eff.pinned["a"] = true
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "z": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	planner := &stubPlanner{evict: map[string][]string{"z": {"a"}}}
	s := newFIFOFuzzy(planner, eff, gf, nil)

	r := reqMeta("z")
	s.OnRequest(r)
	if err := admitErr(t, r); err != nil {
		t.Fatalf("admit err=%v want nil", err)
	}
	if eff.lastServeReq.Model != "a" {
		t.Errorf("served model=%q want a (pinned a protected)", eff.lastServeReq.Model)
	}
}

// TestFIFO_DrainQueue_MaintenanceRejects verifies drainQueue rejects a queued
// request for a maintenance model with ErrMaintenance.
func TestFIFO_DrainQueue_MaintenanceRejects(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.maintenance["a"] = true
	s := newFIFO(&stubPlanner{}, eff)

	r := reqCh("a")
	s.admit(r)
	s.enqueue(r)
	s.drainQueue()
	if err := grantErrorFor(eff, "a"); !errors.Is(err, swaputil.ErrMaintenance) {
		t.Fatalf("queued request err=%v want ErrMaintenance", err)
	}
}

// TestFIFO_DrainQueue_PinnedEvictRejects verifies drainQueue rejects (never
// rewrites) a queued request whose serving would evict a pinned model.
func TestFIFO_DrainQueue_PinnedEvictRejects(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["z"] = process.StateStopped
	eff.pinned["a"] = true
	planner := &stubPlanner{evict: map[string][]string{"z": {"a"}}}
	s := newFIFO(planner, eff)

	r := reqCh("z")
	s.admit(r)
	s.enqueue(r)
	s.drainQueue()
	if err := grantErrorFor(eff, "z"); !errors.Is(err, swaputil.ErrPinnedProtected) {
		t.Fatalf("queued request err=%v want ErrPinnedProtected", err)
	}
}

// TestFIFO_Maintenance_ExplicitRejects verifies an explicit /upstream/<model>/
// request for a maintenance model is rejected with ErrMaintenance immediately
// rather than starting a swap that is guaranteed to fail, and is never
// substituted onto another member of the fuzzy group.
func TestFIFO_Maintenance_ExplicitRejects(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["z"] = process.StateStopped
	eff.maintenance["z"] = true
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "z": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	s := newFIFOFuzzy(&stubPlanner{}, eff, gf, nil)

	r := reqMeta("z")
	r.Explicit = true
	s.OnRequest(r)
	if err := admitErr(t, r); !errors.Is(err, swaputil.ErrMaintenance) {
		t.Fatalf("admit err=%v want ErrMaintenance", err)
	}
	if eff.served("a") != 0 {
		t.Errorf("served(a)=%d want 0 (explicit z must not route to group member a)", eff.served("a"))
	}
	if eff.startsFor("z") != 0 {
		t.Errorf("startSwap(z) called %d times, want 0", eff.startsFor("z"))
	}
}

// TestFIFO_PinProtection_ExplicitRejects verifies an explicit /upstream/<model>/
// request whose serving would evict a pinned model is rejected with
// ErrPinnedProtected, and is never rewritten onto the pinned model even when
// both models belong to the same fuzzy group.
func TestFIFO_PinProtection_ExplicitRejects(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["z"] = process.StateStopped
	eff.pinned["a"] = true
	gf := groupFuzzy{groupOf: map[string]string{"a": "g", "z": "g"}, idleTimeout: map[string]time.Duration{"g": 300 * time.Second}}
	planner := &stubPlanner{evict: map[string][]string{"z": {"a"}}}
	s := newFIFOFuzzy(planner, eff, gf, nil)

	r := reqMeta("z")
	r.Explicit = true
	s.OnRequest(r)
	if err := admitErr(t, r); !errors.Is(err, swaputil.ErrPinnedProtected) {
		t.Fatalf("admit err=%v want ErrPinnedProtected", err)
	}
	if eff.served("a") != 0 {
		t.Errorf("served(a)=%d want 0 (explicit z must not be rewritten to pinned a)", eff.served("a"))
	}
	if eff.startsFor("z") != 0 {
		t.Errorf("startSwap(z) called %d times, want 0", eff.startsFor("z"))
	}
}
