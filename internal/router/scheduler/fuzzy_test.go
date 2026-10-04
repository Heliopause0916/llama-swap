package scheduler

// Tests for fuzzy substitution in swap groups (OnRequest step 0). All tests
// drive the FIFO directly and synchronously like fifo_test.go, with a fake
// clock so the idle-window decisions are deterministic.

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// newFIFOFuzzy builds a FIFO with the fake clock wired in (same pattern as
// newFIFOAging) plus the given fuzzy group snapshot.
func newFIFOFuzzy(planner Swapper, eff Effects, models map[string]config.ModelConfig, fuzzy groupFuzzy) (*FIFO, *fakeClock) {
	s := NewFIFO("test", logmon.NewWriter(io.Discard), planner, config.FifoConfig{QueueTimeout: queueTimeoutInfinite()}, models, eff, fuzzy)
	clock := newFakeClock()
	s.now = clock.now
	return s, clock
}

// fuzzyFor builds a groupFuzzy snapshot for one group: every named model is a
// member, and the group gets the given idle window in seconds.
func fuzzyFor(members []string, gid string, idleSecs int) groupFuzzy {
	f := groupFuzzy{groupOf: map[string]string{}, idleTimeout: map[string]time.Duration{}}
	for _, m := range members {
		f.groupOf[m] = gid
	}
	f.idleTimeout[gid] = time.Duration(idleSecs) * time.Second
	return f
}

// reqMeta creates a request whose context carries a ReqContextData with an
// empty shared metadata map, matching what the server's middleware seeds, so
// grantHandler's SetReqData writes are observable in tests.
func reqMeta(model string) HandlerReq {
	r := req(model)
	r.Ctx = swaputil.SetContext(context.Background(), swaputil.ReqContextData{
		Model:    model,
		ModelID:  model,
		Metadata: map[string]string{},
	})
	return r
}

// reqMetaCh is reqMeta plus a Respond channel so OnCancel can identify it.
func reqMetaCh(model string) HandlerReq {
	r := reqMeta(model)
	r.Respond = make(chan HandlerResp, 1)
	return r
}

// metaValue reads a metadata key written through the request's shared context.
func metaValue(t *testing.T, r HandlerReq, key string) (string, bool) {
	t.Helper()
	data, ok := swaputil.ReadContext(r.Ctx)
	if !ok {
		return "", false
	}
	v, found := data.Metadata[key]
	return v, found
}

func TestFIFO_Fuzzy_DisabledByDefault(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	s, _ := newFIFOFuzzy(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff, nil, groupFuzzy{})

	r := req("b")
	s.OnRequest(r)

	if eff.startsFor("b") != 1 {
		t.Fatalf("starts for b=%d want 1 (real switch, no fuzzy groups configured)", eff.startsFor("b"))
	}
	if len(eff.grants) != 0 {
		t.Fatalf("grants=%v want none", eff.grants)
	}
}

func TestFIFO_Fuzzy_BusyServesWithRunningModel(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))
	s.inFlight["a"] = 1 // group busy

	r := reqMeta("b")
	s.OnRequest(r)

	if eff.served("b") != 0 || eff.served("a") != 1 {
		t.Fatalf("served b=%d a=%d want b=0 a=1", eff.served("b"), eff.served("a"))
	}
	if eff.lastServeReq.RequestedModel != "b" {
		t.Fatalf("RequestedModel=%q want %q", eff.lastServeReq.RequestedModel, "b")
	}
	if v, ok := metaValue(t, r, "served_model"); !ok || v != "a" {
		t.Fatalf("metadata served_model=%q (found=%v) want %q", v, ok, "a")
	}
}

func TestFIFO_Fuzzy_BusyQueuedPendingRewrites(t *testing.T) {
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 2},
	}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, models, fuzzyFor([]string{"a", "b"}, "g", 300))

	// Two fast-path grants fill the model (reserved=2); a third "a" queues on
	// capacity (reserved 3 > limit 2).
	r1 := req("a")
	s.OnRequest(r1)
	r2 := req("a")
	s.OnRequest(r2)
	r3 := reqCh("a")
	s.OnRequest(r3)
	if len(s.queued) != 1 {
		t.Fatalf("queued=%d want 1", len(s.queued))
	}

	// A fuzzy request arrives while the group is busy: rewritten to "a" and
	// queued under "a"'s capacity rules.
	r4 := reqMetaCh("b")
	s.OnRequest(r4)
	if len(s.queued) != 2 {
		t.Fatalf("queued=%d want 2", len(s.queued))
	}
	for i, item := range s.queued {
		if item.Req.Model != "a" {
			t.Fatalf("queued[%d].Model=%q want %q", i, item.Req.Model, "a")
		}
	}
	if got := s.queued[1].Req.RequestedModel; got != "b" {
		t.Fatalf("queued[1].RequestedModel=%q want %q", got, "b")
	}

	// Cancelling the fuzzy waiter releases its reservation without panicking.
	s.OnCancel(r4)
	if len(s.queued) != 1 {
		t.Fatalf("queued=%d want 1 after cancel", len(s.queued))
	}
	if s.reserved["a"] != 3 {
		t.Fatalf("reserved[a]=%d want 3 after cancel", s.reserved["a"])
	}
}

func TestFIFO_Fuzzy_ActiveSwapTargetWins(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStarting
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b", "c"}, "g", 300))
	s.active["b"] = &activeSwap{modelID: "b", evict: []string{"a"}}

	r := reqMetaCh("c")
	s.OnRequest(r)

	// Rewritten onto the swap target and joined to its waiters.
	if len(s.active["b"].waiters) != 1 {
		t.Fatalf("waiters=%d want 1", len(s.active["b"].waiters))
	}
	s.OnSwapDone(SwapDone{ModelID: "b"})

	if eff.served("b") != 1 {
		t.Fatalf("served b=%d want 1", eff.served("b"))
	}
	if eff.lastServeReq.RequestedModel != "c" {
		t.Fatalf("RequestedModel=%q want %q", eff.lastServeReq.RequestedModel, "c")
	}
	if v, ok := metaValue(t, r, "served_model"); !ok || v != "b" {
		t.Fatalf("served_model=%q (found=%v) want %q", v, ok, "b")
	}
}

func TestFIFO_Fuzzy_IdleWithinWindowKeepsModel(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	s, clock := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))
	s.lastServeDone["g"] = clock.now().Add(-10 * time.Second)

	r := reqMeta("b")
	s.OnRequest(r)

	if eff.served("a") != 1 {
		t.Fatalf("served a=%d want 1", eff.served("a"))
	}
	if eff.startsFor("b") != 0 {
		t.Fatalf("starts for b=%d want 0", eff.startsFor("b"))
	}
	if v, ok := metaValue(t, r, "served_model"); !ok || v != "a" {
		t.Fatalf("served_model=%q (found=%v) want %q", v, ok, "a")
	}
}

func TestFIFO_Fuzzy_IdlePastWindowSwitches(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	s, clock := newFIFOFuzzy(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))
	s.lastServeDone["g"] = clock.now().Add(-301 * time.Second)

	r := reqMeta("b")
	s.OnRequest(r)

	if eff.startsFor("b") != 1 {
		t.Fatalf("starts for b=%d want 1", eff.startsFor("b"))
	}
	if len(eff.grants) != 0 {
		t.Fatalf("grants=%v want none", eff.grants)
	}
	if _, ok := metaValue(t, r, "served_model"); ok {
		t.Fatal("served_model written for a real switch")
	}
}

func TestFIFO_Fuzzy_NeverServedRealSwitch(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	s, _ := newFIFOFuzzy(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))

	r := req("b")
	s.OnRequest(r)

	if eff.startsFor("b") != 1 {
		t.Fatalf("starts for b=%d want 1", eff.startsFor("b"))
	}
	if len(eff.grants) != 0 {
		t.Fatalf("grants=%v want none", eff.grants)
	}
}

func TestFIFO_Fuzzy_ZeroTimeoutOnlyWhenBusy(t *testing.T) {
	// Idle (never served): real switch.
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	s, _ := newFIFOFuzzy(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 0))
	r := req("b")
	s.OnRequest(r)
	if eff.startsFor("b") != 1 {
		t.Fatalf("idle: starts for b=%d want 1", eff.startsFor("b"))
	}

	// Busy: rewrite to the running model.
	eff2 := newFakeEffects()
	eff2.states["a"] = process.StateReady
	s2, _ := newFIFOFuzzy(&stubPlanner{}, eff2, nil, fuzzyFor([]string{"a", "b"}, "g", 0))
	s2.inFlight["a"] = 1
	r2 := reqMeta("b")
	s2.OnRequest(r2)
	if eff2.served("a") != 1 {
		t.Fatalf("busy: served a=%d want 1", eff2.served("a"))
	}
	if eff2.lastServeReq.RequestedModel != "b" {
		t.Fatalf("busy: RequestedModel=%q want %q", eff2.lastServeReq.RequestedModel, "b")
	}
}

func TestFIFO_Fuzzy_ServeDoneRefreshesWindow(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	s, clock := newFIFOFuzzy(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))

	// Serve "a" once: the grant does not open the window, only the completion
	// does.
	rA := req("a")
	s.OnRequest(rA)
	if eff.served("a") != 1 {
		t.Fatalf("served a=%d want 1", eff.served("a"))
	}
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})

	// Long after that serve the idle group is outside the window.
	clock.advance(310 * time.Second)
	if s.fuzzyApplies("g") {
		t.Fatal("fuzzyApplies=true want false after the window has passed")
	}

	// Serve "a" again: the completion stamps the exact refresh time.
	rA2 := req("a")
	s.OnRequest(rA2)
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	want, ok := s.lastServeDone["g"]
	if !ok || !want.Equal(clock.now()) {
		t.Fatalf("lastServeDone[g]=%v (found=%v) want %v", want, ok, clock.now())
	}

	// Inside the rebuilt window the next request is fuzzed onto "a".
	clock.advance(290 * time.Second)
	if !s.fuzzyApplies("g") {
		t.Fatal("fuzzyApplies=false want true within the refreshed window")
	}
	rB := reqMeta("b")
	s.OnRequest(rB)
	if eff.served("a") != 3 {
		t.Fatalf("served a=%d want 3", eff.served("a"))
	}
	if eff.startsFor("b") != 0 {
		t.Fatalf("starts for b=%d want 0", eff.startsFor("b"))
	}
}

func TestFIFO_Fuzzy_CancelDoesNotRefresh(t *testing.T) {
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 2},
	}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	s, clock := newFIFOFuzzy(&stubPlanner{}, eff, models, fuzzyFor([]string{"a", "b"}, "g", 300))

	// Two fast-path grants fill the 2-slot model; the third "a" queues.
	r1 := req("a")
	s.OnRequest(r1)
	r2 := req("a")
	s.OnRequest(r2)
	r3 := reqCh("a")
	s.OnRequest(r3)
	if len(s.queued) != 1 {
		t.Fatalf("queued=%d want 1", len(s.queued))
	}

	// The queued request is cancelled: it was never served, so the window
	// must not be refreshed.
	clock.advance(400 * time.Second)
	s.OnCancel(r3)
	if _, ok := s.lastServeDone["g"]; ok {
		t.Fatal("lastServeDone[g] refreshed by a cancelled request")
	}
}

func TestFIFO_Fuzzy_BookkeepingKeysUseRewrittenModel(t *testing.T) {
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 2},
	}
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, models, fuzzyFor([]string{"a", "b"}, "g", 300))

	// Two fast-path grants fill the 2-slot model; both fuzzy requests queue.
	rA := req("a")
	s.OnRequest(rA)
	rA2 := req("a")
	s.OnRequest(rA2)
	rB := reqMetaCh("b")
	s.OnRequest(rB)
	rC := reqMetaCh("b")
	s.OnRequest(rC)
	if len(s.queued) != 2 {
		t.Fatalf("queued=%d want 2", len(s.queued))
	}
	if s.reserved["a"] != 4 {
		t.Fatalf("reserved[a]=%d want 4", s.reserved["a"])
	}

	// Cancellations and completions unwind the rewritten key only.
	s.OnCancel(rC)
	s.OnCancel(rB)
	if s.reserved["a"] != 2 {
		t.Fatalf("reserved[a]=%d want 2 after cancels", s.reserved["a"])
	}
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if _, ok := s.reserved["a"]; ok {
		t.Fatalf("reserved[a]=%d want none after completions", s.reserved["a"])
	}
}

func TestFIFO_Fuzzy_MultipleRunningPrefersReady(t *testing.T) {
	// A non-Ready member running and a Ready member: the Ready one wins.
	eff := newFakeEffects()
	eff.states["a"] = process.StateStarting
	eff.states["b"] = process.StateReady
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b", "c"}, "g", 300))
	s.inFlight["a"] = 1 // busy

	r := reqMeta("c")
	s.OnRequest(r)
	if v, ok := metaValue(t, r, "served_model"); !ok || v != "b" {
		t.Fatalf("served_model=%q (found=%v) want %q (Ready preferred)", v, ok, "b")
	}

	// Both non-Ready: the lexicographically first member is chosen. The
	// rewrite is observable in the swap the scheduler starts.
	eff2 := newFakeEffects()
	eff2.states["a"] = process.StateStarting
	eff2.states["b"] = process.StateStarting
	eff2.states["c"] = process.StateStopped
	s2, _ := newFIFOFuzzy(&stubPlanner{}, eff2, nil, fuzzyFor([]string{"a", "b", "c"}, "g", 300))
	s2.inFlight["a"] = 1

	r2 := reqMeta("c")
	s2.OnRequest(r2)
	if eff2.startsFor("a") != 1 {
		t.Fatalf("starts for a=%d want 1 (lexicographically first)", eff2.startsFor("a"))
	}
	sw, ok := s2.active["a"]
	if !ok {
		t.Fatal("rewritten request did not start a swap for the chosen model")
	}
	if got := sw.waiters[0].RequestedModel; got != "c" {
		t.Fatalf("RequestedModel=%q want %q", got, "c")
	}
}

func TestFIFO_Fuzzy_NonMemberUnaffected(t *testing.T) {
	// Regression baseline: a model outside every fuzzy group behaves exactly
	// as it did before the feature existed.
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["c"] = process.StateStopped
	s, _ := newFIFOFuzzy(&stubPlanner{evict: map[string][]string{"c": {"a"}}}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))

	// Baseline fast-path grant for the fuzzy group member still works.
	r2 := reqMeta("a")
	s.OnRequest(r2)
	if eff.served("a") != 1 {
		t.Fatalf("served a=%d want 1", eff.served("a"))
	}
	if eff.lastServeReq.RequestedModel != "" {
		t.Fatalf("RequestedModel=%q want empty", eff.lastServeReq.RequestedModel)
	}
	if _, ok := metaValue(t, r2, "served_model"); ok {
		t.Fatal("served_model written for a non-fuzzy request")
	}

	// Finish serving, then a non-member request still performs a real switch.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	r := req("c")
	s.OnRequest(r)
	if eff.startsFor("c") != 1 {
		t.Fatalf("starts for c=%d want 1", eff.startsFor("c"))
	}
}

func TestFIFO_Fuzzy_GrantErrorNoMetadata(t *testing.T) {
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.serveResult["a"] = false // the caller is gone by grant time
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))
	s.inFlight["a"] = 1 // busy

	r := reqMeta("b")
	s.OnRequest(r)

	if eff.served("a") != 0 {
		t.Fatalf("served a=%d want 0", eff.served("a"))
	}
	if s.reserved["a"] != 0 {
		t.Fatalf("reserved[a]=%d want 0 after failed grant", s.reserved["a"])
	}
	if _, ok := metaValue(t, r, "served_model"); ok {
		t.Fatal("served_model leaked on a failed grant")
	}
}

func TestFIFO_Fuzzy_ForceSwitch_IdleWindowRealSwitch(t *testing.T) {
	// Inside the idle window a plain request is rewritten (see
	// TestFIFO_Fuzzy_IdleWithinWindowKeepsModel); a ForceSwitch request skips
	// the rewrite entirely and runs the normal decision tree — a real switch
	// to the asked model, evicting the online sibling.
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	s, clock := newFIFOFuzzy(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))
	s.lastServeDone["g"] = clock.now().Add(-10 * time.Second)

	r := reqMeta("b")
	r.ForceSwitch = true
	s.OnRequest(r)

	if eff.startsFor("b") != 1 {
		t.Fatalf("starts for b=%d want 1 (real switch despite the idle window)", eff.startsFor("b"))
	}
	if eff.served("a") != 0 {
		t.Fatalf("served a=%d want 0", eff.served("a"))
	}
	if got := s.active["b"].waiters[0].RequestedModel; got != "" {
		t.Fatalf("RequestedModel=%q want empty (no rewrite)", got)
	}
	if _, ok := metaValue(t, r, "served_model"); ok {
		t.Fatal("served_model written for a force-switched request")
	}
}

func TestFIFO_Fuzzy_ForceSwitch_BusyQueuesNotRewrites(t *testing.T) {
	// While the online member is busy, a plain request is fuzzed onto it (see
	// TestFIFO_Fuzzy_BusyServesWithRunningModel). A ForceSwitch request skips
	// the rewrite and takes the normal tree: it would evict the busy sibling,
	// so it parks in the queue under its OWN model until the sibling drains,
	// then starts a real switch.
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	s, _ := newFIFOFuzzy(&stubPlanner{evict: map[string][]string{"b": {"a"}}}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))
	s.inFlight["a"] = 1
	s.reserved["a"] = 1 // the in-flight request holds its reservation

	r := reqMeta("b")
	r.ForceSwitch = true
	s.OnRequest(r)

	if len(s.queued) != 1 {
		t.Fatalf("queued=%d want 1 (queued on the busy sibling, not rewritten)", len(s.queued))
	}
	if s.queued[0].Req.Model != "b" {
		t.Fatalf("queued[0].Model=%q want %q", s.queued[0].Req.Model, "b")
	}
	if got := s.queued[0].Req.RequestedModel; got != "" {
		t.Fatalf("RequestedModel=%q want empty (no rewrite)", got)
	}
	if eff.startsFor("a") != 0 || eff.startsFor("b") != 0 {
		t.Fatalf("starts a=%d b=%d want none while the sibling is busy", eff.startsFor("a"), eff.startsFor("b"))
	}

	// When the busy sibling drains, the queued force-switch request starts a
	// real switch to b.
	s.OnServeDone(ServeDoneEvent{ModelID: "a"})
	if eff.startsFor("b") != 1 {
		t.Fatalf("starts for b=%d want 1 after the sibling drained", eff.startsFor("b"))
	}
	if _, ok := metaValue(t, r, "served_model"); ok {
		t.Fatal("served_model written for a force-switched request")
	}
}

func TestFIFO_Fuzzy_BusyNoOnline_QueueHeadFollows(t *testing.T) {
	// The group's online member was just unloaded, leaving the group busy with
	// queued traffic but no online model and no active swap. The first model
	// the group's traffic is committed to — here the queue head "b" — decides
	// the switch target: a later request for "c" is rewritten to "b" instead
	// of starting a competing swap for c.
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	eff.states["c"] = process.StateStopped
	s, clock := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b", "c"}, "g", 300))
	s.queued = append(s.queued, queuedItem{Req: reqCh("b"), EnqueuedAt: clock.now()})

	r := reqMeta("c")
	s.OnRequest(r)

	if eff.startsFor("b") != 1 {
		t.Fatalf("starts for b=%d want 1 (queue head decides the target)", eff.startsFor("b"))
	}
	if eff.startsFor("c") != 0 {
		t.Fatalf("starts for c=%d want 0", eff.startsFor("c"))
	}
	sw := s.active["b"]
	if len(sw.waiters) != 1 || sw.waiters[0].RequestedModel != "c" {
		t.Fatalf("waiters=%d RequestedModel=%q want 1/%q", len(sw.waiters), sw.waiters[0].RequestedModel, "c")
	}
}

func TestFIFO_Fuzzy_OnUnload_RestoresSwapWaiter(t *testing.T) {
	// Same asymmetry fix as the queued-request restore: a swap waiter that
	// fuzzy substitution rewrote onto the swap target being unloaded must not
	// be killed when the client's original model is still available. The
	// rewrite is undone and the request requeues for a real switch.
	eff := newFakeEffects()
	eff.states["a"] = process.StateStarting // a's swap is in flight
	eff.states["b"] = process.StateStopped
	eff.states["c"] = process.StateStopped
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b", "c"}, "g", 300))

	// A plain request starts the swap to a; a fuzzy request for b joins as a
	// rewritten waiter.
	s.OnRequest(req("a"))
	rB := reqMetaCh("b")
	s.OnRequest(rB)
	if len(s.active["a"].waiters) != 2 {
		t.Fatalf("waiters=%d want 2 (plain + rewritten)", len(s.active["a"].waiters))
	}
	if got := s.active["a"].waiters[1].RequestedModel; got != "b" {
		t.Fatalf("waiter RequestedModel=%q want %q", got, "b")
	}

	// Unloading the swap target a stops it (mirrored in the fake states).
	eff.states["a"] = process.StateStopped
	s.OnUnload([]string{"a"}, time.Second)

	// The plain waiter gets the unload error; the rewritten waiter is
	// restored to b and requeues, where the drain starts a real switch.
	if got := eff.errored("a"); got != 1 {
		t.Fatalf("errored(a)=%d want 1 (plain waiter only)", got)
	}
	if eff.startsFor("b") != 1 {
		t.Fatalf("starts for b=%d want 1 (real switch after restore)", eff.startsFor("b"))
	}
	// a was switched to once by the setup (the in-flight swap); the unload
	// must not start any additional switch for it.
	if eff.startsFor("a") != 1 {
		t.Fatalf("starts for a=%d want 1 (setup switch only)", eff.startsFor("a"))
	}
	sw := s.active["b"]
	if sw == nil || len(sw.waiters) != 1 {
		t.Fatalf("active[b]=%+v want one waiter", sw)
	}
	if got := sw.waiters[0].Model; got != "b" {
		t.Fatalf("waiter Model=%q want %q", got, "b")
	}
	if got := sw.waiters[0].RequestedModel; got != "" {
		t.Fatalf("waiter RequestedModel=%q want empty (rewrite undone)", got)
	}
	if s.reserved["a"] != 0 {
		t.Fatalf("reserved[a]=%d want 0 (both waiters released)", s.reserved["a"])
	}
	if s.reserved["b"] != 1 {
		t.Fatalf("reserved[b]=%d want 1 (reservation moved to b)", s.reserved["b"])
	}
	if len(s.queued) != 0 {
		t.Fatalf("queued=%d want 0", len(s.queued))
	}

	// Completing the switch serves the client's original model without a
	// served_model marker (no rewrite remains).
	s.OnSwapDone(SwapDone{ModelID: "b"})
	if eff.served("b") != 1 {
		t.Fatalf("served b=%d want 1", eff.served("b"))
	}
	if _, ok := metaValue(t, rB, "served_model"); ok {
		t.Fatal("served_model written for a restored waiter")
	}
}

func TestFIFO_Fuzzy_OnUnload_SwapWaiterDropsWhenOriginalAlsoUnloaded(t *testing.T) {
	// Control: when the client's original model is being unloaded too, the
	// rewritten swap waiter keeps the legacy unload rejection.
	eff := newFakeEffects()
	eff.states["a"] = process.StateStarting
	eff.states["b"] = process.StateStopped
	eff.states["c"] = process.StateStopped
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b", "c"}, "g", 300))

	s.OnRequest(req("a"))
	s.OnRequest(reqMetaCh("b"))

	eff.states["a"] = process.StateStopped
	s.OnUnload([]string{"a", "b"}, time.Second)

	if got := eff.errored("a"); got != 2 {
		t.Fatalf("errored(a)=%d want 2 (both waiters dropped)", got)
	}
	if eff.startsFor("b") != 0 {
		t.Fatalf("starts for b=%d want 0", eff.startsFor("b"))
	}
	if len(s.queued) != 0 {
		t.Fatalf("queued=%d want 0", len(s.queued))
	}
}

func TestFIFO_Fuzzy_OnlineExcludesEvictingMember(t *testing.T) {
	// A member an in-flight swap is evicting (its process not stopped yet) is
	// not offered as a fuzzy rewrite target: a request rewritten onto it would
	// collide with the swap, queue, and then pay a second full switch to the
	// same model. The request keeps its own model and switches for real.
	eff := newFakeEffects()
	eff.states["a"] = process.StateReady
	eff.states["c"] = process.StateStopped
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b", "c"}, "g", 300))
	// A swap to non-member x is evicting the online group member a.
	s.active["x"] = &activeSwap{modelID: "x", evict: []string{"a"}}

	r := reqMetaCh("c")
	s.OnRequest(r)

	if eff.startsFor("c") != 1 {
		t.Fatalf("starts for c=%d want 1 (real switch, evicting member not followed)", eff.startsFor("c"))
	}
	if eff.startsFor("a") != 0 {
		t.Fatalf("starts for a=%d want 0", eff.startsFor("a"))
	}
	if got := s.active["c"].waiters[0].RequestedModel; got != "" {
		t.Fatalf("RequestedModel=%q want empty (no rewrite)", got)
	}

	// Control: with the eviction targeting a non-member instead, the healthy
	// online member a is followed as usual (group busy via member b's
	// in-flight request).
	eff2 := newFakeEffects()
	eff2.states["a"] = process.StateReady
	eff2.states["b"] = process.StateStopped
	eff2.states["c"] = process.StateStopped
	s2, _ := newFIFOFuzzy(&stubPlanner{}, eff2, nil, fuzzyFor([]string{"a", "b", "c"}, "g", 300))
	s2.active["x"] = &activeSwap{modelID: "x", evict: []string{"z"}}
	s2.inFlight["b"] = 1

	r2 := reqMeta("c")
	s2.OnRequest(r2)
	if eff2.served("a") != 1 {
		t.Fatalf("served a=%d want 1 (healthy online member followed)", eff2.served("a"))
	}
	if eff2.lastServeReq.RequestedModel != "c" {
		t.Fatalf("RequestedModel=%q want %q", eff2.lastServeReq.RequestedModel, "c")
	}
	if v, ok := metaValue(t, r2, "served_model"); !ok || v != "a" {
		t.Fatalf("served_model=%q (found=%v) want %q", v, ok, "a")
	}
}

func TestFIFO_Fuzzy_BusyNoOnline_DyingInflightRealSwitch(t *testing.T) {
	// Queue empty but a member still has in-flight requests: those handlers
	// are "dying" — draining from a member that was just stopped manually or
	// crashed (fuzzyOnline is empty). Following them would bounce the group
	// back onto the model that was just removed, so the new request keeps its
	// own model and performs a real switch.
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))
	s.inFlight["a"] = 1

	r := reqMeta("b")
	s.OnRequest(r)

	if eff.startsFor("b") != 1 {
		t.Fatalf("starts for b=%d want 1 (real switch, dying in-flight is not followed)", eff.startsFor("b"))
	}
	if eff.startsFor("a") != 0 {
		t.Fatalf("starts for a=%d want 0", eff.startsFor("a"))
	}
	sw := s.active["b"]
	if len(sw.waiters) != 1 || sw.waiters[0].RequestedModel != "" {
		t.Fatalf("waiters=%d RequestedModel=%q want 1/empty (no rewrite)", len(sw.waiters), sw.waiters[0].RequestedModel)
	}
}

func TestFIFO_Fuzzy_BusyNoOnline_NotBusyRealSwitch(t *testing.T) {
	// Control: with no online member and no busy traffic at all, the rule must
	// not fire — the request keeps its own model and performs a real switch.
	eff := newFakeEffects()
	eff.states["a"] = process.StateStopped
	eff.states["b"] = process.StateStopped
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, nil, fuzzyFor([]string{"a", "b"}, "g", 300))

	r := reqMeta("b")
	s.OnRequest(r)

	if eff.startsFor("b") != 1 {
		t.Fatalf("starts for b=%d want 1 (real switch, group not busy)", eff.startsFor("b"))
	}
	if eff.startsFor("a") != 0 {
		t.Fatalf("starts for a=%d want 0", eff.startsFor("a"))
	}
	if got := s.active["b"].waiters[0].RequestedModel; got != "" {
		t.Fatalf("RequestedModel=%q want empty (no rewrite)", got)
	}
}

// fillAQueuesRewrittenB is the shared setup for the OnUnload restore tests:
// model a (limit 2) is online and filled to its concurrency limit with two
// plain grants, so a fuzzy request for b is rewritten onto a and parks in the
// queue on a's capacity rules (reserved[a]=3 > limit 2). models is the per-model
// config passed to the FIFO (may be nil; use it to tune the original model's
// limits). Returns the queued request's HandlerReq for metadata assertions.
func fillAQueuesRewrittenB(t *testing.T, eff *fakeEffects, models map[string]config.ModelConfig) (*FIFO, HandlerReq) {
	t.Helper()
	if models == nil {
		// Default: model a's 2-slot limit is what parks the rewritten request.
		models = map[string]config.ModelConfig{"a": {ConcurrencyLimit: 2}}
	}
	s, _ := newFIFOFuzzy(&stubPlanner{}, eff, models, fuzzyFor([]string{"a", "b", "c"}, "g", 300))
	eff.states["a"] = process.StateReady
	eff.states["b"] = process.StateStopped
	eff.states["c"] = process.StateStopped

	s.OnRequest(req("a"))
	s.OnRequest(req("a"))
	rB := reqMetaCh("b")
	s.OnRequest(rB)
	if len(s.queued) != 1 {
		t.Fatalf("queued=%d want 1 (rewritten b request parked on capacity)", len(s.queued))
	}
	if got := s.queued[0].Req.RequestedModel; got != "b" {
		t.Fatalf("queued[0].RequestedModel=%q want %q", got, "b")
	}
	return s, rB
}

func TestFIFO_Fuzzy_OnUnload_RestoresRequestedModel(t *testing.T) {
	// Unloading the online member must not kill queued requests that fuzzy
	// substitution rewrote onto it when the client's original model is still
	// available: the rewrite is undone, the reservation moves back to the
	// original model, and the request proceeds through the normal decision
	// tree — a real switch to the model the client actually asked for.
	eff := newFakeEffects()
	s, rB := fillAQueuesRewrittenB(t, eff, nil)

	// The unload stops a; fakeEffects.StopProcesses only records, so mirror
	// the post-stop state before driving OnUnload.
	eff.states["a"] = process.StateStopped
	s.OnUnload([]string{"a"}, time.Second)

	if got := eff.errored(""); got != 0 {
		t.Fatalf("error grants=%d want 0 (rewritten request must not be dropped)", got)
	}
	if eff.startsFor("b") != 1 {
		t.Fatalf("starts for b=%d want 1 (real switch after restore)", eff.startsFor("b"))
	}
	if eff.startsFor("a") != 0 {
		t.Fatalf("starts for a=%d want 0", eff.startsFor("a"))
	}
	if len(eff.stops) != 1 || len(eff.stops[0].ids) != 1 || eff.stops[0].ids[0] != "a" {
		t.Fatalf("StopProcesses=%+v want one call stopping [a]", eff.stops)
	}

	// The restored request swapped to b under its own name, no longer carrying
	// the rewrite marker; its reservation moved from a back to b.
	sw := s.active["b"]
	if sw == nil || len(sw.waiters) != 1 {
		t.Fatalf("active[b] waiters=%+v want one", sw.waiters)
	}
	if got := sw.waiters[0].Model; got != "b" {
		t.Fatalf("waiter Model=%q want %q", got, "b")
	}
	if got := sw.waiters[0].RequestedModel; got != "" {
		t.Fatalf("waiter RequestedModel=%q want empty (rewrite undone)", got)
	}
	if s.reserved["a"] != 2 {
		t.Fatalf("reserved[a]=%d want 2 (rewritten slot released)", s.reserved["a"])
	}
	if s.reserved["b"] != 1 {
		t.Fatalf("reserved[b]=%d want 1 (reservation moved to b)", s.reserved["b"])
	}

	// Completing the switch serves the client's original model without a
	// served_model marker (no rewrite remains).
	s.OnSwapDone(SwapDone{ModelID: "b"})
	if eff.served("b") != 1 {
		t.Fatalf("served b=%d want 1", eff.served("b"))
	}
	if _, ok := metaValue(t, rB, "served_model"); ok {
		t.Fatal("served_model written for a restored request")
	}
	if _, ok := metaValue(t, rB, "fifo_priority"); !ok {
		t.Fatal("fifo_priority not written on grant")
	}
}

func TestFIFO_Fuzzy_OnUnload_DropsWhenOriginalAlsoUnloaded(t *testing.T) {
	// Control: when the client's original model is being unloaded too, the
	// rewritten queued request keeps the legacy drop behavior.
	eff := newFakeEffects()
	s, _ := fillAQueuesRewrittenB(t, eff, nil)

	eff.states["a"] = process.StateStopped
	s.OnUnload([]string{"a", "b"}, time.Second)

	if got := eff.errored("a"); got != 1 {
		t.Fatalf("errored(a)=%d want 1 (queued request dropped)", got)
	}
	if eff.startsFor("b") != 0 {
		t.Fatalf("starts for b=%d want 0", eff.startsFor("b"))
	}
	if len(s.queued) != 0 {
		t.Fatalf("queued=%d want 0", len(s.queued))
	}
	if s.reserved["a"] != 2 {
		t.Fatalf("reserved[a]=%d want 2 (drop released the rewritten slot)", s.reserved["a"])
	}
}

func TestFIFO_Fuzzy_OnUnload_RestoredItemAnchorsQueueHead(t *testing.T) {
	// A restored request can stay queued (here: b is at its in-flight limit),
	// and it then anchors the group exactly like any other queued member: a
	// later fuzzy request for c follows the queue head b instead of starting a
	// competing switch to c. Without the restore the queue would be empty and
	// c would have switched for real.
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 2},
		"b": {ConcurrencyLimit: 1},
	}
	eff := newFakeEffects()
	s, _ := fillAQueuesRewrittenB(t, eff, models)
	// Keep the restored request queued: b is already serving one request.
	s.inFlight["b"] = 1

	eff.states["a"] = process.StateStopped
	s.OnUnload([]string{"a"}, time.Second)

	if got := eff.errored(""); got != 0 {
		t.Fatalf("error grants=%d want 0", got)
	}
	if len(s.queued) != 1 {
		t.Fatalf("queued=%d want 1 (restored request waits at b's limit)", len(s.queued))
	}
	if got := s.queued[0].Req.Model; got != "b" {
		t.Fatalf("queued[0].Model=%q want %q (rewrite undone)", got, "b")
	}
	if got := s.queued[0].Req.RequestedModel; got != "" {
		t.Fatalf("queued[0].RequestedModel=%q want empty", got)
	}

	// A fuzzy request for c now follows the restored queue head.
	rC := reqMetaCh("c")
	s.OnRequest(rC)
	if eff.startsFor("c") != 0 {
		t.Fatalf("starts for c=%d want 0 (follows the restored queue head)", eff.startsFor("c"))
	}
	if len(s.queued) != 2 {
		t.Fatalf("queued=%d want 2", len(s.queued))
	}
	if got := s.queued[1].Req.Model; got != "b" {
		t.Fatalf("queued[1].Model=%q want %q (c followed b)", got, "b")
	}
	if got := s.queued[1].Req.RequestedModel; got != "c" {
		t.Fatalf("queued[1].RequestedModel=%q want %q", got, "c")
	}
}

func TestFIFO_Fuzzy_OnUnload_RestoreNoCapacityErrors(t *testing.T) {
	// If the original model's admission capacity is exhausted, the restored
	// request gets the same rejection a fresh admission would have received
	// instead of silently overshooting the capacity bookkeeping.
	models := map[string]config.ModelConfig{
		"a": {ConcurrencyLimit: 2},
		"b": {ConcurrencyLimit: 2},
	}
	eff := newFakeEffects()
	s, _ := fillAQueuesRewrittenB(t, eff, models)
	// Simulate b's queue being full: limit 2 + default queueDepth 10 slots.
	s.reserved["b"] = 12

	eff.states["a"] = process.StateStopped
	s.OnUnload([]string{"a"}, time.Second)

	if got := eff.errored("a"); got != 1 {
		t.Fatalf("errored(a)=%d want 1 (no capacity for the original model)", got)
	}
	if eff.startsFor("b") != 0 {
		t.Fatalf("starts for b=%d want 0", eff.startsFor("b"))
	}
	if len(s.queued) != 0 {
		t.Fatalf("queued=%d want 0", len(s.queued))
	}
	if s.reserved["b"] != 12 {
		t.Fatalf("reserved[b]=%d want 12 (untouched)", s.reserved["b"])
	}
	if s.reserved["a"] != 2 {
		t.Fatalf("reserved[a]=%d want 2", s.reserved["a"])
	}
}
