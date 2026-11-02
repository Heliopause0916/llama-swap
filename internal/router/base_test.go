package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/hint"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/router/scheduler"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// These tests cover baseRouter's own machinery — the run loop, process
// lifecycle (doSwap), grant/ServeHTTP plumbing, Unload, and Shutdown. The
// scheduling decision logic (queueing, collation, eviction collisions) lives in
// the scheduler package and is tested directly there; see fifo_test.go.

// stubPlanner evicts configured targets. baseRouter tests drive the run loop
// through the default FIFO scheduler without exercising router planner details.
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

func newTestBase(t *testing.T, processes map[string]process.Process, planner scheduler.Swapper) *baseRouter {
	t.Helper()
	conf := config.Config{HealthCheckTimeout: 5}
	return newTestBaseWithConfig(t, conf, processes, planner)
}

func newTestBaseWithConfig(t *testing.T, conf config.Config, processes map[string]process.Process, planner scheduler.Swapper) *baseRouter {
	t.Helper()
	b, err := newBaseRouter("test", conf, processes, logmon.NewWriter(io.Discard), planner)
	if err != nil {
		t.Fatalf("newBaseRouter: %v", err)
	}
	b.testProcessed = make(chan struct{}, 64)
	go b.run()
	t.Cleanup(func() {
		if !b.shuttingDown.Load() {
			_ = b.Shutdown(time.Second)
		}
	})
	return b
}

// newTrackedServeBase builds a baseRouter whose scheduler ignores serve events,
// so trackedServe can be driven directly without running a real admission (the
// FIFO's OnServeDone would otherwise panic on a model that never held a
// reservation). The scheduler is swapped before run() starts.
func newTrackedServeBase(t *testing.T, processes map[string]process.Process) *baseRouter {
	t.Helper()
	b, err := newBaseRouter("test", config.Config{HealthCheckTimeout: 5}, processes,
		logmon.NewWriter(io.Discard), &stubPlanner{})
	if err != nil {
		t.Fatalf("newBaseRouter: %v", err)
	}
	b.schedule = &captureScheduler{got: make(chan scheduler.HandlerReq, 1)}
	b.testProcessed = make(chan struct{}, 64)
	go b.run()
	t.Cleanup(func() {
		if !b.shuttingDown.Load() {
			_ = b.Shutdown(time.Second)
		}
	})
	return b
}

func TestBaseRouter_RunningModels(t *testing.T) {
	ready := newFakeProcess("ready")
	ready.markReady()
	starting := newFakeProcess("starting")
	starting.setState(process.StateStarting)
	stopped := newFakeProcess("stopped")

	b := newTestBase(t, map[string]process.Process{
		"ready": ready, "starting": starting, "stopped": stopped,
	}, &stubPlanner{})

	running := b.RunningModels()
	if len(running) != 2 {
		t.Fatalf("running=%v want 2 entries", running)
	}
	if running["ready"] != process.StateReady {
		t.Errorf("ready state=%q want ready", running["ready"])
	}
	if running["starting"] != process.StateStarting {
		t.Errorf("starting state=%q want starting", running["starting"])
	}
	if _, ok := running["stopped"]; ok {
		t.Errorf("stopped process should be excluded from RunningModels")
	}
}

func TestBaseRouter_RunningStatus(t *testing.T) {
	ready := newFakeProcess("ready")
	before := time.Now()
	ready.markReady()
	starting := newFakeProcess("starting")
	starting.setState(process.StateStarting)
	stopped := newFakeProcess("stopped")

	b := newTestBase(t, map[string]process.Process{
		"ready": ready, "starting": starting, "stopped": stopped,
	}, &stubPlanner{})

	running := b.RunningStatus()
	if len(running) != 2 {
		t.Fatalf("running=%v want 2 entries", running)
	}
	if st := running["ready"]; st.State != process.StateReady || st.ReadySince.Before(before) {
		t.Errorf("ready status=%+v, want ready with ReadySince after %v", st, before)
	}
	if st := running["starting"]; st.State != process.StateStarting || !st.ReadySince.IsZero() {
		t.Errorf("starting status=%+v, want starting with zero ReadySince", st)
	}

	ready.setState(process.StateStopped)
	if _, ok := b.RunningStatus()["ready"]; ok {
		t.Errorf("stopped process should be excluded from RunningStatus")
	}
}

func TestBaseRouter_CancelSwapDuringEviction(t *testing.T) {
	target := newFakeProcess("target")
	victim := newFakeProcess("victim")
	victim.markReady()
	victim.stopBlock = make(chan struct{})
	b := newTestBase(t, map[string]process.Process{"target": target, "victim": victim}, &stubPlanner{})
	releaseStop := sync.OnceFunc(func() { close(victim.stopBlock) })
	t.Cleanup(releaseStop)

	ctx, cancel := context.WithCancel(b.shutdownCtx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.doSwap(ctx, "target", 1, []string{"victim"})
	}()
	waitSignal(t, victim.stopStarted, "eviction started")

	// Unload invalidates the swap before its eviction finishes. It must never
	// proceed to EnsureReady, even though stopping the victim still completes.
	cancel()
	releaseStop()
	waitSignal(t, done, "cancelled swap exited")
	if got := target.runCalls.Load(); got != 0 {
		t.Errorf("target started %d times after its swap was cancelled", got)
	}
	if target.State() != process.StateStopped {
		t.Errorf("target state=%q want stopped", target.State())
	}
}

func TestBaseRouter_CancelSwapJoinsGoroutine(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(fmt.Sprintf("ready=%t", ready), func(t *testing.T) {
			p := newFakeProcess("a")
			p.autoReady = ready
			// Leave the event loop stopped so a successful swap cannot deliver
			// its completion. Cancellation must release that send as well as
			// an EnsureReady that is still waiting for readiness.
			b, err := newBaseRouter("test", config.Config{}, map[string]process.Process{"a": p}, logmon.NewWriter(io.Discard), &stubPlanner{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(b.shutdownFn)
			t.Cleanup(b.procCancel)
			cancel := b.StartSwap("a", 1, nil)
			waitSignal(t, p.runStarted, "swap started")
			done := make(chan struct{})
			go func() {
				cancel()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("cancel did not join the swap goroutine")
			}
			b.StopProcesses(time.Second, []string{"a"})
			if p.State() != process.StateStopped {
				t.Errorf("state=%q want stopped after cancelling and stopping", p.State())
			}
		})
	}
}

func TestBaseRouter_UnloadAll(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()
	c := newFakeProcess("c")
	c.markReady()

	b := newTestBase(t, map[string]process.Process{"a": a, "c": c}, &stubPlanner{})
	b.Unload(time.Second)

	if a.State() != process.StateStopped || c.State() != process.StateStopped {
		t.Fatalf("Unload() should stop every process: a=%q c=%q", a.State(), c.State())
	}
}

func TestBaseRouter_UnloadSpecificModel(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()
	c := newFakeProcess("c")
	c.markReady()

	b := newTestBase(t, map[string]process.Process{"a": a, "c": c}, &stubPlanner{})
	b.Unload(time.Second, "a")

	if a.State() != process.StateStopped {
		t.Errorf("a should be stopped, got %q", a.State())
	}
	if c.State() != process.StateReady {
		t.Errorf("c should remain ready, got %q", c.State())
	}
}

func TestBaseRouter_UnloadSpecificModelUsesConfiguredTimeout(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()
	c := newFakeProcess("c")
	c.markReady()

	conf := config.Config{
		HealthCheckTimeout: 5,
		UnloadTimeout:      25,
		Models: map[string]config.ModelConfig{
			"a": {UnloadTimeout: 45},
			"c": {UnloadTimeout: 25},
		},
	}
	b := newTestBaseWithConfig(t, conf, map[string]process.Process{"a": a, "c": c}, &stubPlanner{})
	b.Unload(0, "a")

	if a.lastStopTimeout() != 45*time.Second {
		t.Errorf("a stop timeout=%v want 45s", a.lastStopTimeout())
	}
	if got := c.stopCalls.Load(); got != 0 {
		t.Errorf("c stopCalls=%d want 0", got)
	}
}

func TestBaseRouter_UnloadAllUsesConfiguredTimeouts(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()
	c := newFakeProcess("c")
	c.markReady()

	conf := config.Config{
		HealthCheckTimeout: 5,
		UnloadTimeout:      25,
		Models: map[string]config.ModelConfig{
			"a": {UnloadTimeout: 45},
			"c": {UnloadTimeout: 25},
		},
	}
	b := newTestBaseWithConfig(t, conf, map[string]process.Process{"a": a, "c": c}, &stubPlanner{})
	b.Unload(0)

	if a.lastStopTimeout() != 45*time.Second {
		t.Errorf("a stop timeout=%v want 45s", a.lastStopTimeout())
	}
	if c.lastStopTimeout() != 25*time.Second {
		t.Errorf("c stop timeout=%v want 25s", c.lastStopTimeout())
	}
}

func TestBaseRouter_UnloadStopsSmallestTimeoutFirst(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()
	c := newFakeProcess("c")
	c.markReady()
	e := newFakeProcess("e")
	e.markReady()

	var mu sync.Mutex
	var order []string
	record := func(id string) {
		mu.Lock()
		order = append(order, id)
		mu.Unlock()
	}
	a.onStop = record
	c.onStop = record
	e.onStop = record

	conf := config.Config{
		HealthCheckTimeout: 5,
		UnloadTimeout:      25,
		Models: map[string]config.ModelConfig{
			"a": {UnloadTimeout: 45},
			"c": {UnloadTimeout: 10},
			"e": {UnloadTimeout: 25},
		},
	}
	b := newTestBaseWithConfig(t, conf, map[string]process.Process{"a": a, "c": c, "e": e}, &stubPlanner{})
	// Named in descending timeout order; Unload must re-order ascending.
	b.Unload(0, "a", "e", "c")

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"c", "e", "a"}; !slices.Equal(order, want) {
		t.Errorf("stop order=%v want %v", order, want)
	}
}

// TestBaseRouter_UnloadZeroStopsSameTimeoutInParallel verifies that models
// resolving to the same unloadTimeout share one unload request and stop
// concurrently, rather than one request per model. Both fakeProcess.Stop
// calls are pinned via stopBlock; the test only releases them after
// observing both stopStarted, which deadlocks if the stops were sequential.
func TestBaseRouter_UnloadZeroStopsSameTimeoutInParallel(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()
	a.stopBlock = make(chan struct{})
	c := newFakeProcess("c")
	c.markReady()
	c.stopBlock = make(chan struct{})

	conf := config.Config{
		HealthCheckTimeout: 5,
		UnloadTimeout:      25,
		// no per-model values: both models inherit the global 25s
	}
	b := newTestBaseWithConfig(t, conf, map[string]process.Process{"a": a, "c": c}, &stubPlanner{})

	unloadDone := make(chan struct{})
	go func() {
		b.Unload(0)
		close(unloadDone)
	}()

	for _, p := range []*fakeProcess{a, c} {
		select {
		case <-p.stopStarted:
		case <-time.After(2 * time.Second):
			t.Fatalf("Stop on %s never started — same-timeout unloads are not parallel", p.id)
		}
	}
	close(a.stopBlock)
	close(c.stopBlock)

	select {
	case <-unloadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Unload did not return after stops were released")
	}
	if a.lastStopTimeout() != 25*time.Second || c.lastStopTimeout() != 25*time.Second {
		t.Errorf("stop timeouts a=%v c=%v want 25s each", a.lastStopTimeout(), c.lastStopTimeout())
	}
}

func TestBaseRouter_UnloadPositiveTimeoutOverridesConfigured(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()

	conf := config.Config{
		HealthCheckTimeout: 5,
		UnloadTimeout:      25,
		Models: map[string]config.ModelConfig{
			"a": {UnloadTimeout: 45},
		},
	}
	b := newTestBaseWithConfig(t, conf, map[string]process.Process{"a": a}, &stubPlanner{})
	b.Unload(time.Second, "a")

	if a.lastStopTimeout() != time.Second {
		t.Errorf("a stop timeout=%v want 1s", a.lastStopTimeout())
	}
}

// TestBaseRouter_Unload_StopsInParallel verifies that Unload fans out its
// Stop calls concurrently rather than stopping each process serially. Each
// fakeProcess.Stop is pinned via stopBlock; the test only releases them
// after observing every stopStarted, proving all three Stops were in
// flight simultaneously.
func TestBaseRouter_Unload_StopsInParallel(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()
	a.stopBlock = make(chan struct{})
	pb := newFakeProcess("b")
	pb.markReady()
	pb.stopBlock = make(chan struct{})
	pc := newFakeProcess("c")
	pc.markReady()
	pc.stopBlock = make(chan struct{})

	b := newTestBase(t, map[string]process.Process{"a": a, "b": pb, "c": pc}, &stubPlanner{})

	unloadDone := make(chan struct{})
	go func() {
		b.Unload(time.Second, "a", "b", "c")
		close(unloadDone)
	}()

	// All three Stop calls must start before any of them are allowed to
	// complete. If Unload was serial, only one stopStarted would fire
	// until we released its stopBlock, and this would deadlock.
	for _, p := range []*fakeProcess{a, pb, pc} {
		select {
		case <-p.stopStarted:
		case <-time.After(2 * time.Second):
			t.Fatalf("Stop on %s never started — Unload is not parallel", p.id)
		}
	}

	// Release them; Unload should now return.
	close(a.stopBlock)
	close(pb.stopBlock)
	close(pc.stopBlock)

	select {
	case <-unloadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Unload did not return after stops released")
	}

	for _, p := range []*fakeProcess{a, pb, pc} {
		if p.State() != process.StateStopped {
			t.Errorf("%s state=%q want stopped", p.id, p.State())
		}
		if got := p.stopCalls.Load(); got != 1 {
			t.Errorf("%s stopCalls=%d want 1", p.id, got)
		}
	}
}

func TestBaseRouter_OnDemandStart(t *testing.T) {
	a := newFakeProcess("a")
	a.autoReady = true

	b := newTestBase(t, map[string]process.Process{"a": a}, &stubPlanner{})

	w := httptest.NewRecorder()
	b.ServeHTTP(w, newRequest("a"))

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
	}
	if got := a.runCalls.Load(); got != 1 {
		t.Errorf("runCalls=%d want 1", got)
	}
	if got := a.serveCalls.Load(); got != 1 {
		t.Errorf("serveCalls=%d want 1", got)
	}
}

func TestBaseRouter_StartGateRefusesSwap(t *testing.T) {
	a := newFakeProcess("a")
	a.autoReady = true

	b := newTestBase(t, map[string]process.Process{"a": a}, &stubPlanner{})
	b.SetStartGate(func() error { return swaputil.ErrInferencePaused })

	w := httptest.NewRecorder()
	b.ServeHTTP(w, newRequest("a"))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503; body=%q", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), swaputil.ErrInferencePaused.Error()) {
		t.Errorf("body=%q want the gate's error", w.Body.String())
	}
	if got := a.runCalls.Load(); got != 0 {
		t.Errorf("runCalls=%d want 0 while the gate refuses", got)
	}
	if got := a.serveCalls.Load(); got != 0 {
		t.Errorf("serveCalls=%d want 0 while the gate refuses", got)
	}
}

func TestBaseRouter_StartGateClearedAllowsSwap(t *testing.T) {
	a := newFakeProcess("a")
	a.autoReady = true

	b := newTestBase(t, map[string]process.Process{"a": a}, &stubPlanner{})
	b.SetStartGate(func() error { return swaputil.ErrInferencePaused })
	b.SetStartGate(nil)

	w := httptest.NewRecorder()
	b.ServeHTTP(w, newRequest("a"))

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
	}
	if got := a.runCalls.Load(); got != 1 {
		t.Errorf("runCalls=%d want 1", got)
	}
}

func TestBaseRouter_IgnoreWebsocketsRejectsModelUnlessReady(t *testing.T) {
	for _, state := range []process.ProcessState{process.StateStopped, process.StateStarting} {
		t.Run(string(state), func(t *testing.T) {
			a := newFakeProcess("a")
			if state != process.StateStopped {
				a.setState(state)
			}
			conf := config.Config{
				HealthCheckTimeout: 5,
				Models: map[string]config.ModelConfig{
					"a": {Compat: config.CompatConfig{IgnoreWebsockets: true}},
				},
			}
			b := newTestBaseWithConfig(t, conf, map[string]process.Process{"a": a}, &stubPlanner{})

			r := httptest.NewRequest(http.MethodGet, "/props?model=a", nil)
			r.Header.Set("Connection", "keep-alive, Upgrade")
			r.Header.Set("Upgrade", "websocket")
			w := httptest.NewRecorder()
			b.ServeHTTP(w, r)

			if w.Code != http.StatusConflict {
				t.Fatalf("status=%d want %d body=%q", w.Code, http.StatusConflict, w.Body.String())
			}
			if got := a.runCalls.Load(); got != 0 {
				t.Errorf("runCalls=%d want 0", got)
			}
			if got := a.serveCalls.Load(); got != 0 {
				t.Errorf("serveCalls=%d want 0", got)
			}
		})
	}
}

func TestBaseRouter_WebsocketStartsModelWhenCompatDisabled(t *testing.T) {
	a := newFakeProcess("a")
	a.autoReady = true
	conf := config.Config{
		HealthCheckTimeout: 5,
		Models:             map[string]config.ModelConfig{"a": {}},
	}
	b := newTestBaseWithConfig(t, conf, map[string]process.Process{"a": a}, &stubPlanner{})

	r := httptest.NewRequest(http.MethodGet, "/props?model=a", nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	w := httptest.NewRecorder()
	b.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want %d body=%q", w.Code, http.StatusOK, w.Body.String())
	}
	if got := a.runCalls.Load(); got != 1 {
		t.Errorf("runCalls=%d want 1", got)
	}
}

func TestBaseRouter_IgnoreWebsocketsDoesNotBlockSwap(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()
	a.serveBlock = make(chan struct{})
	pb := newFakeProcess("b")
	pb.autoReady = true
	conf := config.Config{
		HealthCheckTimeout: 5,
		Models: map[string]config.ModelConfig{
			"a": {Compat: config.CompatConfig{IgnoreWebsockets: true}},
			"b": {},
		},
	}
	b := newTestBaseWithConfig(t, conf, map[string]process.Process{"a": a, "b": pb}, &stubPlanner{
		evict: map[string][]string{"b": {"a"}},
	})

	websocketDone := make(chan struct{})
	go func() {
		defer close(websocketDone)
		r := httptest.NewRequest(http.MethodGet, "/props?model=a", nil)
		r.Header.Set("Connection", "Upgrade")
		r.Header.Set("Upgrade", "websocket")
		b.ServeHTTP(httptest.NewRecorder(), r)
	}()
	waitSignal(t, a.serveStarted, "websocket request start")

	w := httptest.NewRecorder()
	b.ServeHTTP(w, newRequest("b"))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
	}
	if !a.stoppedWhileServing.Load() {
		t.Fatal("ignored websocket prevented the conflicting model from swapping in")
	}

	close(a.serveBlock)
	waitSignal(t, websocketDone, "websocket request finish")
}

// TestBaseRouter_RequestDuringStop is the router-level regression test for
// issue #946. A process being stopped outside the router's knowledge (a TTL
// unload, a crash, an operator kill) must not wedge the swap machinery: the
// request has to wait for the stop to finish and then start the model.
//
// Before the fix doSwap read State(), saw StateStopping, skipped the start, and
// then subscribed to a process nobody would ever start — stranding the swap, so
// every later request for the model joined the same zombie swap and hung.
func TestBaseRouter_RequestDuringStop(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()
	a.autoReady = true
	// Pin Stop so the process sits in StateStopping while the request arrives.
	a.stopBlock = make(chan struct{})

	b := newTestBase(t, map[string]process.Process{"a": a}, &stubPlanner{})

	// Stop the process directly, the way the process's own TTL goroutine does —
	// the router is never told about it.
	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		_ = a.Stop(time.Second)
	}()
	waitSignal(t, a.stopStarted, "a.stopStarted")

	if got := a.State(); got != process.StateStopping {
		t.Fatalf("State()=%s want %s before request", got, process.StateStopping)
	}

	w := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		defer close(served)
		b.ServeHTTP(w, newRequest("a"))
	}()

	// The router must ask the process to start even though it is mid-stop, and
	// leave the process to decide when. The stop is still pinned here, so this
	// signal can only arrive from a start requested during StateStopping —
	// which is precisely what the old State()-then-Run code refused to do.
	waitSignal(t, a.ensureAsked, "a.ensureAsked")

	// Let the unload complete. The request must now start the model itself.
	close(a.stopBlock)
	<-stopDone

	select {
	case <-served:
	case <-t.Context().Done():
		t.Fatalf("request during stop never completed: %v", context.Cause(t.Context()))
	}

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
	}
	if got := a.runCalls.Load(); got != 1 {
		t.Errorf("runCalls=%d want 1 (model must be restarted after the unload)", got)
	}
	if got := a.serveCalls.Load(); got != 1 {
		t.Errorf("serveCalls=%d want 1", got)
	}
}

func TestBaseRouter_ContextCancel(t *testing.T) {
	a := newFakeProcess("a")
	// autoReady=false so swap parks forever until we mark ready.

	b := newTestBase(t, map[string]process.Process{"a": a}, &stubPlanner{})

	ctx, cancel := context.WithCancel(context.Background())
	w1 := httptest.NewRecorder()
	done1 := make(chan struct{})
	go func() {
		b.ServeHTTP(w1, newRequestCtx(ctx, "a"))
		close(done1)
	}()

	w2 := httptest.NewRecorder()
	done2 := make(chan struct{})
	go func() {
		b.ServeHTTP(w2, newRequest("a"))
		close(done2)
	}()

	waitProcessed(t, b.testProcessed, 2) // both requests joined the active swap
	<-a.runStarted

	cancel()
	select {
	case <-done1:
	case <-time.After(time.Second):
		t.Fatal("cancelled ServeHTTP did not return after ctx cancel")
	}

	a.markReady()
	select {
	case <-done2:
	case <-time.After(time.Second):
		t.Fatal("non-cancelled ServeHTTP did not complete after swap")
	}
	if w2.Code != http.StatusOK {
		t.Errorf("second request status=%d body=%q", w2.Code, w2.Body.String())
	}
}

func TestBaseRouter_ModelNotFound(t *testing.T) {
	a := newFakeProcess("a")
	b := newTestBase(t, map[string]process.Process{"a": a}, &stubPlanner{})

	w := httptest.NewRecorder()
	b.ServeHTTP(w, newRequest("unknown"))

	if w.Code != http.StatusNotFound {
		t.Errorf("status=%d want %d body=%q", w.Code, http.StatusNotFound, w.Body.String())
	}
}

func TestBaseRouter_ConcurrencyLimitRejectsBeforeLoadingStream(t *testing.T) {
	sendLoading := true
	// queueDepth=0 keeps over-limit requests rejected at admission (429)
	// rather than queued, which is the concurrency-limit semantics this test
	// exercises (the scheduler default of 10 would queue the 3rd request).
	qd := 0
	conf := config.Config{
		HealthCheckTimeout: 5,
		Routing: config.RoutingConfig{
			Scheduler: config.SchedulerConfig{
				Settings: config.SchedulerSettings{
					Fifo: config.FifoConfig{QueueDepth: &qd},
				},
			},
		},
		Models: map[string]config.ModelConfig{
			"a": {ConcurrencyLimit: 2, SendLoadingState: &sendLoading},
			"b": {},
		},
	}
	a := newFakeProcess("a")
	a.autoReady = true
	bProc := newFakeProcess("b")
	bProc.autoReady = true
	bProc.serveBlock = make(chan struct{})

	b := newTestBaseWithConfig(t, conf, map[string]process.Process{"a": a, "b": bProc}, &stubPlanner{
		evict: map[string][]string{"a": {"b"}},
	})

	bDone := make(chan struct{})
	go func() {
		b.ServeHTTP(httptest.NewRecorder(), newStreamRequest("b"))
		close(bDone)
	}()
	waitSignal(t, bProc.serveStarted, "b request start")
	waitProcessed(t, b.testProcessed, 2)

	aDone1 := make(chan struct{})
	aDone2 := make(chan struct{})
	go func() {
		b.ServeHTTP(httptest.NewRecorder(), newStreamRequest("a"))
		close(aDone1)
	}()
	go func() {
		b.ServeHTTP(httptest.NewRecorder(), newStreamRequest("a"))
		close(aDone2)
	}()
	waitProcessed(t, b.testProcessed, 2)

	w := httptest.NewRecorder()
	b.ServeHTTP(w, newStreamRequest("a"))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d want 429 body=%q", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type=%q want application/json", got)
	}
	if strings.Contains(w.Body.String(), "llama-swap loading model") {
		t.Fatalf("429 body contains loading stream: %q", w.Body.String())
	}
	// OpenAI clients read body["error"]["message"], so "error" must decode as
	// an object rather than a bare string.
	var envelope swaputil.ErrorEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("429 body is not an OpenAI error envelope: %v, body=%q", err, w.Body.String())
	}
	if envelope.Error.Message == "" || envelope.Error.Type != swaputil.ErrorTypeRateLimit {
		t.Fatalf("429 error=%+v, want a rate_limit_error with a message", envelope.Error)
	}

	close(bProc.serveBlock)
	for name, ch := range map[string]chan struct{}{"b": bDone, "a1": aDone1, "a2": aDone2} {
		waitSignal(t, ch, name+" request finish")
	}
}

// TestBaseRouter_DispatchErrorFramedIntoLoadingStream covers the second half of
// #1029. Once the loading stream has committed its 200, an error can only reach
// the client in-band: swaputil.SendError's status is dropped and its JSON body
// lands as a bare line that every SSE parser discards, leaving the caller with
// a truncated stream, no [DONE], and no reason.
func TestBaseRouter_DispatchErrorFramedIntoLoadingStream(t *testing.T) {
	sendLoading := true
	conf := config.Config{
		HealthCheckTimeout: 5,
		Models:             map[string]config.ModelConfig{"a": {SendLoadingState: &sendLoading}},
	}
	a := newFakeProcess("a")
	a.ensureErr = fmt.Errorf("upstream command exited prematurely")

	b := newTestBaseWithConfig(t, conf, map[string]process.Process{"a": a}, &stubPlanner{})

	w := httptest.NewRecorder()
	b.ServeHTTP(w, newStreamRequest("a"))

	body := w.Body.String()
	// The loading text is streamed a few characters per frame, so reassemble it.
	if content := extractStreamedContent(body); !strings.Contains(content, "llama-swap loading model") {
		t.Fatalf("loading stream did not start, so this is not the path under test: %q", content)
	}
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if line != "" && !strings.HasPrefix(line, "data: ") {
			t.Errorf("line %q is not an SSE field; a client would silently ignore it", line)
		}
	}
	if !strings.Contains(body, "upstream command exited prematurely") {
		t.Errorf("dispatch error never reached the client: %q", body)
	}
	if !strings.HasSuffix(strings.TrimRight(body, "\n"), "data: [DONE]") {
		t.Errorf("stream not terminated with [DONE]: %q", body)
	}
}

func TestBaseRouter_Shutdown_StopsAllProcesses(t *testing.T) {
	a := newFakeProcess("a")
	a.markReady()
	go a.Run(0)
	pb := newFakeProcess("b")
	pb.markReady()
	go pb.Run(0)

	b := newTestBase(t, map[string]process.Process{"a": a, "b": pb}, &stubPlanner{})

	if err := b.Shutdown(time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := a.stopCalls.Load(); got != 1 {
		t.Errorf("a.stopCalls=%d want 1", got)
	}
	if got := pb.stopCalls.Load(); got != 1 {
		t.Errorf("b.stopCalls=%d want 1", got)
	}

	// Subsequent ServeHTTP should report 5xx.
	w := httptest.NewRecorder()
	b.ServeHTTP(w, newRequest("a"))
	if w.Code != http.StatusInternalServerError && w.Code != http.StatusServiceUnavailable {
		t.Errorf("post-shutdown status=%d want 5xx body=%q", w.Code, w.Body.String())
	}

	// Second Shutdown should report already in progress.
	if err := b.Shutdown(0); err == nil {
		t.Errorf("second Shutdown returned nil, want error")
	}
}

func TestBaseRouter_SetsLocalHint(t *testing.T) {
	a := newFakeProcess("a")
	a.autoReady = true
	a.upstream = hint.VLLM

	b := newTestBase(t, map[string]process.Process{"a": a}, &stubPlanner{})

	ctx, h := hint.New(context.Background())
	w := httptest.NewRecorder()
	b.ServeHTTP(w, newRequest("a").WithContext(ctx))

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
	}
	if h.Route != hint.RouteLocal || h.PeerID != "" || h.Upstream != hint.VLLM {
		t.Errorf("hint = %+v, want local vllm", *h)
	}
}

// captureScheduler implements scheduler.Scheduler, capturing the HandlerReq it
// receives so tests can read the request context's metadata, and answering
// admission so ServeHTTP proceeds. It never grants: callers must cancel the
// request context to unblock ServeHTTP.
type captureScheduler struct {
	got chan scheduler.HandlerReq
}

func (s *captureScheduler) OnRequest(req scheduler.HandlerReq) {
	s.got <- req
	req.Admit <- nil
}
func (s *captureScheduler) OnCancel(scheduler.HandlerReq)        {}
func (s *captureScheduler) OnSwapDone(scheduler.SwapDone)        {}
func (s *captureScheduler) OnServeDone(scheduler.ServeDoneEvent) {}
func (s *captureScheduler) OnUnload([]string, time.Duration)     {}
func (s *captureScheduler) OnShutdown(error)                     {}
func (s *captureScheduler) QueueSnapshot() []scheduler.QueueEntry {
	return nil
}

// forwardingCapture wraps a fakeProcess and records the request it was asked to
// serve: the raw body, the model the process would validate, and the context.
// This is the exact point where a real backend checks the model name, so it is
// where the forwarded-model rewrite (trackedServe) is observable.
type forwardingCapture struct {
	*fakeProcess
	mu    sync.Mutex
	model string
	body  string
	ctx   context.Context
}

func newForwardingCapture(id string) *forwardingCapture {
	return &forwardingCapture{fakeProcess: newFakeProcess(id)}
}

func (c *forwardingCapture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err == nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	m, err := swaputil.ExtractModel(r)
	if err != nil {
		m = "<error: " + err.Error() + ">"
	}
	c.mu.Lock()
	c.model = m
	c.body = string(body)
	c.ctx = r.Context()
	c.mu.Unlock()
	c.fakeProcess.ServeHTTP(w, r)
}

func (c *forwardingCapture) modelSeen(t *testing.T) string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.model
}

func (c *forwardingCapture) bodySeen() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.body
}

func (c *forwardingCapture) contextSeen() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ctx
}

// TestBaseRouter_TrackedServe_RewritesForwardedModel verifies that when the
// serving model differs from the one the client asked for (fuzzy substitution,
// alias resolution), trackedServe rewrites the model field in the forwarded
// request before it reaches the process — otherwise a strict backend rejects it
// with "model does not exist". The capture lives in the process's ServeHTTP,
// the same point where a real upstream would validate the model field.
func TestBaseRouter_TrackedServe_RewritesForwardedModel(t *testing.T) {
	capture := newForwardingCapture("a")
	b := newTrackedServeBase(t, map[string]process.Process{"a": capture})

	// The client asked for "b"; the scheduler handed the request to process
	// "a" instead (a fuzzy rewrite decision that lives outside this test).
	// The request that reaches the forwarding chain must carry model "a".
	req := newRequest("b")
	req = req.WithContext(swaputil.SetContext(req.Context(), swaputil.ReqContextData{
		Model:    "b",
		ModelID:  "b",
		Metadata: make(map[string]string),
	}))
	w := httptest.NewRecorder()
	b.trackedServe("a", capture)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
	}
	if got := capture.modelSeen(t); got != "a" {
		t.Fatalf("model in forwarded request = %q, want %q", got, "a")
	}
	// ReplaceRequestModel invalidates the cached request context; the rewritten
	// request must keep the original one, since the defer and downstream
	// bookkeeping read InflightID/metadata from it.
	if _, ok := swaputil.ReadContext(capture.contextSeen()); !ok {
		t.Error("request context data lost after model rewrite")
	}
}

// TestBaseRouter_TrackedServe_NoRewriteWhenModelMatches verifies a direct send
// (client asked for exactly the serving model) forwards the body verbatim.
func TestBaseRouter_TrackedServe_NoRewriteWhenModelMatches(t *testing.T) {
	capture := newForwardingCapture("a")
	b := newTrackedServeBase(t, map[string]process.Process{"a": capture})

	body := `{"model":"a","prompt":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(swaputil.SetContext(req.Context(), swaputil.ReqContextData{
		Model:    "a",
		ModelID:  "a",
		Metadata: make(map[string]string),
	}))
	b.trackedServe("a", capture)(httptest.NewRecorder(), req)

	if got := capture.modelSeen(t); got != "a" {
		t.Fatalf("model in forwarded request = %q, want %q", got, "a")
	}
	if got := capture.bodySeen(); got != body {
		t.Fatalf("forwarded body rewritten: %q, want original %q", got, body)
	}
}

// fuzzyQwen and fuzzyAblit are the two members of the "qwen-forever" fuzzy group
// used by the integration tests below. The group is configured with fuzzy: true
// so a request for one member is served by the other when the group is busy or
// recently served.
const (
	fuzzyQwen  = "Qwen3.8-27B"
	fuzzyAblit = "Qwen3.8-27B-Abliterated"
)

// fuzzyGroupConfig builds the fuzzy swap group config shared by the Group-level
// integration tests, mirroring a user's "Abliterated already started" state.
func fuzzyGroupConfig() config.Config {
	return config.Config{
		HealthCheckTimeout: 5,
		Routing: config.RoutingConfig{
			Router: config.RouterConfig{
				Use: "group",
				Settings: config.RouterSettings{
					Groups: map[string]config.GroupConfig{
						"qwen-forever": {
							Swap:       true,
							Exclusive:  false,
							Persistent: true,
							Fuzzy:      true,
							Members:    []string{fuzzyQwen, fuzzyAblit},
						},
					},
				},
			},
		},
		Models: map[string]config.ModelConfig{
			fuzzyQwen:  {},
			fuzzyAblit: {},
		},
	}
}

// TestGroup_Fuzzy_ForwardedModelRewritten drives the full Group.ServeHTTP path:
// the scheduler fuzzy-substitutes a request for Qwen onto the loaded Abliterated,
// and trackedServe must rewrite the forwarded model field to the actually-serving
// model before it reaches the process. A backend that validates model names
// strictly would otherwise reject the request with "model does not exist".
func TestGroup_Fuzzy_ForwardedModelRewritten(t *testing.T) {
	conf := fuzzyGroupConfig()
	qwen := newFakeProcess(fuzzyQwen)
	qwen.autoReady = true
	ablit := newForwardingCapture(fuzzyAblit)
	ablit.autoReady = true
	ablit.markReady()

	g := newTestGroup(t, conf, map[string]process.Process{fuzzyQwen: qwen, fuzzyAblit: ablit})

	// Serve Abliterated once so the group is within its fuzzy idle window.
	w1 := httptest.NewRecorder()
	g.ServeHTTP(w1, newRequest(fuzzyAblit))
	if w1.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%q", w1.Code, w1.Body.String())
	}
	time.Sleep(50 * time.Millisecond)

	// Request Qwen: fuzzy substitutes it onto the loaded Abliterated.
	w2 := httptest.NewRecorder()
	g.ServeHTTP(w2, newRequest(fuzzyQwen))
	t.Logf("status=%d body=%q", w2.Code, w2.Body.String())

	if ablit.serveCalls.Load() != 2 {
		t.Fatalf("ablit.serveCalls=%d want 2 (first Abliterated + fuzzed Qwen)", ablit.serveCalls.Load())
	}
	got := ablit.modelSeen(t)
	t.Logf("forwarded model seen by process = %q", got)
	if got != fuzzyAblit {
		t.Errorf("forwarded model = %q, want %q (serving model); a strict backend would 404 'model does not exist'", got, fuzzyAblit)
	}
}

// TestGroup_Upstream_ExplicitBypassesFuzzy drives the full Group.ServeHTTP path
// with an explicit /upstream/<model>/ context marker: the scheduler must serve
// the named Qwen (loading it and stopping Abliterated) rather than fuzzing the
// request onto Abliterated, and never rewrite it onto another group member.
func TestGroup_Upstream_ExplicitBypassesFuzzy(t *testing.T) {
	conf := fuzzyGroupConfig()
	qwen := newFakeProcess(fuzzyQwen)
	qwen.autoReady = true
	ablit := newForwardingCapture(fuzzyAblit)
	ablit.autoReady = true
	ablit.markReady()

	g := newTestGroup(t, conf, map[string]process.Process{fuzzyQwen: qwen, fuzzyAblit: ablit})

	w1 := httptest.NewRecorder()
	g.ServeHTTP(w1, newRequest(fuzzyAblit))
	if w1.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%q", w1.Code, w1.Body.String())
	}
	time.Sleep(50 * time.Millisecond)

	// Simulate handleUpstream for /upstream/Qwen3.8-27B/: strip prefix, set the
	// context model and mark the target explicit, then local.ServeHTTP.
	req := httptest.NewRequest(http.MethodGet, "/upstream/"+fuzzyQwen+"/", nil)
	ctx := swaputil.SetContext(req.Context(), swaputil.ReqContextData{
		Model:    fuzzyQwen,
		ModelID:  fuzzyQwen,
		Metadata: make(map[string]string),
	})
	req = req.WithContext(swaputil.WithExplicitModel(ctx))
	w2 := httptest.NewRecorder()
	g.ServeHTTP(w2, req)
	t.Logf("status=%d body=%q", w2.Code, w2.Body.String())

	// Explicit target: Qwen is loaded and served, not fuzzed onto Abliterated.
	// Abliterated is stopped (same group, swap=true) and its serve count is
	// unchanged by the /upstream request.
	if ablit.serveCalls.Load() != 1 {
		t.Errorf("ablit.serveCalls=%d want 1 (only the first request; /upstream must not fuzz onto Abliterated)", ablit.serveCalls.Load())
	}
	if qwen.runCalls.Load() == 0 {
		t.Errorf("qwen.runCalls=%d want >0 (explicit /upstream must load Qwen3.8-27B)", qwen.runCalls.Load())
	}
	if w2.Body.String() != "ok:"+fuzzyQwen {
		t.Errorf("served body=%q want %q (explicit target serves the real Qwen3.8-27B)", w2.Body.String(), "ok:"+fuzzyQwen)
	}
}

// TestBaseRouter_IngressHeaderSnapshot drives the real ServeHTTP path and
// asserts the six ingress request headers land in ReqContextData.Metadata
// under their snake_case keys. It covers the full matrix requested by the
// feature: all six headers present (with the X-Forwarded-For proxy chain kept
// whole), no headers present, a subset, and headers whose value is empty.
// Missing/empty headers must leave no metadata key behind.
func TestBaseRouter_IngressHeaderSnapshot(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
		want    map[string]string // metadata key → value; absent key means "not present"
	}{
		{
			name: "all six headers snapshotted verbatim",
			headers: map[string]string{
				"User-Agent":         "llama-bot/1.0",
				"X-Forwarded-For":    "203.0.113.9, 198.51.100.2",
				"X-Forwarded-Host":   "api.example.com",
				"X-Forwarded-Proto":  "https",
				"X-Real-Ip":          "203.0.113.9",
				"X-Request-Priority": "high",
			},
			want: map[string]string{
				"user_agent":         "llama-bot/1.0",
				"x_forwarded_for":    "203.0.113.9, 198.51.100.2", // full chain, not first hop
				"x_forwarded_host":   "api.example.com",
				"x_forwarded_proto":  "https",
				"x_real_ip":          "203.0.113.9",
				"x_request_priority": "high",
			},
		},
		{
			name:    "no headers write no keys",
			headers: nil,
			want:    map[string]string{},
		},
		{
			name: "missing headers leave no key",
			headers: map[string]string{
				"User-Agent": "curl/8.0",
				"X-Real-Ip":  "198.51.100.7",
			},
			want: map[string]string{
				"user_agent": "curl/8.0",
				"x_real_ip":  "198.51.100.7",
			},
		},
		{
			name: "empty header value writes no key",
			headers: map[string]string{
				"X-Forwarded-For":    "",
				"X-Request-Priority": "",
			},
			want: map[string]string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := config.Config{HealthCheckTimeout: 5, Audit: config.AuditConfig{RequestHeaders: true}}
			conf.Routing.Scheduler.Settings.Fifo.PriorityHeader = "X-Request-Priority"
			b := newTestBaseWithConfig(t, conf, nil, &stubPlanner{})
			cap := &captureScheduler{got: make(chan scheduler.HandlerReq, 1)}
			b.schedule = cap

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := newRequest("a").WithContext(ctx)
			for name, value := range tc.headers {
				r.Header.Set(name, value)
			}

			serveDone := make(chan struct{})
			go func() {
				b.ServeHTTP(httptest.NewRecorder(), r)
				close(serveDone)
			}()

			var req scheduler.HandlerReq
			select {
			case req = <-cap.got:
			case <-time.After(2 * time.Second):
				t.Fatal("scheduler never received the request")
			}

			data, ok := swaputil.ReadContext(req.Ctx)
			if !ok {
				t.Fatal("request context data missing")
			}
			for _, h := range ingressHeaderSnapshots {
				want, wantPresent := tc.want[h.key]
				got, gotPresent := data.Metadata[h.key]
				switch {
				case !wantPresent && gotPresent:
					t.Errorf("metadata[%q]=%q, want no key", h.key, got)
				case wantPresent && !gotPresent:
					t.Errorf("metadata[%q] missing, want %q", h.key, want)
				case wantPresent && got != want:
					t.Errorf("metadata[%q]=%q, want %q", h.key, got, want)
				}
			}

			cancel()
			select {
			case <-serveDone:
			case <-time.After(time.Second):
				t.Fatal("ServeHTTP did not return after context cancel")
			}
		})
	}
}

// TestBaseRouter_IngressHeaderSnapshot_MultiLineXFF covers HTTP/1.1's
// multi-line delivery of the same header name for X-Forwarded-For, which some
// proxies emit. Header.Get alone would silently keep only the first line and
// drop later hops — this test pins the join behavior: every line is kept, in
// order, joined by ", " (probed: Header.Get returns "10.0.0.1, 10.0.0.2" here,
// so the old first-line-only implementation would fail this assertion).
func TestBaseRouter_IngressHeaderSnapshot_MultiLineXFF(t *testing.T) {
	conf := config.Config{HealthCheckTimeout: 5, Audit: config.AuditConfig{RequestHeaders: true}}
	b := newTestBaseWithConfig(t, conf, nil, &stubPlanner{})
	cap := &captureScheduler{got: make(chan scheduler.HandlerReq, 1)}
	b.schedule = cap

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newRequest("a").WithContext(ctx)
	r.Header.Add("X-Forwarded-For", "10.0.0.1, 10.0.0.2")
	r.Header.Add("X-Forwarded-For", "172.16.0.1")

	serveDone := make(chan struct{})
	go func() {
		b.ServeHTTP(httptest.NewRecorder(), r)
		close(serveDone)
	}()

	var req scheduler.HandlerReq
	select {
	case req = <-cap.got:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler never received the request")
	}

	data, ok := swaputil.ReadContext(req.Ctx)
	if !ok {
		t.Fatal("request context data missing")
	}
	if got := data.Metadata["x_forwarded_for"]; got != "10.0.0.1, 10.0.0.2, 172.16.0.1" {
		t.Errorf("x_forwarded_for=%q, want %q (full multi-line chain)", got, "10.0.0.1, 10.0.0.2, 172.16.0.1")
	}

	cancel()
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("ServeHTTP did not return after context cancel")
	}
}

// TestBaseRouter_IngressHeaderSnapshot_CustomPriorityHeader pins that the
// x_request_priority snapshot follows the configured fifo.priorityHeader, not
// a fixed literal. With a custom header name the scheduler resolves that name
// (base.go), so the audit snapshot must read the same header: a value on the
// literal X-Request-Priority is ignored, and the value on the custom header is
// recorded.
func TestBaseRouter_IngressHeaderSnapshot_CustomPriorityHeader(t *testing.T) {
	conf := config.Config{HealthCheckTimeout: 5, Audit: config.AuditConfig{RequestHeaders: true}}
	conf.Routing.Scheduler.Settings.Fifo.PriorityHeader = "X-Priority"
	b := newTestBaseWithConfig(t, conf, nil, &stubPlanner{})
	cap := &captureScheduler{got: make(chan scheduler.HandlerReq, 1)}
	b.schedule = cap

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newRequest("a").WithContext(ctx)
	r.Header.Set("X-Priority", "high")
	r.Header.Set("X-Request-Priority", "low")

	serveDone := make(chan struct{})
	go func() {
		b.ServeHTTP(httptest.NewRecorder(), r)
		close(serveDone)
	}()

	var req scheduler.HandlerReq
	select {
	case req = <-cap.got:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler never received the request")
	}

	data, ok := swaputil.ReadContext(req.Ctx)
	if !ok {
		t.Fatal("request context data missing")
	}
	if got := data.Metadata["x_request_priority"]; got != "high" {
		t.Errorf("x_request_priority=%q, want %q from the configured X-Priority header", got, "high")
	}

	cancel()
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("ServeHTTP did not return after context cancel")
	}
}

// TestBaseRouter_IngressHeaderSnapshot_Disabled pins the audit.requestHeaders
// default (off): with the default config (switch unset) ServeHTTP must not run
// the ingress header snapshot, so none of the six snake_case keys appear in the
// request metadata even when every header is present.
func TestBaseRouter_IngressHeaderSnapshot_Disabled(t *testing.T) {
	b := newTestBase(t, nil, &stubPlanner{})
	cap := &captureScheduler{got: make(chan scheduler.HandlerReq, 1)}
	b.schedule = cap

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newRequest("a").WithContext(ctx)
	r.Header.Set("User-Agent", "llama-bot/1.0")
	r.Header.Set("X-Forwarded-For", "203.0.113.9, 198.51.100.2")
	r.Header.Set("X-Forwarded-Host", "api.example.com")
	r.Header.Set("X-Forwarded-Proto", "https")
	r.Header.Set("X-Real-Ip", "203.0.113.9")
	r.Header.Set("X-Request-Priority", "high")

	serveDone := make(chan struct{})
	go func() {
		b.ServeHTTP(httptest.NewRecorder(), r)
		close(serveDone)
	}()

	var req scheduler.HandlerReq
	select {
	case req = <-cap.got:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler never received the request")
	}

	data, ok := swaputil.ReadContext(req.Ctx)
	if !ok {
		t.Fatal("request context data missing")
	}
	for _, h := range ingressHeaderSnapshots {
		if got, present := data.Metadata[h.key]; present {
			t.Errorf("metadata[%q]=%q present with audit.requestHeaders disabled, want no key", h.key, got)
		}
	}

	cancel()
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("ServeHTTP did not return after context cancel")
	}
}
