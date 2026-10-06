package router

// Tests for the forwarding-chain model rewrite in trackedServe
// (internal/router/base.go): when the client asked for a model that differs
// from the model actually serving the request (fuzzy substitution, alias
// resolution), the request handed to the process must carry the serving
// model's name, or a strict backend rejects it with "model does not exist".
// The capture lives in the process's ServeHTTP — the same point where a real
// upstream would validate the model field.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// newTrackedServeBase builds a baseRouter whose scheduler ignores serve
// events, so trackedServe can be driven directly without running a real
// admission (the FIFO's OnServeDone would otherwise panic on a model that
// never held a reservation). The scheduler is swapped before run() starts.
func newTrackedServeBase(t *testing.T, processes map[string]process.Process) *baseRouter {
	t.Helper()
	b, err := newBaseRouter("test", config.Config{HealthCheckTimeout: 5}, processes,
		logmon.NewWriter(io.Discard), &stubPlanner{})
	if err != nil {
		t.Fatalf("newBaseRouter: %v", err)
	}
	b.schedule = &stubQueueScheduler{}
	b.testProcessed = make(chan struct{}, 64)
	go b.run()
	t.Cleanup(func() {
		if !b.shuttingDown.Load() {
			_ = b.Shutdown(time.Second)
		}
	})
	return b
}

// forwardingCapture wraps a fakeProcess and records the request it was asked
// to serve: the raw body (so tests can assert what the backend truly
// received) and the request context (to verify the original ctx survives the
// rewrite). The inner fakeProcess then serves exactly like the real one.
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
	// Identify the model as the backend would: the body for POSTs, the query
	// string for GETs. This runs on the exact request handed to the process,
	// keeping the capture point at the forwarding chain.
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

// contextSnap returns a copy of the context observed by the process.
func (c *forwardingCapture) contextSeen() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ctx
}

// bodySeen returns the raw body observed by the process.
func (c *forwardingCapture) bodySeen() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.body
}

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

	// ReplaceRequestModel invalidates the cached request context; the
	// rewritten request must keep the original one, since the defer and
	// downstream bookkeeping read InflightID/metadata from it.
	if _, ok := swaputil.ReadContext(capture.contextSeen()); !ok {
		t.Error("request context data lost after model rewrite")
	}
}

func TestBaseRouter_TrackedServe_RewritesForwardedQueryModel(t *testing.T) {
	capture := newForwardingCapture("a")
	b := newTrackedServeBase(t, map[string]process.Process{"a": capture})

	// GET requests carry the model in the query string; the rewrite must hit
	// the query too.
	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions?model=b&stream=true", nil)
	req = req.WithContext(swaputil.SetContext(req.Context(), swaputil.ReqContextData{
		Model:    "b",
		ModelID:  "b",
		Metadata: make(map[string]string),
	}))
	b.trackedServe("a", capture)(httptest.NewRecorder(), req)

	if got := capture.modelSeen(t); got != "a" {
		t.Fatalf("model in forwarded request = %q, want %q", got, "a")
	}
}

func TestBaseRouter_TrackedServe_NoRewriteWhenModelMatches(t *testing.T) {
	capture := newForwardingCapture("a")
	b := newTrackedServeBase(t, map[string]process.Process{"a": capture})

	// Direct send: the client asked for exactly the serving model, so the
	// forwarded body must be untouched.
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

func TestBaseRouter_TrackedServe_KeepsBodyWhenContextModelDiffers(t *testing.T) {
	capture := newForwardingCapture("a")
	b := newTrackedServeBase(t, map[string]process.Process{"a": capture})

	// Edge case shared with Peer.ServeHTTP: the context's model ("b", e.g.
	// resolved from an /upstream/ path or a middleware) disagrees with the
	// model the body actually carries ("a"). ReplaceRequestModel skips the
	// rewrite (current != model) and returns the request unchanged; the body
	// is forwarded verbatim and no error is raised.
	body := `{"model":"a","prompt":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(swaputil.SetContext(req.Context(), swaputil.ReqContextData{
		Model:    "b",
		ModelID:  "b",
		Metadata: make(map[string]string),
	}))
	b.trackedServe("a", capture)(httptest.NewRecorder(), req)

	if got := capture.bodySeen(); got != body {
		t.Fatalf("forwarded body = %q, want original %q", got, body)
	}
	if _, ok := swaputil.ReadContext(capture.contextSeen()); !ok {
		t.Error("request context data lost after skipped rewrite")
	}
}

func TestGroup_ServeHTTP_FuzzyRewritesForwardedModel(t *testing.T) {
	captureA := newForwardingCapture("a")
	captureA.markReady()
	b := newFakeProcess("b")

	conf := config.Config{
		HealthCheckTimeout: 5,
		Routing: groupRouting(map[string]config.GroupConfig{
			"g": {Swap: true, Fuzzy: true, Members: []string{"a", "b"}},
		}),
		Models: map[string]config.ModelConfig{
			"a": {},
			"b": {},
		},
	}
	g := newTestGroup(t, conf, map[string]process.Process{"a": captureA, "b": b})

	// First request for the online member "a": served directly and completes
	// the group's busy state, starting the fuzzy idle window.
	w1 := httptest.NewRecorder()
	g.ServeHTTP(w1, newRequest("a"))
	if w1.Code != http.StatusOK {
		t.Fatalf("first request status=%d body=%q", w1.Code, w1.Body.String())
	}
	if got := captureA.modelSeen(t); got != "a" {
		t.Fatalf("first request forwarded model = %q, want %q", got, "a")
	}

	// Second request for "b" inside the idle window: fuzzy substitution serves
	// it with the online member "a". The request actually forwarded to the
	// process must carry model "a" — a strict backend would otherwise return
	// 404 for "b" — and "b" must never be started.
	w2 := httptest.NewRecorder()
	g.ServeHTTP(w2, newRequest("b"))
	if w2.Code != http.StatusOK {
		t.Fatalf("fuzzy request status=%d body=%q", w2.Code, w2.Body.String())
	}
	if got := captureA.modelSeen(t); got != "a" {
		t.Fatalf("fuzzy request forwarded model = %q, want %q", got, "a")
	}
	if got := b.serveCalls.Load(); got != 0 {
		t.Errorf("b.serveCalls=%d want 0 (fuzzy must not start b)", got)
	}
	if got := b.runCalls.Load(); got != 0 {
		t.Errorf("b.runCalls=%d want 0 (fuzzy must not start b)", got)
	}

	// The client's originally requested name stays in the forwarded request's
	// context (it is only the model field in the body that is rewritten).
	data, ok := swaputil.ReadContext(captureA.contextSeen())
	if !ok {
		t.Fatal("fuzzy request context data missing")
	}
	if data.Model != "b" {
		t.Errorf("forwarded request context model = %q, want %q (client-asked name)", data.Model, "b")
	}
}

func TestGroup_ServeHTTP_AliasRewritesForwardedModel(t *testing.T) {
	captureA := newForwardingCapture("a")
	captureA.markReady()

	// The alias map is private to config.Config, so build the configuration
	// the same way production does: load the YAML, then swap in fake
	// processes for the router machinery.
	conf, err := config.LoadConfigFromReader(strings.NewReader(`
models:
  a:
    proxy: "http://127.0.0.1:9999"
    aliases: [alias]
routing:
  router:
    use: group
    settings:
      groups:
        g:
          members: [a]
`))
	if err != nil {
		t.Fatal(err)
	}
	g := newTestGroup(t, conf, map[string]process.Process{"a": captureA})

	// Client asks for the alias; the forwarding chain must carry the real
	// model name, or the backend 404s on "alias".
	w := httptest.NewRecorder()
	g.ServeHTTP(w, newRequest("alias"))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
	}
	if got := captureA.modelSeen(t); got != "a" {
		t.Fatalf("model in forwarded request = %q, want %q", got, "a")
	}
}
