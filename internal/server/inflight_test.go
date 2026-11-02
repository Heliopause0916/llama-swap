package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// TestServer_InflightServedModelMetadataRefresh verifies that a served_model
// key written to the shared request metadata after Add (the scheduler writes it
// at grant time, which is later than the Add snapshot) is surfaced into the
// inflight entry by the snapshot sync.
func TestServer_InflightServedModelMetadataRefresh(t *testing.T) {
	tracker := newInflightTracker()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req = req.WithContext(swaputil.SetContext(req.Context(), swaputil.ReqContextData{
		ModelID:  "m1",
		Metadata: map[string]string{},
	}))
	tracker.Add(req, func() {})

	// The scheduler writes served_model at grant time, after Add captured the
	// metadata snapshot. It lands in the shared context metadata map.
	if err := swaputil.SetReqData(req.Context(), "served_model", "a"); err != nil {
		t.Fatalf("SetReqData: %v", err)
	}

	current := tracker.Current()
	if len(current.Requests) != 1 {
		t.Fatalf("inflight requests = %d, want 1", len(current.Requests))
	}
	if got := current.Requests[0].Metadata["served_model"]; got != "a" {
		t.Fatalf("served_model = %q, want a", got)
	}
}

// TestServer_InflightPriorityFromMiddleware verifies that CreateRequestContextMiddleware
// resolves the request-priority header and writes the band into the request
// metadata, where the inflight Priority column reads it.
func TestServer_InflightPriorityFromMiddleware(t *testing.T) {
	cfg := config.Config{
		Routing: config.RoutingConfig{
			Scheduler: config.SchedulerConfig{
				Settings: config.SchedulerSettings{
					Fifo: config.FifoConfig{
						PriorityHeader:  "X-Request-Priority",
						RequestPriority: map[string]int{"high": 100},
						DefaultPriority: 60,
					},
				},
			},
		},
	}

	var got swaputil.ReqContextData
	handler := CreateRequestContextMiddleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, ok := swaputil.ReadContext(r.Context())
		if !ok {
			t.Fatalf("no request context data")
		}
		got = data
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req = req.WithContext(swaputil.SetContext(req.Context(), swaputil.ReqContextData{
		ModelID:  "m1",
		Metadata: map[string]string{},
	}))
	req.Header.Set("X-Request-Priority", "high")

	handler.ServeHTTP(httptest.NewRecorder(), req)

	if got.Metadata["priority"] != "100" {
		t.Fatalf("priority metadata = %q, want 100", got.Metadata["priority"])
	}
}

// TestServer_InflightStageMiddlewareInjectsID verifies CreateInflightMiddleware
// makes the Add() id retrievable from the downstream handler's request context
// via swaputil.InflightID.
func TestServer_InflightStageMiddlewareInjectsID(t *testing.T) {
	tracker := newInflightTracker()
	mw := CreateInflightMiddleware(tracker, config.Config{})

	var gotID string
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := swaputil.InflightID(r.Context())
		if !ok {
			t.Fatal("InflightID not present in downstream context")
		}
		gotID = id
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req = req.WithContext(swaputil.SetContext(req.Context(), swaputil.ReqContextData{ModelID: "m1"}))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if gotID == "" {
		t.Fatal("middleware did not inject a non-empty inflight id")
	}
}
