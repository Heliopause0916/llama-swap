package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/router"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// TestServer_InflightMiddlewareStampsInflightID verifies CreateInflightMiddleware
// carries the tracker's request ID in the downstream request context via
// swaputil.WithInflightID, which is what the scheduler queue snapshot matches
// on later.
func TestServer_InflightMiddlewareStampsInflightID(t *testing.T) {
	tracker := newInflightTrackerWithPublisher(16, func(swaputil.InFlightRequestsEvent) {})

	// The entry is read inside the handler: by the time ServeHTTP returns,
	// the middleware's deferred Remove has already dropped the entry.
	var gotID string
	var captured *swaputil.InflightRequestEntry
	mw := CreateInflightMiddleware(tracker, config.Config{})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID, _ = swaputil.InflightID(r.Context())
		tracker.mu.RLock()
		if req, ok := tracker.requests[gotID]; ok {
			entry := req.entry
			captured = &entry
		}
		tracker.mu.RUnlock()
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if gotID == "" {
		t.Fatal("downstream request context carried no inflight ID")
	}
	if captured == nil {
		t.Fatalf("tracked request %q not found in tracker", gotID)
	}

	// The tracked entry starts out serving with no queue position.
	if captured.Stage != "serving" {
		t.Errorf("initial stage=%q want serving", captured.Stage)
	}
	if captured.QueuePosition != 0 {
		t.Errorf("initial queue_position=%d want 0", captured.QueuePosition)
	}
}

// TestServer_InflightStageUpdate verifies updateStages flips tracked requests
// between "queued"/"serving" (with 1-indexed queue positions) according to the
// injected scheduler snapshot and only pushes an upsert event when the stage
// actually changes.
//
// updateStages is invoked directly rather than waiting for the 500ms
// stage-UpdateInterval ticker, and entries are inserted without Add so the
// real stage-updater goroutine never races this test's event accounting.
func TestServer_InflightStageUpdate(t *testing.T) {
	positions := map[string]int{}
	queueSnapshot := func() []router.QueueInfo {
		out := make([]router.QueueInfo, 0, len(positions))
		for id, pos := range positions {
			out = append(out, router.QueueInfo{RequestID: id, Model: "m", QueuePosition: pos})
		}
		return out
	}

	var mu sync.Mutex
	var stages []string
	tracker := newInflightTrackerWithPublisher(64, func(u swaputil.InFlightRequestsEvent) {
		if u.Operation == inflightOperationUpsert && u.Request != nil {
			mu.Lock()
			stages = append(stages, u.Request.Stage)
			mu.Unlock()
		}
	})
	tracker.queueSnapshot = queueSnapshot

	// Seed one tracked entry directly (no Add) so the stage-updater goroutine
	// is never launched by an Add and cannot add stray events to the count.
	// The entry starts serving, matching Add's initialization.
	const id = "1"
	tracker.requests[id] = &inflightRequest{
		entry: swaputil.InflightRequestEntry{ID: id, Stage: "serving"},
	}

	// 1. Not in the queue snapshot: still serving, no event.
	if !tracker.updateStages() {
		t.Fatal("updateStages()=false want true while requests are tracked")
	}
	if got := tracker.requests[id].entry.Stage; got != "serving" {
		t.Fatalf("stage=%q want serving when absent from snapshot", got)
	}
	waitForStageEvents(t, &mu, &stages, 0)

	// 2. Appears in the queue: queued at position 3, one event.
	positions[id] = 3
	tracker.updateStages()
	if got := tracker.requests[id].entry.Stage; got != "queued" {
		t.Fatalf("stage=%q want queued", got)
	}
	if got := tracker.requests[id].entry.QueuePosition; got != 3 {
		t.Fatalf("queue_position=%d want 3", got)
	}
	waitForStageEvents(t, &mu, &stages, 1)

	// 3. Same position, repeated poll: no change, no new event.
	tracker.updateStages()
	waitForStageEvents(t, &mu, &stages, 1)

	// 4. Leaves the queue: serving again, one event.
	delete(positions, id)
	tracker.updateStages()
	if got := tracker.requests[id].entry.Stage; got != "serving" {
		t.Fatalf("stage=%q want serving after leaving queue", got)
	}
	if got := tracker.requests[id].entry.QueuePosition; got != 0 {
		t.Fatalf("queue_position=%d want 0 after leaving queue", got)
	}
	waitForStageEvents(t, &mu, &stages, 2)

	mu.Lock()
	got := append([]string(nil), stages...)
	mu.Unlock()
	want := []string{"queued", "serving"}
	if len(got) != len(want) {
		t.Fatalf("stage events=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stage events=%v want %v", got, want)
		}
	}
}

// TestServer_InflightStageUpdateExitConditions verifies updateStages returns
// false (telling the updater goroutine to exit) when there is no queue
// snapshot source or no tracked request.
func TestServer_InflightStageUpdateExitConditions(t *testing.T) {
	tracker := newInflightTrackerWithPublisher(8, func(swaputil.InFlightRequestsEvent) {})

	if tracker.updateStages() {
		t.Error("updateStages()=true want false when queueSnapshot is nil")
	}

	tracker.queueSnapshot = func() []router.QueueInfo { return nil }
	if tracker.updateStages() {
		t.Error("updateStages()=true want false when no requests are tracked")
	}
}

// TestServer_InflightStageUpdaterNoSnapshotNoGoroutine verifies the L3 guard:
// with no queue snapshot source wired (queueDepth=0 or a non-FIFO scheduler),
// startStageUpdater must not launch a poller goroutine, because updateStages
// would always bail out on the nil snapshot.
func TestServer_InflightStageUpdaterNoSnapshotNoGoroutine(t *testing.T) {
	tracker := newInflightTrackerWithPublisher(8, func(swaputil.InFlightRequestsEvent) {})
	tracker.startStageUpdater() // queueSnapshot == nil
	if tracker.stageUpdaterRunning.Load() {
		t.Error("stage updater started without a queue snapshot source")
	}
}

// TestServer_InflightStageUpdaterReclaimsIdleSlot verifies the H1 idle-exit
// defense: a request added during the poller's exit window (when its
// stageUpdaterRunning flag is still true, so Add's startStageUpdater CAS
// loses) must be picked up by the exiting poller, which releases the slot and
// re-claims it so the request does not get stuck on "serving" forever.
func TestServer_InflightStageUpdaterReclaimsIdleSlot(t *testing.T) {
	positions := map[string]int{}
	tracker := newInflightTrackerWithPublisher(16, func(swaputil.InFlightRequestsEvent) {})
	tracker.queueSnapshot = func() []router.QueueInfo {
		out := make([]router.QueueInfo, 0, len(positions))
		for id, pos := range positions {
			out = append(out, router.QueueInfo{RequestID: id, Model: "m", QueuePosition: pos})
		}
		return out
	}

	const id = "1"
	// Simulate the exit window: the poller still holds the running flag, and
	// an Add slips in. In the real race the Add's startStageUpdater CAS fails
	// against that flag, so the entry lands in requests with no poller behind
	// it. Seed the entry directly to reproduce that state.
	tracker.mu.Lock()
	tracker.requests[id] = &inflightRequest{
		entry: swaputil.InflightRequestEntry{ID: id, Stage: "serving"},
	}
	tracker.mu.Unlock()

	// Set the snapshot before the poller starts: queueSnapshot runs on the
	// poller goroutine, so the positions map must be stable once it runs.
	positions[id] = 1

	// The exiting poller releases the flag and must reclaim the slot because a
	// request arrived during the exit window.
	tracker.stageUpdaterRunning.Store(true)
	tracker.reclaimStageUpdater()

	if !tracker.stageUpdaterRunning.Load() {
		t.Fatal("stage updater did not reclaim its running slot after an Add raced the idle exit")
	}

	// The reclaimed poller observes the request in the queue snapshot and
	// flips it to queued.
	deadline := time.Now().Add(3 * time.Second)
	for {
		tracker.mu.RLock()
		stage := tracker.requests[id].entry.Stage
		pos := tracker.requests[id].entry.QueuePosition
		tracker.mu.RUnlock()
		if stage == "queued" {
			if pos != 1 {
				t.Fatalf("queue_position=%d want 1", pos)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reclaimed stage updater never marked raced request queued (stage=%q)", stage)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Let the poller exit cleanly so no goroutine outlives the test.
	tracker.mu.Lock()
	delete(tracker.requests, id)
	tracker.mu.Unlock()
}

// TestServer_InflightPriorityMetadata_HeaderBand verifies that with request
// priority enabled, CreateInflightMiddleware resolves the X-Request-Priority
// band header and writes it into the request metadata BEFORE Add snapshots it,
// so the tracked entry's Metadata carries "priority" = band value. The write is
// display-only: it feeds the UI Priority column and never affects scheduling.
func TestServer_InflightPriorityMetadata_HeaderBand(t *testing.T) {
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
	tracker := newInflightTrackerWithPublisher(16, func(swaputil.InFlightRequestsEvent) {})
	var captured *swaputil.InflightRequestEntry
	mw := CreateInflightMiddleware(tracker, cfg)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID, _ := swaputil.InflightID(r.Context())
		tracker.mu.RLock()
		if req, ok := tracker.requests[gotID]; ok {
			entry := req.entry
			captured = &entry
		}
		tracker.mu.RUnlock()
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Request-Priority", "high")
	// The real chain seeds ReqContextData (with a metadata map) before the
	// inflight middleware; mirror that so SetReqData has a map to write into.
	req = req.WithContext(swaputil.SetContext(req.Context(),
		swaputil.ReqContextData{Model: "m1", ModelID: "m1", Metadata: make(map[string]string)}))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if captured == nil {
		t.Fatal("tracked request not found in tracker")
	}
	if got := captured.Metadata["priority"]; got != "100" {
		t.Fatalf("metadata.priority=%q want \"100\"", got)
	}
}

// TestServer_InflightPriorityMetadata_FeatureOff verifies that when request
// priority is disabled (empty RequestPriority map) the inflight middleware
// writes no "priority" metadata key, so the UI Priority column renders "—"
// for every row (feature off is indistinguishable from "unset").
func TestServer_InflightPriorityMetadata_FeatureOff(t *testing.T) {
	tracker := newInflightTrackerWithPublisher(16, func(swaputil.InFlightRequestsEvent) {})
	var captured *swaputil.InflightRequestEntry
	mw := CreateInflightMiddleware(tracker, config.Config{}) // RequestPriority nil
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID, _ := swaputil.InflightID(r.Context())
		tracker.mu.RLock()
		if req, ok := tracker.requests[gotID]; ok {
			entry := req.entry
			captured = &entry
		}
		tracker.mu.RUnlock()
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Request-Priority", "high")
	req = req.WithContext(swaputil.SetContext(req.Context(),
		swaputil.ReqContextData{Model: "m1", ModelID: "m1", Metadata: make(map[string]string)}))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if captured == nil {
		t.Fatal("tracked request not found in tracker")
	}
	if _, ok := captured.Metadata["priority"]; ok {
		t.Fatalf("metadata.priority present for feature-off config: %v", captured.Metadata)
	}
}

// TestServer_InflightPriorityMetadata_HeaderMissingDefaultsToDefault verifies
// the display path's branch C: with request priority enabled but no
// X-Request-Priority header, the middleware resolves the default band and
// stamps it into the tracked entry's Metadata as strconv of DefaultPriority.
func TestServer_InflightPriorityMetadata_HeaderMissingDefaultsToDefault(t *testing.T) {
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
	tracker := newInflightTrackerWithPublisher(16, func(swaputil.InFlightRequestsEvent) {})
	var captured *swaputil.InflightRequestEntry
	mw := CreateInflightMiddleware(tracker, cfg)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID, _ := swaputil.InflightID(r.Context())
		tracker.mu.RLock()
		if req, ok := tracker.requests[gotID]; ok {
			entry := req.entry
			captured = &entry
		}
		tracker.mu.RUnlock()
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil) // no priority header
	req = req.WithContext(swaputil.SetContext(req.Context(),
		swaputil.ReqContextData{Model: "m1", ModelID: "m1", Metadata: make(map[string]string)}))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if captured == nil {
		t.Fatal("tracked request not found in tracker")
	}
	if got := captured.Metadata["priority"]; got != "60" {
		t.Fatalf("metadata.priority=%q want \"60\" (default band)", got)
	}
}

// TestServer_InflightPriorityMetadata_BlankHeaderFallsBackToDefault verifies
// the display path's branch B': a blank/whitespace X-Request-Priority header
// resolves to the default band (returned silently by the display path, which
// passes a nil warnOnce) and is stamped into the tracked entry's Metadata.
func TestServer_InflightPriorityMetadata_BlankHeaderFallsBackToDefault(t *testing.T) {
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
	tracker := newInflightTrackerWithPublisher(16, func(swaputil.InFlightRequestsEvent) {})
	var captured *swaputil.InflightRequestEntry
	mw := CreateInflightMiddleware(tracker, cfg)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID, _ := swaputil.InflightID(r.Context())
		tracker.mu.RLock()
		if req, ok := tracker.requests[gotID]; ok {
			entry := req.entry
			captured = &entry
		}
		tracker.mu.RUnlock()
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Request-Priority", "   ") // branch B′: whitespace only
	req = req.WithContext(swaputil.SetContext(req.Context(),
		swaputil.ReqContextData{Model: "m1", ModelID: "m1", Metadata: make(map[string]string)}))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if captured == nil {
		t.Fatal("tracked request not found in tracker")
	}
	if got := captured.Metadata["priority"]; got != "60" {
		t.Fatalf("metadata.priority=%q want \"60\" (default band after B′)", got)
	}
}

// waitForStageEvents blocks until at least n stage events have been published
// (the publish callback runs on the tracker's async publisher goroutine).
func waitForStageEvents(t *testing.T, mu *sync.Mutex, stages *[]string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		got := len(*stages)
		mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d stage events, got %d", n, got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestServer_InflightServedModelMetadataRefresh verifies that served_model,
// written into the shared request metadata map after Add snapshots the entry,
// is surfaced onto the tracked entry (with an upsert event) by the next
// updateStages tick.
func TestServer_InflightServedModelMetadataRefresh(t *testing.T) {
	var mu sync.Mutex
	var upsertCount int
	var upsertMeta map[string]string
	tracker := newInflightTrackerWithPublisher(16, func(u swaputil.InFlightRequestsEvent) {
		if u.Operation == inflightOperationUpsert && u.Request != nil {
			mu.Lock()
			upsertCount++
			upsertMeta = u.Request.Metadata
			mu.Unlock()
		}
	})
	tracker.queueSnapshot = func() []router.QueueInfo { return nil }

	ctx := swaputil.SetContext(context.Background(), swaputil.ReqContextData{
		Model:    "b",
		ModelID:  "b",
		Metadata: map[string]string{},
	})
	tracker.mu.Lock()
	tracker.requests["1"] = &inflightRequest{
		entry: swaputil.InflightRequestEntry{ID: "1", Stage: "serving", Metadata: map[string]string{}},
		ctx:   ctx,
	}
	tracker.mu.Unlock()

	// No served_model yet: the tick must not produce an upsert.
	tracker.updateStages()
	mu.Lock()
	if upsertCount != 0 {
		mu.Unlock()
		t.Fatalf("upserts=%d want 0 before served_model is written", upsertCount)
	}
	mu.Unlock()

	// The scheduler grants the request and writes served_model afterwards.
	if err := swaputil.SetReqData(ctx, "served_model", "a"); err != nil {
		t.Fatalf("SetReqData: %v", err)
	}

	if !tracker.updateStages() {
		t.Fatal("updateStages()=false want true while requests are tracked")
	}

	tracker.mu.RLock()
	got := tracker.requests["1"].entry.Metadata["served_model"]
	tracker.mu.RUnlock()
	if got != "a" {
		t.Fatalf("entry served_model=%q want %q", got, "a")
	}
	waitForUpserts(t, &mu, &upsertCount, 1)
	mu.Lock()
	defer mu.Unlock()
	if upsertMeta["served_model"] != "a" {
		t.Fatalf("upsert served_model=%q want %q", upsertMeta["served_model"], "a")
	}
}

// waitForUpserts blocks until at least n upsert events have been published
// (the publish callback runs on the tracker's async publisher goroutine).
func waitForUpserts(t *testing.T, mu *sync.Mutex, count *int, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		got := *count
		mu.Unlock()
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d upserts, got %d", want, got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestServer_InflightServedModelNoKeyNoChurn verifies that a request whose
// metadata never receives served_model produces no extra upserts across ticks
// (no metadata churn), while the stage updater keeps running.
func TestServer_InflightServedModelNoKeyNoChurn(t *testing.T) {
	var mu sync.Mutex
	var upserts int
	tracker := newInflightTrackerWithPublisher(16, func(u swaputil.InFlightRequestsEvent) {
		if u.Operation == inflightOperationUpsert && u.Request != nil {
			mu.Lock()
			upserts++
			mu.Unlock()
		}
	})
	tracker.queueSnapshot = func() []router.QueueInfo { return nil }

	ctx := swaputil.SetContext(context.Background(), swaputil.ReqContextData{
		Model:    "b",
		ModelID:  "b",
		Metadata: map[string]string{"priority": "60"},
	})
	tracker.mu.Lock()
	tracker.requests["1"] = &inflightRequest{
		entry: swaputil.InflightRequestEntry{ID: "1", Stage: "serving", Metadata: map[string]string{"priority": "60"}},
		ctx:   ctx,
	}
	tracker.mu.Unlock()

	for i := 0; i < 3; i++ {
		if !tracker.updateStages() {
			t.Fatal("updateStages()=false want true while requests are tracked")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if upserts != 0 {
		t.Fatalf("upserts=%d want 0 when served_model is never written", upserts)
	}
}
