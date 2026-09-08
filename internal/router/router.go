package router

import (
	"net/http"
	"time"

	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/router/scheduler"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

var (
	ErrNoRouterFound     = swaputil.ErrNoRouterFound
	ErrNoPeerModelFound  = swaputil.ErrNoPeerModelFound
	ErrNoLocalModelFound = swaputil.ErrNoLocalModelFound
)

type Router interface {
	// Shutdown blocks until the router has shutdown returning nil
	// when the router has shutdown successfully.
	//
	// timeout controls how long to wait for inflight requests to finish. After
	// the timeout all inflight requests will be cancelled.
	Shutdown(timeout time.Duration) error

	// ServeHTTP implements the http.Handler and requests coming in will
	// trigger any model swapping and routing logic.
	ServeHTTP(http.ResponseWriter, *http.Request)

	// Handles reports whether this router can serve requests for the given model.
	Handles(model string) bool
}

// QueueInfo describes one request currently waiting in the router's scheduler
// queue. It is the router-facing projection of scheduler.QueuedInfo, tagged for
// direct reuse by the in-flight SSE payloads.
type QueueInfo struct {
	RequestID     string `json:"request_id"`
	Model         string `json:"model"`
	QueuePosition int    `json:"queue_position"`
}

// queueSnapshotProvider is implemented by schedulers that can report their
// queue contents. baseRouter discovers it via a type assertion rather than
// extending the Scheduler interface, so a future scheduler without a queue
// keeps working and simply reports nil snapshots.
type queueSnapshotProvider interface {
	Queued() []scheduler.QueuedInfo
}

// LocalRouter is a Router backed by local processes whose state can be
// inspected and which can be individually stopped. Peer routers, which only
// forward to remote hosts, do not implement it.
type LocalRouter interface {
	Router

	// RunningModels returns the current state of every process that is not
	// stopped or shut down, keyed by model ID.
	RunningModels() map[string]process.ProcessState

	// Unload stops the named models, or every running model when none are
	// named. It blocks until each targeted process has stopped. A timeout <= 0
	// gives each process its configured unloadTimeout to stop gracefully:
	// models sharing a timeout stop in parallel, smaller timeouts before
	// larger ones. A positive timeout overrides the configured values for
	// every target.
	Unload(timeout time.Duration, models ...string)

	// ProcessLogger returns the log monitor for the named model's process.
	// modelID must be a real (non-alias) config key. Returns false when the
	// model is not known to this router.
	ProcessLogger(modelID string) (*logmon.Monitor, bool)

	// QueueSnapshot returns the scheduler queue's current contents: one entry
	// per queued request, in service order with 1-indexed positions. It
	// returns nil when the router's scheduler does not expose its queue.
	QueueSnapshot() []QueueInfo
}
