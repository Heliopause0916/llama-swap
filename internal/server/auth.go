package server

import (
	"net/http"
	"strconv"

	"github.com/mostlygeek/llama-swap/internal/chain"
	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// CreateAuthMiddleware returns middleware that validates API keys when the
// config declares any. It accepts the key via Authorization: Bearer,
// Authorization: Basic (password field), or x-api-key. When no keys are
// configured the middleware is a pass-through.
func CreateAuthMiddleware(cfg config.Config) chain.Middleware {
	keys := cfg.RequiredAPIKeys
	return func(next http.Handler) http.Handler {
		if len(keys) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := swaputil.ExtractAPIKey(r)

			valid := false
			for _, key := range keys {
				if provided == key {
					valid = true
					break
				}
			}
			if !valid {
				w.Header().Set("WWW-Authenticate", `Basic realm="llama-swap"`)
				swaputil.SendResponse(w, r, http.StatusUnauthorized, "unauthorized: invalid or missing API key")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// CreateRequestContextMiddleware returns middleware that extracts model and
// auth info from the request into the context. Requests where no model can be
// identified are rejected with a 404, and bodies over the size cap with a 413.
//
// JSON and multipart bodies without a Content-Length are first read here, so
// this is where their size cap is usually hit.
func CreateRequestContextMiddleware(cfg config.Config) chain.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r = markInflightStart(r)
			data, err := swaputil.FetchContext(r, cfg)
			if err != nil {
				if bodyErr := bodyTooLargeFrom(err); bodyErr != nil {
					swaputil.SendResponse(w, r, bodyErr.status, bodyErr.message)
					return
				}
				swaputil.SendError(w, r, swaputil.ErrNoModelInContext)
				return
			}
			_ = data
			// Resolve the request-priority band into the request metadata so
			// the in-flight Priority column can display it. The scheduler
			// resolves the same header separately (base.go) for its ordering
			// key; both read through the same FifoConfig so they stay in sync.
			fifo := cfg.Routing.Scheduler.Settings.Fifo
			if p, on := fifo.ResolvePriority(r.Header.Get(fifo.PriorityHeader)); on {
				if err := swaputil.SetReqData(r.Context(), "priority", strconv.Itoa(p)); err != nil {
					// The context has no Metadata map (e.g. /upstream path),
					// so there is nothing to record — skip silently.
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
