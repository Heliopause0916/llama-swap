package router

import "strings"

// ResolveRequestPriority maps the request priority band header to its numeric
// priority value. It is the shared implementation of the §7 fallback chain
// (docs/design/request-priority.md), used by both the router's authoritative
// ingress resolution (baseRouter.ServeHTTP) and the inflight middleware's
// display-only metadata write.
//
// Branch labels match the fallback chain:
//
//	C  — header absent: silently returns defaultPriority (normal steady state,
//	     no log)
//	B' — header present but blank/whitespace: warns, returns defaultPriority
//	A  — trimmed, case-insensitive value matches a declared band: returns it
//	B  — unknown word: warns, returns defaultPriority
//
// When prio is empty the feature is OFF (§6): the header is never read and
// every request silently resolves to defaultPriority. warnOnce is invoked on
// branches B/B' and may be nil — the inflight display path passes nil so the
// authoritative router remains the only logger, keeping the B/B' dedup by raw
// value single-sourced. The caller owns normalizing a 0 result to
// defaultPriority (baseRouter.ServeHTTP is the single write point, D12).
func ResolveRequestPriority(headers []string, prio map[string]int, defaultPriority int, warnOnce func(raw string, target int)) int {
	if len(prio) == 0 { // feature off — header never read (§6/D5)
		return defaultPriority
	}
	if len(headers) == 0 { // branch C: header absent
		return defaultPriority // silent, normal steady state, no log
	}
	raw := strings.TrimSpace(headers[0]) // multiple same-name headers: first wins (D13)
	if raw == "" {                       // branch B': present but blank/whitespace
		if warnOnce != nil {
			warnOnce(raw, defaultPriority) // warning, deduped by raw
		}
		return defaultPriority
	}
	if p, ok := prio[strings.ToLower(raw)]; ok { // branch A
		return p
	}
	if warnOnce != nil { // branch B: unknown word
		warnOnce(raw, defaultPriority)
	}
	return defaultPriority
}
