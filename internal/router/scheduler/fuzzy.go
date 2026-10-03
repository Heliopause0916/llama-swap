package scheduler

import (
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
)

// defaultFuzzyIdleTimeout is the idle window when fuzzyIdleTimeout is unset.
const defaultFuzzyIdleTimeout = 300 * time.Second

// groupFuzzy is the construction-time snapshot of the fuzzy configuration:
// which models belong to fuzzy-enabled swap groups, and each group's idle
// window. Empty maps make the feature inert (matrix router, no fuzzy groups).
type groupFuzzy struct {
	groupOf     map[string]string        // modelID -> groupID (fuzzy groups only)
	idleTimeout map[string]time.Duration // groupID -> window (0 = busy-only)
}

// newGroupFuzzy resolves conf.Routing.Router.Settings.Groups into groupFuzzy.
// Called once from scheduler.New; nil/absent groups yield empty maps. Only
// groups with Fuzzy && Swap are indexed (load validation already rejects fuzzy
// without swap; this is a second guard).
func newGroupFuzzy(conf config.Config) groupFuzzy {
	fuzzy := groupFuzzy{
		groupOf:     make(map[string]string),
		idleTimeout: make(map[string]time.Duration),
	}
	for gid, gcfg := range conf.Routing.Router.Settings.Groups {
		if !gcfg.Fuzzy || !gcfg.Swap {
			continue
		}
		window := defaultFuzzyIdleTimeout
		if gcfg.FuzzyIdleTimeout != nil {
			window = time.Duration(*gcfg.FuzzyIdleTimeout) * time.Second
		}
		fuzzy.idleTimeout[gid] = window
		for _, member := range gcfg.Members {
			fuzzy.groupOf[member] = gid
		}
	}
	return fuzzy
}
