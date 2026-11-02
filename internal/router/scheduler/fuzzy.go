package scheduler

import (
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
)

// defaultFuzzyIdleTimeout is the idle window when fuzzyIdleTimeout is unset.
const defaultFuzzyIdleTimeout = 300 * time.Second

// groupFuzzy is the construction-time snapshot of fuzzy configuration: which
// models belong to fuzzy-enabled swap groups, and each group's idle window.
// Empty maps make the feature inert (matrix/capacity routers, no fuzzy groups).
type groupFuzzy struct {
	groupOf     map[string]string        // modelID -> groupID (fuzzy groups only)
	idleTimeout map[string]time.Duration // groupID -> window (0 = busy-only)
}

// newGroupFuzzy resolves conf.Routing.Router.Settings.Groups into groupFuzzy.
// Called once from scheduler.New; nil/absent groups yield zero maps. Only
// groups with Fuzzy && Swap are indexed (load validation already rejects fuzzy
// without swap; this is a second guard).
func newGroupFuzzy(conf config.Config) groupFuzzy {
	gf := groupFuzzy{
		groupOf:     make(map[string]string),
		idleTimeout: make(map[string]time.Duration),
	}
	for gid, gcfg := range conf.Routing.Router.Settings.Groups {
		if !gcfg.Fuzzy || !gcfg.Swap {
			continue
		}
		win := defaultFuzzyIdleTimeout
		if gcfg.FuzzyIdleTimeout != nil {
			win = time.Duration(*gcfg.FuzzyIdleTimeout) * time.Second
		}
		for _, m := range gcfg.Members {
			gf.groupOf[m] = gid
		}
		gf.idleTimeout[gid] = win
	}
	return gf
}
