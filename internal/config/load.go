package config

import (
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

func LoadConfigFromReader(r io.Reader) (Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return Config{}, err
	}
	yamlStr := string(data)

	// Phase 1: Substitute all ${env.VAR} macros at string level
	// This is safe because env values are simple strings without YAML formatting
	yamlStr, err = substituteEnvMacros(yamlStr)
	if err != nil {
		return Config{}, err
	}

	raw, macroConfig, err := resolveConfigMacros(yamlStr)
	if err != nil {
		return Config{}, err
	}

	var node yaml.Node
	if err = node.Encode(raw); err != nil {
		return Config{}, err
	}

	// Decode the resolved values into the full Config with defaults.
	config := Config{
		HealthCheckTimeout: 120,
		StartPort:          5800,
		LogLevel:           "info",
		LogTimeFormat:      "",
		LogToStdout:        LogToStdoutProxy,
		MetricsMaxInMemory: 1000,
		CaptureBuffer:      5,
		GlobalTTL:          0,
		UnloadTimeout:      DEFAULT_UNLOAD_TIMEOUT,
		UI: UIConfig{Activity: UIActivityConfig{SessionID: []string{
			"X-Session-ID",
			"X-Litellm-Session-Id",
		}}},
	}
	if err = node.Decode(&config); err != nil {
		return Config{}, err
	}
	config.Macros = macroConfig.Macros
	for modelID, modelConfig := range config.Models {
		modelConfig.Macros = macroConfig.Models[modelID].Macros
		config.Models[modelID] = modelConfig
	}

	if config.HealthCheckTimeout < 15 {
		config.HealthCheckTimeout = 15
	}

	// Apply defaults for performance config when section is missing
	if config.Performance.Every == 0 {
		config.Performance.Every = 5 * time.Second
	}
	if err = config.Performance.Validate(); err != nil {
		return Config{}, fmt.Errorf("performance: %w", err)
	}

	if config.StartPort < 1 {
		return Config{}, fmt.Errorf("startPort must be greater than 1")
	}

	if config.GlobalTTL < 0 {
		return Config{}, fmt.Errorf("globalTTL must be >= 0")
	}

	if config.UnloadTimeout < 0 {
		return Config{}, fmt.Errorf("unloadTimeout must be >= 0")
	}
	if config.UnloadTimeout == 0 {
		config.UnloadTimeout = DEFAULT_UNLOAD_TIMEOUT
	}

	config.UI.Activity.SessionID = normalizeHeaderNames(config.UI.Activity.SessionID)

	if config.Store != nil {
		if err := validateStorePath(config.Store.Path); err != nil {
			return Config{}, err
		}
	}

	// Apply default for upstream.ignorePaths when not specified. The default
	// matches common static-asset suffixes so they do not trigger a swap.
	if len(config.Upstream.IgnorePaths) == 0 {
		config.Upstream.IgnorePaths = DefaultUpstreamIgnorePaths()
	}

	switch config.LogToStdout {
	case LogToStdoutProxy, LogToStdoutUpstream, LogToStdoutBoth, LogToStdoutNone:
	default:
		return Config{}, fmt.Errorf("logToStdout must be one of: proxy, upstream, both, none")
	}

	// Populate the aliases map
	config.aliases = make(map[string]string)
	for modelName, modelConfig := range config.Models {
		for _, alias := range modelConfig.Aliases {
			if _, found := config.aliases[alias]; found {
				return Config{}, fmt.Errorf("duplicate alias %s found in model: %s", alias, modelName)
			}
			config.aliases[alias] = modelName
		}
	}

	// Sort model IDs for deterministic validation and normalization.
	modelIds := make([]string, 0, len(config.Models))
	for modelId := range config.Models {
		modelIds = append(modelIds, modelId)
	}
	sort.Strings(modelIds)

	for _, modelId := range modelIds {
		modelConfig := config.Models[modelId]
		modelConfig.HealthCheckTimeout = config.HealthCheckTimeout
		if modelId == ComfyUIModelID {
			if modelConfig.ConcurrencyLimit < comfyUIConcurrencyLimit {
				modelConfig.ConcurrencyLimit = comfyUIConcurrencyLimit
			}
			modelConfig.Compat.IgnoreWebsockets = true
		}

		// set model TTL to globalTTL it is the default value
		if modelConfig.UnloadAfter == MODEL_CONFIG_DEFAULT_TTL {
			modelConfig.UnloadAfter = config.GlobalTTL
		}

		if modelConfig.UnloadAfter < 0 {
			return Config{}, fmt.Errorf("model %s: invalid TTL value %d", modelId, modelConfig.UnloadAfter)
		}

		// set model unloadTimeout to the global value when left at the default
		if modelConfig.UnloadTimeout < 0 {
			return Config{}, fmt.Errorf("model %s: invalid unloadTimeout value %d", modelId, modelConfig.UnloadTimeout)
		}
		if modelConfig.UnloadTimeout == 0 {
			modelConfig.UnloadTimeout = config.UnloadTimeout
		}

		if err := modelConfig.Capabilities.Validate(); err != nil {
			return Config{}, fmt.Errorf("model %s: %w", modelId, err)
		}

		// Auto-register setParamsByID keys as aliases (skip the model's own ID)
		for key := range modelConfig.Filters.SetParamsByID {
			if key == modelId {
				continue
			}
			if _, exists := config.Models[key]; exists {
				return Config{}, fmt.Errorf("model %s filters.setParamsByID: key '%s' conflicts with an existing model ID", modelId, key)
			}
			if existingModel, exists := config.aliases[key]; exists {
				if existingModel != modelId {
					return Config{}, fmt.Errorf("duplicate alias '%s' in model %s filters.setParamsByID, already used by model %s", key, modelId, existingModel)
				}
				continue // already registered as explicit alias for this model
			}
			config.aliases[key] = modelId
			modelConfig.Aliases = append(modelConfig.Aliases, key)
		}

		if _, err := url.Parse(modelConfig.Proxy); err != nil {
			return Config{}, fmt.Errorf("model %s: invalid proxy URL: %w", modelId, err)
		}

		if modelConfig.SendLoadingState == nil {
			v := config.SendLoadingState
			modelConfig.SendLoadingState = &v
		}

		config.Models[modelId] = modelConfig
	}

	// Normalize routing config. The legacy top-level `matrix`/`groups` keys and
	// the new `routing.router` block are mutually exclusive: a config may use
	// either style, never both.
	hasTopLevel := config.Matrix != nil || len(config.Groups) > 0
	rtr := config.Routing.Router
	hasRouting := rtr.Use != "" || rtr.Settings.Matrix != nil || len(rtr.Settings.Groups) > 0

	if hasTopLevel && hasRouting {
		return Config{}, fmt.Errorf("config uses both the legacy top-level 'matrix'/'groups' keys and the new 'routing.router' block; please migrate the top-level keys into 'routing.router' and remove them")
	}

	if !hasTopLevel {
		// Both groups and matrix may be defined under routing.router.settings;
		// routing.router.use selects which one is active, so there is no conflict.
		rs := config.Routing.Router.Settings
		switch config.Routing.Router.Use {
		case "matrix":
			if rs.Matrix == nil {
				return Config{}, fmt.Errorf("routing.router.use is 'matrix' but routing.router.settings.matrix is not set")
			}
			config.Matrix = rs.Matrix
		case "group", "":
			config.Groups = rs.Groups
		default:
			return Config{}, fmt.Errorf("routing.router.use: unknown router %q (valid: group, matrix)", config.Routing.Router.Use)
		}
	}

	// groups XOR matrix
	if config.Matrix != nil && len(config.Groups) > 0 {
		return Config{}, fmt.Errorf("config cannot use both 'groups' and 'matrix'")
	}

	if config.Matrix != nil {
		if err := ValidateMatrix(config.Matrix, config.Models); err != nil {
			return Config{}, fmt.Errorf("matrix: %w", err)
		}
	} else {
		config = AddDefaultGroupToConfig(config)

		// Validate group members
		memberUsage := make(map[string]string)
		for groupID, groupConfig := range config.Groups {
			prevSet := make(map[string]bool)
			for _, member := range groupConfig.Members {
				if _, found := prevSet[member]; found {
					return Config{}, fmt.Errorf("duplicate model member %s found in group: %s", member, groupID)
				}
				prevSet[member] = true

				if existingGroup, exists := memberUsage[member]; exists {
					return Config{}, fmt.Errorf("model member %s is used in multiple groups: %s and %s", member, existingGroup, groupID)
				}
				memberUsage[member] = groupID
			}
		}
	}

	// Build the canonical Config.Routing from the effective result. Both legacy
	// and new-style configs converge here. The Matrix pointer is shared so the
	// compiled matrix program stays in one place.
	if config.Matrix != nil {
		config.Routing.Router.Use = "matrix"
	} else {
		config.Routing.Router.Use = "group"
	}
	config.Routing.Router.Settings.Matrix = config.Matrix
	config.Routing.Router.Settings.Groups = config.Groups

	if config.Routing.Scheduler.Use == "" {
		config.Routing.Scheduler.Use = "fifo"
	}
	if config.Routing.Scheduler.Use != "fifo" {
		return Config{}, fmt.Errorf("routing.scheduler.use: unknown scheduler %q (valid: fifo)", config.Routing.Scheduler.Use)
	}

	// Request-level priority (docs/design/request-priority.md §6). The legacy
	// model-level `priority` key was removed (D1): a leftover entry is accepted
	// for upstream v255 config compatibility and ignored, with a one-time
	// deprecation warning (D14). yaml.v3's lenient decode would otherwise
	// silently drop the key from FifoConfig, so it is detected here against the
	// raw, macro-expanded YAML tree.
	fifo := &config.Routing.Scheduler.Settings.Fifo
	if fifo.PriorityHeader == "" {
		fifo.PriorityHeader = "X-Request-Priority"
	}
	if fifo.DefaultPriority == 0 {
		// 0 is the legacy "unset" sentinel; normalize to the documented default
		// (same zero-handling pattern as unloadTimeout above).
		fifo.DefaultPriority = 60
	}
	warnLegacyFifoPriority(raw)

	// Empty requestPriority => feature off, accepted as-is. Non-empty =>
	// validate: keys non-empty after trimming and normalized to lowercase
	// (the stored form), values > 0 and unique across bands, and
	// defaultPriority equal to one of the declared band values.
	if len(fifo.RequestPriority) > 0 {
		normalized := make(map[string]int, len(fifo.RequestPriority))
		values := make(map[int]string, len(fifo.RequestPriority))
		for band, value := range fifo.RequestPriority {
			key := strings.ToLower(strings.TrimSpace(band))
			switch {
			case key == "":
				return Config{}, fmt.Errorf("routing.scheduler.settings.fifo.requestPriority: band name %q is empty after trimming", band)
			case value <= 0:
				return Config{}, fmt.Errorf("routing.scheduler.settings.fifo.requestPriority.%s: value %d must be > 0 (0 is reserved for legacy unset records)", key, value)
			}
			if prev, dup := values[value]; dup {
				return Config{}, fmt.Errorf("routing.scheduler.settings.fifo.requestPriority: bands %q and %q both map to value %d; band values must be unique", prev, key, value)
			}
			if _, dup := normalized[key]; dup {
				return Config{}, fmt.Errorf("routing.scheduler.settings.fifo.requestPriority: band name %q normalizes to %q which is already declared", band, key)
			}
			normalized[key] = value
			values[value] = key
		}
		if _, ok := values[fifo.DefaultPriority]; !ok {
			return Config{}, fmt.Errorf("routing.scheduler.settings.fifo.defaultPriority: %d is not one of the declared requestPriority band values; defaultPriority must equal a declared band", fifo.DefaultPriority)
		}
		fifo.RequestPriority = normalized
	}

	// Clean up hooks preload
	if len(config.Hooks.OnStartup.Preload) > 0 {
		var toPreload []string
		for _, modelID := range config.Hooks.OnStartup.Preload {
			modelID = strings.TrimSpace(modelID)
			if modelID == "" {
				continue
			}
			if real, found := config.RealModelName(modelID); found {
				toPreload = append(toPreload, real)
			}
		}
		config.Hooks.OnStartup.Preload = toPreload
	}

	// Validate API keys (env macros already substituted at string level)
	for i, apikey := range config.RequiredAPIKeys {
		if apikey == "" {
			return Config{}, fmt.Errorf("empty api key found in apiKeys")
		}
		if strings.Contains(apikey, " ") {
			return Config{}, fmt.Errorf("apiKeys[%d]: api key cannot contain spaces", i)
		}
		config.RequiredAPIKeys[i] = apikey
	}

	if err := ValidatePeerNamespace(config); err != nil {
		return Config{}, err
	}

	if err := validateSelectors(config); err != nil {
		return Config{}, err
	}

	if err := validateProfiles(config); err != nil {
		return Config{}, err
	}

	if err := validateTailcatConfig(&config); err != nil {
		return Config{}, err
	}

	return config, nil
}

func validateProfiles(config Config) error {
	for profileName, profile := range config.Profiles {
		if strings.TrimSpace(profileName) == "" {
			return fmt.Errorf("profiles: profile names cannot be empty")
		}
		if len(profile.Pins) == 0 {
			return fmt.Errorf("profiles.%s.pins must contain at least one entry", profileName)
		}
		for pin, target := range profile.Pins {
			if strings.TrimSpace(pin) == "" {
				return fmt.Errorf("profiles.%s.pins: pin names cannot be empty", profileName)
			}
			if target == "" {
				continue
			}
			if _, found := config.ResolveBaseModel(target); !found {
				if _, found := config.Selectors[target]; found {
					continue
				}
				return fmt.Errorf("profiles.%s.pins.%s references unknown model %q", profileName, pin, target)
			}
		}
	}
	if profile := config.Hooks.OnStartup.Profile; profile != "" {
		if _, found := config.Profiles[profile]; !found {
			return fmt.Errorf("hooks.on_startup.profile references unknown profile %q", profile)
		}
	}
	return nil
}

func normalizeHeaderNames(names []string) []string {
	normalized := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized
}

// warnLegacyFifoPriority emits a one-time deprecation warning when a leftover
// `priority:` key is present under routing.scheduler.settings.fifo. The
// model-level key was removed with the request-priority feature (D1), but the
// official upstream v255 config shape still declares it, and upstream-release
// compat is a hard requirement on this fork: the key must be accepted and
// ignored, not rejected (D14). The key is detected against the raw,
// macro-expanded YAML tree because yaml.v3's lenient decode silently drops
// unknown keys from FifoConfig. Logged via the standard log/slog default
// logger: internal/config owns no logger of its own, and this repository's
// binaries (llama-swap.go, cmd/wol-proxy) already emit through slog.
func warnLegacyFifoPriority(raw map[string]any) {
	routing, ok := raw["routing"].(map[string]any)
	if !ok {
		return
	}
	scheduler, ok := routing["scheduler"].(map[string]any)
	if !ok {
		return
	}
	settings, ok := scheduler["settings"].(map[string]any)
	if !ok {
		return
	}
	fifo, ok := settings["fifo"].(map[string]any)
	if !ok {
		return
	}
	if _, ok := fifo["priority"]; ok {
		slog.Warn(`deprecated "routing.scheduler.settings.fifo.priority" key is accepted for upstream compatibility but ignored: model-level priority was removed in favor of request-level priority (requestPriority / X-Request-Priority header); see docs/design/request-priority.md`)
	}
}
