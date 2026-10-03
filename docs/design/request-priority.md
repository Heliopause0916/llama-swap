# Request-level priority via `X-Request-Priority`

> **Status** — L1 approved spec (frozen design, v1).
> **Scope** — config, ingress resolution, FIFO queue ordering, observability.
> **UI** — none in L1.
> This document is the authoritative specification for the L1 feature
> *request-level priority via the `X-Request-Priority` header + FIFO queue
> ordering* in this fork. Implementations must encode exactly the semantics
> below and must not silently add or drop behavior.

## 1. Overview

Every request handled by the router carries a priority chosen by the fronting
**new-api gateway**, which injects/overwrites the `X-Request-Priority` header
(a trusted source; the fork does not defend against forgery). The priority is
resolved **once** at ingress into an integer value, attached to the `HandlerReq`,
and used as the FIFO queue's sort key while the queue services requests under
capacity competition. It never preempts, never reserves slots for the future,
and never alters swap/eviction decisions.

```
 HTTP request                     Activity (post-serve)
     │                                   ▲
     ▼                                   │ fifo_priority metadata (grant-time)
 ServeHTTP                              │
     │  FetchContext -> resolvePriority ─┘
     ▼
 HandlerReq{Priority} --handlerCh--> FIFO.OnRequest
                                        │  enqueue sorts by Priority
                                        ▼
                                    queue service order
                                        ▼
                              (fast-path / swap-join / swaps: no priority input)
```

## 2. Background and deployment topology

This repository is a fork of llama-swap. Two fork facts matter for this feature:

- The FIFO scheduler already supports **over-limit queueing**
  (`queueDepth` / `queueTimeout`, three-state `*int`): `nil` ⇒ defaults 10 / 60s,
  `0` ⇒ queueing disabled (over-limit requests fall back to the upstream 429),
  `>0` ⇒ active. The queue is already **priority-ordered** (descending numeric,
  stable for equal values).
- The queue's **capacity gate drains on in-flight ≥ limit, not reserved**. A
  reserved-based gate dead-locks (every queued item holds a reservation and would
  strand itself); this was fixed in PATCH-v255-queueing and must not regress.
  This rule is carried forward into the L2 per-band caps design (§15).

Deployment topology:

| Server | Shapes | Notes |
|--------|--------|-------|
| Server 1 | 1 model × `concurrencyLimit: 8` | sustained multi-request load |
| Server 2 | 7 models × `concurrencyLimit: 1` | swap-heavy |
| Global | scheduler FIFO `queueDepth: 64`, `queueTimeout: 600` | applies per model |

The deployment does **not** configure the legacy model-level `cfg.Priority`.

## 3. Goals

1. Let the gateway express a per-request service priority through a single header.
2. Order the FIFO queue by that priority: **higher numeric value is served first**;
   equal values keep arrival (FIFO) order.
3. Be strictly behavior-preserving for existing unconfigured deployments: the
   feature is off until `requestPriority` is populated, and the legacy model-level
   priority key is removed.
4. Keep queueing, swapping, fast-path, and eviction semantics untouched.

## 4. Non-Goals (L1)

- No preemption and no interruption of a request that has been granted service.
- No per-band concurrency caps or per-band quotas (see §15).
- No UI changes; the in-flight table and the activity view keep their current
  fields (priority is observable via activity metadata only).
- No defense against header forgery — that is the gateway's responsibility.
- No interplay with model-level priorities (removed, see D1).

## 5. Bands and numeric space

| Word (gateway vocabulary) | Value (example) | Notes                                     |
|---------------------------|-----------------|-------------------------------------------|
| `high`                    | 100             |                                           |
| `medium`                  | 60              | also the default fallback band (see §6)   |
| `low`                     | 30              |                                           |
| `batch`                   | 10              |                                           |

Rules:

- **Higher number wins.** Values are example defaults; the word→value mapping is
  configurable. The numeric space (10/30/60/100) leaves headroom for more bands.
- **Sentinel `0` = "unset/unknown"** is a hidden sentinel only. It is not a band,
  is rejected at config load (a band may not map to 0), and is normalized away at
  the single write point (§7), so it never reaches the scheduler at runtime. It
  survives only in legacy SQLite activity records and, conceptually, on the
  bypass parsing paths that read those records.

## 6. Configuration

All keys live under `routing.scheduler.settings.fifo`.

| Key                | Type            | Default             | Semantics                                        |
|--------------------|-----------------|---------------------|--------------------------------------------------|
| `priorityHeader`   | `string`        | `X-Request-Priority`| header name read at ingress (multiple same-name values: first wins) |
| `requestPriority`  | `map[string]int`| `nil` (feature off) | band word → value; see below                      |
| `defaultPriority`  | `int`           | `60`                | fallback when the header is absent / blank / unknown |
| `queueDepth`, `queueTimeout` | `*int` | unchanged    | unaffected by this feature                       |

The legacy key **`priority` (model-level) is removed**. For upstream v255
config compatibility it is **accepted (with a one-time deprecation warning) and
ignored**, its value has no effect (D14).

### Feature off / on

- **Empty or absent `requestPriority` ⇒ feature OFF**: the header is **never
  read**, every request resolves to `defaultPriority`, and **no parsing log lines
  are emitted**. This makes zero-config deployments behavior-identical to today.
- **Non-empty `requestPriority` ⇒ feature ON**, and config loading validates:
  1. every key is non-empty after trimming;
  2. keys are **normalized to lowercase at load** (the stored form);
  3. every value is `> 0` — a mapping to `0` is a load error;
  4. values are **unique across bands** — two words may not share a value;
  5. `defaultPriority` must equal **one of the declared band values** (a default
     outside the bands is a load error — no orphan defaults).

A blank `X-Request-Priority` **value** is not a config error; it is handled at
resolution time (branch B′, §7).

### Example — enabled (target production shape)

```yaml
routing:
  scheduler:
    use: fifo
    settings:
      fifo:
        queueDepth: 64
        queueTimeout: 600             # PATCH(v255) queueing, unaffected
        priorityHeader: X-Request-Priority
        requestPriority:
          high: 100
          medium: 60
          low: 30
          batch: 10
        defaultPriority: 60
```

### Example — feature off

```yaml
routing:
  scheduler:
    use: fifo
    settings:
      fifo:
        queueDepth: 64
        queueTimeout: 600
        requestPriority: {}           # or omit entirely
```

### Migration note

Because the `priority` key no longer exists on `FifoConfig`, a leftover
`priority:` entry in an old config depends on the YAML strictness of
`internal/config`. The load code **accepts the leftover key for upstream
compatibility and ignores it**, emitting a one-time deprecation warning that
points at the requestPriority migration; it does not reject it (D14). Detected
against the raw, macro-expanded YAML tree because yaml.v3's lenient decode
would otherwise silently drop the key.

## 7. Resolution algorithm

Location: `baseRouter.ServeHTTP` (`internal/router/base.go`), **after**
`FetchContext` resolved `Model` and **before** the `HandlerReq` literal is built
(`base.go:514` → `base.go:540`). The websocket-ignore branch (`base.go:525-538`)
returns before the `HandlerReq` literal and never reaches the scheduler, so it
never computes a priority. Resolution is a pure, side-effect-free read — apart
from the deduped warning logs on the error branches.

```go
// resolvePriority maps the X-Request-Priority header to a band value.
// Branch labels match the fallback chain below. warnOnce emits a warning on the
// first occurrence of a given raw value, then debug afterwards.
func resolvePriority(r *http.Request, cfg config.FifoConfig, warnOnce func(raw string, target int)) int {
    vals := r.Header.Values(cfg.PriorityHeader)
    if len(vals) == 0 {                       // branch C: header absent
        return cfg.DefaultPriority            // silent, normal steady state, no log
    }
    raw := strings.TrimSpace(vals[0])         // multiple same-name headers: first wins
    if raw == "" {                            // branch B': present but blank/whitespace
        warnOnce(raw, cfg.DefaultPriority)    //     warning, deduped by raw
        return cfg.DefaultPriority
    }
    if p, ok := cfg.RequestPriority[strings.ToLower(raw)]; ok { // branch A
        return p
    }
    warnOnce(raw, cfg.DefaultPriority)        // branch B: unknown word
    return cfg.DefaultPriority
}
```

Rules encoded above:

- **Matching**: `TrimSpace`, then **case-insensitive** — config keys were
  lowercased at load, so `raw` is lowercased for the lookup.
- **Multiple same-name headers**: only `Values()[0]` is examined (after trimming);
  the rest are ignored deterministically.
- **Branch B / B′ logging**: deduplicated **by raw value** — the first occurrence
  of a given raw emits a warning (branch B's message includes both `raw` and the
  fallback target); subsequent identical raws degrade to debug. A future Prometheus
  counter `fifo_priority_miss_total{raw,target}` is an agreed extension point
  (§15), not part of L1.
- **Branch C** is deliberately silent: an absent header is the normal steady state.

### Normalization — the single write point

At the `HandlerReq` construction site only:

```go
p := resolvePriority(req, fifoCfg)
if p == 0 {                                // single normalization point
    p = fifoCfg.DefaultPriority
}
hr := scheduler.HandlerReq{ /* ... */ Priority: p }
```

This is the **only** place where `0` is rewritten to a band value. Because
load-time validation guarantees every band value and `defaultPriority` are
`> 0`, resolution can never actually return `0`; the guard is a defensive token
that keeps the runtime invariant honest even if a future bypass path leaks a `0`.

## 8. Invariants & boundaries

Invariants:

1. A request's priority is resolved **exactly once**, deterministically, with no
   side effects other than the (deduped) warning log.
2. For every normal model request the final priority is **a member of the declared
   band values** — `0` never reaches the scheduler.
3. `0` exists only in legacy data.

Decision boundaries (L1):

- **No preemption.** Once a request is granted, it runs to completion. Priority only
  decides the *order* in which the queue service picks queued requests when capacity
  is contested; it does not preempt, reserve, or hold a place.
- **fast-path** (model ready, nothing to evict, no collision — `fifo.go:200-205`)
  and **swap-waiter join** (`fifo.go:190-195`) **never consult priority**; their
  behavior is unchanged.
- An **explicit `medium`** and an **absent header** are indistinguishable (both land
  on the default band). Accepted limitation for this release.
- Equal priorities keep arrival order (stable FIFO within a band; see
  `fifo.go:511-524`).

## 9. Observability

- The `fifo_priority` metadata key written by `grantHandler`
  (`fifo.go:378-381` via `swaputil.SetReqData`) **keeps its name** — the SQLite
  `activity.metadata_json` field stays stable for third-party consumers — while its
  **value semantics upgrade** from "the model's configured priority (0 when
  unset)" to **"the request's effective priority"** (a band value such as `60`).
  Old rows (`0` / missing) read as *unset*; new rows carry the effective value.
- Warning logs on branches B/B′ are deduped per raw value (§7).
- The **in-flight list shows the resolved band** as a `Priority` column: the
  display-only `priority` metadata key is written by `CreateInflightMiddleware`
  *before* `inflightTracker.Add` shallow-copies metadata (§16), closing the
  ordering gap where the grant-time `fifo_priority` write lands after the copy.
- No new metrics ship in L1; `fifo_priority_miss_total{raw,target}` is the agreed
  extension point.

## 10. Backward compatibility

- The deployment never set the legacy model-level `cfg.Priority`, so under the old
  code every request's sort key was `0` (Go map zero value) ⇒ purely arrival-order
  service. After this change, any request without a resolvable header lands on
  `defaultPriority` (a single band) ⇒ **still pure arrival order. Behavior is
  equivalent for any deployment that never used model priority.**
- `fifo_priority` metadata consumers keep working (key unchanged; missing/`0` still
  reads as "unset" for old rows).
- `queueDepth` / `queueTimeout` semantics and the in-flight capacity gates are
  untouched.

## 11. Decision log

| # | Decision | Rationale (EN) | 备注 |
|---|----------|----------------|------|
| D1 | Model-level `cfg.Priority` (`FifoConfig.Priority`) is **removed entirely**; request priority is orthogonal to model identity. | The deployment never configured it; request priority is a property of the gateway's admission decision, not a static model attribute; two competing key domains would create ambiguous ordering/admission semantics. | 正交性结论已采纳：模型优先级不再参与请求排序/准入。 |
| D2 | `X-Request-Priority` is a **trusted header**. | The new-api gateway forcibly writes/overwrites it, and the llama-swap port is internal-only; forgery defense belongs to the gateway, not the fork. | 网关可信源；fork 不做防伪。 |
| D3 | Sentinel `0` = "unset", not a band; normalized at the single write point (§7). | Bands start at 10; `0` marks legacy "unset" without squatting a band; one guard point makes the runtime invariant checkable by inspection. | 0 只存在于旧 SQLite 记录；运行时彻底消灭。 |
| D4 | All fallback branches (absent / blank / unknown header) resolve to `defaultPriority`; **absent header is silent**. | A single deterministic fallback chain; an absent header is the normal steady state and must not generate log noise. | 缺失头=正常态，零日志。 |
| D5 | Empty `requestPriority` map ⇒ **feature off** (header never read, constant `defaultPriority`, zero logs). | Zero-config deployments stay behavior-identical; the map doubles as the feature switch. | 空 map=整体关闭，兼作 feature flag。 |
| D6 | When enabled, `defaultPriority` must be **one of the declared band values**. | Prevents an orphan default that no header value could ever select; "default = an explicit band". | 默认档必须是显式档。 |
| D7 | Matching is `TrimSpace` + **case-insensitive**; config keys lowercased at load. | Tolerant of casing variations from the gateway without surface-level normalization burden. | 键加载时小写化，raw 匹配时同等小写化。 |
| D8 | Warnings on B/B′ deduped **by raw**; later occurrences degrade to debug. | A burst of identical unknown/blank header values must not flood logs; a counter is the agreed future telemetry. | 按 raw 去重：首记 warning、后续 debug。 |
| D9 | No preemption; priority orders queue service only; fast-path and swap-waiter join excluded. | Smallest correct L1 scope; serving/swap/eviction lifecycle untouched. | 优先级仅在容量竞争时影响排队顺序。 |
| D10 | `fifo_priority` key and metadata pipeline **unchanged**; value semantics upgraded. | `activity.metadata_json` stability for consumers; the change is purely value-semantics. | 键名稳定；值语义升级为请求有效优先级。 |
| D11 | No UI changes in L1. | Scope control; the feature is observable via activity metadata and logs. | In-flight/活动表展示优先级列为观察项。 |
| D12 | Normalization has a **single write point** (HandlerReq construction). | Makes "runtime 0 impossible" locally verifiable. | 归一化唯一写入点。 |
| D13 | Multiple same-name headers: **first wins**, rest ignored. | Deterministic without extra machinery. | 多同名头取第一个，其余不检查。 |
| D14 | Legacy `priority` key is **accepted-with-warning, not rejected**. | Upstream-release compat is a hard requirement: any legal official v255 config must load on this fork without error. The value is ignored; request semantics are replaced by `requestPriority` / the `X-Request-Priority` header. | 官方 v255 配置零报错是硬性要求；键接受但值忽略。 |
| D15 | The in-flight list exposes a `Priority` column fed by a display-only `priority` metadata key written by `CreateInflightMiddleware` before `Add` snapshots (§16). | The resolution happens late in `baseRouter.ServeHTTP`, after the tracker's shallow copy, so the middleware resolves once more at ingress for display; empty `RequestPriority` ⇒ no key ⇒ column renders "—". Display never changes scheduling semantics. | In-flight 列表新增 Priority 列，值为解析后数值档位；纯展示，不入调度。 |
| D16 | `queuedItem` gains `EnqueuedAt time.Time`, written exactly once at enqueue (`fifo.go:524`), never reset on drain skips. | Reconstructing the stamp from `Deadline` fails when `queueTimeout: 0` (no deadline) and would couple promotion to the timeout machinery; a field matches the existing `Deadline` pattern and is always derivable. | 采用独立入队时刻字段；deadline 反推在 queueTimeout=0 时不可用且会让 aging 耦合超时机制，故放弃。 |
| D17 | Promoted tier is a fixed internal sentinel `P_max = 2147483647` (math.MaxInt32), outside the band space; validation forbids band values ≥ it. | A `maxBand+1`-derived tier would be a *relative* value that shifts across config reloads and is undefined when `requestPriority` is empty (D21); a fixed sentinel makes "P_max is not a band" locally checkable at zero practical cost. | 升档档位固定为 Int32 上限哨兵，不随配置推导；校验禁止 band 触顶，排序键永不碰撞。 |
| D18 | Promotion materializes as a `sort.SliceStable` re-rank by the dual key (effective priority desc, `EnqueuedAt` asc) at the top of `drainQueue`, guarded by `promoteAfter > 0`; `enqueue` stays a resolved-band insert. | Re-rank on drain is O(n log n) worst case but zero cost when off; enqueue stays O(n), and the explicit (eff desc, EnqueuedAt asc) comparison yields the lock-in lemma for every later arrival, whatever enqueue's band insert did (§17.7). | 重排只在 drain 开头一次性做；enqueue 不动；比较器为 (有效优先级降序, EnqueuedAt 升序)——同档显式按到达序，稳定排序仅作同刻兜底。 |
| D19 | The promotion clock is monotone: skipped items keep their `EnqueuedAt`, wait time accumulates continuously. | A collision/capacity block must not reset the countdown or the bounded-wait guarantee becomes meaningless. | 碰撞/容量阻挡停留队中的项等待时间连续累计，enqueuedAt 只写一次。 |
| D20 | `effective queueTimeout ≤ promoteAfter` (both active) is a **load-time error**. | Both clocks start at enqueue and the lazy prune (`fifo.go:544-548`) would drop every item before promotion could ever fire — a provably dead feature fails fast (D6 philosophy). Only configs containing the new key can trigger it, so the v255 zero-error principle is untouched. | 两者同起点计时；promoteAfter 不小于有效 queueTimeout 时老化永不生效，加载期报错；仅涉及 fork 新键，无损官方配置兼容。 |
| D21 | Aging is independent of the `requestPriority` feature flag: `promoteAfter > 0` with an empty/single-band map yields a pure maximum-wait FIFO. | A single-band queue has identical structure whether or not the header is parsed; gating aging on the map adds a surprising dependency and leaves single-band deployments without the guarantee. | aging 独立于优先级 feature 开关；单档/feature-off 下仍可作纯最大等待保障。 |
| D22 | Display/observability unchanged: `HandlerReq.Priority`, `fifo_priority`, in-flight `priority` / UI `Priority` column all keep the resolved band; no new SSE fields; `queue_position` reflects the effective order (forward jumps are expected and documented for the UI). | Exposing a synthetic tier through existing channels would corrupt L1 value semantics for third-party consumers; scheduling-internal by design. | 展示一律保留原始档位，aging 纯调度内部；queue_position 反映重排后的有效顺序，低优前移为预期行为。 |
| D23 | FIFO gains an injectable clock `now func() time.Time` (default `time.Now`) shared by the deadline and aging computations, for deterministic tests. | Threshold aging is untestable with real sleeps; a single clock source (no default-path behavior change) is the minimal testability enabler. | 为了让 aging 阈值可确定性测试，给 FIFO 增加可注入时钟 `now`，默认 time.Now，行为零变化。 |

## 12. Known limitations (accepted for L1)

- No preemption (restated in §8).
- An explicit `medium` and an absent header are indistinguishable.
- Unknown/blank header values silently degrade to `defaultPriority` (with a
  deduped warning); the requesting client is never told its priority was rejected.
- `fifo_priority` shows in activity metadata; the in-flight table shows the
  same resolved band via the `priority` metadata key (§16). The activity view
  itself has no dedicated priority column.
- The feature is **global** (one `routing.scheduler.settings.fifo` block); there is
  no per-model or per-router enablement.

## 13. Implementation plan

| # | File | Change | Anchor |
|---|------|--------|--------|
| 1 | `internal/config/config.go` | `FifoConfig`: delete `Priority map[string]int`; add `PriorityHeader string` (`yaml:"priorityHeader"`), `RequestPriority map[string]int` (`yaml:"requestPriority"`), `DefaultPriority int` (`yaml:"defaultPriority"`); defaults `X-Request-Priority` / `60`. | FifoConfig, `config.go:241-256` |
| 2 | `internal/config/load.go` | Replace the model-priority validation block with the §6 checks: empty map ⇒ off; else trim/lowercase keys, values>0 and dedup, no 0 mappings; `defaultPriority` must be a declared value. Accept a leftover `priority:` key with a one-time deprecation warning (upstream v255 compat, D14). | `load.go:264-268` |
| 3 | `internal/router/scheduler/scheduler.go` | Add `Priority int` to `HandlerReq` with a comment stating it is the effective request-level priority (`> 0`) and the FIFO queue sort key. | `scheduler.go:111-118` |
| 4 | `internal/router/base.go` | Implement `resolvePriority` (§7: branches A/B/B′/C) + single normalization; call it in `ServeHTTP` after `FetchContext` (`base.go:514`) and before the `HandlerReq` literal (`base.go:540`); assign `Priority`. Warning dedup keyed by raw. | `base.go:508-549` |
| 5 | `internal/router/scheduler/fifo.go` | `enqueue`: sort key becomes `req.Priority` — insert before the first item whose priority is strictly lower ⇒ descending order; equal priorities keep arrival order. | `fifo.go:511-524` |
| 6 | `internal/router/scheduler/fifo.go` | `grantHandler`: write `fifo_priority` metadata as `req.Priority` instead of `s.cfg.Priority[req.Model]`. | `fifo.go:378-381` |
| 7 | tests | `internal/router/scheduler/fifo_test.go`, `internal/config/config_test.go` (see §14). | — |

Intentionally **untouched**: the queueing-capacity branch (`fifo.go:184-188`),
fast-path (`fifo.go:200-205`), swap-waiter join (`fifo.go:190-195`), the
admission gate (`fifo.go:400-409`), the in-flight capacity gate
(`fifo.go:553`) and the join cap gate (`fifo.go:570-574`) — the last three
continue counting **in-flight**, not reserved.

## 14. Test plan

Unit tests in `internal/router/scheduler/fifo_test.go`, named following the repo
convention `TestFIFO_Priority_<scenario>` (extends the established
`TestSchedulerName_<scenario>` pattern).

| Test | Scenario | Expectation |
|------|----------|-------------|
| `TestFIFO_Priority_HeaderMissing` | no header sent | branch C ⇒ `defaultPriority`, silent |
| `TestFIFO_Priority_HeaderBlank` | `X-Request-Priority: "  "` | branch B′ ⇒ warning + `defaultPriority` |
| `TestFIFO_Priority_HeaderUnknown` | `"whale"` | branch B ⇒ warning (raw + target) + `defaultPriority` |
| `TestFIFO_Priority_HeaderCaseInsensitive` | `"HiGh"` | branch A via lowercase ⇒ 100 |
| `TestFIFO_Priority_HeaderTrimmed` | `" high "` | branch A after trim ⇒ 100 |
| `TestFIFO_Priority_MultiHeaderFirstWins` | two same-name headers | first value used |
| `TestFIFO_Priority_DisabledEmptyMap` | `requestPriority: {}` | header ignored ⇒ always `defaultPriority`; no parsing logs |
| `TestFIFO_Priority_QueueOrdering` | mixed high/medium/low queued | drains highest numeric first (descending) |
| `TestFIFO_Priority_QueueStableForEqual` | several requests, one band | arrival order preserved |
| `TestFIFO_Priority_ZeroNormalized` | resolver path yields 0 (synthetic) | normalization ⇒ `defaultPriority`; scheduler never sees 0 |
| `TestFIFO_Priority_GrantMetadata` | after grant | `fifo_priority` equals the effective value |

Config validation tests in `internal/config/config_test.go`:

| Scenario | Expectation |
|----------|-------------|
| enabled map, `defaultPriority: 42` not in bands | load error (D6) |
| band mapping to 0 (`batch: 0`) | load error |
| duplicate values (`high: 60, medium: 60`) | load error |
| blank key / uppercase key (`" ": 10`, `"HIGH": 100`) | blank ⇒ error; `HIGH` ⇒ stored as `high` |
| empty map | accepted ⇒ feature off |
| leftover `priority: {gemma: 5}` | loads successfully; one-time deprecation warning pointing at `requestPriority` (D14) — `TestConfig_RequestPriority_LegacyPriorityKeyAccepted` |
| official v255 fixture (`testdata/upstream-v255.example.yaml`, priority block from git 6384ea93~1) | zero load errors; `priority` ignored; deprecation warning emitted — `TestConfig_UpstreamV255PriorityCompat` |

Acceptance:

```
go test -v -run TestFIFO_Priority ./internal/router/scheduler/
go test -v -run Priority ./internal/config/
make test-dev
```

Note: if `resolvePriority` lands as an unexported method on `baseRouter` rather
than in the scheduler package, put its branch-level cases in `base_test.go` but
keep the `TestFIFO_Priority_*` naming so the `-run` regex covers both.

## 15. Future work (L2+)

> Draft only. Parameters below are to be calibrated against **real queueing and
> timeout telemetry collected after L1 ships** — not specified ahead of data.

1. **Per-band concurrency caps** (keyed per `(model, band)`):
   - admit/serve a request only while its `(model, band)` counter is under the cap;
   - the drain capacity gate continues to use **in-flight, never reserved** (§2);
   - **fast-path must also honor band thresholds** (it currently bypasses all gates);
   - meaningful only for multi-concurrency models — the 7×1 server does not configure it.
   - Config follows the three-state `*int` vocabulary already used by the queue:
     `nil` = unlimited / `0` = deny that band / `>0` = cap.
2. **Activity-table priority column** (outlined in §16): the in-flight table
   already shows the resolved band; extending the same metadata key to the
   activity table's column set is remaining UI work.
3. **`fifo_priority_miss_total{raw,target}` counter** for gateway-word drift alarms.
4. Per-router or per-model enablement, only if post-L1 data shows a need.

## 16. UI display convention (D15)

The **In-flight Requests** table gains a `Priority` column (default visible,
positioned right after Stage) showing each request's **resolved numeric band**
(e.g. `60`, `100`). The activity log will also surface the same key/value in
its metadata bag — that is an accepted side effect, not a regression.
Explicitly: whenever the feature is on, the request-scoped
`ReqContextData.Metadata["priority"]` key rides the same metadata pipeline as
the scheduler's grant-time `fifo_priority` write, so activity rows carry the
same numeric value under both keys; this duplication is **by design** (accepted
side product of §16's display path) and must not be treated as a display-side
bug or suppressed.

### Data path

Resolution happens late — inside `baseRouter.ServeHTTP` (`base.go:547-554`),
which is *after* `inflightTracker.Add` shallow-copies the entry metadata
(`inflight.go:125-128`). A grant-time write can therefore never reach the
in-flight list. The middleware closes that gap by resolving **once more, at
ingress**:

1. `CreateInflightMiddleware` (`internal/server/inflight.go`) calls the shared
   `router.ResolveRequestPriority` — the same §7 C/A/B/B′ chain the router uses,
   extracted in D15 without changing its behavior — when
   `routing.scheduler.settings.fifo.requestPriority` is non-empty.
2. The result is written as `swaputil.SetReqData(ctx, "priority", strconv.Itoa(p))`
   **before** `t.Add`, so the shallow copy at Add time captures it into
   `entry.Metadata`.
3. The UI reads `metadata.priority`; a missing value renders `—`.

### Conventions

- **Key**: `"priority"`, value is the **parsed numeric band as a string** (the
  middleware mirrors the router's single normalization point, so `0` never
  reaches the metadata either).
- **Feature off** (empty/absent `requestPriority`): no key is written, so every
  row renders `—`. "Feature disabled" and "unset" are intentionally
  indistinguishable.
- **Warnings stay single-sourced**: the middleware calls
  `ResolveRequestPriority` with a nil `warnOnce` callback, so the authoritative
  router remains the only logger for B/B′ misses and the raw-keyed dedup (§7/D8)
  is unchanged.
- **Display-only**: the column never feeds the scheduler; queue ordering and
  admission semantics are untouched by this extension.
- **Persistence**: the column joins the existing in-flight column
  visibility/order stores; the default-order backfill mechanism appends it for
  users with saved layouts.

## Document location note

This file intentionally lives at `docs/design/request-priority.md` and must NOT
be placed under `docs/kb/` (that tree is fed to the frontend Docs Agent). The
repository's only other design precedent, `internal/router/design.md`, is a
package tutorial for the router internals and is not part of the docs tree.

## 17. L1.5 — Request-priority aging: threshold promotion (design)

> **Status** — L1.5 design draft, pending approval. The mechanism itself is
> pre-agreed and treated as fixed input (threshold promotion at
> `routing.scheduler.settings.fifo.promoteAfter`, effective-priority re-rank at
> drain time); the deltas left open for the designer — enqueued-at source,
> promoted-tier value, conflicting-config handling — are resolved below and
> recorded in D16+ so the D-series stays continuous.
> **Scope** — scheduler-internal ordering only. The only contract surface this
> feature adds is one new `*int` config key plus its load-time validation; no
> new SSE fields, no UI column, no metadata keys (D22).
> **Document lineage** — appended to the L1 spec so this fork keeps a single
> authoritative design document. §1–§16 and D1–D15 above are frozen and
> untouched; every reference below is forward-referenced from — never written
> back into — the existing sections (§17.12).

### 17.1 Motivation and goals

Under L1, FIFO service order is resolved band descending with strict stability
per band (§8, `fifo.go:513-526`). A continuously arriving flood of high-priority
work can therefore pin low/medium work in `s.queued` indefinitely. The only
exit hatches are the `queueTimeout` lazy prune (429 + Retry-After: 1,
`fifo.go:544-548`) when it is configured, or the client giving up. The observed
failure is a *timeout lottery*: the low-priority request repeatedly waits out
its deadline and is killed while high-priority arrivals keep the queue pinned —
the request is effectively starved, then retried into the same tail.

L1.5 replaces that lottery with a bounded-wait guarantee, in its simplest
possible form:

1. When enabled, any request waiting in `s.queued` for ≥ `promoteAfter` is
   assigned an **effective (scheduling) priority of `P_max`**, an internal tier
   strictly above every configured band, at the next drain.
2. The drain re-ranks the global queue by effective priority (descending) with
   strict arrival order within each tier — promotion therefore becomes a
   **lock-in** (a promoted item can no longer be overtaken by any later
   arrival, §17.7).
3. **Zero cost when off**: `promoteAfter` nil/0 keeps `drainQueue` byte-for-byte
   L1 behavior; the re-rank is guarded by a single branch.
4. The feature stays **scheduler-internal**: `HandlerReq.Priority`,
   `fifo_priority`, the in-flight `priority` column, and the SSE
   `stage`/`queue_position` contract are unchanged in shape (§17.8).
5. The L1 global-queue property that "a high-priority *rank* never blocks a
   grant for another model" is preserved under promotion (§17.6).

### 17.2 Terminology

- **Resolved band** — the `> 0` numeric value L1 resolves at ingress (§7) and
  stores in `HandlerReq.Priority`; what `fifo_priority` and the in-flight
  `priority` column show. L1 §9 already calls this "the request's effective
  priority"; to avoid colliding with the aging sort key below, L1.5 uses
  *resolved band* for it.
- **Effective (scheduling) priority** — `eff(x)`; the sort key used by the
  drain re-rank. Equals `Req.Priority` normally and `P_max` once `x`'s queue age
  ≥ `promoteAfter`. Scheduler-internal; never observable through any existing
  channel (D22). The re-rank orders by `eff` descending and, within every tier
  (including the pinned `P_max` tier), by `EnqueuedAt` ascending — the strict
  arrival order is an explicit part of the sort key, so the lock-in lemma holds
  even for a later arrival that itself ages to pinned (§17.7).
- **Pinned** — an item with `eff(x) = P_max`: it rides the exclusive top tier.
  Promotion lasts for the rest of the item's queue lifetime (age only grows);
  "temporary" in the pre-agreed spec means it is a transient *rank override*,
  not a mutation of the resolved band.

### 17.3 Configuration schema (draft)

New key under `routing.scheduler.settings.fifo`, reusing the three-state `*int`
vocabulary of `queueDepth` / `queueTimeout` (§2, `config.go:241-271`):

| Key            | Type   | Default | Semantics                                            |
|----------------|--------|---------|------------------------------------------------------|
| `promoteAfter` | `*int` | `nil`   | seconds; `nil`/`0` = feature off (L1 unchanged); `>0` = queued requests waiting ≥ this long are pinned to `P_max` at the next drain |

Validation rules (all evaluated at load, `internal/config/load.go` fifo block,
currently `load.go:272-311`):

1. `nil` or `0` → off, accepted silently.
2. `< 0` → load error.
3. `> 0` → on. Converted like `queueTimeout`: `time.Duration(*cfg.PromoteAfter)
   * time.Second` in `NewFIFO` (mirroring `fifo.go:123-130`). Extreme values
   keep the same leniency `queueTimeout` already exhibits — no new overflow
   machinery, matching existing behavior.
4. **Conflict rule (D20)**: when the *effective* `queueTimeout` is active
   (`> 0`, whether explicit or the `nil` default of 60s, §2) and
   `effective queueTimeout ≤ promoteAfter`, loading fails. Both clocks start at
   enqueue, so a `queueTimeout` no larger than `promoteAfter` means the lazy
   prune would drop every item before promotion could ever have effect —
   a provably dead feature fails fast (same philosophy as D6). A
   `queueTimeout: 0` (infinite) pairs with any `promoteAfter`.
5. **Top-tier exclusivity (D17)**: every declared band value (and therefore
   `defaultPriority`, which must be a band) must be `< P_max = 2147483647`.
   Essentially unreachable for real configs; the rule exists so the invariant
   "`P_max` is not a band" is locally checkable by inspection.
6. **Independence (D21)**: `promoteAfter` does *not* require `requestPriority`
   to be populated. With an empty map every request resolves to the single
   `defaultPriority` band (`§6`, `resolve.go:25-46`) and the queue is otherwise
   pure FIFO; promotion then acts as a pure maximum-wait guarantee. Operators
   who want untouched L1 semantics must leave `promoteAfter` unset/0.

Because `promoteAfter` is a **fork-new** key, no official upstream v255
configuration can ever contain it; the D14 principle ("any legal official v255
config loads without error") is untouched by every rule above.

#### Example — enabled (target production shape)

```yaml
routing:
  scheduler:
    use: fifo
    settings:
      fifo:
        queueDepth: 64
        queueTimeout: 600             # must be > promoteAfter (D20)
        priorityHeader: X-Request-Priority
        requestPriority:
          high: 100
          medium: 60
          low: 30
          batch: 10
        defaultPriority: 60
        promoteAfter: 120             # L1.5: pin queued requests older than 120s
```

#### Example — feature off (L1 behavior)

```yaml
routing:
  scheduler:
    use: fifo
    settings:
      fifo:
        queueDepth: 64
        queueTimeout: 600
        requestPriority: { high: 100, medium: 60, low: 30 }
        defaultPriority: 60
        # promoteAfter omitted or 0: no re-rank, drain identical to L1
```

### 17.4 Algorithm (draft)

**Data structure (D16).** `queuedItem` (`fifo.go:41-44`) gains one field:

```go
type queuedItem struct {
    Req        HandlerReq
    Deadline   time.Time // PATCH(v255): queueTimeout, unchanged
    EnqueuedAt time.Time // L1.5: written once at enqueue, never reset (D16)
}
```

*Trade-off — field vs deriving the stamp from `Deadline`.* `Deadline` is set at
enqueue to `now + queueTimeout` (or zero, `fifo.go:474-479`), so a stamp could
be reconstructed as `Deadline - queueTimeout` — but *only* when `queueTimeout >
0`. That fails exactly in the `queueTimeout: 0` (infinite) shape that pairs
naturally with aging (rule 4), and it couples promotion semantics to the
timeout machinery. A dedicated field costs 16 bytes, matches the existing
`Deadline` pattern, and makes the promotion clock always derivable. **D16 adopts
the field.**

**enqueue (`fifo.go:513-526`) — unchanged.** A freshly enqueued item has age
< `promoteAfter`, so inserting by its resolved band is exactly right; the aging
effect only materializes at drain time. The only code change is writing
`EnqueuedAt: time.Now()` beside the existing `Deadline` write (`fifo.go:524`).

**drainQueue (`fifo.go:532-601`) — effective-order re-rank at the top.**

```go
// D17: reserved internal top tier, strictly above every configured band.
const promotedPriority = 2147483647 // math.MaxInt32

// effective scheduling priority — scheduler-internal only (§17.2).
func (s *FIFO) effective(item queuedItem) int {
    if s.promoteAfter > 0 && time.Since(item.EnqueuedAt) >= s.promoteAfter {
        return promotedPriority
    }
    return item.Req.Priority
}

func (s *FIFO) drainQueue() {
    if len(s.queued) == 0 {
        return
    }
    // L1.5 (feature off => this branch is never taken, zero cost).
    // Sort by the dual key (effective priority desc, EnqueuedAt asc): within
    // every tier — including the pinned P_max tier — the order is the explicit
    // arrival order, so the lock-in lemma holds strictly for any later
    // arrival, whatever enqueue's band insert did (§17.7). EnqueuedAt ties
    // fall back to the stable sort's preserved relative order.
    if s.promoteAfter > 0 {
        sort.SliceStable(s.queued, func(i, j int) bool {
            if e1, e2 := s.effective(s.queued[i]), s.effective(s.queued[j]); e1 != e2 {
                return e1 > e2
            }
            return s.queued[i].EnqueuedAt.Before(s.queued[j].EnqueuedAt)
        })
    }
    // … the existing single-pass walk below is unchanged …
    // (lazy timeout prune fifo.go:544-548, in-flight capacity gate
    //  fifo.go:551-558, model state, swap join fifo.go:565-580,
    //  fast-path fifo.go:581-587, collision fifo.go:588-591,
    //  evict-busy fifo.go:592-595, startSwap fifo.go:596-597)
}
```

Ordering and lifecycle notes:

- **Promotion enters lazily.** An item becomes `P_max` at the *first drain*
  whose wall-clock age is ≥ `promoteAfter` (≥ semantics, boundary-tested in
  §17.9). Because the re-rank precedes the walk, a newly pinned item is
  evaluated as pinned from the instant its state flips in the same drain.
- **Skipped items keep their stamp.** Items the walk passes over (capacity gate
  `fifo.go:555`, collision `fifo.go:589`, evict-busy `fifo.go:593`, join cap
  `fifo.go:573`) are re-appended in order to `remaining` with `EnqueuedAt`
  untouched — wait time accrues **continuously** across skips (D19). A swap /
  collision block must not "forgive" the wait; the guarantee would be
  meaningless otherwise.
- **Join exits aging scope.** A queued item that joins an active swap inside
  the walk (`fifo.go:565-580`) leaves `s.queued` and is committed to that swap;
  it is granted at `OnSwapDone` (`fifo.go:272-288`), and its `EnqueuedAt`
  becomes moot. The same applies to ingress-time joins (`fifo.go:190-195`),
  which never queue at all.
- **Removal paths are unchanged.** `OnCancel` pruning (`fifo.go:231-266`),
  `OnUnload` drops (`fifo.go:338-348`) and `OnShutdown` grants
  (`fifo.go:361-371`) operate exactly as in L1; a removed item stops aging with
  its removal.
- **Staleness caveat.** Promotion (like the timeout prune it mirrors) is only
  evaluated when a drain runs. Drains are event-driven (serve/swap completion,
  unload), so in steady state the activation latency is one inter-event gap;
  a pathological pause (e.g. a hanging swap) would delay promotion exactly as
  it already delays timeout evaluation — accepted, inherited behavior.

### 17.5 Boundaries (consistent with L1 §8 / D9)

| Context | Participates in aging? | Why |
|---|---|---|
| `s.queued` (fifo.go:66, the single global queue) | **yes** — the only aging domain | the queue is the only place order competition exists |
| fast-path (`fifo.go:200-205`) | no | zero wait, never queued |
| ingress-time swap-waiter join (`fifo.go:190-195`) | no | swap-driven commit, not ordering-driven (same exclusion as L1 §8) |
| queued item joining a swap during drain (`fifo.go:565-580`) | exits on join | committed to a swap; granted at `OnSwapDone` |
| drain-skipped items staying queued | yes, continuously | wait time accumulates across skips by design (D19) |

### 17.6 Cross-model non-interference — preserved under aging

The global queue with per-model, in-flight-counting gates has always had the
property that a high *rank* of model A never *blocks* a grant for model B: the
single-pass walk (`fifo.go:538-598`) evaluates each item against its own
model's live state independently, granting every satisfiable item in one drain
(§2's in-flight-not-reserved gate, `fifo.go:551-558`). A promotion re-rank only
**permutes the walk order**; it cannot make a satisfiable item of another model
ungrantable. The one genuinely new effect is intra-model: among the *same
model's* items an aged low band now ranks above newer high bands — which is the
intended mechanism. The L1 conclusion "模型 A 高优不干扰模型 B 低优" therefore
still holds verbatim under aging; the queue_position of B's items may shift, but
grant eligibility per model does not (test row 9, §17.9).

### 17.7 Lock-in and wait bound

**Lemma (lock-in, 升档即锁位).** Let `d₀` be the first drain at which `x` is
pinned. The re-rank places every effective-`P_max` item ahead of all non-pinned
items, and orders the pinned set by arrival — the comparator is `(eff desc,
EnqueuedAt asc)`, so within a tier the explicit `EnqueuedAt` comparison, not
sequence stability, decides (§17.4). After `d₀`:

> - every item ahead of `x` is pinned **and** arrived earlier than `x`;
> - any later arrival `y` — *whatever its resolved band* — sorts behind `x` on
>   every later drain: while not pinned, `eff(y) < P_max`; once pinned, the
>   explicit `EnqueuedAt` ordering keeps it behind `x` because
>   `EnqueuedAt(y) > EnqueuedAt(x)`.

So `rank(x)` is **monotone non-increasing** from `d₀` onward: promotion locks
`x`'s place at the exclusive top tier, ahead of everything that arrives after
it, forever.

**Wait bound (deterministic order, loose time).** Let `R₀` be the number of
items ordered ahead of `x` right after the `d₀` re-rank (= `position(x) - 1` in
effective order), `S_max` a worst-case single-request service duration, `B_M`
the already-serving in-flight backlog of `x`'s model at `d₀` (each frees a slot
and triggers a drain at its own completion via `OnServeDone`,
`fifo.go:293-309`), and `δ₀` the latency to the first drain after `promoteAfter`
(a one inter-event gap in steady state). Then:

```
W(x) ≤ promoteAfter + δ₀ + (R₀ + B_M) × S_max
```

`R₀` is bounded by the number of *previously pinned, earlier-arrived* items — a
set that only shrinks. Collapsed to the intuitive form operators will reason
about:

> 低优请求等待上界 ≈ `promoteAfter` + 当前最高档存量请求数 × 平均服务时长。

The bound is a hard cap on **order** (newcomers can never extend it) and a
loose cap on **time**; the `≈` acknowledges that a tight closed form depends on
per-model capacity dynamics. Items of other models ranked ahead do not extend
`W(x)` beyond ordering (§17.6).

### 17.8 Display and observability (D22)

- `HandlerReq.Priority` keeps the resolved band; `grantHandler` continues to
  write `fifo_priority = req.Priority` (`fifo.go:378-381`). **Observers never
  see `P_max`** through any existing channel.
- The in-flight `priority` metadata key / UI `Priority` column (§16) keep
  showing the resolved band.
- The SSE contract is untouched: `stage` and `queue_position` keep their json
  shapes (`internal/swaputil/events.go:74-77`); no "promoted" flag ships in
  L1.5.
- `queue_position` is derived from `s.queued` in service order (`Queued()`,
  `fifo.go:94-107`; `broadcastQueuePositions`, `fifo.go:700-717`) and therefore
  reflects the **effective order after re-rank**. UI-side convention (given to
  the UI, not a bug report): a queued low-priority request's position may
  visibly *decrease* over time — that is the feature working, and the position
  is best-guess service order, never a reservation slot.
- No per-event logging of promotions: `P_max` must not leak into logs either.
  An optional debug-level trace ("first promoted") is a possible diagnostics
  nicety, to be agreed at implementation, not part of this spec.

### 17.9 Test matrix (design-level; writing code is out of scope)

Unit tests in `internal/router/scheduler/fifo_test.go` under the repo naming
convention `TestFIFO_Aging_<scenario>` (extends the `TestFIFO_Priority_*` +
`TestSchedulerName_*` pattern); config checks in `internal/config/config_test.go`
as `TestConfig_PromoteAfter_*`.

| # | Test / scenario | Setup | Expectation |
|---|-----------------|-------|-------------|
| 1 | `TestFIFO_Aging_PromotesAfterThreshold` | low-priority request queued at `t₀`; continuous high-priority arrivals keep enqueueing; a drain occurs after `t₀ + promoteAfter` | low request pinned and granted ahead of the newer high arrivals |
| 2 | `TestFIFO_Aging_StableAmongPromoted` | two low requests promoted in *different* drains | earlier-arrived stays ranked before later-arrived regardless of original bands (stability) |
| 3 | `TestFIFO_Aging_PromotedNeverOvertaken` | test a pinned item's rank across several drains while new high-priority items arrive | `rank(x)` monotone non-increasing (lock-in lemma §17.7) |
| 4 | `TestFIFO_Aging_FeatureOffNil` | `promoteAfter: nil`; re-run the L1 §14 ordering cases | drain sequence identical to L1 baseline; no re-rank path executed |
| 5 | `TestFIFO_Aging_FeatureOffZero` | `promoteAfter: 0` | same as row 4 |
| 6 | `TestFIFO_Aging_CancelDoesNotAge` | aged-but-cancelled request, then a drain | pruned by `OnCancel`; no grant; reservation released |
| 7 | `TestFIFO_Aging_SkipKeepsEnqueuedAt` | request blocked by collision / capacity across several drains, then unblocked | promoted via continuous age — `EnqueuedAt` never reset (D19) |
| 8 | `TestFIFO_Aging_JoinExitsScope` | pinned request joins an active swap during drain | granted at `OnSwapDone`; no further aging semantics apply |
| 9 | `TestFIFO_Aging_CrossModelNoBlocking` | model A pinned item ranks first; model B item has free capacity | B still granted in the same drain pass (§17.6) |
| 10 | `TestFIFO_Aging_QueuePositionEffective` | promote a low request mid-queue, then drain | `PositionCh` broadcasts / `Queued()` show the new (front-lifted) order (§17.8) |
| 11 | `TestFIFO_Aging_TimeoutPruneStillWins` | `queueTimeout: 10, promoteAfter: 5`, capacity never frees | item is pinned but still dropped at 10s with 429 + Retry-After: 1 (lazy prune order unchanged) |
| 12 | `TestFIFO_Aging_ClockBoundary` | age exactly == `promoteAfter` vs just below | ≥ promotes; < does not |

Config validation (`TestConfig_PromoteAfter_*`):

| Scenario | Expectation |
|----------|-------------|
| `promoteAfter: -5` | load error |
| `promoteAfter: nil` / `0` | accepted → off |
| `promoteAfter: 30` + explicit `queueTimeout: 60` | accepted |
| `promoteAfter: 300` + explicit `queueTimeout: 60` | load error (D20) |
| `promoteAfter: 300` + `queueTimeout` omitted (effective default 60s) | load error on the *effective* pair (D20) |
| `promoteAfter: 300` + `queueTimeout: 0` (infinite) | accepted |
| `promoteAfter: 30` + band value `2147483648` (`high`) | load error (D17 sentinel exclusivity; checked only while promoteAfter is enabled — feature off keeps the L1 lenient band range) |
| `promoteAfter: 30` + `requestPriority` empty | accepted (aging independent of the feature flag, D21) |

Acceptance:

```
go test -v -run 'Aging|Priority' ./internal/router/scheduler/
go test -v -run 'PromoteAfter|Priority' ./internal/config/
make test-dev
```

Implementation note for deterministic tests: the existing suite drives the wall
clock implicitly; threshold-based aging makes real-time tests flaky. The
minimal enabler is an injectable clock on `FIFO` — `now func() time.Time`
(defaulting to `time.Now`), shared by `deadline()` (no behavior change) and the
aging check (D23).

### 17.10 Decision log (D16+)

| # | Decision | Rationale (EN) | 备注 |
|---|----------|----------------|------|
| D16 | `queuedItem` gains `EnqueuedAt time.Time`, written exactly once at enqueue (`fifo.go:524`), never reset on drain skips. | Reconstructing the stamp from `Deadline` fails when `queueTimeout: 0` (no deadline) and would couple promotion to the timeout machinery; a field matches the existing `Deadline` pattern and is always derivable. | 采用独立入队时刻字段；deadline 反推在 queueTimeout=0 时不可用且会让 aging 耦合超时机制，故放弃。 |
| D17 | Promoted tier is a fixed internal sentinel `P_max = 2147483647` (math.MaxInt32), outside the band space; validation forbids band values ≥ it. | A `maxBand+1`-derived tier would be a *relative* value that shifts across config reloads and is undefined when `requestPriority` is empty (D21); a fixed sentinel makes "P_max is not a band" locally checkable at zero practical cost. | 升档档位固定为 Int32 上限哨兵，不随配置推导；校验禁止 band 触顶，排序键永不碰撞。 |
| D18 | Promotion materializes as a `sort.SliceStable` re-rank by the dual key (effective priority desc, `EnqueuedAt` asc) at the top of `drainQueue`, guarded by `promoteAfter > 0`; `enqueue` stays a resolved-band insert. | Re-rank on drain is O(n log n) worst case but zero cost when off; enqueue stays O(n), and the explicit (eff desc, EnqueuedAt asc) comparison yields the lock-in lemma for every later arrival, whatever enqueue's band insert did (§17.7). | 重排只在 drain 开头一次性做；enqueue 不动；比较器为 (有效优先级降序, EnqueuedAt 升序)——同档显式按到达序，稳定排序仅作同刻兜底。 |
| D19 | The promotion clock is monotone: skipped items keep their `EnqueuedAt`, wait time accumulates continuously. | A collision/capacity block must not reset the countdown or the bounded-wait guarantee becomes meaningless. | 碰撞/容量阻挡停留队中的项等待时间连续累计，enqueuedAt 只写一次。 |
| D20 | `effective queueTimeout ≤ promoteAfter` (both active) is a **load-time error**. | Both clocks start at enqueue and the lazy prune (`fifo.go:544-548`) would drop every item before promotion could ever fire — a provably dead feature fails fast (D6 philosophy). Only configs containing the new key can trigger it, so the v255 zero-error principle is untouched. | 两者同起点计时；promoteAfter 不小于有效 queueTimeout 时老化永不生效，加载期报错；仅涉及 fork 新键，无损官方配置兼容。 |
| D21 | Aging is independent of the `requestPriority` feature flag: `promoteAfter > 0` with an empty/single-band map yields a pure maximum-wait FIFO. | A single-band queue has identical structure whether or not the header is parsed; gating aging on the map adds a surprising dependency and leaves single-band deployments without the guarantee. | aging 独立于优先级 feature 开关；单档/feature-off 下仍可作纯最大等待保障。 |
| D22 | Display/observability unchanged: `HandlerReq.Priority`, `fifo_priority`, in-flight `priority` / UI `Priority` column all keep the resolved band; no new SSE fields; `queue_position` reflects the effective order (forward jumps are expected and documented for the UI). | Exposing a synthetic tier through existing channels would corrupt L1 value semantics for third-party consumers; scheduling-internal by design. | 展示一律保留原始档位，aging 纯调度内部；queue_position 反映重排后的有效顺序，低优前移为预期行为。 |
| D23 | FIFO gains an injectable clock `now func() time.Time` (default `time.Now`) shared by the deadline and aging computations, for deterministic tests. | Threshold aging is untestable with real sleeps; a single clock source (no default-path behavior change) is the minimal testability enabler. | 为了让 aging 阈值可确定性测试，给 FIFO 增加可注入时钟 `now`，默认 time.Now，行为零变化。 |

### 17.11 Algorithmic complexity and cost

- Feature off: zero re-ranks; `drainQueue` is L1 code (a single inserted `if`).
- Feature on: one `sort.SliceStable` of `n = len(s.queued)` at the top of each
  drain — O(n log n) worst case. Recommended micro-optimization: precompute one
  `eff` value per item per drain so `time.Since` is not called inside the
  comparator. `enqueue` keeps its O(n) insert. `Queued()` /
  `broadcastQueuePositions` are unchanged and naturally consume the re-ranked
  order at no extra cost.

### 17.12 Forward references to the frozen L1 sections — declared here only

Per the document rules, the frozen §9 and §15 are not edited; the two
forward-references L1.5 needs are recorded here instead:

- **§9 (Observability)** — "…has upgraded from `0` to *the request's effective
  priority*". At L1.5 that phrase must be read as **the resolved band at
  ingress**; the aging-adjusted `P_max` is deliberately invisible everywhere
  §9 describes. If promotion telemetry is ever wanted, the agreed extension
  point is a §9-style counter (`fifo_promote_total{model}`, mirroring the
  existing `fifo_priority_miss_total`), to be specified as an L2 item.
- **§15 (Future work)** — two L2 candidates surface from L1.5 and should be
  picked up there when L2 is specced: (a) promotion observability (above,
  counter and/or per-request "promoted" flag in a future SSE extension);
  (b) `promoteAfter` calibration guidance once real queueing/timeout telemetry
  exists — the §15 "calibrate against real data" discipline applies unchanged
  to the new parameter.
