---
title: Running several models at once with groups and matrix
summary: Choosing between the group and matrix routers, and how each decides what gets unloaded.
category: guides
tags: [routing, groups, matrix, concurrency, swap, vram]
config_keys: [routing, routing.router.use, routing.router.settings.groups, routing.router.settings.groups.*.fuzzy, routing.router.settings.groups.*.fuzzyIdleTimeout, routing.router.settings.matrix]
updated: 2026-10-03
---

# Running several models at once: groups and matrix

Out of the box llama-swap runs one model at a time. The `routing` section
changes that. There are two engines and you pick one:

```yaml
routing:
  router:
    use: group     # or: matrix
```

## `group` — the default, simpler

You define named groups of models and set two flags per group.

```yaml
routing:
  router:
    use: group
    settings:
      groups:
        # default behaviour: one model at a time, instance-wide
        main:
          swap: true         # only one member runs at a time
          exclusive: true    # running a member unloads every other group
          members: [llama, qwen]

        # these three run together, but any other group evicts them
        small:
          swap: false        # all members can be loaded at once
          exclusive: false   # loading one doesn't unload other groups
          members: [embeddings, reranker, whisper]

        # never unloaded by anything else
        forever:
          persistent: true
          swap: false
          exclusive: false
          members: [always-on-embeddings]
```

The three flags:

- **`swap`** (default `true`) — how members behave among *themselves*. `true`
  means one at a time; `false` means they all coexist.
- **`exclusive`** (default `true`) — how the group affects *other* groups.
  `true` means loading a member unloads everything else.
- **`persistent`** (default `false`) — other groups can never unload this one.

**A model can belong to only one group**, and every member must be a real model
ID.

The classic setup is one exclusive group for the big LLMs and one non-exclusive
group for small always-useful models like embeddings and rerankers.

## Fuzzy substitution (`fuzzy`)

On a `swap: true` group, loading another member normally means a full model
switch — up to minutes on large models. `fuzzy` trades strict model identity
for availability: while the group is busy, or during an idle window after its
last served request, a request for a *different* member is served by the
currently online model instead of triggering a switch.

```yaml
routing:
  router:
    use: group
    settings:
      groups:
        main:
          swap: true
          fuzzy: true
          fuzzyIdleTimeout: 300
          members: [llama, qwen]
```

How it decides:

- The group is **busy** (any member is serving, queued, or the target of an
  in-flight swap) → requests for another member are fuzzed onto the online
  model.
- The group is **idle** and it has served at least once within
  `fuzzyIdleTimeout` seconds → still fuzzed onto the online model.
- The group is **idle** and that window has expired, or it has never served a
  request → a real switch happens as usual.

`fuzzyIdleTimeout` is optional, default `300` seconds; `0` restricts fuzzy
substitution to busy periods only. It requires `swap: true` — configuration
fails to load otherwise (which also keeps spillover selectors, which require
`swap: false` groups, incompatible by construction).

Things worth knowing:

- **The request body is not rewritten.** The upstream backend receives the
  model name the client asked for. Backends that strictly validate the `model`
  field need a matching `filters`/`useModelName` setup (or must tolerate the
  original name).
- **The Activity view shows the truth.** Fuzzed requests record
  `served_model` in their metadata, rendered as the Served column of the
  In-flight table and included in the activity record metadata. No key means
  no substitution happened.
- **Hot reloads reset the idle clock.** Rebuilding the server on a config
  change loses the last-served timestamp, so the first request after a reload
  may trigger one real switch.
- Priority and queueing (`routing.scheduler.settings.fifo.*`) apply to the
  rewritten target exactly as before; fuzzy only changes *which* model is
  chosen, not how requests are ordered or admitted.

## `matrix` — more work, far more flexible

The matrix router takes a list of model combinations that are allowed to run
concurrently and solves for the cheapest way to satisfy each request.

```yaml
routing:
  router:
    use: matrix
    settings:
      matrix:
        vars:
          g: gemma-model
          q: qwen-model
          v: voxtral-model

        evict_costs:
          v: 50              # vLLM, slow cold start — avoid evicting
          llama-70B: 30

        sets:
          standard:    "(g | q) & v"
          creative:    "(g | q) & stable-diffusion"
          full:        "llama-70B"
```

`sets` values are expressions:

| operator | meaning |
| --- | --- |
| `&` | AND — these run together |
| `\|` | OR — alternatives |
| `()` | grouping |
| `+name` | inline another set's expression |

`"(g \| q) & v"` expands to `[gemma, voxtral]` and `[qwen, voxtral]`.
Parenthesize mixed expressions so their intended capacity rule is obvious, and
test every requested combination: an expression that excludes a needed set can
make the router evict a model unexpectedly.

How the solver works when a request for model X arrives:

1. If X is already running, forward the request.
2. Otherwise collect every set containing X.
3. For each set, sum the `evict_costs` of running models *not* in that set.
4. Pick the lowest-cost set, ties broken by definition order.
5. Evict the models outside it, start X, forward the request.

Two things worth internalising:

- **Subsets are permitted.** A set `[a, b, c]` also allows `[a, b]`, `[a]` and
  so on. Only the requested model is started; the rest are not preloaded.
- **A model in no set can only run alone.**

`evict_costs` (default 1) is how you express "this one is painful to reload".
Give slow cold-starting backends a high cost.

When you deploy the head end with the kubeswap Helm chart, the chart's
`config.matrix` values can *generate* this whole section from the model
roster instead of you writing the DSL (see the kubeswap-kubernetes
article's matrix-builder section); hand-written routing and the builder are
mutually exclusive.

## Which one?

Use **group** if your setup is describable as "these run together, those swap
out". It is easier to read and easier to get right.

Use **matrix** when the combinations depend on which models are involved — a
70B that needs every GPU alone, versus several small models that fit together,
versus a mid-size LLM plus TTS. Groups cannot express that; matrix can.

## Request ordering

Queued requests are FIFO. Requests can declare a priority band via the
`X-Request-Priority` header, mapped to a numeric value by the scheduler:

```yaml
routing:
  scheduler:
    use: fifo
    settings:
      fifo:
        requestPriority:      # empty map = feature off
          high: 100
          medium: 60          # also the default fallback band
          low: 30
        defaultPriority: 60
```

Higher numbers are serviced first; equal priorities keep arrival order.
Requests without a recognizable `X-Request-Priority` header land on
`defaultPriority`. The legacy model-level `priority` key has been removed.

## Related

- `reference/config/routing` — the full annotated section
- `guides/model-runtime/ttl-and-unloading` — reclaiming VRAM from idle models
- `guides/operations/kubeswap-kubernetes` — the kubeswap Helm chart, whose
  `config.matrix` builds this section from the model roster
