# Performance Budget And Baseline

This project targets near-real-time interaction on modern desktop hardware with 100k+ events.

## Budgets

- Drag timeline range (continuous): p95 <= 16ms per input frame, p99 <= 24ms.
- Playback tick updates: p95 <= 16ms per rendered step.
- Filter typing feedback (input to visible list/network update): p95 <= 120ms.
- Focus update (alliance or edge click to synchronized views): p95 <= 80ms.
- Main thread long tasks: zero tasks >= 50ms during drag/play/filter interaction windows.
- Score loader cache-hit readiness: `ready` <= 50ms after attempt start when cache identity matches.
- Score loader cold path first usable state: <= 400ms to first `cache-hit` or `ready` on local dev data.

## Baseline Scenarios

Run each scenario for at least 10 seconds on the same dataset and browser build.

1. Drag timeline window continuously left/right.
2. Start playback at 4x for 10 seconds.
3. Type 10-20 characters into search filter at natural speed.
4. Click multiple alliance nodes/edges to move focus context.
5. Reload with warm cache and verify score status reaches `ready` in <= 50ms.
6. Clear score cache (or bump manifest hash) and verify first usable score state in <= 400ms.

## Instrumentation

The runtime perf collector is exposed as `window.__timelapsePerf`.

- Enable with `?perf=1` in URL, or `VITE_TIMELAPSE_PERF=true`.
- Reset counters before each run:

```js
window.__timelapsePerf?.reset();
```

- Print aggregated metrics after each run:

```js
window.__timelapsePerf?.report();
```

Key metrics currently tracked:

- `selectEvents`
- `deriveNetworkEdges`
- `buildPulseSeries`
- `app.buildPulseSeries`
- `network.graph.build`
- `network.renderer.refresh`
- `network.flagSprites.draw`
- `timeline.datazoom.flush`
- `filter.text.debounced`
- `playhead.input`
- `range.input`
- `url.replaceState`
- `window.__timelapsePerf.scoreLoads` ring buffer entries for score attempts

## Acceptance Gate

Do not proceed to deeper architecture migrations unless the baseline report is captured and checked against the budgets above.

## Hard Verification Gates

- Score artifact size: `public/data/alliance_scores_v2.msgpack` must be <= 10 MiB.
- Score decode contract: runtime loader must parse only schema version `2` for score payloads.
- Score toggle lifecycle: `sizeByScore` must support failure -> retry -> success without requiring app reload.
- Toggle-to-render latency: p95 <= 200ms from enabling score sizing to first `network.graph.build` with `scoreSizingActive=true`.
- Graph build stability: `network.graph.build` p95 must remain <= 80ms during score sizing runs.
- Score loader diagnostics: each attempt must finish in `ready` or an explicit terminal failure (`error-timeout`, `error-http`, `error-network`, `error-decode`, `error-abort`, `error-manifest-missing`, `error-worker-unavailable`, `error-worker-failure`) with no silent indefinite loading.
