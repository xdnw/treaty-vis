# Performance Budget And Baseline

This project targets near-real-time interaction on modern desktop hardware with 100k+ events.

## Budgets

- Drag timeline range (continuous): p95 <= 16ms per input frame, p99 <= 24ms.
- Playback tick updates: p95 <= 16ms per rendered step.
- Filter typing feedback (input to visible list/network update): p95 <= 120ms.
- Focus update (alliance or edge click to synchronized views): p95 <= 80ms.
- Main thread long tasks: zero tasks >= 50ms during drag/play/filter interaction windows.

## Baseline Scenarios

Run each scenario for at least 10 seconds on the same dataset and browser build.

1. Drag timeline window continuously left/right.
2. Start playback at 4x for 10 seconds.
3. Type 10-20 characters into search filter at natural speed.
4. Click multiple alliance nodes/edges to move focus context.

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

## Acceptance Gate

Do not proceed to deeper architecture migrations unless the baseline report is captured and checked against the budgets above.
