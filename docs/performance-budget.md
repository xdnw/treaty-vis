# Performance Guardrails (Dev-only)

Runtime perf instrumentation remains development-only.

- Use `npm run dev` plus `?perf=1` (or `VITE_TIMELAPSE_PERF=true`) for diagnostics.
- Production builds must not depend on perf collectors, benchmark tooling, or exposed perf globals.

## Network Guardrails

The network view now tracks three baseline guardrails while rendering:

- `network.worker.turnaround`: keep median worker turnaround under `16ms`.
- `network.worker.queueAge`: keep typical queue delay under `20ms` during autoplay.
- `network.graph.build`: keep median frame build time under `16ms`.
- `network.graph.apply.diff`: keep typical incremental graph apply under `8ms`.
- `network.renderer.refresh`: keep typical refresh cost under `10ms`.
- `network.layout.displacement.max`: keep per-tick non-anchored movement under policy caps (`6` default, `12` during one-shot relax).

`network.layout.displacement.avg` is also emitted to monitor temporal churn trends across neighboring playheads.

Network request lifecycle emits these timing stages for diagnostics:
- `requestedAt`: request scheduled on UI thread.
- `startedAt`: worker begins handling that request.
- `finishedAt`: worker completes compute.
- `appliedAt`: graph apply phase consumes that response.

Derived metrics:
- `network.worker.queueAge`: `startedAt - requestedAt`.
- `network.worker.turnaround`: `finishedAt - startedAt`.
- `network.worker.applyLag`: `appliedAt - finishedAt`.
- `network.worker.endToEnd`: `appliedAt - requestedAt`.

### Hard Gate

- Treat these budgets as blocking acceptance criteria for network layout changes.
- If any hotspot is over budget in diagnostics, pause feature work and fix that hotspot first.
- Use `window.__timelapsePerf.reset()` and `window.__timelapsePerf.report()` under `?perf=1` to confirm p50/p95 trends before sign-off.

## Validation Checklist

- Validate at multiple playback speeds and LOD budgets.
- Validate both normal motion and a manual `Re-pack / Relax` invocation.
- For over-budget stages, optimize only the measured bottleneck first:
- Worker: reduce passes/iterations and reuse neighboring-playhead component anchors.
- Graph apply: tighten diff granularity and avoid unchanged property writes.
- Renderer refresh: trigger refresh only when visible graph state changes.
