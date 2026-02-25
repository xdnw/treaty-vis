# Performance Guardrails (Dev-only)

Runtime perf instrumentation remains development-only.

- Use `npm run dev` plus `?perf=1` (or `VITE_TIMELAPSE_PERF=true`) for diagnostics.
- Production builds must not depend on perf collectors, benchmark tooling, or exposed perf globals.

## Network Guardrails

The network view now tracks three baseline guardrails while rendering:

- `network.graph.build`: keep median frame build time under `16ms`.
- `network.renderer.refresh`: keep typical refresh cost under `10ms`.
- `network.layout.displacement.max`: keep per-tick non-anchored movement under policy caps (`6` default, `12` during one-shot relax).

`network.layout.displacement.avg` is also emitted to monitor temporal churn trends across neighboring playheads.

## Validation Checklist

- Validate at multiple playback speeds and LOD budgets.
- Validate both normal motion and a manual `Re-pack / Relax` invocation.
- Tune only policy constants in `src/features/network/networkViewPolicy.ts` unless algorithmic changes are required.
