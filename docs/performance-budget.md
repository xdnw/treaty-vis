# Performance Note

This document is intentionally retired from production guidance.

- Runtime perf instrumentation is development-only.
- Use `npm run dev` plus `?perf=1` (or `VITE_TIMELAPSE_PERF=true`) when local diagnostics are needed.
- Production builds must not depend on perf collectors, benchmark tooling, or exposed perf globals.
