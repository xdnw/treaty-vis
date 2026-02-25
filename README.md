# Treaty Timelapse Web

Frontend viewer and data pipeline for reconciled treaty timelapse datasets.

## Stack

- React + TypeScript + Vite
- Tailwind CSS
- ECharts (timeline)
- Sigma.js + Graphology (network)
- Zustand (state)
- Zod (schema validation)

## Quick Start

1. Install dependencies.

```bash
npm install
```

2. Build core datasets.

```bash
npm run data:generate
npm run data:generate:top50
npm run data:score
npm run data:flags
```

3. (Optional) Refresh full pipeline in one command.

```bash
npm run data:refresh
```

4. Start the app.

```bash
npm run dev
```

## NPM Scripts

### App

- `npm run dev`
- `npm run build`
- `npm run preview`
- `npm run typecheck`
- `npm run test`
- `npm run test:py`

### Data Pipeline

- `npm run data:generate` (reconciled base dataset)
- `npm run data:generate:top50` (strict top-50 variant)
- `npm run data:generate:dry-run` (validation-only, no writes)
- `npm run data:score` (score ranks and related outputs)
- `npm run data:flags`
- `npm run data:flags:legacy`
- `npm run data:sync` (rebuild manifest from existing `public/data/*`)
- `npm run data:refresh` (generate + score + flags + sync)

### Raw Ingestion Helpers

- `npm run data:archive:scrape`
- `npm run data:archive:to-treaties`
- `npm run data:bot:scrape`
- `npm run data:bot:to-treaties`

## Alliance Snapshot Downloads

Deletion inference uses daily alliance snapshots from `https://politicsandwar.com/data/alliances/`.
Missing `alliances-YYYY-MM-DD.csv.zip` files are downloaded to `data/alliance_downloads/` by default.
To run against local files only, pass `--skip-alliance-download --alliances-dir <path>`.

## Diagnostics

- Dev-only perf diagnostics: run `npm run dev` and use `?perf=1` (or `VITE_TIMELAPSE_PERF=true`).
- Production builds must not depend on perf globals or dev-only instrumentation.
- Additional guardrails: `docs/performance-budget.md`.

## Network Layout

- Default layout is deterministic and topology-biased.
- Non-anchored nodes are damped and displacement-capped each tick to reduce temporal churn.
- `Re-pack / Relax` applies a stronger one-shot local relaxation pass.
- Policy constants live in `src/features/network/networkViewPolicy.ts`.

## Data Files Expected

Primary artifacts in `public/data/`:

- `treaty_changes_reconciled.msgpack`
- `treaty_changes_reconciled_summary.msgpack`
- `treaty_changes_reconciled_flags.msgpack`
- `treaty_changes_reconciled_top50_strict.msgpack`
- `treaty_changes_reconciled_top50_strict_summary.msgpack`
- `treaty_changes_reconciled_top50_strict_flags.msgpack`
- `flags.msgpack`
- `flag_assets.msgpack`
- `alliance_score_ranks_daily.msgpack`
- `alliance_scores_v2.msgpack`
- `manifest.json`

`manifest.json` indexes hashes and sizes for served artifacts.
