# Treaty Timelapse Web

Frontend viewer for reconciled treaty timelapse data.

## Stack

- React + TypeScript + Vite
- Tailwind CSS
- ECharts (timeline)
- Cytoscape.js (network)
- Zustand (state)
- Zod (schema validation)

## Setup

1. Install dependencies:

```bash
npm install
```

```bash
npm run data:generate
npm run data:generate:top50
npm run data:refresh
```

Deletion inference now uses daily alliance snapshots from `https://politicsandwar.com/data/alliances/`.
Missing `alliances-YYYY-MM-DD.csv.zip` files are downloaded to `web/data/alliance_downloads/` automatically.
Use `--skip-alliance-download --alliances-dir <path>` to run from pre-downloaded local files only.


3. Sync generated data into web app data folder:

```bash
npm run data:sync
```

`generate_timelapse_data.py` already writes the web dataset files to `public/data/` as MessagePack and refreshes `public/data/manifest.json`.
`npm run data:sync` only rebuilds `manifest.json` from files currently in `public/data/`.

4. Start dev server:

```bash
npm run dev
```

## Build

```bash
npm run build
```

## Performance Baseline

- Budgets and baseline procedure: `docs/performance-budget.md`.
- Runtime profiling can be enabled with `?perf=1` (or `VITE_TIMELAPSE_PERF=true`).
- Open browser devtools console and run:

```js
window.__timelapsePerf?.reset();
window.__timelapsePerf?.report();
```

## Data files expected

- `treaty_changes_reconciled.msgpack`
- `treaty_changes_reconciled_summary.msgpack`
- `treaty_changes_reconciled_flags.msgpack`
- `flags.msgpack`

These are stored in `public/data/` and indexed by `public/data/manifest.json`.
