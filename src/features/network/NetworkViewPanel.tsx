import { EDGE_LEGEND_ITEMS } from "@/features/network/networkViewLegend";
import type { ScoreFailureDiagnostic } from "@/features/network/NetworkView";

type Props = {
  isFullscreen: boolean;
  graph: {
    renderedEdges: number;
    budgetLabel: string;
    adaptiveBudget: number;
    nodes: Array<unknown>;
    scoreSizingActive: boolean;
    scoreSizedNodeCount: number;
    scoreDay: string | null;
  };
  budgetPreset: "auto" | "500" | "1000" | "2000" | "unlimited";
  anchoredCount: number;
  onBudgetChange: (value: "auto" | "500" | "1000" | "2000" | "unlimited") => void;
  onClearAnchors: () => void;
  onEnterFullscreen: () => void;
  onExitFullscreen: () => void;
  scoreStatusRows: ScoreFailureDiagnostic | null;
  onRetryScoreLoad: () => void;
};

export function NetworkViewPanel({
  isFullscreen,
  graph,
  budgetPreset,
  anchoredCount,
  onBudgetChange,
  onClearAnchors,
  onEnterFullscreen,
  onExitFullscreen,
  scoreStatusRows,
  onRetryScoreLoad
}: Props) {
  return (
    <>
      {isFullscreen ? (
        <button
          type="button"
          className="absolute right-3 top-3 z-10 rounded border border-slate-300 bg-white/95 px-2 py-1 text-xs hover:bg-slate-100"
          onClick={onExitFullscreen}
        >
          Exit Fullscreen
        </button>
      ) : (
        <header className="mb-2 flex items-center justify-between">
          <h2 className="text-lg">Network Explorer</h2>
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted">
              {graph.renderedEdges} edges / {graph.budgetLabel} LOD budget
            </span>
            <button
              type="button"
              className="rounded border border-slate-300 px-2 py-0.5 text-xs hover:bg-slate-100"
              onClick={onEnterFullscreen}
            >
              Fullscreen
            </button>
          </div>
        </header>
      )}
      {!isFullscreen ? (
        <div className="mb-3 space-y-2">
          <div className="flex flex-wrap items-center gap-2 text-xs text-muted">
            <label htmlFor="lod-budget">LOD budget</label>
            <select
              id="lod-budget"
              className="rounded border border-slate-300 px-1 py-0.5"
              value={budgetPreset}
              onChange={(event) => onBudgetChange(event.target.value as typeof budgetPreset)}
            >
              <option value="auto">Auto ({graph.adaptiveBudget})</option>
              <option value="500">500</option>
              <option value="1000">1000</option>
              <option value="2000">2000</option>
              <option value="unlimited">Unlimited</option>
            </select>
            <span className="ml-1">Anchored: {anchoredCount}</span>
            <button
              type="button"
              className="rounded border border-slate-300 px-1 py-0.5 disabled:cursor-not-allowed disabled:opacity-60"
              onClick={onClearAnchors}
              disabled={anchoredCount === 0}
            >
              Clear anchors
            </button>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-[11px] text-slate-700">
            <div className="uppercase tracking-wide text-slate-500">Edge legend</div>
            <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1">
              {EDGE_LEGEND_ITEMS.map((item) => (
                <div key={item.key} className="flex items-center gap-1">
                  <span
                    aria-hidden="true"
                    className="inline-block h-2.5 w-2.5 rounded-sm"
                    style={{ backgroundColor: item.color }}
                  />
                  <span>{item.label}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      ) : null}
      {!isFullscreen ? (
        <div className="mb-2 text-xs text-muted">
          {graph.nodes.length} alliances shown | {graph.renderedEdges} treaties shown | Node size by {graph.scoreSizingActive ? "score" : "connections"}
          {graph.scoreSizingActive ? ` | scored ${graph.scoreSizedNodeCount}/${graph.nodes.length} (${graph.scoreDay ?? "n/a"})` : ""}
        </div>
      ) : null}
      {!isFullscreen && scoreStatusRows ? (
        <div className="mb-2 rounded border border-rose-200 bg-rose-50 px-2 py-2 text-[11px] text-rose-900">
          <div className="font-medium">{scoreStatusRows.title}</div>
          <div className="mt-0.5">{scoreStatusRows.message}</div>
          {scoreStatusRows.details ? <div className="mt-0.5 text-rose-700">{scoreStatusRows.details}</div> : null}
          {scoreStatusRows.actionableRetry ? (
            <div className="mt-2">
              <button
                type="button"
                className="rounded border border-rose-300 bg-white px-2 py-0.5 text-[11px] hover:bg-rose-100"
                onClick={onRetryScoreLoad}
              >
                Retry score load
              </button>
            </div>
          ) : null}
        </div>
      ) : null}
    </>
  );
}
