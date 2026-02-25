import { EDGE_LEGEND_ITEMS } from "@/features/network/networkViewLegend";
import type { ScoreFailureDiagnostic } from "@/features/network/NetworkView";
import type { NetworkLayoutStrategyField } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyControls";
import type { NetworkLayoutStrategy, NetworkLayoutStrategyConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";

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
  strategy: NetworkLayoutStrategy;
  strategyLabel: string;
  strategyConfigSummary: string;
  strategyOptions: Array<{ value: NetworkLayoutStrategy; label: string }>;
  showStrategyConfig: boolean;
  strategyFields: NetworkLayoutStrategyField[];
  strategyConfig: NetworkLayoutStrategyConfig;
  onBudgetChange: (value: "auto" | "500" | "1000" | "2000" | "unlimited") => void;
  onStrategyChange: (value: NetworkLayoutStrategy) => void;
  onToggleStrategyConfig: () => void;
  onStrategyFieldChange: (key: string, value: number) => void;
  onClearAnchors: () => void;
  onRecalculateHoldStart: () => void;
  onRecalculateHoldEnd: () => void;
  onRecalculateLayout: () => void;
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
  strategy,
  strategyLabel,
  strategyConfigSummary,
  strategyOptions,
  showStrategyConfig,
  strategyFields,
  strategyConfig,
  onBudgetChange,
  onStrategyChange,
  onToggleStrategyConfig,
  onStrategyFieldChange,
  onClearAnchors,
  onRecalculateHoldStart,
  onRecalculateHoldEnd,
  onRecalculateLayout,
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
            <label htmlFor="layout-strategy" className="ml-2">Layout</label>
            <select
              id="layout-strategy"
              className="rounded border border-slate-300 px-1 py-0.5"
              value={strategy}
              onChange={(event) => onStrategyChange(event.target.value as NetworkLayoutStrategy)}
            >
              {strategyOptions.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
            <button
              type="button"
              className="rounded border border-slate-300 px-1 py-0.5 hover:bg-slate-100"
              onClick={onToggleStrategyConfig}
            >
              {showStrategyConfig ? "Hide config" : "Show config"}
            </button>
            <span className="text-slate-600">{strategyLabel}: {strategyConfigSummary}</span>
            <button
              type="button"
              className="rounded border border-slate-300 px-1 py-0.5 disabled:cursor-not-allowed disabled:opacity-60"
              onClick={onClearAnchors}
              disabled={anchoredCount === 0}
            >
              Clear anchors
            </button>
            <button
              type="button"
              className="rounded border border-slate-300 px-1 py-0.5 hover:bg-slate-100"
              onPointerDown={(event) => {
                if (event.button !== 0) {
                  return;
                }
                event.currentTarget.setPointerCapture(event.pointerId);
                onRecalculateHoldStart();
              }}
              onPointerUp={() => {
                onRecalculateHoldEnd();
              }}
              onPointerCancel={() => {
                onRecalculateHoldEnd();
              }}
              onPointerLeave={() => {
                onRecalculateHoldEnd();
              }}
              onBlur={() => {
                onRecalculateHoldEnd();
              }}
              onKeyDown={(event) => {
                if (event.key !== "Enter" && event.key !== " ") {
                  return;
                }
                event.preventDefault();
                onRecalculateLayout();
              }}
              disabled={graph.nodes.length === 0}
            >
              Recalculate
            </button>
          </div>
          {showStrategyConfig ? <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-2 text-[11px] text-slate-700">
            <div className="uppercase tracking-wide text-slate-500">Strategy config</div>
            <div className="mt-1 grid grid-cols-1 gap-2 md:grid-cols-2">
              {strategyFields.map((field) => {
                const valueRaw = strategyConfig[field.key];
                const value = Number(valueRaw);
                return (
                  <label key={field.key} className="flex items-center justify-between gap-2">
                    <span>{field.label}</span>
                    <input
                      type="number"
                      className="w-28 rounded border border-slate-300 px-1 py-0.5"
                      min={field.min}
                      max={field.max}
                      step={field.step}
                      value={Number.isFinite(value) ? value : field.min}
                      onChange={(event) => {
                        const next = Number(event.target.value);
                        if (!Number.isFinite(next)) {
                          return;
                        }
                        onStrategyFieldChange(field.key, next);
                      }}
                    />
                  </label>
                );
              })}
            </div>
          </div> : null}
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
