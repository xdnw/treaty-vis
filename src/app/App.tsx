import { Suspense, lazy, useCallback, useEffect, useMemo, useState } from "react";
import { resolveAllianceFlagSnapshot } from "@/domain/timelapse/loader";
import type { AllianceFlagSnapshot } from "@/domain/timelapse/schema";
import { countByAction } from "@/domain/timelapse/selectors";
import { FilterBar } from "@/features/filters/FilterBar";
import { PlaybackControls } from "@/features/filters/PlaybackControls";
import { InspectorView } from "@/features/inspector/InspectorView";
import { NetworkAllianceHint, type NetworkAllianceHintData } from "@/features/network/NetworkAllianceHint";
import { shouldExitNetworkFullscreenOnEscape } from "@/features/network/networkViewPolicy";
import { formatNumber } from "@/lib/format";
import { useAppQueryState } from "@/app/hooks/useAppQueryState";
import { usePlaybackTicker } from "@/app/hooks/usePlaybackTicker";
import { useTimelapseData } from "@/app/hooks/useTimelapseData";
import { useTimelapseWorkerSelection } from "@/app/hooks/useTimelapseWorkerSelection";
import { useUrlStateSync } from "@/app/hooks/useUrlStateSync";

const TimelineView = lazy(() => import("@/features/timeline/TimelineView").then((module) => ({ default: module.TimelineView })));
const NetworkView = lazy(() => import("@/features/network/NetworkView").then((module) => ({ default: module.NetworkView })));

function uniqueDayTicks(timestamps: string[]): string[] {
  const days = new Set<string>();
  for (const timestamp of timestamps) {
    const day = timestamp.slice(0, 10);
    days.add(`${day}T23:59:59.999Z`);
  }
  return [...days].sort((a, b) => a.localeCompare(b));
}

function downloadObject(filename: string, payload: string, contentType: string): void {
  const blob = new Blob([payload], { type: contentType });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

export function App() {
  const { query, actions } = useAppQueryState();
  const showFlags = query.filters.showFlags;
  const [networkFullscreenHint, setNetworkFullscreenHint] = useState<NetworkAllianceHintData | null>(null);

  useUrlStateSync(query, actions.setStateFromUrl);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (!shouldExitNetworkFullscreenOnEscape(event.key, actions.isNetworkFullscreen)) {
        return;
      }
      event.preventDefault();
      actions.setNetworkFullscreen(false);
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [actions]);

  useEffect(() => {
    if (!actions.isNetworkFullscreen) {
      setNetworkFullscreenHint(null);
      return;
    }

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previousOverflow;
    };
  }, [actions.isNetworkFullscreen]);

  const {
    bundle,
    loading,
    error,
    setError,
    allianceScores,
    scoreLoadSnapshot,
    retryScoreLoad,
    scoreFileDeclared,
    hasScoreRankData
  } = useTimelapseData(showFlags, query.filters.sizeByScore);

  const baseQuery = useMemo(
    () => ({
      ...query,
      playback: {
        ...query.playback,
        playhead: null,
        isPlaying: false
      }
    }),
    [
      query.time,
      query.focus,
      query.filters,
      query.textQuery,
      query.sort,
      query.playback.speed
    ]
  );

  const derivedEvents = bundle?.events ?? [];
  const { scopedSelectionIndexes, pulse } = useTimelapseWorkerSelection({
    bundle,
    baseQuery,
    onError: setError
  });

  const scopedEvents = useMemo(
    () => scopedSelectionIndexes.map((index) => derivedEvents[index]).filter(Boolean),
    [derivedEvents, scopedSelectionIndexes]
  );

  const timelineTicks = useMemo(() => uniqueDayTicks(scopedEvents.map((event) => event.timestamp)), [scopedEvents]);

  useEffect(() => {
    if (!bundle) {
      return;
    }
    if (!query.playback.playhead && bundle.indices.maxTimestamp) {
      actions.setPlayhead(bundle.indices.maxTimestamp);
    }
  }, [actions, bundle, query.playback.playhead]);

  usePlaybackTicker({
    playback: query.playback,
    timelineTicks,
    setPlayhead: actions.setPlayhead,
    setPlaying: actions.setPlaying
  });

  const hasScoreData = scoreFileDeclared;
  const allianceScoreDays = useMemo(() => allianceScores?.dayKeys ?? [], [allianceScores]);

  const resolveAllianceFlagAtPlayhead = useMemo(
    () => (allianceId: number, playhead: string | null): AllianceFlagSnapshot | null => {
      if (!showFlags || !bundle) {
        return null;
      }
      return resolveAllianceFlagSnapshot(bundle.allianceFlagTimelines, allianceId, playhead);
    },
    [bundle, showFlags]
  );

  const countsByAction = useMemo(() => countByAction(scopedEvents), [scopedEvents]);

  const selectionSnapshot = useMemo(
    () => ({
      eventCount: scopedEvents.length,
      focusedAllianceId: query.focus.allianceId,
      focusedEdgeKey: query.focus.edgeKey,
      playhead: query.playback.playhead,
      range: query.time
    }),
    [query.focus.allianceId, query.focus.edgeKey, query.playback.playhead, query.time, scopedEvents.length]
  );

  const handleSetRange = useCallback(
    (start: string | null, end: string | null) => {
      actions.setTimeRange(start, end);
    },
    [actions]
  );

  const handleExportCsv = () => {
    const header = [
      "timestamp",
      "action",
      "treaty_type",
      "from_alliance_id",
      "from_alliance_name",
      "to_alliance_id",
      "to_alliance_name",
      "source",
      "confidence",
      "inferred"
    ];
    const rows = scopedEvents.map((event) =>
      [
        event.timestamp,
        event.action,
        event.treaty_type,
        event.from_alliance_id,
        `"${(event.from_alliance_name || "").replaceAll('"', '""')}"`,
        event.to_alliance_id,
        `"${(event.to_alliance_name || "").replaceAll('"', '""')}"`,
        event.source || "unknown",
        event.confidence,
        event.inferred ? "true" : "false"
      ].join(",")
    );
    downloadObject("treaty-explorer-events.csv", [header.join(","), ...rows].join("\n"), "text/csv;charset=utf-8");
  };

  const handleExportJson = () => {
    downloadObject(
      "treaty-explorer-context.json",
      JSON.stringify({ query, selection: selectionSnapshot, events: scopedEvents }, null, 2),
      "application/json"
    );
  };

  const handleShare = async () => {
    const url = window.location.href;
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(url);
    }
  };

  const networkViewSharedProps = useMemo(
    () => ({
      allEvents: bundle?.events ?? [],
      scopedIndexes: scopedSelectionIndexes,
      baseQuery,
      playhead: query.playback.playhead,
      focusedAllianceId: query.focus.allianceId,
      focusedEdgeKey: query.focus.edgeKey,
      sizeByScore: query.filters.sizeByScore,
      scoreSizeContrast: query.filters.scoreSizeContrast,
      maxNodeRadius: query.filters.maxNodeRadius,
      showFlags,
      flagAssetsPayload: bundle?.flagAssetsPayload ?? null,
      allianceScoresByDay: allianceScores?.byDay ?? null,
      allianceScoreDays,
      scoreLoadSnapshot,
      scoreManifestDeclared: scoreFileDeclared,
      onRetryScoreLoad: retryScoreLoad,
      resolveAllianceFlagAtPlayhead,
      onFocusAlliance: (allianceId: number | null) => actions.setFocus({ allianceId, edgeKey: null, eventId: null }),
      onFocusEdge: (edgeKey: string | null) =>
        actions.setFocus({ allianceId: query.focus.allianceId, edgeKey, eventId: null }),
      onEnterFullscreen: () => actions.setNetworkFullscreen(true),
      onExitFullscreen: () => actions.setNetworkFullscreen(false),
      isPlaying: query.playback.isPlaying
    }),
    [
      actions,
      allianceScoreDays,
      allianceScores?.byDay,
      baseQuery,
      bundle?.events,
      bundle?.flagAssetsPayload,
      query.filters.maxNodeRadius,
      query.filters.scoreSizeContrast,
      query.filters.sizeByScore,
      query.focus.allianceId,
      query.focus.edgeKey,
      query.playback.isPlaying,
      query.playback.playhead,
      resolveAllianceFlagAtPlayhead,
      retryScoreLoad,
      scopedSelectionIndexes,
      scoreFileDeclared,
      scoreLoadSnapshot,
      showFlags
    ]
  );

  const renderNetworkView = useCallback(
    (overrides: {
      isFullscreen: boolean;
      forceFullscreenLabels: boolean;
      onFullscreenHintChange?: (hint: NetworkAllianceHintData | null) => void;
    }) => (
      <NetworkView
        {...networkViewSharedProps}
        isFullscreen={overrides.isFullscreen}
        forceFullscreenLabels={overrides.forceFullscreenLabels}
        onFullscreenHintChange={overrides.onFullscreenHintChange}
      />
    ),
    [networkViewSharedProps]
  );

  if (loading) {
    return <main className="mx-auto max-w-7xl p-6">Loading timelapse dataset...</main>;
  }

  if (error || !bundle) {
    return (
      <main className="mx-auto max-w-7xl p-6">
        <section className="panel p-4">
          <h1 className="text-2xl">Treaty Timelapse</h1>
          <p className="mt-2 text-sm text-slate-700">{error ?? "Failed to load dataset."}</p>
          <p className="mt-2 text-xs text-slate-500">
            Run <code>npm run data:sync</code> after generating reconciled output files.
          </p>
        </section>
      </main>
    );
  }

  return (
    <>
      {!actions.isNetworkFullscreen ? (
        <main className="mx-auto max-w-7xl space-y-4 p-5 lg:p-8">
          <section className="panel overflow-hidden p-5">
            <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
              <div>
                <h1 className="text-3xl">Treaty Explorer</h1>
                <p className="text-sm text-muted">
                  Explore treaty history with synchronized timeline, network, and event-level inspection.
                </p>
              </div>
              <div className="text-xs text-muted">
                Dataset: {bundle.manifest?.datasetId ?? "unversioned"} | Generated: {bundle.summary.generated_at}
              </div>
            </div>

            <div className="mt-3 flex flex-wrap items-center gap-2">
              <button
                type="button"
                className="rounded-md border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50"
                onClick={handleExportCsv}
              >
                Export CSV
              </button>
              <button
                type="button"
                className="rounded-md border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50"
                onClick={handleExportJson}
              >
                Export JSON Context
              </button>
              <button
                type="button"
                className="rounded-md border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50"
                onClick={() => {
                  void handleShare();
                }}
              >
                Copy Share Link
              </button>
              <button
                type="button"
                className="rounded-md border bg-red-200 border-red-300 px-2 py-1 text-xs hover:bg-red-50 active:bg-red-100"
                onClick={actions.resetAll}
              >
                Reset All State
              </button>
              <div className="text-xs text-muted">
                Focus: Alliance {query.focus.allianceId ?? "none"} | Edge {query.focus.edgeKey ?? "none"}
              </div>
              <button
                type="button"
                className="rounded-md border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                onClick={actions.clearFocus}
                disabled={query.focus.allianceId === null && query.focus.edgeKey === null}
              >
                Clear Focus
              </button>
            </div>

            <div className="mt-4 grid gap-3 md:grid-cols-4">
              <MetricCard label="Scoped Events" value={formatNumber(scopedEvents.length)} />
              <MetricCard label="Signed" value={formatNumber(countsByAction.signed ?? 0)} />
              <MetricCard
                label="Terminal"
                value={formatNumber(
                  (countsByAction.cancelled ?? 0) +
                    (countsByAction.expired ?? 0) +
                    (countsByAction.ended ?? 0) +
                    (countsByAction.inferred_cancelled ?? 0)
                )}
              />
              <MetricCard label="Flags" value={formatNumber(bundle.flags.length)} />
            </div>
          </section>

          <FilterBar
            indices={bundle.indices}
            timelineTicks={timelineTicks}
            hasScoreData={hasScoreData}
            hasScoreRankData={hasScoreRankData}
          />

          <section className="grid gap-4 xl:grid-cols-2 [&>*]:min-w-0">
            <Suspense fallback={<section className="panel p-4 text-sm text-muted">Loading timeline view...</section>}>
              <TimelineView
                pulse={pulse}
                playhead={query.playback.playhead}
                timeRange={query.time}
                onSetPlayhead={actions.setPlayhead}
                onSetRange={handleSetRange}
              />
            </Suspense>
            <Suspense fallback={<section className="panel p-4 text-sm text-muted">Loading network view...</section>}>
              {renderNetworkView({ isFullscreen: false, forceFullscreenLabels: false })}
            </Suspense>
          </section>

          <InspectorView
            events={scopedEvents}
            onSelectPlayhead={actions.setPlayhead}
            onFocusAlliance={(allianceId) => actions.setFocus({ allianceId, edgeKey: null, eventId: query.focus.eventId })}
          />
        </main>
      ) : null}

      {actions.isNetworkFullscreen ? (
        <div className="fixed inset-0 z-50 bg-white p-2 md:p-3">
          <div className="grid h-full min-h-0 grid-rows-[1fr_auto] gap-2 md:grid-cols-[1fr_340px] md:grid-rows-1 md:gap-3">
            <div className="min-h-0 md:row-span-1">
              <Suspense fallback={<section className="panel h-full p-4 text-sm text-muted">Loading network view...</section>}>
                {renderNetworkView({
                  isFullscreen: true,
                  forceFullscreenLabels: true,
                  onFullscreenHintChange: setNetworkFullscreenHint
                })}
              </Suspense>
            </div>
            <aside className="panel flex min-h-0 flex-col gap-3 p-3 md:overflow-auto">
              <PlaybackControls timelineTicks={timelineTicks} />
              {networkFullscreenHint ? (
                <NetworkAllianceHint
                  hint={networkFullscreenHint}
                  flagAssetsPayload={bundle.flagAssetsPayload}
                  className="rounded-md border border-slate-300 bg-slate-50 p-3 text-xs text-slate-700"
                />
              ) : (
                <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
                  Hover a node for details. Shift+click a node to toggle an anchor while preserving normal click-to-focus behavior.
                </div>
              )}
            </aside>
          </div>
        </div>
      ) : null}
    </>
  );
}

type MetricCardProps = {
  label: string;
  value: string;
};

function MetricCard({ label, value }: MetricCardProps) {
  return (
    <article className="rounded-xl border border-slate-200 bg-slate-50 p-3">
      <div className="text-xs uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-1 text-2xl font-semibold text-slate-900">{value}</div>
    </article>
  );
}
