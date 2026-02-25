import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  loadTimelapseBundle,
  resolveAllianceFlagSnapshot,
  selectTimelapseIndexes,
  selectTimelapsePulse,
  type TimelapseDataBundle
} from "@/domain/timelapse/loader";
import { loadScoreRuntime, type ScoreLoaderSnapshot } from "@/domain/timelapse/scoreLoader";
import type { AllianceFlagSnapshot, AllianceScoresRuntime } from "@/domain/timelapse/schema";
import { buildPulseSeries, countByAction, selectEvents, type PulsePoint } from "@/domain/timelapse/selectors";
import {
  deserializeQueryState,
  serializeQueryState,
  type QueryState,
  useFilterStore
} from "@/features/filters/filterStore";
import { FilterBar } from "@/features/filters/FilterBar";
import { InspectorView } from "@/features/inspector/InspectorView";
import { formatNumber } from "@/lib/format";

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
  const [bundle, setBundle] = useState<TimelapseDataBundle | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [workerScopedIndexes, setWorkerScopedIndexes] = useState<number[] | null>(null);
  const [workerPulse, setWorkerPulse] = useState<PulsePoint[] | null>(null);
  const [allianceScores, setAllianceScores] = useState<AllianceScoresRuntime | null>(null);
  const [scoreLoadSnapshot, setScoreLoadSnapshot] = useState<ScoreLoaderSnapshot | null>(null);
  const [scoreRetryNonce, setScoreRetryNonce] = useState(0);
  const urlSyncTimerRef = useRef<number | null>(null);
  const selectionRequestRef = useRef(0);
  const pulseRequestRef = useRef(0);
  const scoreLoadRequestRef = useRef(0);

  const time = useFilterStore((state) => state.query.time);
  const playback = useFilterStore((state) => state.query.playback);
  const focus = useFilterStore((state) => state.query.focus);
  const filters = useFilterStore((state) => state.query.filters);
  const showFlags = filters.showFlags;
  const textQuery = useFilterStore((state) => state.query.textQuery);
  const sort = useFilterStore((state) => state.query.sort);
  const setStateFromUrl = useFilterStore((state) => state.setStateFromUrl);
  const setTimeRange = useFilterStore((state) => state.setTimeRange);
  const setPlayhead = useFilterStore((state) => state.setPlayhead);
  const setPlaying = useFilterStore((state) => state.setPlaying);
  const setSizeByScore = useFilterStore((state) => state.setSizeByScore);
  const setFocus = useFilterStore((state) => state.setFocus);
  const clearFocus = useFilterStore((state) => state.clearFocus);
  const resetAll = useFilterStore((state) => state.resetAll);

  const query = useMemo<QueryState>(
    () => ({
      time,
      playback,
      focus,
      filters,
      textQuery,
      sort
    }),
    [filters, focus, playback, sort, textQuery, time]
  );

  useEffect(() => {
    setStateFromUrl(deserializeQueryState(window.location.search));
  }, [setStateFromUrl]);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    loadTimelapseBundle({ showFlags })
      .then((result) => {
        if (!mounted) {
          return;
        }
        setBundle(result);
      })
      .catch((reason) => {
        if (!mounted) {
          return;
        }
        setError(reason instanceof Error ? reason.message : "Unknown loading error");
      })
      .finally(() => {
        if (mounted) {
          setLoading(false);
        }
      });

    return () => {
      mounted = false;
    };
  }, [showFlags]);

  useEffect(() => {
    if (urlSyncTimerRef.current !== null) {
      window.clearTimeout(urlSyncTimerRef.current);
      urlSyncTimerRef.current = null;
    }

    if (query.playback.isPlaying) {
      return;
    }

    urlSyncTimerRef.current = window.setTimeout(() => {
      const serialized = serializeQueryState({
        ...query,
        playback: {
          ...query.playback,
          isPlaying: false
        }
      });
      const nextSearch = serialized ? `?${serialized}` : "";
      if (window.location.search !== nextSearch) {
        window.history.replaceState({}, "", `${window.location.pathname}${nextSearch}`);
      }
    }, 180);

    return () => {
      if (urlSyncTimerRef.current !== null) {
        window.clearTimeout(urlSyncTimerRef.current);
        urlSyncTimerRef.current = null;
      }
    };
  }, [query]);

  const baseQuery = useMemo(
    () => ({
      ...query,
      playback: {
        ...query.playback,
        playhead: null,
        isPlaying: false
      }
    }),
    [query]
  );

  const derived = useMemo(
    () => {
      if (!bundle) {
        return {
          datasetKey: "__loading__",
          events: [],
          indices: {
            allEventIndexes: [],
            byAction: {},
            byType: {},
            bySource: {},
            byAlliance: {},
            eventIdToIndex: {},
            allActions: [],
            allTypes: [],
            allSources: [],
            alliances: [],
            minTimestamp: null,
            maxTimestamp: null
          },
          allianceScoreRanksByDay: null
        };
      }

      const firstEventId = bundle.events[0]?.event_id ?? "none";
      const lastEventId = bundle.events[bundle.events.length - 1]?.event_id ?? "none";
      const datasetKey =
        bundle.manifest?.datasetId ??
        `unversioned:${bundle.summary.generated_at}:${bundle.events.length}:${firstEventId}:${lastEventId}`;

      return {
        datasetKey,
        events: bundle.events,
        indices: bundle.indices,
        allianceScoreRanksByDay: bundle.allianceScoreRanksByDay
      };
    },
    [bundle]
  );

  const scoreFileDeclared = Boolean(bundle?.manifest?.files?.["alliance_scores_v2.msgpack"]);
  const hasScoreRankData = useMemo(() => {
    if (!bundle?.allianceScoreRanksByDay) {
      return false;
    }
    return Object.keys(bundle.allianceScoreRanksByDay).length > 0;
  }, [bundle?.allianceScoreRanksByDay]);

  useEffect(() => {
    if (!bundle) {
      return;
    }

    if (!scoreFileDeclared && hasScoreRankData) {
      console.warn(
        "[timelapse] Score sizing disabled: manifest missing 'alliance_scores_v2.msgpack'. " +
          "Run 'npm run data:sync' after generating score artifacts."
      );
    }
  }, [bundle, hasScoreRankData, scoreFileDeclared]);

  useEffect(() => {
    if (!bundle) {
      return;
    }

    setAllianceScores(null);
    setScoreLoadSnapshot(null);
    setScoreRetryNonce(0);
  }, [bundle?.manifest?.datasetId]);

  useEffect(() => {
    if (!bundle?.manifest) {
      return;
    }

    if (!scoreFileDeclared) {
      return;
    }

    let mounted = true;
    scoreLoadRequestRef.current += 1;
    const nextRequestId = `${bundle.manifest.datasetId}:score:${scoreLoadRequestRef.current}:${scoreRetryNonce}`;

    void loadScoreRuntime({
      manifest: bundle.manifest,
      requestId: nextRequestId,
      forceNetwork: scoreRetryNonce > 0,
      onEvent: (snapshot) => {
        if (!mounted || snapshot.requestId !== nextRequestId) {
          return;
        }
        setScoreLoadSnapshot(snapshot);
        if (snapshot.runtime) {
          setAllianceScores(snapshot.runtime);
        }
      }
    });

    return () => {
      mounted = false;
    };
  }, [bundle?.manifest, scoreFileDeclared, scoreRetryNonce]);

  useEffect(() => {
    if (!bundle) {
      return;
    }

    if (query.filters.sizeByScore && !scoreFileDeclared) {
      console.warn(
        "[timelapse] Ignoring 'sizeByScore=1' because manifest does not declare 'alliance_scores_v2.msgpack'."
      );
      setSizeByScore(false);
      return;
    }
  }, [bundle, query.filters.sizeByScore, scoreFileDeclared, setSizeByScore]);

  useEffect(() => {
    if (!bundle) {
      setWorkerScopedIndexes(null);
      return;
    }

    selectionRequestRef.current += 1;
    const requestId = selectionRequestRef.current;
    setWorkerScopedIndexes(null);

    void selectTimelapseIndexes(baseQuery).then((workerIndexes) => {
      if (selectionRequestRef.current !== requestId) {
        return;
      }
      if (workerIndexes) {
        setWorkerScopedIndexes(Array.from(workerIndexes));
        return;
      }
      setWorkerScopedIndexes(selectEvents(derived, baseQuery).indexes);
    });
  }, [baseQuery, bundle, derived]);

  const scopedSelectionIndexes = useMemo(() => {
    if (workerScopedIndexes !== null) {
      return workerScopedIndexes;
    }
    return selectEvents(derived, baseQuery).indexes;
  }, [baseQuery, derived, workerScopedIndexes]);

  const scopedEvents = useMemo(
    () => scopedSelectionIndexes.map((index) => derived.events[index]),
    [derived.events, scopedSelectionIndexes]
  );

  useEffect(() => {
    if (!bundle) {
      setWorkerPulse(null);
      return;
    }

    pulseRequestRef.current += 1;
    const requestId = pulseRequestRef.current;
    setWorkerPulse(null);

    void selectTimelapsePulse(baseQuery, 280, null).then((pulseFromWorker) => {
      if (pulseRequestRef.current !== requestId) {
        return;
      }
      if (pulseFromWorker) {
        setWorkerPulse(pulseFromWorker);
        return;
      }
      setWorkerPulse(buildPulseSeries(bundle.events, scopedSelectionIndexes, 280, null));
    });
  }, [baseQuery, bundle, scopedSelectionIndexes]);

  const timelineTicks = useMemo(
    () => uniqueDayTicks(scopedEvents.map((event) => event.timestamp)),
    [scopedEvents]
  );

  useEffect(() => {
    if (!bundle) {
      return;
    }
    if (!query.playback.playhead) {
      setPlayhead(bundle.indices.maxTimestamp);
    }
  }, [bundle, query.playback.playhead, setPlayhead]);

  const handleSetPlayhead = useCallback(
    (nextPlayhead: string | null) => {
      setPlayhead(nextPlayhead);
    },
    [setPlayhead]
  );

  const handleSetRange = useCallback(
    (start: string | null, end: string | null) => {
      setTimeRange(start, end);
    },
    [setTimeRange]
  );

  useEffect(() => {
    if (!query.playback.isPlaying || timelineTicks.length === 0) {
      return;
    }

    let frame = 0;
    let last = performance.now();
    let carry = 0;
    const frameBudgetMs = 380 / query.playback.speed;
    const currentIndex = Math.max(
      timelineTicks.findIndex((item) => item === query.playback.playhead),
      0
    );
    let nextIndex = currentIndex;

    const tick = (now: number) => {
      const delta = now - last;
      last = now;
      carry += delta;

      if (carry >= frameBudgetMs) {
        carry = 0;
        nextIndex += 1;
        if (nextIndex >= timelineTicks.length) {
          setPlaying(false);
          return;
        }
        handleSetPlayhead(timelineTicks[nextIndex]);
      }
      frame = requestAnimationFrame(tick);
    };

    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [handleSetPlayhead, query.playback.isPlaying, query.playback.playhead, query.playback.speed, setPlaying, timelineTicks]);

  const pulse = useMemo(
    () => workerPulse ?? buildPulseSeries(bundle?.events ?? [], scopedSelectionIndexes, 280, null),
    [bundle?.events, scopedSelectionIndexes, workerPulse]
  );

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
            Run <code>npm run data:sync</code> in <code>web/</code> after generating reconciled output files.
          </p>
        </section>
      </main>
    );
  }

  return (
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
            className="rounded-md border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50"
            onClick={resetAll}
          >
            Reset All State
          </button>
          <div className="text-xs text-muted">
            Focus: Alliance {query.focus.allianceId ?? "none"} | Edge {query.focus.edgeKey ?? "none"}
          </div>
          <button
            type="button"
            className="rounded-md border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
            onClick={clearFocus}
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
            onSetPlayhead={handleSetPlayhead}
            onSetRange={handleSetRange}
          />
        </Suspense>
        <Suspense fallback={<section className="panel p-4 text-sm text-muted">Loading network view...</section>}>
          <NetworkView
            allEvents={bundle.events}
            scopedIndexes={scopedSelectionIndexes}
            baseQuery={baseQuery}
            playhead={query.playback.playhead}
            focusedAllianceId={query.focus.allianceId}
            focusedEdgeKey={query.focus.edgeKey}
            sizeByScore={query.filters.sizeByScore}
            showFlags={showFlags}
            flagAssetsPayload={bundle.flagAssetsPayload}
            allianceScoresByDay={allianceScores?.byDay ?? null}
            allianceScoreDays={allianceScoreDays}
            scoreLoadSnapshot={scoreLoadSnapshot}
            scoreManifestDeclared={scoreFileDeclared}
            onRetryScoreLoad={() => setScoreRetryNonce((current) => current + 1)}
            resolveAllianceFlagAtPlayhead={resolveAllianceFlagAtPlayhead}
            onFocusAlliance={(allianceId) => setFocus({ allianceId, edgeKey: null, eventId: null })}
            onFocusEdge={(edgeKey) => setFocus({ edgeKey, eventId: null })}
          />
        </Suspense>
      </section>

      <InspectorView
        events={scopedEvents}
        onSelectPlayhead={handleSetPlayhead}
        onFocusAlliance={(allianceId) => setFocus({ allianceId, edgeKey: null })}
      />
    </main>
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
