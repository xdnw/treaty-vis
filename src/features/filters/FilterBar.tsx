import { useEffect, useMemo, useState } from "react";
import {
  NODE_MAX_RADIUS_MAX,
  NODE_MAX_RADIUS_MIN,
  SCORE_SIZE_CONTRAST_MAX,
  SCORE_SIZE_CONTRAST_MIN,
  useFilterStore,
  type SortDirection,
  type SortField
} from "@/features/filters/filterStore";
import { PlaybackControls } from "@/features/filters/PlaybackControls";
import type { TimelapseIndices } from "@/domain/timelapse/selectors";

type Props = {
  indices: TimelapseIndices;
  timelineTicks: string[];
  hasScoreData: boolean;
  hasScoreRankData: boolean;
};

const TOP_X_OPTIONS = [5, 10, 25, 50, 100];

const SORT_FIELDS: Array<{ value: SortField; label: string }> = [
  { value: "timestamp", label: "Time" },
  { value: "action", label: "Action" },
  { value: "type", label: "Treaty Type" },
  { value: "from", label: "From Alliance" },
  { value: "to", label: "To Alliance" },
  { value: "source", label: "Source" }
];

function isoToDateTimeLocal(value: string | null): string {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const year = String(date.getFullYear());
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function dateTimeLocalToIso(value: string): string | null {
  if (!value) {
    return null;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString();
}

export function FilterBar({ indices, timelineTicks, hasScoreData, hasScoreRankData }: Props) {
  const query = useFilterStore((state) => state.query);
  const setTextQuery = useFilterStore((state) => state.setTextQuery);
  const setSort = useFilterStore((state) => state.setSort);
  const setTimeRange = useFilterStore((state) => state.setTimeRange);
  const toggleAlliance = useFilterStore((state) => state.toggleAlliance);
  const toggleAction = useFilterStore((state) => state.toggleAction);
  const toggleTreatyType = useFilterStore((state) => state.toggleTreatyType);
  const toggleSource = useFilterStore((state) => state.toggleSource);
  const setIncludeInferred = useFilterStore((state) => state.setIncludeInferred);
  const setIncludeNoise = useFilterStore((state) => state.setIncludeNoise);
  const setEvidenceMode = useFilterStore((state) => state.setEvidenceMode);
  const setTopXByScore = useFilterStore((state) => state.setTopXByScore);
  const setSizeByScore = useFilterStore((state) => state.setSizeByScore);
  const setScoreSizeContrast = useFilterStore((state) => state.setScoreSizeContrast);
  const setMaxNodeRadius = useFilterStore((state) => state.setMaxNodeRadius);
  const setShowFlags = useFilterStore((state) => state.setShowFlags);
  const clearFilters = useFilterStore((state) => state.clearFilters);
  const isNetworkFullscreen = useFilterStore((state) => state.isNetworkFullscreen);
  const [allianceSearch, setAllianceSearch] = useState("");
  const [activeAllianceOption, setActiveAllianceOption] = useState(0);
  const [pendingTextQuery, setPendingTextQuery] = useState(query.textQuery);

  useEffect(() => {
    setPendingTextQuery(query.textQuery);
  }, [query.textQuery]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      if (pendingTextQuery !== query.textQuery) {
        setTextQuery(pendingTextQuery);
      }
    }, 160);
    return () => window.clearTimeout(timer);
  }, [pendingTextQuery, query.textQuery, setTextQuery]);

  const actions = indices.allActions;
  const treatyTypes = indices.allTypes;
  const sources = indices.allSources;
  const allianceOptions = useMemo(() => {
    const trimmed = allianceSearch.trim().toLowerCase();
    if (!trimmed) {
      return indices.alliances.slice(0, 80);
    }
    return indices.alliances
      .filter((alliance) =>
        `${alliance.name} ${alliance.id}`.toLowerCase().includes(trimmed)
      )
      .slice(0, 80);
  }, [allianceSearch, indices.alliances]);

  return (
    <section className="panel p-4">
      <header className="mb-3 flex items-center justify-between">
        <h2 className="text-lg">Explorer Controls</h2>
        <button
          className="rounded-md border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50"
          onClick={clearFilters}
          type="button"
        >
          Reset Filters
        </button>
      </header>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className="space-y-2">
          <h3 className="text-xs font-semibold uppercase text-slate-600">Search</h3>
          <input
            className="w-full rounded-md border border-slate-300 px-2 py-1 text-sm"
            placeholder="Alliance, type, source, id..."
            value={pendingTextQuery}
            onChange={(event) => setPendingTextQuery(event.target.value)}
          />
          <div className="grid grid-cols-2 gap-2">
            <select
              className="rounded-md border border-slate-300 px-2 py-1 text-sm"
              value={query.sort.field}
              onChange={(event) => setSort(event.target.value as SortField, query.sort.direction)}
            >
              {SORT_FIELDS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
            <select
              className="rounded-md border border-slate-300 px-2 py-1 text-sm"
              value={query.sort.direction}
              onChange={(event) => setSort(query.sort.field, event.target.value as SortDirection)}
            >
              <option value="desc">Descending</option>
              <option value="asc">Ascending</option>
            </select>
          </div>
        </div>

        <div className="space-y-2">
          <h3 className="text-xs font-semibold uppercase text-slate-600">Time Range</h3>
          <input
            className="w-full rounded-md border border-slate-300 px-2 py-1 text-sm"
            type="datetime-local"
            value={isoToDateTimeLocal(query.time.start)}
            onChange={(event) => setTimeRange(dateTimeLocalToIso(event.target.value), query.time.end)}
          />
          <input
            className="w-full rounded-md border border-slate-300 px-2 py-1 text-sm"
            type="datetime-local"
            value={isoToDateTimeLocal(query.time.end)}
            onChange={(event) => setTimeRange(query.time.start, dateTimeLocalToIso(event.target.value))}
          />
          <div className="text-xs text-muted">
            Dataset window: {indices.minTimestamp?.slice(0, 10) ?? "n/a"} to {indices.maxTimestamp?.slice(0, 10) ?? "n/a"}
          </div>
        </div>

        {!isNetworkFullscreen ? <PlaybackControls timelineTicks={timelineTicks} className="space-y-2" /> : null}

        <div>
          <h3 className="mb-1 text-xs font-semibold uppercase text-slate-600">Action Filters</h3>
          <div className="flex flex-wrap gap-2">
            {actions.map((action) => {
              const enabled = query.filters.actions.includes(action);
              return (
                <button
                  key={action}
                  className="rounded-full border px-2 py-1 text-xs"
                  style={{
                    borderColor: enabled ? "#0c8599" : "#c9d4e2",
                    backgroundColor: enabled ? "rgba(12, 133, 153, 0.12)" : "#fff"
                  }}
                  onClick={() => toggleAction(action)}
                  type="button"
                >
                  {action}
                </button>
              );
            })}
          </div>
        </div>

        <div>
          <h3 className="mb-1 text-xs font-semibold uppercase text-slate-600">Treaty Type Filters</h3>
          <div className="flex flex-wrap gap-2">
            {treatyTypes.map((type) => {
              const enabled = query.filters.treatyTypes.includes(type);
              return (
                <button
                  key={type}
                  className="rounded-full border px-2 py-1 text-xs"
                  style={{
                    borderColor: enabled ? "#364fc7" : "#c9d4e2",
                    backgroundColor: enabled ? "rgba(54, 79, 199, 0.12)" : "#fff"
                  }}
                  onClick={() => toggleTreatyType(type)}
                  type="button"
                >
                  {type}
                </button>
              );
            })}
          </div>
        </div>

        <div>
          <h3 className="mb-1 text-xs font-semibold uppercase text-slate-600">Source Filters</h3>
          <div className="flex flex-wrap gap-2">
            {sources.map((source) => {
              const enabled = query.filters.sources.includes(source);
              return (
                <button
                  key={source}
                  className="rounded-full border px-2 py-1 text-xs"
                  style={{
                    borderColor: enabled ? "#e67700" : "#c9d4e2",
                    backgroundColor: enabled ? "rgba(230, 119, 0, 0.12)" : "#fff"
                  }}
                  onClick={() => toggleSource(source)}
                  type="button"
                >
                  {source}
                </button>
              );
            })}
          </div>
        </div>

        <div className="space-y-2 text-sm text-slate-700">
          <label className="flex items-center gap-2">
            <input
              checked={query.filters.includeInferred}
              onChange={(event) => setIncludeInferred(event.target.checked)}
              type="checkbox"
            />
            Include inferred events
          </label>
          <label className="flex items-center gap-2">
            <input
              checked={query.filters.includeNoise}
              onChange={(event) => setIncludeNoise(event.target.checked)}
              type="checkbox"
            />
            Include noise-tagged events
          </label>
          <label className="flex items-center gap-2">
            Evidence filter
            <select
              className="rounded border border-slate-300 px-1 py-0.5"
              value={query.filters.evidenceMode}
              onChange={(event) =>
                setEvidenceMode(event.target.value as "all" | "one-confirmed" | "both-confirmed")
              }
            >
              <option value="all">All evidence levels</option>
              <option value="one-confirmed">At least one side confirmed</option>
              <option value="both-confirmed">Both sides confirmed</option>
            </select>
          </label>
          <label className="flex items-center gap-2">
            Top-X by score
            <select
              className="rounded border border-slate-300 px-1 py-0.5"
              value={query.filters.topXByScore ?? 0}
              disabled={!hasScoreRankData}
              title={hasScoreRankData ? undefined : "Ranking data unavailable"}
              onChange={(event) => {
                const next = Number(event.target.value);
                setTopXByScore(Number.isFinite(next) && next > 0 ? next : null);
              }}
            >
              <option value={0}>Off</option>
              {TOP_X_OPTIONS.map((value) => (
                <option key={value} value={value}>
                  Top {value}
                </option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-2">
            <input
              checked={query.filters.sizeByScore && hasScoreData}
              disabled={!hasScoreData}
              title={hasScoreData ? undefined : "Score data unavailable"}
              onChange={(event) => setSizeByScore(event.target.checked)}
              type="checkbox"
            />
            Size nodes by score
          </label>
          <label className="flex items-center gap-2">
            Score size contrast
            <input
              aria-label="Score size contrast"
              className="w-24"
              type="range"
              min={SCORE_SIZE_CONTRAST_MIN}
              max={SCORE_SIZE_CONTRAST_MAX}
              step={0.05}
              value={query.filters.scoreSizeContrast}
              disabled={!hasScoreData}
              title={hasScoreData ? undefined : "Score data unavailable"}
              onChange={(event) => setScoreSizeContrast(Number(event.target.value))}
            />
            <span className="tabular-nums text-xs text-muted">{query.filters.scoreSizeContrast.toFixed(2)}</span>
          </label>
          <label className="flex items-center gap-2">
            Max node radius
            <input
              aria-label="Max node radius"
              className="w-24"
              type="range"
              min={NODE_MAX_RADIUS_MIN}
              max={NODE_MAX_RADIUS_MAX}
              step={1}
              value={query.filters.maxNodeRadius}
              onChange={(event) => setMaxNodeRadius(Number(event.target.value))}
            />
            <span className="tabular-nums text-xs text-muted">{query.filters.maxNodeRadius}</span>
          </label>
          <label className="flex items-center gap-2">
            <input
              checked={query.filters.showFlags}
              onChange={(event) => setShowFlags(event.target.checked)}
              type="checkbox"
            />
            Show alliance flags
          </label>
          {!hasScoreRankData ? <p className="text-xs text-muted">Top-X ranking unavailable</p> : null}
          {!hasScoreData ? <p className="text-xs text-muted">Score sizing unavailable</p> : null}
        </div>

        <div>
          <h3 className="mb-1 text-xs font-semibold uppercase text-slate-600">Alliance Focus</h3>
          <div className="mb-2 flex items-center gap-2">
            <input
              className="w-full rounded-md border border-slate-300 px-2 py-1 text-sm"
              value={allianceSearch}
              onChange={(event) => {
                setAllianceSearch(event.target.value);
                setActiveAllianceOption(0);
              }}
              onKeyDown={(event) => {
                if (event.key === "ArrowDown") {
                  event.preventDefault();
                  setActiveAllianceOption((current) => Math.min(current + 1, Math.max(0, allianceOptions.length - 1)));
                } else if (event.key === "ArrowUp") {
                  event.preventDefault();
                  setActiveAllianceOption((current) => Math.max(current - 1, 0));
                } else if (event.key === "Enter") {
                  const selected = allianceOptions[activeAllianceOption];
                  if (selected) {
                    event.preventDefault();
                    toggleAlliance(selected.id);
                  }
                } else if (event.key === "Escape") {
                  setAllianceSearch("");
                  setActiveAllianceOption(0);
                }
              }}
              placeholder="Search alliance name or ID..."
            />
            <button
              type="button"
              className="rounded-md border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50"
              onClick={() => {
                setAllianceSearch("");
                setActiveAllianceOption(0);
              }}
            >
              Clear
            </button>
          </div>
          <div className="max-h-28 overflow-auto rounded-md border border-slate-200 p-2">
            <div className="flex flex-wrap gap-2">
              {allianceOptions.map((alliance, index) => {
                const enabled = query.filters.alliances.includes(alliance.id);
                return (
                  <button
                    key={alliance.id}
                    className="rounded-full border px-2 py-1 text-xs"
                    style={{
                      borderColor: enabled ? "#2b8a3e" : index === activeAllianceOption ? "#1d4ed8" : "#c9d4e2",
                      backgroundColor:
                        enabled ? "rgba(43, 138, 62, 0.12)" : index === activeAllianceOption ? "rgba(29, 78, 216, 0.08)" : "#fff"
                    }}
                    onClick={() => toggleAlliance(alliance.id)}
                    type="button"
                    title={`${alliance.name} (${alliance.count} events)`}
                  >
                    {alliance.name} ({alliance.count})
                  </button>
                );
              })}
              {allianceOptions.length === 0 ? (
                <span className="text-xs text-muted">No alliances match your search.</span>
              ) : null}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
