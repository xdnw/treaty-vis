import { create } from "zustand";

export type EvidenceMode = "all" | "one-confirmed" | "both-confirmed";
export type SortField = "timestamp" | "action" | "type" | "from" | "to" | "source";
export type SortDirection = "asc" | "desc";
export type PlaybackSpeed = 1 | 2 | 4 | 8 | 16 | 32;

export const SCORE_SIZE_CONTRAST_DEFAULT = 1;
export const SCORE_SIZE_CONTRAST_MIN = 0.25;
export const SCORE_SIZE_CONTRAST_MAX = 3;
export const NODE_MAX_RADIUS_DEFAULT = 14;
export const NODE_MAX_RADIUS_MIN = 8;
export const NODE_MAX_RADIUS_MAX = 40;

export type QueryFilters = {
  alliances: number[];
  anchoredAllianceIds: number[];
  treatyTypes: string[];
  actions: string[];
  sources: string[];
  includeInferred: boolean;
  includeNoise: boolean;
  evidenceMode: EvidenceMode;
  topXByScore: number | null;
  sizeByScore: boolean;
  scoreSizeContrast: number;
  maxNodeRadius: number;
  showFlags: boolean;
};

export type TimeSlice = {
  start: string | null;
  end: string | null;
};

export type PlaybackSlice = {
  playhead: string | null;
  isPlaying: boolean;
  speed: PlaybackSpeed;
};

export type FocusSlice = {
  allianceId: number | null;
  edgeKey: string | null;
  eventId: string | null;
};

export type QueryState = {
  time: TimeSlice;
  playback: PlaybackSlice;
  focus: FocusSlice;
  filters: QueryFilters;
  textQuery: string;
  sort: {
    field: SortField;
    direction: SortDirection;
  };
};

export type FilterStore = {
  query: QueryState;
  isNetworkFullscreen: boolean;
  setStateFromUrl: (state: Partial<QueryState>) => void;
  setNetworkFullscreen: (value: boolean) => void;
  setTimeRange: (start: string | null, end: string | null) => void;
  setPlayhead: (playhead: string | null) => void;
  setPlaying: (isPlaying: boolean) => void;
  setPlaybackSpeed: (speed: PlaybackSpeed) => void;
  setTextQuery: (query: string) => void;
  setSort: (field: SortField, direction: SortDirection) => void;
  setFocus: (focus: Partial<FocusSlice>) => void;
  clearFocus: () => void;
  toggleAction: (action: string) => void;
  toggleTreatyType: (type: string) => void;
  toggleSource: (source: string) => void;
  toggleAlliance: (allianceId: number) => void;
  setAnchoredAllianceIds: (allianceIds: number[]) => void;
  setIncludeInferred: (value: boolean) => void;
  setIncludeNoise: (value: boolean) => void;
  setEvidenceMode: (mode: EvidenceMode) => void;
  setTopXByScore: (value: number | null) => void;
  setSizeByScore: (value: boolean) => void;
  setScoreSizeContrast: (value: number) => void;
  setMaxNodeRadius: (value: number) => void;
  setShowFlags: (value: boolean) => void;
  clearFilters: () => void;
  resetAll: () => void;
};

function normalizeBoundedNumber(value: number | null | undefined, options: {
  fallback: number;
  min: number;
  max: number;
  decimals: number;
}): number {
  const numericValue = Number.isFinite(value) ? Number(value) : options.fallback;
  const clamped = Math.max(options.min, Math.min(options.max, numericValue));
  const scale = 10 ** options.decimals;
  return Math.round(clamped * scale) / scale;
}

export function normalizeScoreSizeContrast(value: number | null | undefined): number {
  return normalizeBoundedNumber(value, {
    fallback: SCORE_SIZE_CONTRAST_DEFAULT,
    min: SCORE_SIZE_CONTRAST_MIN,
    max: SCORE_SIZE_CONTRAST_MAX,
    decimals: 2
  });
}

export function normalizeMaxNodeRadius(value: number | null | undefined): number {
  return normalizeBoundedNumber(value, {
    fallback: NODE_MAX_RADIUS_DEFAULT,
    min: NODE_MAX_RADIUS_MIN,
    max: NODE_MAX_RADIUS_MAX,
    decimals: 0
  });
}

const defaultQueryState: QueryState = {
  time: {
    start: null,
    end: null
  },
  playback: {
    playhead: null,
    isPlaying: false,
    speed: 1
  },
  focus: {
    allianceId: null,
    edgeKey: null,
    eventId: null
  },
  filters: {
    alliances: [],
    anchoredAllianceIds: [],
    treatyTypes: [],
    actions: [],
    sources: [],
    includeInferred: true,
    includeNoise: true,
    evidenceMode: "all",
    topXByScore: null,
    sizeByScore: false,
    scoreSizeContrast: SCORE_SIZE_CONTRAST_DEFAULT,
    maxNodeRadius: NODE_MAX_RADIUS_DEFAULT,
    showFlags: false
  },
  textQuery: "",
  sort: {
    field: "timestamp",
    direction: "desc"
  }
};

function createDefaultQueryState(): QueryState {
  return {
    time: {
      ...defaultQueryState.time
    },
    playback: {
      ...defaultQueryState.playback
    },
    focus: {
      ...defaultQueryState.focus
    },
    filters: {
      alliances: [...defaultQueryState.filters.alliances],
      anchoredAllianceIds: [...defaultQueryState.filters.anchoredAllianceIds],
      treatyTypes: [...defaultQueryState.filters.treatyTypes],
      actions: [...defaultQueryState.filters.actions],
      sources: [...defaultQueryState.filters.sources],
      includeInferred: defaultQueryState.filters.includeInferred,
      includeNoise: defaultQueryState.filters.includeNoise,
      evidenceMode: defaultQueryState.filters.evidenceMode,
      topXByScore: defaultQueryState.filters.topXByScore,
      sizeByScore: defaultQueryState.filters.sizeByScore,
      scoreSizeContrast: defaultQueryState.filters.scoreSizeContrast,
      maxNodeRadius: defaultQueryState.filters.maxNodeRadius,
      showFlags: defaultQueryState.filters.showFlags
    },
    textQuery: defaultQueryState.textQuery,
    sort: {
      ...defaultQueryState.sort
    }
  };
}

function toggleArrayString(values: string[], value: string): string[] {
  if (values.includes(value)) {
    return values.filter((item) => item !== value);
  }
  return [...values, value];
}

function toggleArrayNumber(values: number[], value: number): number[] {
  if (values.includes(value)) {
    return values.filter((item) => item !== value);
  }
  return [...values, value];
}

function readCsvParam(raw: string | null): string[] {
  if (!raw) {
    return [];
  }
  return raw
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function readNumberCsvParam(raw: string | null): number[] {
  return readCsvParam(raw)
    .map((item) => Number(item))
    .filter((value) => Number.isFinite(value));
}

function readPositiveNumberParam(raw: string | null): number | null {
  if (!raw) {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  const normalized = Math.floor(parsed);
  return normalized > 0 ? normalized : null;
}

function readFiniteNumberParam(raw: string | null): number | null {
  if (!raw) {
    return null;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

export function deserializeQueryState(search: string): Partial<QueryState> {
  const params = new URLSearchParams(search);
  const evidenceModeRaw = params.get("evidence");
  const sortFieldRaw = params.get("sortField");
  const sortDirectionRaw = params.get("sortDirection");
  const speedRaw = Number(params.get("speed") ?? "1");

  const evidenceMode: EvidenceMode =
    evidenceModeRaw === "one-confirmed" || evidenceModeRaw === "both-confirmed" || evidenceModeRaw === "all"
      ? evidenceModeRaw
      : "all";

  const focusAllianceRaw = params.get("focusAlliance");
  const focusAlliance = focusAllianceRaw === null ? null : Number(focusAllianceRaw);

  const sortField: SortField =
    sortFieldRaw === "action" ||
    sortFieldRaw === "type" ||
    sortFieldRaw === "from" ||
    sortFieldRaw === "to" ||
    sortFieldRaw === "source"
      ? sortFieldRaw
      : "timestamp";

  const sortDirection: SortDirection = sortDirectionRaw === "asc" ? "asc" : "desc";

  const speed: PlaybackSpeed =
    speedRaw === 2 || speedRaw === 4 || speedRaw === 8 || speedRaw === 16 || speedRaw === 32 ? speedRaw : 1;

  return {
    time: {
      start: params.get("start") || null,
      end: params.get("end") || null
    },
    playback: {
      playhead: params.get("playhead") || null,
      isPlaying: params.get("playing") === "1",
      speed
    },
    focus: {
      allianceId: Number.isFinite(focusAlliance) ? focusAlliance : null,
      edgeKey: params.get("focusEdge") || null,
      eventId: params.get("focusEvent") || null
    },
    filters: {
      alliances: readNumberCsvParam(params.get("alliances")),
      anchoredAllianceIds: readNumberCsvParam(params.get("anchors")),
      treatyTypes: readCsvParam(params.get("types")),
      actions: readCsvParam(params.get("actions")),
      sources: readCsvParam(params.get("sources")),
      includeInferred: params.get("includeInferred") !== "0",
      includeNoise: params.get("includeNoise") !== "0",
      evidenceMode,
      topXByScore: readPositiveNumberParam(params.get("topXByScore")),
      sizeByScore: params.get("sizeByScore") === "1",
      scoreSizeContrast: normalizeScoreSizeContrast(readFiniteNumberParam(params.get("scoreSizeContrast"))),
      maxNodeRadius: normalizeMaxNodeRadius(readFiniteNumberParam(params.get("maxNodeRadius"))),
      showFlags: params.get("showFlags") === "1"
    },
    textQuery: params.get("q") ?? "",
    sort: {
      field: sortField,
      direction: sortDirection
    }
  };
}

export function serializeQueryState(query: QueryState): string {
  const params = new URLSearchParams();
  if (query.time.start) {
    params.set("start", query.time.start);
  }
  if (query.time.end) {
    params.set("end", query.time.end);
  }
  if (query.playback.playhead) {
    params.set("playhead", query.playback.playhead);
  }
  if (query.playback.isPlaying) {
    params.set("playing", "1");
  }
  if (query.playback.speed !== 1) {
    params.set("speed", String(query.playback.speed));
  }
  if (query.focus.allianceId !== null) {
    params.set("focusAlliance", String(query.focus.allianceId));
  }
  if (query.focus.edgeKey) {
    params.set("focusEdge", query.focus.edgeKey);
  }
  if (query.focus.eventId) {
    params.set("focusEvent", query.focus.eventId);
  }
  if (query.filters.alliances.length > 0) {
    params.set("alliances", query.filters.alliances.join(","));
  }
  if (query.filters.anchoredAllianceIds.length > 0) {
    params.set("anchors", query.filters.anchoredAllianceIds.join(","));
  }
  if (query.filters.treatyTypes.length > 0) {
    params.set("types", query.filters.treatyTypes.join(","));
  }
  if (query.filters.actions.length > 0) {
    params.set("actions", query.filters.actions.join(","));
  }
  if (query.filters.sources.length > 0) {
    params.set("sources", query.filters.sources.join(","));
  }
  if (!query.filters.includeInferred) {
    params.set("includeInferred", "0");
  }
  if (!query.filters.includeNoise) {
    params.set("includeNoise", "0");
  }
  if (query.filters.evidenceMode !== "all") {
    params.set("evidence", query.filters.evidenceMode);
  }
  if (query.filters.topXByScore !== null && query.filters.topXByScore > 0) {
    params.set("topXByScore", String(query.filters.topXByScore));
  }
  if (query.filters.sizeByScore) {
    params.set("sizeByScore", "1");
  }
  if (Math.abs(query.filters.scoreSizeContrast - SCORE_SIZE_CONTRAST_DEFAULT) > Number.EPSILON) {
    params.set("scoreSizeContrast", String(normalizeScoreSizeContrast(query.filters.scoreSizeContrast)));
  }
  if (query.filters.maxNodeRadius !== NODE_MAX_RADIUS_DEFAULT) {
    params.set("maxNodeRadius", String(normalizeMaxNodeRadius(query.filters.maxNodeRadius)));
  }
  if (query.filters.showFlags) {
    params.set("showFlags", "1");
  }
  if (query.textQuery.trim()) {
    params.set("q", query.textQuery.trim());
  }
  if (query.sort.field !== "timestamp") {
    params.set("sortField", query.sort.field);
  }
  if (query.sort.direction !== "desc") {
    params.set("sortDirection", query.sort.direction);
  }
  return params.toString();
}

export const useFilterStore = create<FilterStore>((set) => {
  const updateQuery = (updater: (query: QueryState) => QueryState): void => {
    set((state) => ({ query: updater(state.query) }));
  };

  const updateFilters = (updater: (filters: QueryFilters) => QueryFilters): void => {
    updateQuery((query) => ({
      ...query,
      filters: updater(query.filters)
    }));
  };

  const resetQueryState = (): void => {
    set({ query: createDefaultQueryState(), isNetworkFullscreen: false });
  };

  return {
    query: createDefaultQueryState(),
    isNetworkFullscreen: false,
    setStateFromUrl: (state) => {
      updateQuery((query) => ({
        ...query,
        ...state,
        time: {
          ...query.time,
          ...(state.time ?? {})
        },
        playback: {
          ...query.playback,
          ...(state.playback ?? {})
        },
        focus: {
          ...query.focus,
          ...(state.focus ?? {})
        },
        filters: {
          ...query.filters,
          ...(state.filters ?? {})
        },
        sort: {
          ...query.sort,
          ...(state.sort ?? {})
        }
      }));
    },
    setNetworkFullscreen: (value) => {
      set({ isNetworkFullscreen: value });
    },
    setTimeRange: (start, end) => {
      updateQuery((query) => ({
        ...query,
        time: {
          start,
          end
        }
      }));
    },
    setPlayhead: (playhead) => {
      updateQuery((query) => ({
        ...query,
        playback: {
          ...query.playback,
          playhead
        }
      }));
    },
    setPlaying: (isPlaying) => {
      updateQuery((query) => ({
        ...query,
        playback: {
          ...query.playback,
          isPlaying
        }
      }));
    },
    setPlaybackSpeed: (speed) => {
      updateQuery((query) => ({
        ...query,
        playback: {
          ...query.playback,
          speed
        }
      }));
    },
    setTextQuery: (queryText) => {
      updateQuery((query) => ({ ...query, textQuery: queryText }));
    },
    setSort: (field, direction) => {
      updateQuery((query) => ({
        ...query,
        sort: {
          field,
          direction
        }
      }));
    },
    setFocus: (focus) => {
      updateQuery((query) => ({
        ...query,
        focus: {
          ...query.focus,
          ...focus
        }
      }));
    },
    clearFocus: () => {
      updateQuery((query) => ({
        ...query,
        focus: {
          allianceId: null,
          edgeKey: null,
          eventId: null
        }
      }));
    },
    toggleAction: (action) => {
      updateFilters((filters) => ({
        ...filters,
        actions: toggleArrayString(filters.actions, action)
      }));
    },
    toggleTreatyType: (type) => {
      updateFilters((filters) => ({
        ...filters,
        treatyTypes: toggleArrayString(filters.treatyTypes, type)
      }));
    },
    toggleSource: (source) => {
      updateFilters((filters) => ({
        ...filters,
        sources: toggleArrayString(filters.sources, source)
      }));
    },
    toggleAlliance: (allianceId) => {
      updateFilters((filters) => ({
        ...filters,
        alliances: toggleArrayNumber(filters.alliances, allianceId)
      }));
    },
    setAnchoredAllianceIds: (allianceIds) => {
      updateFilters((filters) => ({
        ...filters,
        anchoredAllianceIds: [...new Set(allianceIds.filter((value) => Number.isFinite(value)))].sort((a, b) => a - b)
      }));
    },
    setIncludeInferred: (value) => {
      updateFilters((filters) => ({ ...filters, includeInferred: value }));
    },
    setIncludeNoise: (value) => {
      updateFilters((filters) => ({ ...filters, includeNoise: value }));
    },
    setEvidenceMode: (mode) => {
      updateFilters((filters) => ({ ...filters, evidenceMode: mode }));
    },
    setTopXByScore: (value) => {
      const normalized = value !== null && Number.isFinite(value) && value > 0 ? Math.floor(value) : null;
      updateFilters((filters) => ({ ...filters, topXByScore: normalized }));
    },
    setSizeByScore: (value) => {
      updateFilters((filters) => ({ ...filters, sizeByScore: value }));
    },
    setScoreSizeContrast: (value) => {
      const normalized = normalizeScoreSizeContrast(value);
      updateFilters((filters) => ({ ...filters, scoreSizeContrast: normalized }));
    },
    setMaxNodeRadius: (value) => {
      const normalized = normalizeMaxNodeRadius(value);
      updateFilters((filters) => ({ ...filters, maxNodeRadius: normalized }));
    },
    setShowFlags: (value) => {
      updateFilters((filters) => ({ ...filters, showFlags: value }));
    },
    clearFilters: () => {
      resetQueryState();
    },
    resetAll: () => {
      resetQueryState();
    }
  };
});
