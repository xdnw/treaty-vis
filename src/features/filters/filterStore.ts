import { create } from "zustand";

export type EvidenceMode = "all" | "one-confirmed" | "both-confirmed";
export type SortField = "timestamp" | "action" | "type" | "from" | "to" | "source";
export type SortDirection = "asc" | "desc";
export type PlaybackSpeed = 1 | 2 | 4 | 8 | 16 | 32;

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

type FilterStore = {
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
  setShowFlags: (value: boolean) => void;
  clearFilters: () => void;
  resetAll: () => void;
};

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

export const useFilterStore = create<FilterStore>((set) => ({
  query: createDefaultQueryState(),
  isNetworkFullscreen: false,
  setStateFromUrl: (state) => {
    set((current) => ({
      query: {
        ...current.query,
        ...state,
        time: {
          ...current.query.time,
          ...(state.time ?? {})
        },
        playback: {
          ...current.query.playback,
          ...(state.playback ?? {})
        },
        focus: {
          ...current.query.focus,
          ...(state.focus ?? {})
        },
        filters: {
          ...current.query.filters,
          ...(state.filters ?? {})
        },
        sort: {
          ...current.query.sort,
          ...(state.sort ?? {})
        }
      }
    }));
  },
  setNetworkFullscreen: (value) => {
    set({ isNetworkFullscreen: value });
  },
  setTimeRange: (start, end) => {
    set((state) => ({
      query: {
        ...state.query,
        time: {
          start,
          end
        }
      }
    }));
  },
  setPlayhead: (playhead) => {
    set((state) => ({
      query: {
        ...state.query,
        playback: {
          ...state.query.playback,
          playhead
        }
      }
    }));
  },
  setPlaying: (isPlaying) => {
    set((state) => ({
      query: {
        ...state.query,
        playback: {
          ...state.query.playback,
          isPlaying
        }
      }
    }));
  },
  setPlaybackSpeed: (speed) => {
    set((state) => ({
      query: {
        ...state.query,
        playback: {
          ...state.query.playback,
          speed
        }
      }
    }));
  },
  setTextQuery: (queryText) => {
    set((state) => ({
      query: {
        ...state.query,
        textQuery: queryText
      }
    }));
  },
  setSort: (field, direction) => {
    set((state) => ({
      query: {
        ...state.query,
        sort: {
          field,
          direction
        }
      }
    }));
  },
  setFocus: (focus) => {
    set((state) => ({
      query: {
        ...state.query,
        focus: {
          ...state.query.focus,
          ...focus
        }
      }
    }));
  },
  clearFocus: () => {
    set((state) => ({
      query: {
        ...state.query,
        focus: {
          allianceId: null,
          edgeKey: null,
          eventId: null
        }
      }
    }));
  },
  toggleAction: (action) => {
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          actions: toggleArrayString(state.query.filters.actions, action)
        }
      }
    }));
  },
  toggleTreatyType: (type) => {
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          treatyTypes: toggleArrayString(state.query.filters.treatyTypes, type)
        }
      }
    }));
  },
  toggleSource: (source) => {
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          sources: toggleArrayString(state.query.filters.sources, source)
        }
      }
    }));
  },
  toggleAlliance: (allianceId) => {
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          alliances: toggleArrayNumber(state.query.filters.alliances, allianceId)
        }
      }
    }));
  },
  setAnchoredAllianceIds: (allianceIds) => {
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          anchoredAllianceIds: [...new Set(allianceIds.filter((value) => Number.isFinite(value)))].sort((a, b) => a - b)
        }
      }
    }));
  },
  setIncludeInferred: (value) => {
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          includeInferred: value
        }
      }
    }));
  },
  setIncludeNoise: (value) => {
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          includeNoise: value
        }
      }
    }));
  },
  setEvidenceMode: (mode) => {
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          evidenceMode: mode
        }
      }
    }));
  },
  setTopXByScore: (value) => {
    const normalized = value !== null && Number.isFinite(value) && value > 0 ? Math.floor(value) : null;
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          topXByScore: normalized
        }
      }
    }));
  },
  setSizeByScore: (value) => {
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          sizeByScore: value
        }
      }
    }));
  },
  setShowFlags: (value) => {
    set((state) => ({
      query: {
        ...state.query,
        filters: {
          ...state.query.filters,
          showFlags: value
        }
      }
    }));
  },
  clearFilters: () => {
    set({ query: createDefaultQueryState(), isNetworkFullscreen: false });
  },
  resetAll: () => {
    set({ query: createDefaultQueryState(), isNetworkFullscreen: false });
  }
}));
