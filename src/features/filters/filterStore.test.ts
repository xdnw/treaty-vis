import { describe, expect, it } from "vitest";
import {
  deserializeQueryState,
  NODE_MAX_RADIUS_DEFAULT,
  NODE_MAX_RADIUS_MAX,
  NODE_MAX_RADIUS_MIN,
  normalizeMaxNodeRadius,
  normalizeScoreSizeContrast,
  SCORE_SIZE_CONTRAST_DEFAULT,
  SCORE_SIZE_CONTRAST_MAX,
  SCORE_SIZE_CONTRAST_MIN,
  serializeQueryState,
  type QueryState,
  useFilterStore
} from "@/features/filters/filterStore";

function makeQuery(): QueryState {
  return {
    time: { start: null, end: null },
    playback: { playhead: null, isPlaying: false, speed: 1 },
    focus: { allianceId: null, edgeKey: null, eventId: null },
    filters: {
      alliances: [],
      anchoredAllianceIds: [99, 7, 42],
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
    sort: { field: "timestamp", direction: "desc" }
  };
}

function makeDefaultQuery(): QueryState {
  return {
    time: { start: null, end: null },
    playback: { playhead: null, isPlaying: false, speed: 1 },
    focus: { allianceId: null, edgeKey: null, eventId: null },
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
    sort: { field: "timestamp", direction: "desc" }
  };
}

function makeNonDefaultQuery(): QueryState {
  return {
    time: { start: "2020-01-01T00:00:00.000Z", end: "2020-12-31T00:00:00.000Z" },
    playback: { playhead: "2020-03-01T00:00:00.000Z", isPlaying: true, speed: 4 },
    focus: { allianceId: 42, edgeKey: "42-7", eventId: "evt-1" },
    filters: {
      alliances: [1, 2],
      anchoredAllianceIds: [2, 1],
      treatyTypes: ["mdp"],
      actions: ["signed"],
      sources: ["bot"],
      includeInferred: false,
      includeNoise: false,
      evidenceMode: "both-confirmed",
      topXByScore: 25,
      sizeByScore: true,
      scoreSizeContrast: 1.8,
      maxNodeRadius: 30,
      showFlags: true
    },
    textQuery: "non-default",
    sort: { field: "type", direction: "asc" }
  };
}

describe("filterStore anchor URL state", () => {
  it("serializes anchors into query string", () => {
    const serialized = serializeQueryState(makeQuery());
    expect(serialized).toContain("anchors=99%2C7%2C42");
  });

  it("deserializes anchors from query string", () => {
    const parsed = deserializeQueryState("?anchors=4,10,25");
    expect(parsed.filters?.anchoredAllianceIds).toEqual([4, 10, 25]);
  });

  it("keeps showFlags default off when param is absent", () => {
    const parsed = deserializeQueryState("");
    expect(parsed.filters?.showFlags).toBe(false);
  });

  it("round-trips showFlags only when enabled", () => {
    const query = makeQuery();
    query.filters.showFlags = true;

    const serialized = serializeQueryState(query);
    expect(serialized).toContain("showFlags=1");

    const parsed = deserializeQueryState(`?${serialized}`);
    expect(parsed.filters?.showFlags).toBe(true);
  });

  it("round-trips scoreSizeContrast when non-default", () => {
    const query = makeQuery();
    query.filters.scoreSizeContrast = 2.25;

    const serialized = serializeQueryState(query);
    expect(serialized).toContain("scoreSizeContrast=2.25");

    const parsed = deserializeQueryState(`?${serialized}`);
    expect(parsed.filters?.scoreSizeContrast).toBe(2.25);
  });

  it("uses neutral scoreSizeContrast default when absent", () => {
    const parsed = deserializeQueryState("");
    expect(parsed.filters?.scoreSizeContrast).toBe(SCORE_SIZE_CONTRAST_DEFAULT);
  });

  it("round-trips maxNodeRadius when non-default", () => {
    const query = makeQuery();
    query.filters.maxNodeRadius = 32;

    const serialized = serializeQueryState(query);
    expect(serialized).toContain("maxNodeRadius=32");

    const parsed = deserializeQueryState(`?${serialized}`);
    expect(parsed.filters?.maxNodeRadius).toBe(32);
  });

  it("uses default maxNodeRadius when absent", () => {
    const parsed = deserializeQueryState("");
    expect(parsed.filters?.maxNodeRadius).toBe(NODE_MAX_RADIUS_DEFAULT);
  });
});

describe("filterStore reset semantics", () => {
  it("clearFilters resets filters, text query, time, playback, focus, and sort", () => {
    useFilterStore.setState({ query: makeNonDefaultQuery() });

    useFilterStore.getState().clearFilters();

    expect(useFilterStore.getState().query).toEqual(makeDefaultQuery());
    expect(serializeQueryState(useFilterStore.getState().query)).toBe("");
  });

  it("resetAll matches full default query state", () => {
    useFilterStore.setState({ query: makeNonDefaultQuery() });

    useFilterStore.getState().resetAll();

    expect(useFilterStore.getState().query).toEqual(makeDefaultQuery());
  });

  it("clearFilters and resetAll always exit network fullscreen", () => {
    useFilterStore.setState({ query: makeNonDefaultQuery(), isNetworkFullscreen: true });

    useFilterStore.getState().clearFilters();
    expect(useFilterStore.getState().isNetworkFullscreen).toBe(false);

    useFilterStore.setState({ query: makeNonDefaultQuery(), isNetworkFullscreen: true });
    useFilterStore.getState().resetAll();
    expect(useFilterStore.getState().isNetworkFullscreen).toBe(false);
  });
});

describe("filterStore playback speed parsing", () => {
  it("deserializes speed 16 and 32", () => {
    expect(deserializeQueryState("?speed=16").playback?.speed).toBe(16);
    expect(deserializeQueryState("?speed=32").playback?.speed).toBe(32);
  });

  it("serializes non-default 16 and 32 speeds", () => {
    const speed16 = makeDefaultQuery();
    speed16.playback.speed = 16;
    expect(serializeQueryState(speed16)).toContain("speed=16");

    const speed32 = makeDefaultQuery();
    speed32.playback.speed = 32;
    expect(serializeQueryState(speed32)).toContain("speed=32");
  });

  it("allows fullscreen setter transitions", () => {
    useFilterStore.setState({ isNetworkFullscreen: false });
    useFilterStore.getState().setNetworkFullscreen(true);
    expect(useFilterStore.getState().isNetworkFullscreen).toBe(true);

    useFilterStore.getState().setNetworkFullscreen(false);
    expect(useFilterStore.getState().isNetworkFullscreen).toBe(false);
  });
});

describe("filterStore score size contrast setter", () => {
  it("clamps and normalizes scoreSizeContrast values", () => {
    useFilterStore.setState({ query: makeDefaultQuery() });

    useFilterStore.getState().setScoreSizeContrast(SCORE_SIZE_CONTRAST_MIN - 1);
    expect(useFilterStore.getState().query.filters.scoreSizeContrast).toBe(SCORE_SIZE_CONTRAST_MIN);

    useFilterStore.getState().setScoreSizeContrast(SCORE_SIZE_CONTRAST_MAX + 1);
    expect(useFilterStore.getState().query.filters.scoreSizeContrast).toBe(SCORE_SIZE_CONTRAST_MAX);

    useFilterStore.getState().setScoreSizeContrast(1.234);
    expect(useFilterStore.getState().query.filters.scoreSizeContrast).toBe(1.23);
  });

  it("falls back to neutral for non-finite values", () => {
    expect(normalizeScoreSizeContrast(Number.NaN)).toBe(SCORE_SIZE_CONTRAST_DEFAULT);
    expect(normalizeScoreSizeContrast(Number.POSITIVE_INFINITY)).toBe(SCORE_SIZE_CONTRAST_DEFAULT);
  });
});

describe("filterStore max node radius setter", () => {
  it("clamps and normalizes maxNodeRadius values", () => {
    useFilterStore.setState({ query: makeDefaultQuery() });

    useFilterStore.getState().setMaxNodeRadius(NODE_MAX_RADIUS_MIN - 1);
    expect(useFilterStore.getState().query.filters.maxNodeRadius).toBe(NODE_MAX_RADIUS_MIN);

    useFilterStore.getState().setMaxNodeRadius(NODE_MAX_RADIUS_MAX + 1);
    expect(useFilterStore.getState().query.filters.maxNodeRadius).toBe(NODE_MAX_RADIUS_MAX);

    useFilterStore.getState().setMaxNodeRadius(22.6);
    expect(useFilterStore.getState().query.filters.maxNodeRadius).toBe(23);
  });

  it("falls back to default for non-finite values", () => {
    expect(normalizeMaxNodeRadius(Number.NaN)).toBe(NODE_MAX_RADIUS_DEFAULT);
    expect(normalizeMaxNodeRadius(Number.POSITIVE_INFINITY)).toBe(NODE_MAX_RADIUS_DEFAULT);
  });
});
