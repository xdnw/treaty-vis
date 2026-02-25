import { describe, expect, it } from "vitest";
import {
  buildQuerySelectionKey,
  compareTimelapseEvents,
  computeSelectionIndexes,
  type TimelapseEventLike,
  type TimelapseQueryLike
} from "@/domain/timelapse/queryEngine";

function makeEvent(overrides: Partial<TimelapseEventLike>): TimelapseEventLike {
  return {
    event_id: "evt-a",
    action: "signed",
    treaty_type: "MDP",
    source: "bot",
    timestamp: "2024-01-01T00:00:00.000Z",
    from_alliance_id: 1,
    to_alliance_id: 2,
    from_alliance_name: "Alpha",
    to_alliance_name: "Beta",
    grounded_from: true,
    grounded_to: true,
    inferred: false,
    noise_filtered: false,
    pair_min_id: 1,
    pair_max_id: 2,
    ...overrides
  };
}

function makeQuery(overrides?: Partial<TimelapseQueryLike>): TimelapseQueryLike {
  return {
    time: { start: null, end: null },
    playback: { playhead: null },
    focus: { allianceId: null, edgeKey: null, eventId: null },
    filters: {
      alliances: [],
      treatyTypes: [],
      actions: [],
      sources: [],
      includeInferred: true,
      includeNoise: true,
      evidenceMode: "all",
      topXByScore: null
    },
    textQuery: "",
    sort: { field: "timestamp", direction: "desc" },
    ...overrides
  };
}

describe("queryEngine compareTimelapseEvents", () => {
  it("uses stable event-id tie break across inspector and domain sorting", () => {
    const left = makeEvent({ event_id: "evt-a", timestamp: "2024-01-01T00:00:00.000Z", action: "signed" });
    const right = makeEvent({ event_id: "evt-b", timestamp: "2024-01-01T00:00:00.000Z", action: "signed" });

    expect(compareTimelapseEvents(left, right, "action", "asc")).toBeLessThan(0);
    expect(compareTimelapseEvents(left, right, "action", "desc")).toBeGreaterThan(0);
  });
});

describe("queryEngine computeSelectionIndexes", () => {
  it("produces deterministic keys for equivalent queries", () => {
    const query = makeQuery({
      filters: {
        alliances: [2, 1],
        treatyTypes: ["MDP"],
        actions: ["signed"],
        sources: ["bot"],
        includeInferred: false,
        includeNoise: false,
        evidenceMode: "both-confirmed",
        topXByScore: 50
      },
      textQuery: " alpha "
    });

    const keyA = buildQuerySelectionKey(query);
    const keyB = buildQuerySelectionKey({ ...query });
    expect(keyA).toBe(keyB);
  });

  it("filters by text/time/focus and returns sorted indexes", () => {
    const events = [
      makeEvent({
        event_id: "evt-0",
        timestamp: "2024-01-01T00:00:00.000Z",
        from_alliance_name: "Alpha"
      }),
      makeEvent({
        event_id: "evt-1",
        timestamp: "2024-01-02T00:00:00.000Z",
        from_alliance_name: "Bravo"
      }),
      makeEvent({
        event_id: "evt-2",
        timestamp: "2024-01-03T00:00:00.000Z",
        from_alliance_name: "Alpha"
      })
    ];

    const indexes = computeSelectionIndexes({
      events,
      indices: {
        allEventIndexes: [0, 1, 2],
        byAction: { signed: [0, 1, 2] },
        byType: { MDP: [0, 1, 2] },
        bySource: { bot: [0, 1, 2] },
        byAlliance: { "1": [0, 1, 2], "2": [0, 1, 2] }
      },
      query: makeQuery({
        time: { start: "2024-01-01T00:00:00.000Z", end: "2024-01-03T00:00:00.000Z" },
        focus: { allianceId: 1, edgeKey: null, eventId: null },
        textQuery: "alpha",
        sort: { field: "timestamp", direction: "desc" }
      })
    });

    expect(indexes).toEqual([2, 0]);
  });
});
