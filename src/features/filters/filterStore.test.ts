import { describe, expect, it } from "vitest";
import { deserializeQueryState, serializeQueryState, type QueryState } from "@/features/filters/filterStore";

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
      sizeByScore: false,
      showFlags: false
    },
    textQuery: "",
    sort: { field: "timestamp", direction: "desc" }
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
});
