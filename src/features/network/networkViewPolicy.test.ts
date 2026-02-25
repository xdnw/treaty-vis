import { describe, expect, it } from "vitest";
import {
  FLAG_PRESSURE_SCORE_CRITICAL_RECOVER,
  FLAG_PRESSURE_SCORE_CRITICAL_TRIGGER,
  FLAG_PRESSURE_SCORE_ELEVATED_RECOVER,
  FLAG_PRESSURE_SCORE_ELEVATED_TRIGGER,
  buildHoverResetKey,
  derivePressureLevel
} from "@/features/network/networkViewPolicy";
import type { QueryState } from "@/features/filters/filterStore";

function makeQuery(): QueryState {
  return {
    time: { start: "2020-01-01", end: "2020-12-31" },
    playback: { playhead: null, isPlaying: false, speed: 1 },
    focus: { allianceId: null, edgeKey: null, eventId: null },
    filters: {
      alliances: [4, 2],
      anchoredAllianceIds: [],
      treatyTypes: ["ODP"],
      actions: ["signed"],
      sources: ["game"],
      includeInferred: true,
      includeNoise: false,
      evidenceMode: "all",
      topXByScore: null,
      sizeByScore: false,
      showFlags: false
    },
    textQuery: "  Test Query  ",
    sort: { field: "timestamp", direction: "desc" }
  };
}

describe("networkViewPolicy", () => {
  describe("derivePressureLevel hysteresis boundaries", () => {
    it("keeps none below elevated trigger", () => {
      expect(derivePressureLevel(FLAG_PRESSURE_SCORE_ELEVATED_TRIGGER - 1, "none")).toBe("none");
    });

    it("moves none to elevated at elevated trigger", () => {
      expect(derivePressureLevel(FLAG_PRESSURE_SCORE_ELEVATED_TRIGGER, "none")).toBe("elevated");
    });

    it("moves elevated to critical at critical trigger", () => {
      expect(derivePressureLevel(FLAG_PRESSURE_SCORE_CRITICAL_TRIGGER, "elevated")).toBe("critical");
    });

    it("moves elevated to none at elevated recover threshold", () => {
      expect(derivePressureLevel(FLAG_PRESSURE_SCORE_ELEVATED_RECOVER, "elevated")).toBe("none");
    });

    it("moves critical to elevated at critical recover threshold", () => {
      expect(derivePressureLevel(FLAG_PRESSURE_SCORE_CRITICAL_RECOVER, "critical")).toBe("elevated");
    });
  });

  describe("buildHoverResetKey", () => {
    it("returns the same key when only playhead changes", () => {
      const baseQuery = makeQuery();

      const keyAtT1 = buildHoverResetKey(baseQuery, {
        sizeByScore: true,
        maxEdges: 500,
        allEventsLength: 100,
        scopedIndexes: [2, 5, 7],
        playhead: "2020-02-01"
      });

      const keyAtT2 = buildHoverResetKey(baseQuery, {
        sizeByScore: true,
        maxEdges: 500,
        allEventsLength: 100,
        scopedIndexes: [2, 5, 7],
        playhead: "2020-03-01"
      });

      expect(keyAtT1).toBe(keyAtT2);
    });

    it("changes key when treaty type filters change", () => {
      const original = makeQuery();
      const modified: QueryState = {
        ...original,
        filters: {
          ...original.filters,
          treatyTypes: [...original.filters.treatyTypes, "MDP"]
        }
      };

      const originalKey = buildHoverResetKey(original, {
        sizeByScore: false,
        maxEdges: 700,
        allEventsLength: 80,
        scopedIndexes: [1, 3, 8]
      });

      const modifiedKey = buildHoverResetKey(modified, {
        sizeByScore: false,
        maxEdges: 700,
        allEventsLength: 80,
        scopedIndexes: [1, 3, 8]
      });

      expect(modifiedKey).not.toBe(originalKey);
    });
  });
});