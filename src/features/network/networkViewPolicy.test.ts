import { describe, expect, it } from "vitest";
import {
  FLAG_PRESSURE_SCORE_CRITICAL_RECOVER,
  FLAG_PRESSURE_SCORE_CRITICAL_TRIGGER,
  FLAG_PRESSURE_SCORE_ELEVATED_RECOVER,
  FLAG_PRESSURE_SCORE_ELEVATED_TRIGGER,
  NETWORK_LAYOUT_MAX_STEP_DISPLACEMENT,
  NETWORK_LAYOUT_RELAX_MAX_STEP_DISPLACEMENT,
  buildHoverResetKey,
  derivePressureLevel,
  quantizePlayheadIndexForAutoplay,
  shouldExitNetworkFullscreenOnEscape,
  shouldForceNodeLabel
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
      scoreSizeContrast: 1,
      maxNodeRadius: 25,
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
        scoreSizeContrast: 1,
        maxNodeRadius: 25,
        maxEdges: 500,
        allEventsLength: 100,
        scopedIndexes: [2, 5, 7],
        playhead: "2020-02-01"
      });

      const keyAtT2 = buildHoverResetKey(baseQuery, {
        sizeByScore: true,
        scoreSizeContrast: 1,
        maxNodeRadius: 25,
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
        scoreSizeContrast: 1,
        maxNodeRadius: 25,
        maxEdges: 700,
        allEventsLength: 80,
        scopedIndexes: [1, 3, 8]
      });

      const modifiedKey = buildHoverResetKey(modified, {
        sizeByScore: false,
        scoreSizeContrast: 1,
        maxNodeRadius: 25,
        maxEdges: 700,
        allEventsLength: 80,
        scopedIndexes: [1, 3, 8]
      });

      expect(modifiedKey).not.toBe(originalKey);
    });

    it("changes key when score size contrast changes", () => {
      const baseQuery = makeQuery();

      const contrastOne = buildHoverResetKey(baseQuery, {
        sizeByScore: true,
        scoreSizeContrast: 1,
        maxNodeRadius: 25,
        maxEdges: 400,
        allEventsLength: 120,
        scopedIndexes: [0, 4, 9]
      });

      const contrastHigh = buildHoverResetKey(baseQuery, {
        sizeByScore: true,
        scoreSizeContrast: 2,
        maxNodeRadius: 25,
        maxEdges: 400,
        allEventsLength: 120,
        scopedIndexes: [0, 4, 9]
      });

      expect(contrastHigh).not.toBe(contrastOne);
    });

    it("changes key when max node radius changes", () => {
      const baseQuery = makeQuery();

      const smaller = buildHoverResetKey(baseQuery, {
        sizeByScore: true,
        scoreSizeContrast: 1,
        maxNodeRadius: 20,
        maxEdges: 400,
        allEventsLength: 120,
        scopedIndexes: [0, 4, 9]
      });

      const larger = buildHoverResetKey(baseQuery, {
        sizeByScore: true,
        scoreSizeContrast: 1,
        maxNodeRadius: 30,
        maxEdges: 400,
        allEventsLength: 120,
        scopedIndexes: [0, 4, 9]
      });

      expect(larger).not.toBe(smaller);
    });
  });

  describe("shouldExitNetworkFullscreenOnEscape", () => {
    it("returns true only for Escape while fullscreen is active", () => {
      expect(shouldExitNetworkFullscreenOnEscape("Escape", true)).toBe(true);
      expect(shouldExitNetworkFullscreenOnEscape("Enter", true)).toBe(false);
      expect(shouldExitNetworkFullscreenOnEscape("Escape", false)).toBe(false);
    });
  });

  describe("shouldForceNodeLabel", () => {
    it("forces all node labels in fullscreen when policy flag is enabled", () => {
      expect(shouldForceNodeLabel(true, true, false)).toBe(true);
      expect(shouldForceNodeLabel(true, true, true)).toBe(true);
    });

    it("falls back to priority labels outside fullscreen force mode", () => {
      expect(shouldForceNodeLabel(false, true, true)).toBe(true);
      expect(shouldForceNodeLabel(false, true, false)).toBe(false);
      expect(shouldForceNodeLabel(true, false, true)).toBe(true);
      expect(shouldForceNodeLabel(true, false, false)).toBe(false);
    });
  });

  describe("quantizePlayheadIndexForAutoplay", () => {
    it("keeps full index precision at lower speeds", () => {
      expect(quantizePlayheadIndexForAutoplay(17, 1)).toBe(17);
      expect(quantizePlayheadIndexForAutoplay(17, 8)).toBe(17);
    });

    it("quantizes index more aggressively at high speeds", () => {
      expect(quantizePlayheadIndexForAutoplay(17, 16)).toBe(15);
      expect(quantizePlayheadIndexForAutoplay(17, 32)).toBe(16);
    });
  });

  describe("layout tuning policy", () => {
    it("keeps canonical displacement caps in valid ranges", () => {
      expect(NETWORK_LAYOUT_MAX_STEP_DISPLACEMENT).toBeGreaterThan(0);
      expect(NETWORK_LAYOUT_RELAX_MAX_STEP_DISPLACEMENT).toBeGreaterThanOrEqual(
        NETWORK_LAYOUT_MAX_STEP_DISPLACEMENT
      );
    });
  });
});
