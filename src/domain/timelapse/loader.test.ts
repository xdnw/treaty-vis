import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resolveAllianceFlagSnapshot } from "@/domain/timelapse/loader";
import type { AllianceFlagTimelineByAlliance } from "@/domain/timelapse/schema";

const originalFetch = globalThis.fetch;

beforeEach(() => {
  vi.restoreAllMocks();
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

function buildTimelines(): AllianceFlagTimelineByAlliance {
  return {
    "42": [
      {
        timestamp: "2020-01-05T00:00:00.000Z",
        day: "2020-01-05",
        allianceName: "Alpha",
        action: "initial",
        flagKey: "alpha-v1",
        sourceRef: "ref-1"
      },
      {
        timestamp: "2020-02-01T00:00:00.000Z",
        day: "2020-02-01",
        allianceName: "Alpha",
        action: "changed",
        flagKey: "alpha-v2",
        sourceRef: "ref-2"
      }
    ]
  };
}

describe("resolveAllianceFlagSnapshot", () => {
  it("returns current or prior snapshot for in-range and exact playheads", () => {
    const timelines = buildTimelines();

    const exact = resolveAllianceFlagSnapshot(timelines, 42, "2020-02-01T00:00:00.000Z");
    const inRangePrior = resolveAllianceFlagSnapshot(timelines, 42, "2020-01-20T12:00:00.000Z");

    expect(exact?.flagKey).toBe("alpha-v2");
    expect(exact?.action).toBe("changed");
    expect(inRangePrior?.flagKey).toBe("alpha-v1");
    expect(inRangePrior?.action).toBe("initial");
  });

  it("returns earliest future snapshot when playhead is before first entry", () => {
    const timelines = buildTimelines();

    const result = resolveAllianceFlagSnapshot(timelines, 42, "2019-12-01T00:00:00.000Z");

    expect(result).not.toBeNull();
    expect(result?.flagKey).toBe("alpha-v1");
    expect(result?.timestamp).toBe("2020-01-05T00:00:00.000Z");
  });

  it("returns null for empty alliance timeline", () => {
    const timelines: AllianceFlagTimelineByAlliance = { "42": [] };

    const result = resolveAllianceFlagSnapshot(timelines, 42, "2020-01-01T00:00:00.000Z");

    expect(result).toBeNull();
  });

  it("returns latest snapshot when playhead is null", () => {
    const timelines = buildTimelines();

    const result = resolveAllianceFlagSnapshot(timelines, 42, null);

    expect(result).not.toBeNull();
    expect(result?.flagKey).toBe("alpha-v2");
    expect(result?.timestamp).toBe("2020-02-01T00:00:00.000Z");
  });
});
