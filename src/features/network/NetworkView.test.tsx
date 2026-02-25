import { describe, expect, it } from "vitest";
import type { ScoreLoaderSnapshot, ScoreLoaderState } from "@/domain/timelapse/scoreLoader";
import {
  applyScoreContrast,
  DEFAULT_MAX_NODE_RADIUS,
  deriveScoreFailureDiagnostic,
  scoreRadiusWithContrast
} from "@/features/network/NetworkView";

function makeSnapshot(state: ScoreLoaderState): ScoreLoaderSnapshot {
  return {
    requestId: "req-1",
    datasetId: "dataset-a",
    state,
    startedAtMs: 0,
    atMs: 10,
    elapsedMs: 10,
    stageDurationMs: 10,
    httpStatus: null,
    bytesFetched: 0,
    totalBytes: null,
    decodeMs: null,
    dayCount: 0,
    scoredNodeCount: 0,
    reasonCode: null,
    message: null,
    runtime: null,
    fromCache: false
  };
}

describe("NetworkView score diagnostics", () => {
  it("returns no diagnostics for healthy loader paths", () => {
    const healthy = deriveScoreFailureDiagnostic({
      sizeByScore: false,
      scoreManifestDeclared: true,
      scoreLoadSnapshot: makeSnapshot("ready")
    });

    expect(healthy).toBeNull();
  });

  it("returns compact diagnostics for terminal loader failures", () => {
    const failed = deriveScoreFailureDiagnostic({
      sizeByScore: true,
      scoreManifestDeclared: true,
      scoreLoadSnapshot: {
        ...makeSnapshot("error-timeout"),
        elapsedMs: 15023,
        message: "Timed out after 15000ms"
      }
    });

    expect(failed).not.toBeNull();
    expect(failed?.code).toBe("timeout");
    expect(failed?.actionableRetry).toBe(true);
    expect(failed?.title).toContain("timed out");
  });

  it("returns manifest-missing diagnostics only when score sizing is requested", () => {
    const withoutSizing = deriveScoreFailureDiagnostic({
      sizeByScore: false,
      scoreManifestDeclared: false,
      scoreLoadSnapshot: null
    });

    const withSizing = deriveScoreFailureDiagnostic({
      sizeByScore: true,
      scoreManifestDeclared: false,
      scoreLoadSnapshot: null
    });

    expect(withoutSizing).toBeNull();
    expect(withSizing).not.toBeNull();
    expect(withSizing?.code).toBe("manifest-missing");
    expect(withSizing?.actionableRetry).toBe(false);
  });
});

describe("NetworkView score radius mapping", () => {
  it("keeps neutral contrast equivalent to baseline normalized score", () => {
    const neutral = applyScoreContrast(0.5, 1);
    expect(neutral).toBeCloseTo(0.5, 6);
  });

  it("increases spread at higher contrast around the same midpoint distance", () => {
    const upperLow = applyScoreContrast(0.6, 3);
    const upperHigh = applyScoreContrast(0.95, 3);

    expect(upperLow).toBeLessThan(0.6);
    expect(upperHigh).toBeGreaterThan(0.8);
    expect(upperHigh - upperLow).toBeGreaterThan(0.35);
  });

  it("maps 100k vs 900k to visibly different radii", () => {
    const low = scoreRadiusWithContrast(100_000, 10_000, 1_000_000, 2);
    const high = scoreRadiusWithContrast(900_000, 10_000, 1_000_000, 2);

    expect(high - low).toBeGreaterThan(2);
  });

  it("keeps radii clamped within node bounds at high contrast", () => {
    const nearMin = scoreRadiusWithContrast(1, 1, 10_000, 3);
    const nearMax = scoreRadiusWithContrast(10_000, 1, 10_000, 3);

    expect(nearMin).toBeGreaterThanOrEqual(5);
    expect(nearMax).toBeLessThanOrEqual(DEFAULT_MAX_NODE_RADIUS);
  });
});
