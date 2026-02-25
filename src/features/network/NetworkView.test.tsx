import { describe, expect, it } from "vitest";
import type { ScoreLoaderSnapshot, ScoreLoaderState } from "@/domain/timelapse/scoreLoader";
import { deriveScoreFailureDiagnostic } from "@/features/network/NetworkView";

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
