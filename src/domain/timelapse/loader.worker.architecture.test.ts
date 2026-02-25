import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const currentFile = fileURLToPath(import.meta.url);
const currentDir = path.dirname(currentFile);

function readLoaderWorkerSource(): string {
  const loaderWorkerPath = path.resolve(currentDir, "loader.worker.ts");
  return readFileSync(loaderWorkerPath, "utf8");
}

describe("loader worker layout architecture", () => {
  it("routes layout through the dispatcher only", () => {
    const source = readLoaderWorkerSource();

    expect(source).toContain('from "@/domain/timelapse/networkLayout/NetworkLayoutDispatcher"');
    expect(source).not.toContain("networkLayout/HybridBackboneLayoutAlgorithm");
    expect(source).not.toContain("networkLayout/FA2LineLayoutAlgorithm");
    expect(source).not.toContain("networkLayout/INetworkLayoutAlgorithm");

    const importLines = source
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.startsWith("import ") && line.includes("@/domain/timelapse/networkLayout/"));

    expect(importLines.length).toBeGreaterThan(0);
    for (const line of importLines) {
      const isAllowed =
        line.includes("networkLayout/NetworkLayoutDispatcher") || line.includes("networkLayout/NetworkLayoutTypes");
      expect(isAllowed).toBe(true);
    }
  });

  it("keeps frame-index fast path guarded with legacy fallback", () => {
    const source = readLoaderWorkerSource();

    expect(source).toContain('dataAssetPath("treaty_frame_index_v1.msgpack")');
    expect(source).toContain("function computeNetworkEdgeEventIndexesFromFrameIndex(");
    expect(source).toContain("return computeNetworkEdgeEventIndexesLegacy(");
    expect(source).toContain("treatyFrameIndexV1Schema");
  });
});
