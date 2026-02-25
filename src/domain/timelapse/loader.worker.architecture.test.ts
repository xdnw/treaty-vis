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
  });
});
