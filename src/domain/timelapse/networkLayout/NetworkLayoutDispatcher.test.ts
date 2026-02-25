import { describe, expect, it } from "vitest";
import { runNetworkLayoutStrategy } from "@/domain/timelapse/networkLayout/NetworkLayoutDispatcher";
import type { NetworkLayoutInput } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";

function buildInput(): NetworkLayoutInput {
  const adjacencyByNodeId = new Map<string, Set<string>>();
  const link = (left: string, right: string) => {
    const leftNeighbors = adjacencyByNodeId.get(left) ?? new Set<string>();
    leftNeighbors.add(right);
    adjacencyByNodeId.set(left, leftNeighbors);

    const rightNeighbors = adjacencyByNodeId.get(right) ?? new Set<string>();
    rightNeighbors.add(left);
    adjacencyByNodeId.set(right, rightNeighbors);
  };

  link("1", "2");
  link("2", "3");
  link("3", "4");
  link("5", "6");

  return {
    nodeIds: ["1", "2", "3", "4", "5", "6"],
    adjacencyByNodeId,
    temporalKey: "test-key"
  };
}

function nodePositionSignature(input: ReturnType<typeof runNetworkLayoutStrategy>): string[] {
  return input.layout.nodeTargets.map((target) => `${target.nodeId}:${target.targetX.toFixed(4)},${target.targetY.toFixed(4)}`);
}

describe("NetworkLayoutDispatcher", () => {
  it("switches layout output when strategy changes", () => {
    const input = buildInput();

    const hybrid = runNetworkLayoutStrategy("hybrid-backbone", input);
    const fa2 = runNetworkLayoutStrategy("fa2line", input);

    expect(hybrid.layout.components.length).toBeGreaterThan(0);
    expect(fa2.layout.components.length).toBeGreaterThan(0);
    expect(nodePositionSignature(hybrid)).not.toEqual(nodePositionSignature(fa2));
  });

  it("is deterministic for repeated runs with same input", () => {
    const input = buildInput();

    const first = runNetworkLayoutStrategy("hybrid-backbone", input);
    const second = runNetworkLayoutStrategy("hybrid-backbone", input);

    expect(nodePositionSignature(first)).toEqual(nodePositionSignature(second));
  });

  it("fails fast on unknown strategy", () => {
    const input = buildInput();

    expect(() => runNetworkLayoutStrategy("unknown" as never, input)).toThrow("Unknown strategy");
  });
});
