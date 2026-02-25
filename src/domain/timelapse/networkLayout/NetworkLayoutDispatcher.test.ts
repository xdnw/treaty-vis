import { describe, expect, it, vi } from "vitest";
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

function buildInputFromEdges(nodeIds: string[], edges: Array<[string, string]>, previousState?: unknown): NetworkLayoutInput {
  const adjacencyByNodeId = new Map<string, Set<string>>();
  for (const nodeId of nodeIds) {
    adjacencyByNodeId.set(nodeId, new Set<string>());
  }

  for (const [left, right] of edges) {
    adjacencyByNodeId.get(left)?.add(right);
    adjacencyByNodeId.get(right)?.add(left);
  }

  return {
    nodeIds,
    adjacencyByNodeId,
    temporalKey: "test-key",
    previousState
  };
}

function nodePositionSignature(input: ReturnType<typeof runNetworkLayoutStrategy>): string[] {
  return input.layout.nodeTargets.map((target) => `${target.nodeId}:${target.targetX.toFixed(4)},${target.targetY.toFixed(4)}`);
}

function lockSignature(output: ReturnType<typeof runNetworkLayoutStrategy>): {
  components: string[];
  communities: string[];
  nodes: string[];
} {
  return {
    components: output.layout.components
      .map((c) => `${c.componentId}:${c.anchorX.toFixed(3)},${c.anchorY.toFixed(3)}:${c.nodeIds.length}`)
      .sort(),
    communities: output.layout.communities
      .map((c) => `${c.communityId}:${c.componentId}:${c.anchorX.toFixed(3)},${c.anchorY.toFixed(3)}:${c.nodeIds.length}`)
      .sort(),
    nodes: output.layout.nodeTargets
      .map(
        (n) =>
          `${n.nodeId}:${n.componentId}:${n.communityId}:${n.targetX.toFixed(3)},${n.targetY.toFixed(3)}:${n.neighborX.toFixed(3)},${n.neighborY.toFixed(3)}:${n.anchorX.toFixed(3)},${n.anchorY.toFixed(3)}`
      )
      .sort()
  };
}

function withDeterministicRandom<T>(fn: () => T): T {
  let state = 0x1234abcd;
  const spy = vi.spyOn(Math, "random").mockImplementation(() => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  });
  try {
    return fn();
  } finally {
    spy.mockRestore();
  }
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

  it("preserves fa2 component identity across small membership edits", () => {
    const first = runNetworkLayoutStrategy(
      "fa2line",
      buildInputFromEdges(
        ["1", "2", "3", "4", "5"],
        [
          ["1", "2"],
          ["2", "3"],
          ["3", "4"],
          ["4", "5"]
        ]
      )
    );

    const second = runNetworkLayoutStrategy(
      "fa2line",
      buildInputFromEdges(
        ["1", "2", "3", "4", "5", "6"],
        [
          ["1", "2"],
          ["2", "3"],
          ["3", "4"],
          ["4", "5"],
          ["5", "6"]
        ],
        first.metadata?.state
      )
    );

    const firstLargest = first.layout.components[0]?.componentId;
    const secondLargest = second.layout.components[0]?.componentId;
    expect(firstLargest).toBeDefined();
    expect(secondLargest).toBeDefined();
    expect(secondLargest).toBe(firstLargest);
  });

  it("preserves hybrid community identity across small membership edits", () => {
    const first = runNetworkLayoutStrategy(
      "hybrid-backbone",
      buildInputFromEdges(
        ["10", "11", "12", "13", "14", "15"],
        [
          ["10", "11"],
          ["11", "12"],
          ["12", "10"],
          ["13", "14"],
          ["14", "15"],
          ["15", "13"],
          ["12", "13"]
        ]
      )
    );

    const second = runNetworkLayoutStrategy(
      "hybrid-backbone",
      buildInputFromEdges(
        ["10", "11", "12", "13", "14", "15", "16"],
        [
          ["10", "11"],
          ["11", "12"],
          ["12", "10"],
          ["13", "14"],
          ["14", "15"],
          ["15", "13"],
          ["12", "13"],
          ["15", "16"]
        ],
        first.metadata?.state
      )
    );

    const firstIds = new Set(first.layout.communities.map((community) => community.communityId));
    const secondIds = new Set(second.layout.communities.map((community) => community.communityId));
    const reusedCount = [...secondIds].filter((id) => firstIds.has(id)).length;

    expect(reusedCount).toBeGreaterThan(0);
  });

  it("locks deterministic behavior signature for existing strategies", () => {
    const nodeIds = ["1", "2", "3", "4", "5", "6", "7", "8"];
    const edges: Array<[string, string]> = [
      ["1", "2"],
      ["2", "3"],
      ["3", "1"],
      ["3", "4"],
      ["4", "5"],
      ["5", "6"],
      ["6", "4"],
      ["6", "7"],
      ["7", "8"]
    ];

    const existingStrategies = ["balanced-temporal", "strict-temporal", "hybrid-backbone", "fa2line"] as const;

    for (const strategy of existingStrategies) {
      const first = withDeterministicRandom(() =>
        runNetworkLayoutStrategy(strategy, buildInputFromEdges(nodeIds, edges))
      );
      const second = withDeterministicRandom(() =>
        runNetworkLayoutStrategy(strategy, buildInputFromEdges([...nodeIds, "9"], [...edges, ["8", "9"]], first.metadata?.state))
      );
      const repeatSecond = withDeterministicRandom(() =>
        runNetworkLayoutStrategy(strategy, buildInputFromEdges([...nodeIds, "9"], [...edges, ["8", "9"]], first.metadata?.state))
      );

      expect(lockSignature(second)).toEqual(lockSignature(repeatSecond));
      expect(second.layout.components.length).toBeGreaterThan(0);
      expect(second.layout.nodeTargets.length).toBe(9);
      expect(second.layout.nodeTargets.every((n) => Number.isFinite(n.targetX) && Number.isFinite(n.targetY))).toBe(true);
    }
  });

  it("routes and executes all new strategies", () => {
    const input = buildInputFromEdges(
      ["n1", "n2", "n3", "n4", "n5", "n6"],
      [
        ["n1", "n2"],
        ["n2", "n3"],
        ["n3", "n1"],
        ["n3", "n4"],
        ["n4", "n5"],
        ["n5", "n6"],
        ["n6", "n4"]
      ]
    );

    const strategies = ["bh-fa2", "stress-majorization", "radial-sugiyama"] as const;
    for (const strategy of strategies) {
      const out = runNetworkLayoutStrategy(strategy, input);
      expect(out.layout.nodeTargets.length).toBe(input.nodeIds.length);
      expect(out.layout.components.length).toBeGreaterThan(0);
      expect(out.layout.communities.length).toBeGreaterThan(0);
    }
  });
});
