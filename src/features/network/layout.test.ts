import { describe, expect, it } from "vitest";
import {
  clampDisplacement,
  dampPosition,
  distanceBetween,
  positionForNode,
  resolveLayoutTargets
} from "@/features/network/layout";

function buildAdjacency(pairs: Array<[string, string]>): Map<string, Set<string>> {
  const adjacency = new Map<string, Set<string>>();
  for (const [left, right] of pairs) {
    const leftNeighbors = adjacency.get(left) ?? new Set<string>();
    leftNeighbors.add(right);
    adjacency.set(left, leftNeighbors);

    const rightNeighbors = adjacency.get(right) ?? new Set<string>();
    rightNeighbors.add(left);
    adjacency.set(right, rightNeighbors);
  }
  return adjacency;
}

describe("network layout helpers", () => {
  it("returns deterministic coordinates per node id", () => {
    const first = positionForNode("123");
    const second = positionForNode("123");
    const other = positionForNode("124");

    expect(first).toEqual(second);
    expect(other).not.toEqual(first);
  });

  it("damps movement toward target without overshooting", () => {
    const next = dampPosition({ x: 10, y: -10 }, { x: 30, y: 10 }, 0.25);
    expect(next.x).toBeCloseTo(15);
    expect(next.y).toBeCloseTo(-5);

    const clamped = dampPosition({ x: 0, y: 0 }, { x: 100, y: 100 }, 2);
    expect(clamped).toEqual({ x: 100, y: 100 });
  });

  it("resolves topology-biased targets deterministically", () => {
    const nodeIds = ["10", "11", "12", "13"];
    const adjacencyByNodeId = buildAdjacency([
      ["10", "11"],
      ["11", "12"],
      ["12", "13"]
    ]);

    const first = resolveLayoutTargets({
      nodeIds,
      adjacencyByNodeId,
      strategy: "topology-biased",
      topologyStrength: 0.4,
      topologyMaxOffset: 18
    });
    const second = resolveLayoutTargets({
      nodeIds,
      adjacencyByNodeId,
      strategy: "topology-biased",
      topologyStrength: 0.4,
      topologyMaxOffset: 18
    });

    for (const nodeId of nodeIds) {
      expect(first.get(nodeId)).toEqual(second.get(nodeId));
    }
  });

  it("keeps topology-biased offsets bounded from hash seeds", () => {
    const nodeIds = ["20", "21", "22"];
    const adjacencyByNodeId = buildAdjacency([
      ["20", "21"],
      ["21", "22"]
    ]);

    const maxOffset = 8;
    const targets = resolveLayoutTargets({
      nodeIds,
      adjacencyByNodeId,
      strategy: "topology-biased",
      topologyStrength: 1,
      topologyMaxOffset: maxOffset
    });

    for (const nodeId of nodeIds) {
      const seed = positionForNode(nodeId);
      const target = targets.get(nodeId)!;
      expect(distanceBetween(seed, target)).toBeLessThanOrEqual(maxOffset + 1e-6);
    }
  });

  it("keeps neighboring playhead transitions low-churn with displacement caps", () => {
    const nodeIds = ["30", "31", "32", "33", "34"];
    const t1Adjacency = buildAdjacency([
      ["30", "31"],
      ["31", "32"],
      ["32", "33"]
    ]);
    const t2Adjacency = buildAdjacency([
      ["30", "31"],
      ["31", "32"],
      ["32", "33"],
      ["33", "34"]
    ]);

    const t1Targets = resolveLayoutTargets({
      nodeIds,
      adjacencyByNodeId: t1Adjacency,
      strategy: "topology-biased",
      topologyStrength: 0.38,
      topologyMaxOffset: 18
    });
    const t2Targets = resolveLayoutTargets({
      nodeIds,
      adjacencyByNodeId: t2Adjacency,
      strategy: "topology-biased",
      topologyStrength: 0.38,
      topologyMaxOffset: 18
    });

    let changed = 0;
    let maxStep = 0;
    for (const nodeId of nodeIds) {
      const previous = t1Targets.get(nodeId)!;
      const target = t2Targets.get(nodeId)!;
      const damped = dampPosition(previous, target, 0.22);
      const next = clampDisplacement(previous, damped, 6);
      const step = distanceBetween(previous, next);
      if (step > 0.001) {
        changed += 1;
      }
      if (step > maxStep) {
        maxStep = step;
      }
    }

    expect(changed).toBeLessThanOrEqual(nodeIds.length);
    expect(maxStep).toBeLessThanOrEqual(6 + 1e-6);
  });
});
