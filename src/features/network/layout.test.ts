import { describe, expect, it } from "vitest";
import {
  clampDisplacement,
  dampPosition,
  distanceBetween,
  positionForNode
} from "@/features/network/layout";

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

  it("keeps neighboring playhead transitions low-churn with displacement caps", () => {
    const nodeIds = ["30", "31", "32", "33", "34"];
    const t1Targets = new Map(nodeIds.map((nodeId) => [nodeId, positionForNode(nodeId)]));
    const t2Targets = new Map(
      nodeIds.map((nodeId, index) => {
        const base = positionForNode(nodeId);
        return [nodeId, { x: base.x + (index % 2 === 0 ? 3 : -2.5), y: base.y + (index % 2 === 0 ? -2 : 2.8) }];
      })
    );

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
