import { describe, expect, it } from "vitest";
import { dampPosition, positionForNode } from "@/features/network/layout";

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
});
