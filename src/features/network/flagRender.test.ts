import { describe, expect, it } from "vitest";
import {
  deriveFlagRenderMode,
  resolveAtlasSprite,
  resolveSpriteNodeIds
} from "@/features/network/flagRender";
import type { FlagAssetsPayload } from "@/domain/timelapse/schema";

const ASSETS: FlagAssetsPayload = {
  atlas: {
    webp: "/data/flag_atlas.webp",
    png: "/data/flag_atlas.png",
    width: 512,
    height: 256,
    tile_width: 32,
    tile_height: 24
  },
  assets: {
    alpha: { x: 0, y: 0, w: 32, h: 24, hash: "aaa" },
    beta: { x: 32, y: 0, w: 32, h: 24, hash: "bbb" }
  }
};

describe("flagRender helpers", () => {
  it("forces OFF mode when showFlags is false", () => {
    expect(deriveFlagRenderMode(false, true, 1, 40, "none")).toBe("off");
  });

  it("uses focused-hover-only under critical pressure", () => {
    expect(deriveFlagRenderMode(true, true, 1, 40, "critical")).toBe("focused-hover-only");
  });

  it("uses bounded-full under elevated pressure even when otherwise eligible for full", () => {
    expect(deriveFlagRenderMode(true, true, 1.2, 120, "elevated")).toBe("bounded-full");
  });

  it("uses bounded-full when camera ratio is out of budget", () => {
    expect(deriveFlagRenderMode(true, true, 3, 120, "none")).toBe("bounded-full");
  });

  it("uses full mode only when no pressure and within zoom/node budgets", () => {
    expect(deriveFlagRenderMode(true, true, 1.2, 120, "none")).toBe("full");
  });

  it("returns empty sprite ids in OFF mode", () => {
    expect(resolveSpriteNodeIds("off", ["1", "2"], "1", "2")).toEqual(new Set());
  });

  it("caps full-mode sprites and keeps focus/hover in focused-hover-only mode", () => {
    const full = resolveSpriteNodeIds("full", ["1", "2", "3"], null, null, 2);
    expect(full.size).toBe(2);
    expect(full.has("1")).toBe(true);

    const focusedOnly = resolveSpriteNodeIds("focused-hover-only", ["1", "2"], "2", "1");
    expect([...focusedOnly].sort()).toEqual(["1", "2"]);
  });

  it("prioritizes visible nodes over non-visible nodes", () => {
    const selected = resolveSpriteNodeIds(
      "bounded-full",
      ["n1", "n2", "n3", "n4"],
      null,
      null,
      2,
      {
        visibleNodeIds: new Set(["n3", "n4"]),
        importanceByNodeId: { n1: 100, n2: 90, n3: 1, n4: 2 }
      }
    );

    expect(selected).toEqual(new Set(["n3", "n4"]));
  });

  it("prefers higher importance among nodes with the same visibility rank", () => {
    const selected = resolveSpriteNodeIds(
      "bounded-full",
      ["a", "b", "c"],
      null,
      null,
      2,
      {
        visibleNodeIds: new Set(["a", "b", "c"]),
        importanceByNodeId: new Map([
          ["a", 1],
          ["b", 20],
          ["c", 10]
        ])
      }
    );

    expect(selected).toEqual(new Set(["b", "c"]));
  });

  it("always includes focused and hovered nodes when present", () => {
    const selected = resolveSpriteNodeIds(
      "bounded-full",
      ["focus", "hover", "v1", "v2", "v3"],
      "focus",
      "hover",
      2,
      {
        visibleNodeIds: new Set(["v1", "v2", "v3"]),
        importanceByNodeId: { focus: 0, hover: 0, v1: 100, v2: 99, v3: 98 }
      }
    );

    expect(selected).toEqual(new Set(["focus", "hover"]));
  });

  it("looks up atlas sprites deterministically", () => {
    const sprite = resolveAtlasSprite(ASSETS, "alpha");
    expect(sprite?.key).toBe("alpha");
    expect(sprite?.asset).toEqual(ASSETS.assets.alpha);
    expect(resolveAtlasSprite(ASSETS, "missing")).toBeNull();
  });
});
