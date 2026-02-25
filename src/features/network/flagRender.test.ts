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
    expect(deriveFlagRenderMode(false, true, 1, 40, false)).toBe("off");
  });

  it("degrades to focused-hover-only under frame pressure", () => {
    expect(deriveFlagRenderMode(true, true, 1, 40, true)).toBe("focused-hover-only");
  });

  it("uses full mode only in zoom and node budget", () => {
    expect(deriveFlagRenderMode(true, true, 1.2, 120, false)).toBe("full");
    expect(deriveFlagRenderMode(true, true, 3, 120, false)).toBe("focused-hover-only");
  });

  it("returns empty sprite ids in OFF mode", () => {
    expect(resolveSpriteNodeIds("off", ["1", "2"], "1", "2")).toEqual(new Set());
  });

  it("caps full-mode sprites and keeps focus/hover in degraded mode", () => {
    const full = resolveSpriteNodeIds("full", ["1", "2", "3"], null, null, 2);
    expect([...full]).toEqual(["1", "2"]);

    const focusedOnly = resolveSpriteNodeIds("focused-hover-only", ["1", "2"], "2", "1");
    expect([...focusedOnly].sort()).toEqual(["1", "2"]);
  });

  it("looks up atlas sprites deterministically", () => {
    const sprite = resolveAtlasSprite(ASSETS, "alpha");
    expect(sprite?.key).toBe("alpha");
    expect(sprite?.asset).toEqual(ASSETS.assets.alpha);
    expect(resolveAtlasSprite(ASSETS, "missing")).toBeNull();
  });
});
