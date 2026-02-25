import type { FlagAssetEntry, FlagAssetsPayload } from "@/domain/timelapse/schema";

export const FLAG_FULL_MAX_CAMERA_RATIO = 1.8;
export const FLAG_FULL_MAX_NODES = 300;
export const FLAG_MAX_SPRITES = 220;
export const FLAG_PRESSURE_REFRESH_MS = 24;
export const FLAG_PRESSURE_BUILD_MS = 28;

export type FlagRenderMode = "off" | "full" | "focused-hover-only";

export type AtlasSpriteLookup = {
  key: string;
  asset: FlagAssetEntry;
};

export function deriveFlagRenderMode(
  showFlags: boolean,
  hasFlagAssets: boolean,
  cameraRatio: number,
  nodeCount: number,
  framePressure: boolean
): FlagRenderMode {
  if (!showFlags || !hasFlagAssets) {
    return "off";
  }

  if (framePressure) {
    return "focused-hover-only";
  }

  if (cameraRatio <= FLAG_FULL_MAX_CAMERA_RATIO && nodeCount <= FLAG_FULL_MAX_NODES) {
    return "full";
  }

  return "focused-hover-only";
}

export function resolveAtlasSprite(
  flagAssetsPayload: FlagAssetsPayload | null,
  flagKey: string | null | undefined
): AtlasSpriteLookup | null {
  if (!flagAssetsPayload || !flagKey) {
    return null;
  }
  const asset = flagAssetsPayload.assets[flagKey];
  if (!asset) {
    return null;
  }
  return { key: flagKey, asset };
}

export function resolveSpriteNodeIds(
  mode: FlagRenderMode,
  nodeIds: string[],
  focusedAllianceId: string | null,
  hoveredAllianceId: string | null,
  spriteCap = FLAG_MAX_SPRITES
): Set<string> {
  if (mode === "off") {
    return new Set();
  }

  if (mode === "focused-hover-only") {
    const ids = new Set<string>();
    if (focusedAllianceId) {
      ids.add(focusedAllianceId);
    }
    if (hoveredAllianceId) {
      ids.add(hoveredAllianceId);
    }
    return ids;
  }

  return new Set(nodeIds.slice(0, Math.max(0, spriteCap)));
}
