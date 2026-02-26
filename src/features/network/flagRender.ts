import type { FlagAssetEntry, FlagAssetsPayload } from "@/domain/timelapse/schema";

export const FLAG_FULL_MAX_CAMERA_RATIO = 1.8;
export const FLAG_FULL_MAX_NODES = 300;
export const FLAG_MAX_SPRITES = 220;
export const FLAG_PRESSURE_REFRESH_MS = 24;
export const FLAG_PRESSURE_BUILD_MS = 28;

export type FlagRenderMode = "off" | "full" | "bounded-full" | "focused-hover-only";
export type FlagPressureLevel = "none" | "elevated" | "critical";

export type AtlasSpriteLookup = {
  key: string;
  asset: FlagAssetEntry;
};

export function deriveFlagRenderMode(
  showFlags: boolean,
  hasFlagAssets: boolean,
  cameraRatio: number,
  nodeCount: number,
  framePressureLevel: FlagPressureLevel
): FlagRenderMode {
  if (!showFlags || !hasFlagAssets) {
    return "off";
  }

  if (framePressureLevel === "critical") {
    return "focused-hover-only";
  }

  if (framePressureLevel === "none" && cameraRatio <= FLAG_FULL_MAX_CAMERA_RATIO && nodeCount <= FLAG_FULL_MAX_NODES) {
    return "full";
  }

  return "bounded-full";
}

function stableHashKey(value: string): number {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return hash >>> 0;
}

type ResolveSpriteOptions = {
  visibleNodeIds?: ReadonlySet<string> | null;
  importanceByNodeId?: ReadonlyMap<string, number> | Record<string, number> | null;
};

function resolveImportance(
  importanceByNodeId: ReadonlyMap<string, number> | Record<string, number> | null | undefined,
  nodeId: string
): number {
  if (!importanceByNodeId) {
    return 0;
  }
  const mapLike = importanceByNodeId as ReadonlyMap<string, number>;
  if (typeof mapLike.get === "function") {
    const value = mapLike.get(nodeId);
    return typeof value === "number" && Number.isFinite(value) ? value : 0;
  }
  const recordLike = importanceByNodeId as Record<string, number>;
  const value = recordLike[nodeId];
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
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

export function deriveFlagSpriteCapFromLodBudget(maxEdges: number): number {
  if (!Number.isFinite(maxEdges)) {
    return Number.MAX_SAFE_INTEGER;
  }

  const normalized = Math.floor(maxEdges);
  if (normalized <= 0) {
    return 0;
  }

  return normalized;
}

export function resolveSpriteNodeIds(
  mode: FlagRenderMode,
  nodeIds: string[],
  focusedAllianceId: string | null,
  hoveredAllianceId: string | null,
  spriteCap = FLAG_MAX_SPRITES,
  options?: ResolveSpriteOptions
): Set<string> {
  if (mode === "off") {
    return new Set();
  }

  const nodeIdSet = new Set(nodeIds);
  const forcedIds: string[] = [];
  if (focusedAllianceId && nodeIdSet.has(focusedAllianceId)) {
    forcedIds.push(focusedAllianceId);
  }
  if (hoveredAllianceId && hoveredAllianceId !== focusedAllianceId && nodeIdSet.has(hoveredAllianceId)) {
    forcedIds.push(hoveredAllianceId);
  }

  if (mode === "focused-hover-only") {
    return new Set(forcedIds);
  }

  const cap = Math.max(0, spriteCap);
  if (cap === 0) {
    return new Set(forcedIds);
  }

  const visibleNodeIds = options?.visibleNodeIds ?? null;
  const importanceByNodeId = options?.importanceByNodeId ?? null;
  const selected = new Set<string>(forcedIds);
  if (selected.size >= cap && forcedIds.length > 0) {
    return selected;
  }

  const candidates = nodeIds
    .filter((nodeId) => !selected.has(nodeId))
    .map((nodeId) => ({
      nodeId,
      visibleRank: visibleNodeIds?.has(nodeId) ? 0 : 1,
      importance: resolveImportance(importanceByNodeId, nodeId),
      hash: stableHashKey(nodeId)
    }));

  candidates.sort((left, right) => {
    if (left.visibleRank !== right.visibleRank) {
      return left.visibleRank - right.visibleRank;
    }
    if (left.importance !== right.importance) {
      return right.importance - left.importance;
    }
    if (left.hash !== right.hash) {
      return left.hash - right.hash;
    }
    return left.nodeId.localeCompare(right.nodeId);
  });

  for (const candidate of candidates) {
    if (selected.size >= cap) {
      break;
    }
    selected.add(candidate.nodeId);
  }

  return selected;
}
