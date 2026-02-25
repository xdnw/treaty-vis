import type {
  LoaderWorkerRequest,
  LoaderWorkerResponse,
  WorkerCommunityTarget,
  WorkerComponentTarget,
  WorkerNetworkLayout,
  WorkerNodeTarget,
  WorkerPulsePoint,
  WorkerQueryState
} from "@/domain/timelapse/workerProtocol";
import {
  buildPulseSeries,
  buildQuerySelectionKey,
  buildTopXMembershipLookup,
  compareEventChronology,
  computeSelectionIndexes as computeSharedSelectionIndexes,
  keyForEvent,
  selectStableCappedActiveEvents
} from "@/domain/timelapse/queryEngine";
import { decode } from "@msgpack/msgpack";

type LoaderWorkerPayload = Extract<LoaderWorkerResponse, { kind: "load"; ok: true }>["payload"];

type WorkerLoadedState = {
  payload: LoaderWorkerPayload;
  allEventIndexes: number[];
  includeFlags: boolean;
};

type WorkerBasePayload = Omit<LoaderWorkerPayload, "allianceFlagsRaw" | "flagAssetsRaw">;

type WorkerEvent = {
  event_id: string;
  event_sequence?: number;
  action: string;
  treaty_type: string;
  source?: string | null;
  timestamp: string;
  from_alliance_id: number;
  to_alliance_id: number;
  from_alliance_name?: string;
  to_alliance_name?: string;
  grounded_from?: boolean;
  grounded_to?: boolean;
  inferred?: boolean;
  noise_filtered?: boolean;
  pair_min_id?: number;
  pair_max_id?: number;
};

type WorkerScoreRanksByDay = Record<string, Record<string, number>>;

const selectionCache = new Map<string, Uint32Array>();
const pulseCache = new Map<string, WorkerPulsePoint[]>();
type WorkerNetworkResult = {
  edgeIndexes: Uint32Array;
  layout: WorkerNetworkLayout;
};

type ComponentSnapshot = {
  componentId: string;
  members: Set<string>;
  anchorX: number;
  anchorY: number;
};

type LayoutSnapshot = {
  components: ComponentSnapshot[];
};

type IncrementalActiveState = {
  selectionSignature: string;
  chronologicalIndexes: number[];
  cursor: number;
  playhead: string | null;
  activeByPair: Map<string, number>;
};

const networkCache = new Map<string, WorkerNetworkResult>();
const networkLayoutSnapshotByKey = new Map<string, LayoutSnapshot>();
const incrementalActiveStateByKey = new Map<string, IncrementalActiveState>();
let loadedState: WorkerLoadedState | null = null;
const TERMINAL_ACTIONS = new Set(["cancelled", "expired", "ended", "terminated", "termination", "inferred_cancelled"]);
const TAU = Math.PI * 2;
const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5));
const NETWORK_REFINEMENT_ITERATIONS = 6;
const BACKWARD_RECOMPUTE_THRESHOLD = 80;
type WorkerScope = {
  addEventListener: (type: "message", listener: (event: MessageEvent<LoaderWorkerRequest>) => void) => void;
  postMessage: (message: unknown, transfer?: Transferable[]) => void;
};
const workerScope = self as unknown as WorkerScope;

async function fetchMsgpack<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }
  const body = await response.arrayBuffer();
  return decode(new Uint8Array(body)) as T;
}

function sortEventsByTimestamp(events: unknown[]): unknown[] {
  return [...events].sort((leftRaw, rightRaw) => {
    const left = leftRaw as { timestamp?: string; event_sequence?: number; event_id?: string };
    const right = rightRaw as { timestamp?: string; event_sequence?: number; event_id?: string };

    const timestampOrder = String(left.timestamp ?? "").localeCompare(String(right.timestamp ?? ""));
    if (timestampOrder !== 0) {
      return timestampOrder;
    }
    const leftSequence = Number.isFinite(left.event_sequence) ? Number(left.event_sequence) : Number.POSITIVE_INFINITY;
    const rightSequence = Number.isFinite(right.event_sequence)
      ? Number(right.event_sequence)
      : Number.POSITIVE_INFINITY;
    if (leftSequence !== rightSequence) {
      return leftSequence - rightSequence;
    }
    return String(left.event_id ?? "").localeCompare(String(right.event_id ?? ""));
  });
}

function addToRecordList(record: Record<string, number[]>, key: string, index: number): void {
  if (!record[key]) {
    record[key] = [];
  }
  record[key].push(index);
}

function buildIndexPayload(events: unknown[]) {
  const byAction: Record<string, number[]> = {};
  const byType: Record<string, number[]> = {};
  const bySource: Record<string, number[]> = {};
  const byAlliance: Record<string, number[]> = {};
  const eventIdToIndex: Record<string, number> = {};
  const allianceMeta = new Map<number, { name: string; count: number }>();

  for (let index = 0; index < events.length; index += 1) {
    const event = events[index] as {
      event_id?: string;
      action?: string;
      treaty_type?: string;
      source?: string;
      from_alliance_id?: number;
      from_alliance_name?: string;
      to_alliance_id?: number;
      to_alliance_name?: string;
      timestamp?: string;
    };

    const action = String(event.action ?? "unknown");
    const treatyType = String(event.treaty_type ?? "unknown");
    const source = String(event.source ?? "unknown");
    const fromId = Number(event.from_alliance_id ?? -1);
    const toId = Number(event.to_alliance_id ?? -1);

    addToRecordList(byAction, action, index);
    addToRecordList(byType, treatyType, index);
    addToRecordList(bySource, source, index);
    addToRecordList(byAlliance, String(fromId), index);
    if (fromId !== toId) {
      addToRecordList(byAlliance, String(toId), index);
    }

    eventIdToIndex[String(event.event_id ?? index)] = index;

    const fromMeta = allianceMeta.get(fromId) ?? {
      name: String(event.from_alliance_name ?? fromId),
      count: 0
    };
    fromMeta.count += 1;
    if (!fromMeta.name || fromMeta.name === String(fromId)) {
      fromMeta.name = String(event.from_alliance_name ?? fromId);
    }
    allianceMeta.set(fromId, fromMeta);

    const toMeta = allianceMeta.get(toId) ?? {
      name: String(event.to_alliance_name ?? toId),
      count: 0
    };
    toMeta.count += 1;
    if (!toMeta.name || toMeta.name === String(toId)) {
      toMeta.name = String(event.to_alliance_name ?? toId);
    }
    allianceMeta.set(toId, toMeta);
  }

  return {
    byAction,
    byType,
    bySource,
    byAlliance,
    eventIdToIndex,
    allActions: Object.keys(byAction).sort((a, b) => a.localeCompare(b)),
    allTypes: Object.keys(byType).sort((a, b) => a.localeCompare(b)),
    allSources: Object.keys(bySource).sort((a, b) => a.localeCompare(b)),
    alliances: [...allianceMeta.entries()]
      .map(([id, meta]) => ({ id, name: meta.name || String(id), count: meta.count }))
      .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)),
    minTimestamp: events.length > 0 ? String((events[0] as { timestamp?: string }).timestamp ?? "") : null,
    maxTimestamp:
      events.length > 0 ? String((events[events.length - 1] as { timestamp?: string }).timestamp ?? "") : null
  };
}

async function loadBasePayload(): Promise<WorkerBasePayload> {
  const [eventsRaw, summaryRaw, flagsRaw, scoreRanksRaw, manifestRaw] = await Promise.all([
    fetchMsgpack<unknown[]>("/data/treaty_changes_reconciled.msgpack"),
    fetchMsgpack<unknown>("/data/treaty_changes_reconciled_summary.msgpack"),
    fetchMsgpack<unknown[]>("/data/treaty_changes_reconciled_flags.msgpack"),
    fetch("/data/alliance_score_ranks_daily.msgpack")
      .then(async (response) => {
        if (!response.ok) {
          return null;
        }
        const body = await response.arrayBuffer();
        return decode(new Uint8Array(body)) as unknown;
      })
      .catch(() => null),
    fetch("/data/manifest.json")
      .then(async (response) => {
        if (!response.ok) {
          return null;
        }
        return (await response.json()) as unknown;
      })
      .catch(() => null)
  ]);

  const sorted = sortEventsByTimestamp(eventsRaw);

  return {
    eventsRaw: sorted,
    indicesRaw: buildIndexPayload(sorted),
    summaryRaw,
    flagsRaw,
    scoreRanksRaw,
    manifestRaw
  };
}

function normalizeScoreRanksByDay(raw: unknown | null): WorkerScoreRanksByDay | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const candidate = raw as { schema_version?: unknown; ranks_by_day?: unknown };
  if (Number(candidate.schema_version) !== 2) {
    return null;
  }
  if (!candidate.ranks_by_day || typeof candidate.ranks_by_day !== "object") {
    return null;
  }

  const source = candidate.ranks_by_day as Record<string, unknown>;

  const normalized: WorkerScoreRanksByDay = {};
  for (const [day, rowRaw] of Object.entries(source)) {
    if (!rowRaw || typeof rowRaw !== "object") {
      continue;
    }

    const row = rowRaw as Record<string, unknown>;
    const dayRanks: Record<string, number> = {};
    for (const [allianceId, rankRaw] of Object.entries(row)) {
      const rank = Number(rankRaw);
      if (!Number.isFinite(rank)) {
        continue;
      }
      const normalizedRank = Math.floor(rank);
      if (normalizedRank <= 0) {
        continue;
      }
      dayRanks[String(allianceId)] = normalizedRank;
    }

    if (Object.keys(dayRanks).length > 0) {
      normalized[day] = dayRanks;
    }
  }

  return Object.keys(normalized).length > 0 ? normalized : null;
}


async function loadOptionalFlagPayload(): Promise<Pick<LoaderWorkerPayload, "allianceFlagsRaw" | "flagAssetsRaw">> {
  const [allianceFlagsRaw, flagAssetsRaw] = await Promise.all([
    fetch("/data/flags.msgpack")
      .then(async (response) => {
        if (!response.ok) {
          console.warn("[timelapse] Optional flags.msgpack fetch failed in worker", response.status);
          return null;
        }
        const body = await response.arrayBuffer();
        return decode(new Uint8Array(body)) as unknown;
      })
      .catch(() => null),
    fetch("/data/flag_assets.msgpack")
      .then(async (response) => {
        if (!response.ok) {
          console.warn("[timelapse] Optional flag_assets.msgpack fetch failed in worker", response.status);
          return null;
        }
        const body = await response.arrayBuffer();
        return decode(new Uint8Array(body)) as unknown;
      })
      .catch(() => null)
  ]);

  return {
    allianceFlagsRaw,
    flagAssetsRaw
  };
}

function selectionKey(query: WorkerQueryState): string {
  return buildQuerySelectionKey(query);
}

function computeSelectionIndexesCached(state: WorkerLoadedState, query: WorkerQueryState): Uint32Array {
  const cacheKey = selectionKey(query);
  const cached = selectionCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const events = state.payload.eventsRaw as WorkerEvent[];
  const indices = {
    ...state.payload.indicesRaw,
    allEventIndexes: state.allEventIndexes
  };

  const topX = query.filters.topXByScore ?? null;
  const ranksByDay = normalizeScoreRanksByDay(state.payload.scoreRanksRaw);
  const topMembershipLookup =
    topX !== null && topX > 0 && ranksByDay
      ? buildTopXMembershipLookup(ranksByDay, topX)
      : null;

  const sortedIndexes = computeSharedSelectionIndexes({
    events,
    indices,
    query,
    topXMembershipLookup: topMembershipLookup
  });

  const result = Uint32Array.from(sortedIndexes);
  selectionCache.set(cacheKey, result);
  if (selectionCache.size > 80) {
    const first = selectionCache.keys().next().value;
    if (first) {
      selectionCache.delete(first);
    }
  }
  return result;
}

function pulseCacheKey(selectionKeyValue: string, maxPoints: number, playhead: string | null): string {
  return `${selectionKeyValue}|pulse:${maxPoints}|${playhead ?? ""}`;
}

function computePulseSeries(
  state: WorkerLoadedState,
  query: WorkerQueryState,
  maxPoints: number,
  playhead: string | null
): WorkerPulsePoint[] {
  const baseKey = selectionKey(query);
  const cacheKey = pulseCacheKey(baseKey, maxPoints, playhead);
  const cached = pulseCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const events = state.payload.eventsRaw as WorkerEvent[];
  const indexes = [...computeSelectionIndexesCached(state, query)];
  const bucketed = buildPulseSeries(events, indexes, maxPoints, playhead);

  pulseCache.set(cacheKey, bucketed);
  if (pulseCache.size > 80) {
    const first = pulseCache.keys().next().value;
    if (first) {
      pulseCache.delete(first);
    }
  }
  return bucketed;
}

function networkCacheKey(query: WorkerQueryState, playhead: string | null, maxEdges: number): string {
  return `${selectionKey(query)}|network:${playhead ?? ""}|${maxEdges}`;
}

function networkTemporalKey(query: WorkerQueryState, maxEdges: number): string {
  return `${selectionKey(query)}|network:temporal|${maxEdges}`;
}

function keyForActivePair(event: WorkerEvent): string {
  return keyForEvent(event);
}

function hashId(id: string): number {
  let hash = 2166136261;
  for (let index = 0; index < id.length; index += 1) {
    hash ^= id.charCodeAt(index);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return hash >>> 0;
}

function computeConnectedComponents(
  nodeIds: string[],
  adjacencyByNodeId: Map<string, Set<string>>
): Array<{ nodeIds: string[] }> {
  const visited = new Set<string>();
  const components: Array<{ nodeIds: string[] }> = [];

  const sortedNodeIds = [...nodeIds].sort((left, right) => left.localeCompare(right));
  for (const rootId of sortedNodeIds) {
    if (visited.has(rootId)) {
      continue;
    }

    const queue = [rootId];
    const members: string[] = [];
    visited.add(rootId);

    while (queue.length > 0) {
      const current = queue.shift()!;
      members.push(current);
      const neighbors = adjacencyByNodeId.get(current);
      if (!neighbors) {
        continue;
      }
      const sortedNeighbors = [...neighbors].sort((left, right) => left.localeCompare(right));
      for (const neighbor of sortedNeighbors) {
        if (visited.has(neighbor)) {
          continue;
        }
        visited.add(neighbor);
        queue.push(neighbor);
      }
    }

    members.sort((left, right) => left.localeCompare(right));
    components.push({ nodeIds: members });
  }

  components.sort((left, right) => right.nodeIds.length - left.nodeIds.length || left.nodeIds[0].localeCompare(right.nodeIds[0]));
  return components;
}

function computeCommunities(componentNodeIds: string[], adjacencyByNodeId: Map<string, Set<string>>): string[][] {
  const labels = new Map<string, string>();
  const sortedNodeIds = [...componentNodeIds].sort((left, right) => left.localeCompare(right));
  for (const nodeId of sortedNodeIds) {
    labels.set(nodeId, nodeId);
  }

  const maxIterations = Math.max(2, Math.min(10, Math.ceil(Math.log2(componentNodeIds.length + 1)) + 2));
  for (let iteration = 0; iteration < maxIterations; iteration += 1) {
    let changed = false;
    for (const nodeId of sortedNodeIds) {
      const neighbors = adjacencyByNodeId.get(nodeId);
      if (!neighbors || neighbors.size === 0) {
        continue;
      }

      const frequencies = new Map<string, number>();
      for (const neighborId of neighbors) {
        const label = labels.get(neighborId) ?? neighborId;
        frequencies.set(label, (frequencies.get(label) ?? 0) + 1);
      }

      let bestLabel = labels.get(nodeId) ?? nodeId;
      let bestScore = -1;
      for (const [label, score] of frequencies.entries()) {
        if (score > bestScore || (score === bestScore && label.localeCompare(bestLabel) < 0)) {
          bestLabel = label;
          bestScore = score;
        }
      }

      if (bestLabel !== labels.get(nodeId)) {
        labels.set(nodeId, bestLabel);
        changed = true;
      }
    }

    if (!changed) {
      break;
    }
  }

  const byLabel = new Map<string, string[]>();
  for (const nodeId of sortedNodeIds) {
    const label = labels.get(nodeId) ?? nodeId;
    const members = byLabel.get(label) ?? [];
    members.push(nodeId);
    byLabel.set(label, members);
  }

  const communities = [...byLabel.values()].map((members) => members.sort((left, right) => left.localeCompare(right)));
  communities.sort((left, right) => right.length - left.length || left[0].localeCompare(right[0]));
  return communities;
}

function buildComponentId(componentNodeIds: string[]): string {
  return `component:${componentNodeIds[0] ?? "none"}:${componentNodeIds.length}`;
}

function componentRadius(weight: number): number {
  return 12 + Math.sqrt(Math.max(weight, 1)) * 4.5;
}

function isAnchorPlacementFree(
  placed: Array<{ x: number; y: number; radius: number }>,
  candidateX: number,
  candidateY: number,
  candidateRadius: number
): boolean {
  for (const existing of placed) {
    const dx = candidateX - existing.x;
    const dy = candidateY - existing.y;
    const minDistance = existing.radius + candidateRadius + 10;
    if (dx * dx + dy * dy < minDistance * minDistance) {
      return false;
    }
  }
  return true;
}

function placeAnchorNearPreferred(
  preferred: { x: number; y: number },
  radius: number,
  placed: Array<{ x: number; y: number; radius: number }>,
  seedOffset: number
): { x: number; y: number } {
  if (isAnchorPlacementFree(placed, preferred.x, preferred.y, radius)) {
    return preferred;
  }

  const ringCount = 18;
  for (let ring = 1; ring <= 28; ring += 1) {
    const ringDistance = ring * (radius * 0.9 + 8);
    for (let step = 0; step < ringCount; step += 1) {
      const angle = ((step + seedOffset) / ringCount) * TAU;
      const candidateX = preferred.x + Math.cos(angle) * ringDistance;
      const candidateY = preferred.y + Math.sin(angle) * ringDistance;
      if (isAnchorPlacementFree(placed, candidateX, candidateY, radius)) {
        return { x: candidateX, y: candidateY };
      }
    }
  }

  const fallbackRadius = 40 + seedOffset * 11;
  const fallbackAngle = seedOffset * GOLDEN_ANGLE;
  return {
    x: preferred.x + Math.cos(fallbackAngle) * fallbackRadius,
    y: preferred.y + Math.sin(fallbackAngle) * fallbackRadius
  };
}

function resolveComponentAnchors(
  components: Array<{ componentId: string; nodeIds: string[]; weight: number }>,
  previousSnapshot: LayoutSnapshot | undefined
): Map<string, { x: number; y: number }> {
  const anchors = new Map<string, { x: number; y: number; radius: number }>();
  const claimedPrevious = new Set<string>();
  const placedAnchors: Array<{ x: number; y: number; radius: number }> = [];
  const componentsSorted = [...components].sort(
    (left, right) => right.weight - left.weight || left.componentId.localeCompare(right.componentId)
  );

  for (const component of componentsSorted) {
    const targetRadius = componentRadius(component.weight);
    const deterministicSeed = hashId(component.componentId) % 31;
    let preferred = { x: 0, y: 0 };
    let hasPreferred = false;

    if (!previousSnapshot || previousSnapshot.components.length === 0) {
      const ringRadius = 24 + Math.sqrt(component.weight) * 7;
      const ringAngle = deterministicSeed * GOLDEN_ANGLE;
      preferred = {
        x: Math.cos(ringAngle) * ringRadius,
        y: Math.sin(ringAngle) * ringRadius
      };
      hasPreferred = true;
    } else {
      const currentMembers = new Set(component.nodeIds);
      let best: ComponentSnapshot | null = null;
      let bestOverlap = -1;
      let bestOverlapRatio = -1;

      for (const previous of previousSnapshot.components) {
        if (claimedPrevious.has(previous.componentId)) {
          continue;
        }

        let overlap = 0;
        for (const nodeId of currentMembers) {
          if (previous.members.has(nodeId)) {
            overlap += 1;
          }
        }

        const overlapRatio = overlap / Math.max(1, Math.max(currentMembers.size, previous.members.size));
        if (
          overlap > bestOverlap ||
          (overlap === bestOverlap && overlapRatio > bestOverlapRatio) ||
          (overlap === bestOverlap && overlapRatio === bestOverlapRatio && best !== null && previous.componentId.localeCompare(best.componentId) < 0)
        ) {
          best = previous;
          bestOverlap = overlap;
          bestOverlapRatio = overlapRatio;
        }
      }

      if (best && bestOverlap > 0) {
        preferred = { x: best.anchorX, y: best.anchorY };
        hasPreferred = true;
        claimedPrevious.add(best.componentId);
      }
    }

    if (!hasPreferred) {
      const ringRadius = 24 + Math.sqrt(component.weight) * 7;
      const ringAngle = deterministicSeed * GOLDEN_ANGLE;
      preferred = {
        x: Math.cos(ringAngle) * ringRadius,
        y: Math.sin(ringAngle) * ringRadius
      };
    }

    const packed = placeAnchorNearPreferred(preferred, targetRadius, placedAnchors, deterministicSeed);
    const placed = { x: packed.x, y: packed.y, radius: targetRadius };
    placedAnchors.push(placed);
    anchors.set(component.componentId, placed);
  }

  return new Map([...anchors.entries()].map(([componentId, anchor]) => [componentId, { x: anchor.x, y: anchor.y }]));
}

function refineCommunityTargets(params: {
  communityNodeIds: string[];
  adjacencyByNodeId: Map<string, Set<string>>;
  anchorX: number;
  anchorY: number;
  componentId: string;
  communityId: string;
}): WorkerNodeTarget[] {
  const { communityNodeIds, adjacencyByNodeId, anchorX, anchorY, componentId, communityId } = params;
  const sortedNodeIds = [...communityNodeIds].sort((left, right) => left.localeCompare(right));
  const nodeIdSet = new Set(sortedNodeIds);
  const positions = new Map<string, { x: number; y: number }>();
  const seedRadius = Math.max(8, Math.sqrt(sortedNodeIds.length) * 3.2);

  for (let index = 0; index < sortedNodeIds.length; index += 1) {
    const nodeId = sortedNodeIds[index];
    const seed = hashId(`${communityId}:${nodeId}`);
    const angleOffset = ((seed % 360) / 360) * TAU;
    const angle = ((index / Math.max(sortedNodeIds.length, 1)) * TAU + angleOffset) % TAU;
    const radius = seedRadius * (0.55 + (((seed >>> 8) % 1000) / 1000) * 0.7);
    positions.set(nodeId, {
      x: anchorX + Math.cos(angle) * radius,
      y: anchorY + Math.sin(angle) * radius
    });
  }

  const gridSize = 12;
  const neighborAttract = 0.2;
  const anchorPull = 0.04;
  const repulsionStrength = 1.15;
  const maxStep = 4.2;
  const repulsionCutoff = Math.max(18, seedRadius * 1.8);
  const minNodeGap = 2.6;

  for (let iteration = 0; iteration < NETWORK_REFINEMENT_ITERATIONS; iteration += 1) {
    const nextPositions = new Map<string, { x: number; y: number }>();
    const bucket = new Map<string, string[]>();

    for (const nodeId of sortedNodeIds) {
      const current = positions.get(nodeId)!;
      const bucketX = Math.floor(current.x / gridSize);
      const bucketY = Math.floor(current.y / gridSize);
      const key = `${bucketX}:${bucketY}`;
      const row = bucket.get(key) ?? [];
      row.push(nodeId);
      bucket.set(key, row);
    }

    for (const nodeId of sortedNodeIds) {
      const current = positions.get(nodeId)!;
      const neighbors = adjacencyByNodeId.get(nodeId);
      let attractX = 0;
      let attractY = 0;
      let attractCount = 0;

      if (neighbors && neighbors.size > 0) {
        for (const neighborId of neighbors) {
          if (!nodeIdSet.has(neighborId)) {
            continue;
          }
          const neighborPosition = positions.get(neighborId);
          if (!neighborPosition) {
            continue;
          }
          attractX += neighborPosition.x;
          attractY += neighborPosition.y;
          attractCount += 1;
        }
      }

      let velocityX = 0;
      let velocityY = 0;
      if (attractCount > 0) {
        const meanX = attractX / attractCount;
        const meanY = attractY / attractCount;
        velocityX += (meanX - current.x) * neighborAttract;
        velocityY += (meanY - current.y) * neighborAttract;
      }
      velocityX += (anchorX - current.x) * anchorPull;
      velocityY += (anchorY - current.y) * anchorPull;

      const bucketX = Math.floor(current.x / gridSize);
      const bucketY = Math.floor(current.y / gridSize);
      for (let offsetX = -1; offsetX <= 1; offsetX += 1) {
        for (let offsetY = -1; offsetY <= 1; offsetY += 1) {
          const key = `${bucketX + offsetX}:${bucketY + offsetY}`;
          const candidates = bucket.get(key);
          if (!candidates) {
            continue;
          }
          for (const candidateId of candidates) {
            if (candidateId === nodeId) {
              continue;
            }
            const other = positions.get(candidateId)!;
            const dx = current.x - other.x;
            const dy = current.y - other.y;
            const d2 = dx * dx + dy * dy;
            if (d2 <= 0.0001 || d2 > repulsionCutoff * repulsionCutoff) {
              continue;
            }
            const distance = Math.sqrt(d2);
            const scale = ((repulsionCutoff - distance) / repulsionCutoff) * repulsionStrength;
            velocityX += (dx / distance) * scale;
            velocityY += (dy / distance) * scale;

            if (distance < minNodeGap) {
              const overlap = (minNodeGap - distance) / minNodeGap;
              velocityX += (dx / distance) * overlap * 0.9;
              velocityY += (dy / distance) * overlap * 0.9;
            }
          }
        }
      }

      const speed = Math.sqrt(velocityX * velocityX + velocityY * velocityY);
      if (speed > maxStep && speed > 0) {
        velocityX = (velocityX / speed) * maxStep;
        velocityY = (velocityY / speed) * maxStep;
      }

      nextPositions.set(nodeId, {
        x: current.x + velocityX,
        y: current.y + velocityY
      });
    }

    positions.clear();
    for (const [nodeId, next] of nextPositions.entries()) {
      positions.set(nodeId, next);
    }
  }

  const refined: WorkerNodeTarget[] = [];
  for (const nodeId of sortedNodeIds) {
    const finalPosition = positions.get(nodeId)!;
    const neighbors = adjacencyByNodeId.get(nodeId);
    let neighborX = 0;
    let neighborY = 0;
    let count = 0;
    if (neighbors && neighbors.size > 0) {
      for (const neighborId of neighbors) {
        if (!nodeIdSet.has(neighborId)) {
          continue;
        }
        const neighborPosition = positions.get(neighborId);
        if (!neighborPosition) {
          continue;
        }
        neighborX += neighborPosition.x;
        neighborY += neighborPosition.y;
        count += 1;
      }
    }

    if (count === 0) {
      neighborX = finalPosition.x;
      neighborY = finalPosition.y;
    } else {
      neighborX /= count;
      neighborY /= count;
    }

    refined.push({
      nodeId,
      componentId,
      communityId,
      targetX: finalPosition.x,
      targetY: finalPosition.y,
      neighborX,
      neighborY,
      anchorX,
      anchorY
    });
  }

  return refined;
}

function buildNetworkLayout(nodeIds: string[], adjacencyByNodeId: Map<string, Set<string>>, temporalKey: string): WorkerNetworkLayout {
  const componentsRaw = computeConnectedComponents(nodeIds, adjacencyByNodeId);
  const components = componentsRaw.map((component) => ({
    componentId: buildComponentId(component.nodeIds),
    nodeIds: component.nodeIds,
    weight: component.nodeIds.length
  }));

  const previousSnapshot = networkLayoutSnapshotByKey.get(temporalKey);
  const componentAnchors = resolveComponentAnchors(components, previousSnapshot);

  const componentTargets: WorkerComponentTarget[] = [];
  const communityTargets: WorkerCommunityTarget[] = [];
  const nodeTargets: WorkerNodeTarget[] = [];

  const nextSnapshot: LayoutSnapshot = { components: [] };

  for (const component of components) {
    const componentAnchor = componentAnchors.get(component.componentId) ?? { x: 0, y: 0 };
    componentTargets.push({
      componentId: component.componentId,
      nodeIds: component.nodeIds,
      anchorX: componentAnchor.x,
      anchorY: componentAnchor.y
    });

    nextSnapshot.components.push({
      componentId: component.componentId,
      members: new Set(component.nodeIds),
      anchorX: componentAnchor.x,
      anchorY: componentAnchor.y
    });

    const communities = computeCommunities(component.nodeIds, adjacencyByNodeId);
    const communityAnchors = new Map<string, { x: number; y: number }>();
    const communityRadii: number[] = [];
    for (const communityNodeIds of communities) {
      communityRadii.push(7 + Math.sqrt(communityNodeIds.length) * 3.6);
    }
    const largestCommunityRadius = communityRadii.length > 0 ? Math.max(...communityRadii) : 0;
    const communityPlacementRadius = 12 + largestCommunityRadius * 1.4 + Math.sqrt(communities.length) * 7;

    for (let index = 0; index < communities.length; index += 1) {
      const communityNodeIds = communities[index];
      const communityId = `${component.componentId}:community:${index}:${communityNodeIds[0] ?? "none"}`;
      const angleSeed = hashId(`${communityId}:angle`) % 360;
      const angle = communities.length <= 1 ? 0 : (((index * 53 + angleSeed) % 360) / 360) * TAU;
      const anchor = {
        x: componentAnchor.x + Math.cos(angle) * communityPlacementRadius,
        y: componentAnchor.y + Math.sin(angle) * communityPlacementRadius
      };
      communityAnchors.set(communityId, anchor);
      communityTargets.push({
        communityId,
        componentId: component.componentId,
        nodeIds: communityNodeIds,
        anchorX: anchor.x,
        anchorY: anchor.y
      });
    }

    for (const community of communityTargets.filter((entry) => entry.componentId === component.componentId)) {
      const anchor = communityAnchors.get(community.communityId)!;
      const refined = refineCommunityTargets({
        communityNodeIds: community.nodeIds,
        adjacencyByNodeId,
        anchorX: anchor.x,
        anchorY: anchor.y,
        componentId: component.componentId,
        communityId: community.communityId
      });
      for (const nodeTarget of refined) {
        nodeTargets.push(nodeTarget);
      }
    }
  }

  networkLayoutSnapshotByKey.set(temporalKey, nextSnapshot);
  if (networkLayoutSnapshotByKey.size > 40) {
    const first = networkLayoutSnapshotByKey.keys().next().value;
    if (first) {
      networkLayoutSnapshotByKey.delete(first);
    }
  }

  nodeTargets.sort((left, right) => left.nodeId.localeCompare(right.nodeId));
  return {
    components: componentTargets,
    communities: communityTargets,
    nodeTargets
  };
}

function computeNetworkEdgeEventIndexes(
  state: WorkerLoadedState,
  query: WorkerQueryState,
  playhead: string | null,
  maxEdges: number
): WorkerNetworkResult {
  const cacheKey = networkCacheKey(query, playhead, maxEdges);
  const cached = networkCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const events = state.payload.eventsRaw as WorkerEvent[];
  const selectionSignature = `${selectionKey(query)}|network|${maxEdges}`;
  const temporalKey = networkTemporalKey(query, maxEdges);
  const previous = incrementalActiveStateByKey.get(temporalKey);

  let chronologicalIndexes: number[];
  if (previous && previous.selectionSignature === selectionSignature) {
    chronologicalIndexes = previous.chronologicalIndexes;
  } else {
    chronologicalIndexes = [...computeSelectionIndexesCached(state, query)];
    chronologicalIndexes.sort((leftIndex, rightIndex) => compareEventChronology(events[leftIndex], events[rightIndex]));
  }

  const resolveCursor = (targetPlayhead: string | null): number => {
    if (!targetPlayhead) {
      return chronologicalIndexes.length;
    }
    let lo = 0;
    let hi = chronologicalIndexes.length;
    while (lo < hi) {
      const mid = Math.floor((lo + hi) / 2);
      const event = events[chronologicalIndexes[mid]];
      if (event.timestamp <= targetPlayhead) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  };

  const targetCursor = resolveCursor(playhead);
  let active = new Map<string, number>();

  const canIncrementallyAdvance =
    previous &&
    previous.selectionSignature === selectionSignature &&
    previous.cursor <= targetCursor &&
    (previous.playhead === null || playhead === null || previous.playhead <= playhead);

  const canIncrementallyRewind =
    previous &&
    previous.selectionSignature === selectionSignature &&
    previous.cursor > targetCursor &&
    previous.cursor - targetCursor <= BACKWARD_RECOMPUTE_THRESHOLD;

  if (canIncrementallyAdvance && previous) {
    active = new Map(previous.activeByPair);
    for (let cursor = previous.cursor; cursor < targetCursor; cursor += 1) {
      const eventIndex = chronologicalIndexes[cursor];
      const event = events[eventIndex];
      const key = keyForActivePair(event);
      if (event.action === "signed") {
        active.set(key, eventIndex);
      } else if (TERMINAL_ACTIONS.has(event.action)) {
        active.delete(key);
      }
    }
  } else if (canIncrementallyRewind && previous) {
    // Small rewind: rebuild only to the requested cursor window.
    for (let cursor = 0; cursor < targetCursor; cursor += 1) {
      const eventIndex = chronologicalIndexes[cursor];
      const event = events[eventIndex];
      const key = keyForActivePair(event);
      if (event.action === "signed") {
        active.set(key, eventIndex);
      } else if (TERMINAL_ACTIONS.has(event.action)) {
        active.delete(key);
      }
    }
  } else {
    for (let cursor = 0; cursor < targetCursor; cursor += 1) {
      const eventIndex = chronologicalIndexes[cursor];
      const event = events[eventIndex];
      const key = keyForActivePair(event);
      if (event.action === "signed") {
        active.set(key, eventIndex);
      } else if (TERMINAL_ACTIONS.has(event.action)) {
        active.delete(key);
      }
    }
  }

  incrementalActiveStateByKey.set(temporalKey, {
    selectionSignature,
    chronologicalIndexes,
    cursor: targetCursor,
    playhead,
    activeByPair: active
  });
  if (incrementalActiveStateByKey.size > 40) {
    const first = incrementalActiveStateByKey.keys().next().value;
    if (first) {
      incrementalActiveStateByKey.delete(first);
    }
  }

  const activeIndexes = [...active.values()];
  const keepCount = Math.max(maxEdges, 200);
  const reducedEvents = selectStableCappedActiveEvents(activeIndexes.map((index) => events[index]), keepCount);

  const adjacencyByNodeId = new Map<string, Set<string>>();
  const nodeIdSet = new Set<string>();
  for (const event of reducedEvents) {
    const left = String(event.from_alliance_id);
    const right = String(event.to_alliance_id);
    nodeIdSet.add(left);
    nodeIdSet.add(right);

    const leftNeighbors = adjacencyByNodeId.get(left) ?? new Set<string>();
    leftNeighbors.add(right);
    adjacencyByNodeId.set(left, leftNeighbors);

    const rightNeighbors = adjacencyByNodeId.get(right) ?? new Set<string>();
    rightNeighbors.add(left);
    adjacencyByNodeId.set(right, rightNeighbors);
  }

  for (const nodeId of nodeIdSet) {
    if (!adjacencyByNodeId.has(nodeId)) {
      adjacencyByNodeId.set(nodeId, new Set());
    }
  }

  const reduced = reducedEvents.map((event) => state.payload.indicesRaw.eventIdToIndex[event.event_id]);
  const edgeIndexes = Uint32Array.from(reduced);
  const layout = buildNetworkLayout([...nodeIdSet], adjacencyByNodeId, temporalKey);

  const result: WorkerNetworkResult = {
    edgeIndexes,
    layout
  };

  networkCache.set(cacheKey, result);
  if (networkCache.size > 80) {
    const first = networkCache.keys().next().value;
    if (first) {
      networkCache.delete(first);
    }
  }

  return result;
}

async function ensureLoadedState(includeFlags: boolean): Promise<WorkerLoadedState> {
  if (loadedState && (!includeFlags || loadedState.includeFlags)) {
    return loadedState;
  }

  if (!loadedState) {
    const basePayload = await loadBasePayload();
    const payload: LoaderWorkerPayload = {
      ...basePayload,
      allianceFlagsRaw: null,
      flagAssetsRaw: null
    };
    const allEventIndexes = new Array(payload.eventsRaw.length);
    for (let index = 0; index < payload.eventsRaw.length; index += 1) {
      allEventIndexes[index] = index;
    }
    loadedState = { payload, allEventIndexes, includeFlags: false };
    selectionCache.clear();
    pulseCache.clear();
    networkCache.clear();
    networkLayoutSnapshotByKey.clear();
    incrementalActiveStateByKey.clear();
  }

  if (includeFlags && loadedState && !loadedState.includeFlags) {
    const optionalPayload = await loadOptionalFlagPayload();
    loadedState = {
      ...loadedState,
      includeFlags: true,
      payload: {
        ...loadedState.payload,
        ...optionalPayload
      }
    };
  }

  return loadedState;
}

function payloadForMode(
  payload: LoaderWorkerPayload,
  includeFlags: boolean
): LoaderWorkerPayload {
  if (includeFlags) {
    return payload;
  }

  return {
    ...payload,
    allianceFlagsRaw: null,
    flagAssetsRaw: null
  };
}

function sendWorkerFailure(
  response:
    | { kind: "load"; ok: false; error: string }
    | { kind: "select"; ok: false; requestId: number; error: string }
    | { kind: "pulse"; ok: false; requestId: number; error: string }
    | { kind: "network"; ok: false; requestId: number; error: string }
): void {
  workerScope.postMessage(response);
}

function toWorkerError(reason: unknown, fallbackMessage: string): string {
  return reason instanceof Error && reason.message ? reason.message : fallbackMessage;
}

workerScope.addEventListener("message", async (event: MessageEvent<LoaderWorkerRequest>) => {
  const request = event.data;

  if (request.kind === "load") {
    try {
      const state = await ensureLoadedState(request.includeFlags);
      const response: LoaderWorkerResponse = {
        kind: "load",
        ok: true,
        payload: payloadForMode(state.payload, request.includeFlags)
      };
      workerScope.postMessage(response);
    } catch (reason) {
      sendWorkerFailure({
        kind: "load",
        ok: false,
        error: toWorkerError(reason, "Unknown loader worker error")
      });
    }
    return;
  }

  if (request.kind === "select") {
    try {
      const state = await ensureLoadedState(request.query.filters.showFlags);
      const indexes = computeSelectionIndexesCached(state, request.query);
      const indexesBuffer = new Uint32Array(indexes).buffer;
      const response: LoaderWorkerResponse = {
        kind: "select",
        ok: true,
        requestId: request.requestId,
        indexesBuffer,
        length: indexes.length
      };
      workerScope.postMessage(response, [indexesBuffer]);
    } catch (reason) {
      sendWorkerFailure({
        kind: "select",
        ok: false,
        requestId: request.requestId,
        error: toWorkerError(reason, "Unknown selection worker error")
      });
    }
    return;
  }

  if (request.kind === "network") {
    try {
      const startedAt = performance.now();
      const state = await ensureLoadedState(request.query.filters.showFlags);
      const network = computeNetworkEdgeEventIndexes(state, request.query, request.playhead, request.maxEdges);
      const finishedAt = performance.now();
      const edgeIndexesBuffer = new Uint32Array(network.edgeIndexes).buffer;
      const response: LoaderWorkerResponse = {
        kind: "network",
        ok: true,
        requestId: request.requestId,
        startedAt,
        finishedAt,
        edgeIndexesBuffer,
        length: network.edgeIndexes.length,
        layout: network.layout
      };
      workerScope.postMessage(response, [edgeIndexesBuffer]);
    } catch (reason) {
      sendWorkerFailure({
        kind: "network",
        ok: false,
        requestId: request.requestId,
        error: toWorkerError(reason, "Unknown network worker error")
      });
    }
    return;
  }

  try {
    const state = await ensureLoadedState(request.query.filters.showFlags);
    const pulse = computePulseSeries(state, request.query, request.maxPoints, request.playhead);
    const signed = new Uint32Array(pulse.length);
    const terminal = new Uint32Array(pulse.length);
    const inferred = new Uint32Array(pulse.length);
    const days = new Array<string>(pulse.length);

    for (let index = 0; index < pulse.length; index += 1) {
      const point = pulse[index];
      days[index] = point.day;
      signed[index] = point.signed;
      terminal[index] = point.terminal;
      inferred[index] = point.inferred;
    }

    const response: LoaderWorkerResponse = {
      kind: "pulse",
      ok: true,
      requestId: request.requestId,
      days,
      signedBuffer: signed.buffer,
      terminalBuffer: terminal.buffer,
      inferredBuffer: inferred.buffer,
      length: pulse.length
    };
    workerScope.postMessage(response, [response.signedBuffer, response.terminalBuffer, response.inferredBuffer]);
  } catch (reason) {
    sendWorkerFailure({
      kind: "pulse",
      ok: false,
      requestId: request.requestId,
      error: toWorkerError(reason, "Unknown pulse worker error")
    });
  }
});
