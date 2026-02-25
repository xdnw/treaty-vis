import type {
  LoaderWorkerRequest,
  LoaderWorkerResponse,
  WorkerNetworkLayout,
  WorkerPulsePoint,
  WorkerQueryState
} from "@/domain/timelapse/workerProtocol";
import { runNetworkLayoutStrategy } from "@/domain/timelapse/networkLayout/NetworkLayoutDispatcher";
import type { NetworkLayoutStrategy, NetworkLayoutStrategyConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";
import { treatyFrameIndexV1Schema, type TreatyFrameIndexV1 } from "@/domain/timelapse/schema";
import { dataAssetPath } from "@/lib/assetPaths";
import {
  buildPulseSeries,
  buildQueryStructuralKey,
  buildTopXMembershipLookup,
  compareEventChronology,
  computeSelectionIndexes as computeSharedSelectionIndexes,
  keyForEvent,
  selectStableCappedActiveEvents
} from "@/domain/timelapse/queryEngine";
import { resolveScoreDay } from "@/domain/timelapse/scoreDay";
import { decode } from "@msgpack/msgpack";

type LoaderWorkerPayload = Extract<LoaderWorkerResponse, { kind: "load"; ok: true }>["payload"];

type WorkerLoadedState = {
  payload: LoaderWorkerPayload;
  allEventIndexes: number[];
  includeFlags: boolean;
  scoreRanksByDay: WorkerScoreRanksByDay | null;
  frameIndex: WorkerFrameIndexRuntime | null;
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

type WorkerFrameIndexRuntime = {
  payload: TreatyFrameIndexV1;
  edgeIdByEventIndex: Map<number, number>;
  dayIndexByDayKey: Map<string, number>;
};

const selectionCache = new Map<string, Uint32Array>();
const pulseCache = new Map<string, WorkerPulsePoint[]>();
type WorkerNetworkResult = {
  edgeIndexes: Uint32Array;
  layout: WorkerNetworkLayout;
};

type IncrementalActiveState = {
  selectionSignature: string;
  chronologicalIndexes: number[];
  cursor: number;
  playhead: string | null;
  activeByPair: Map<string, number>;
};

const networkCache = new Map<string, WorkerNetworkResult>();
const networkLayoutStateByKey = new Map<string, unknown>();
const incrementalActiveStateByKey = new Map<string, IncrementalActiveState>();
const frameIndexDayCheckpoint = new Map<string, { dayIndex: number; activeEdgeIds: Set<number> }>();
let loadedState: WorkerLoadedState | null = null;
const TERMINAL_ACTIONS = new Set(["cancelled", "expired", "ended", "terminated", "termination", "inferred_cancelled"]);
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
  const [eventsRaw, summaryRaw, flagsRaw, scoreRanksRaw, frameIndexRaw, manifestRaw] = await Promise.all([
    fetchMsgpack<unknown[]>(dataAssetPath("treaty_changes_reconciled.msgpack")),
    fetchMsgpack<unknown>(dataAssetPath("treaty_changes_reconciled_summary.msgpack")),
    fetchMsgpack<unknown[]>(dataAssetPath("treaty_changes_reconciled_flags.msgpack")),
    fetch(dataAssetPath("alliance_score_ranks_daily.msgpack"))
      .then(async (response) => {
        if (!response.ok) {
          return null;
        }
        const body = await response.arrayBuffer();
        return decode(new Uint8Array(body)) as unknown;
      })
      .catch(() => null),
    fetch(dataAssetPath("treaty_frame_index_v1.msgpack"))
      .then(async (response) => {
        if (!response.ok) {
          return null;
        }
        const body = await response.arrayBuffer();
        return decode(new Uint8Array(body)) as unknown;
      })
      .catch(() => null),
    fetch(dataAssetPath("manifest.json"))
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
    frameIndexRaw,
    manifestRaw
  };
}

function normalizeFrameIndex(raw: unknown | null): WorkerFrameIndexRuntime | null {
  if (!raw) {
    return null;
  }

  const parsed = treatyFrameIndexV1Schema.safeParse(raw);
  if (!parsed.success) {
    console.warn("[timelapse] treaty_frame_index_v1 invalid; falling back to legacy network scan", parsed.error.message);
    return null;
  }

  const payload = parsed.data;
  if (
    payload.day_keys.length === 0 ||
    payload.day_keys.length !== payload.event_end_offset_by_day.length ||
    payload.day_keys.length !== payload.active_edge_delta_by_day.length
  ) {
    console.warn("[timelapse] treaty_frame_index_v1 shape mismatch; falling back to legacy network scan");
    return null;
  }

  const edgeIdByEventIndex = new Map<number, number>();
  for (let edgeId = 0; edgeId < payload.edge_dict.length; edgeId += 1) {
    const eventIndex = payload.edge_dict[edgeId][0];
    edgeIdByEventIndex.set(eventIndex, edgeId);
  }

  const dayIndexByDayKey = new Map<string, number>();
  for (let dayIndex = 0; dayIndex < payload.day_keys.length; dayIndex += 1) {
    dayIndexByDayKey.set(payload.day_keys[dayIndex], dayIndex);
  }

  return {
    payload,
    edgeIdByEventIndex,
    dayIndexByDayKey
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
    fetch(dataAssetPath("flags.msgpack"))
      .then(async (response) => {
        if (!response.ok) {
          console.warn("[timelapse] Optional flags.msgpack fetch failed in worker", response.status);
          return null;
        }
        const body = await response.arrayBuffer();
        return decode(new Uint8Array(body)) as unknown;
      })
      .catch(() => null),
    fetch(dataAssetPath("flag_assets.msgpack"))
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
  return buildQueryStructuralKey(query);
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
  const ranksByDay = state.scoreRanksByDay;
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

function networkCacheKey(
  query: WorkerQueryState,
  playhead: string | null,
  maxEdges: number,
  strategy: NetworkLayoutStrategy,
  strategyConfigSignature: string
): string {
  return `${selectionKey(query)}|network:${strategy}:${strategyConfigSignature}:${playhead ?? ""}|${maxEdges}`;
}

function networkTemporalKey(
  query: WorkerQueryState,
  maxEdges: number,
  strategy: NetworkLayoutStrategy,
  strategyConfigSignature: string
): string {
  return `${selectionKey(query)}|network:temporal:${strategy}:${strategyConfigSignature}|${maxEdges}`;
}

function strategyConfigSignature(strategyConfig: NetworkLayoutStrategyConfig | undefined): string {
  if (!strategyConfig) {
    return "none";
  }
  const ordered = Object.entries(strategyConfig)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => [key, value]);
  return JSON.stringify(ordered);
}

function keyForActivePair(event: WorkerEvent): string {
  return keyForEvent(event);
}

function queryWithoutTopX(query: WorkerQueryState): WorkerQueryState {
  if (query.filters.topXByScore === null) {
    return query;
  }

  return {
    ...query,
    filters: {
      ...query.filters,
      topXByScore: null
    }
  };
}

function topMembershipAtPlayhead(
  ranksByDay: WorkerScoreRanksByDay | null,
  topX: number | null,
  playhead: string | null
): Set<number> | null {
  if (!ranksByDay || topX === null || topX <= 0) {
    return null;
  }

  const rankDays = Object.keys(ranksByDay).sort((left, right) => left.localeCompare(right));
  if (rankDays.length === 0) {
    return null;
  }

  const resolvedDay = resolveScoreDay(rankDays, playhead);
  if (!resolvedDay) {
    return null;
  }

  const rankRow = ranksByDay[resolvedDay] ?? {};
  const members = new Set<number>();
  for (const [allianceIdRaw, rankRaw] of Object.entries(rankRow)) {
    const allianceId = Number(allianceIdRaw);
    const rank = Number(rankRaw);
    if (!Number.isFinite(allianceId) || !Number.isFinite(rank)) {
      continue;
    }
    if (allianceId <= 0 || rank <= 0 || rank > topX) {
      continue;
    }
    members.add(allianceId);
  }
  return members;
}

function computeNetworkEdgeEventIndexesLegacy(
  state: WorkerLoadedState,
  query: WorkerQueryState,
  playhead: string | null,
  maxEdges: number,
  strategy: NetworkLayoutStrategy,
  strategyConfig: NetworkLayoutStrategyConfig | undefined
): WorkerNetworkResult {
  const configSignature = strategyConfigSignature(strategyConfig);
  const cacheKey = networkCacheKey(query, playhead, maxEdges, strategy, configSignature);
  const cached = networkCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const events = state.payload.eventsRaw as WorkerEvent[];
  const reconstructionQuery = queryWithoutTopX(query);
  const selectionSignature = `${selectionKey(reconstructionQuery)}|network|${strategy}|${configSignature}|${maxEdges}`;
  const temporalKey = networkTemporalKey(query, maxEdges, strategy, configSignature);
  const previous = incrementalActiveStateByKey.get(temporalKey);

  let chronologicalIndexes: number[];
  if (previous && previous.selectionSignature === selectionSignature) {
    chronologicalIndexes = previous.chronologicalIndexes;
  } else {
    chronologicalIndexes = [...computeSelectionIndexesCached(state, reconstructionQuery)];
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

  let activeIndexes = [...active.values()];
  const membership = topMembershipAtPlayhead(state.scoreRanksByDay, query.filters.topXByScore, playhead);
  if (membership) {
    activeIndexes = activeIndexes.filter((index) => {
      const event = events[index];
      return membership.has(event.from_alliance_id) && membership.has(event.to_alliance_id);
    });
  }

  const keepCount = Math.max(maxEdges, Math.min(200, Math.ceil(maxEdges * 1.5)));
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

  if (membership) {
    for (const allianceId of membership) {
      const nodeId = String(allianceId);
      if (!nodeIdSet.has(nodeId)) {
        nodeIdSet.add(nodeId);
      }
      if (!adjacencyByNodeId.has(nodeId)) {
        adjacencyByNodeId.set(nodeId, new Set());
      }
    }
  }

  const reduced = reducedEvents.map((event) => state.payload.indicesRaw.eventIdToIndex[event.event_id]);
  const edgeIndexes = Uint32Array.from(reduced);

  const layoutResult = runNetworkLayoutStrategy(strategy, {
    nodeIds: [...nodeIdSet],
    adjacencyByNodeId,
    temporalKey,
    previousState: networkLayoutStateByKey.get(temporalKey),
    strategyConfig
  });
  const layout = layoutResult.layout;
  networkLayoutStateByKey.set(temporalKey, layoutResult.metadata?.state);
  if (networkLayoutStateByKey.size > 40) {
    const first = networkLayoutStateByKey.keys().next().value;
    if (first) {
      networkLayoutStateByKey.delete(first);
    }
  }

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

function canUseFrameIndexFastPath(query: WorkerQueryState): boolean {
  // Fast path currently guarantees parity for broad network queries only.
  if (query.time.start || query.time.end) {
    return false;
  }
  if (query.focus.allianceId !== null || query.focus.edgeKey || query.focus.eventId) {
    return false;
  }
  if (query.filters.actions.length > 0) {
    return false;
  }
  if (query.filters.treatyTypes.length > 0 || query.filters.sources.length > 0 || query.filters.alliances.length > 0) {
    return false;
  }
  if (!query.filters.includeInferred || !query.filters.includeNoise) {
    return false;
  }
  if (query.filters.evidenceMode !== "all") {
    return false;
  }
  if (query.filters.topXByScore !== null) {
    return false;
  }
  if (query.textQuery.trim().length > 0) {
    return false;
  }
  return true;
}

function resolveFrameIndexDay(payload: TreatyFrameIndexV1, playhead: string | null): number {
  if (payload.day_keys.length === 0) {
    return -1;
  }
  if (!playhead) {
    return payload.day_keys.length - 1;
  }
  const target = playhead.slice(0, 10);
  let lo = 0;
  let hi = payload.day_keys.length - 1;
  let best = -1;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (payload.day_keys[mid] <= target) {
      best = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return best;
}

function activeEdgeIdsAtDayEnd(frameIndex: WorkerFrameIndexRuntime, dayIndex: number): Set<number> {
  const cacheKey = "global";
  const cached = frameIndexDayCheckpoint.get(cacheKey);
  let active = new Set<number>();
  let startDay = 0;

  if (cached && cached.dayIndex <= dayIndex) {
    active = new Set(cached.activeEdgeIds);
    startDay = cached.dayIndex + 1;
  }

  for (let index = startDay; index <= dayIndex; index += 1) {
    const delta = frameIndex.payload.active_edge_delta_by_day[index];
    for (const edgeId of delta.remove_edge_ids) {
      active.delete(edgeId);
    }
    for (const edgeId of delta.add_edge_ids) {
      active.add(edgeId);
    }
  }

  frameIndexDayCheckpoint.set(cacheKey, {
    dayIndex,
    activeEdgeIds: new Set(active)
  });
  if (frameIndexDayCheckpoint.size > 8) {
    const first = frameIndexDayCheckpoint.keys().next().value;
    if (first) {
      frameIndexDayCheckpoint.delete(first);
    }
  }

  return active;
}

function computeNetworkEdgeEventIndexesFromFrameIndex(
  state: WorkerLoadedState,
  query: WorkerQueryState,
  playhead: string | null,
  maxEdges: number,
  strategy: NetworkLayoutStrategy,
  strategyConfig: NetworkLayoutStrategyConfig | undefined
): WorkerNetworkResult | null {
  if (!state.frameIndex || !canUseFrameIndexFastPath(query)) {
    return null;
  }

  const frameIndex = state.frameIndex;
  const payload = frameIndex.payload;
  const events = state.payload.eventsRaw as WorkerEvent[];
  if (events.length === 0) {
    return null;
  }
  if (playhead && playhead < events[0].timestamp) {
    return null;
  }

  const dayIndex = resolveFrameIndexDay(payload, playhead);
  if (dayIndex < 0) {
    return null;
  }

  const active = dayIndex > 0 ? activeEdgeIdsAtDayEnd(frameIndex, dayIndex - 1) : new Set<number>();
  const activeByPair = new Map<string, number>();
  for (const edgeId of active) {
    const edge = payload.edge_dict[edgeId];
    const key = `${edge[1]}:${edge[2]}:${edge[3]}`;
    activeByPair.set(key, edgeId);
  }

  const dayStartOffset = dayIndex === 0 ? 0 : payload.event_end_offset_by_day[dayIndex - 1];
  const dayEndOffset = payload.event_end_offset_by_day[dayIndex] ?? events.length;
  const replayUpperBound = playhead ? playhead : "\uffff";
  for (let eventIndex = dayStartOffset; eventIndex < dayEndOffset; eventIndex += 1) {
    const event = events[eventIndex];
    if (event.timestamp > replayUpperBound) {
      break;
    }
    const pairKey = keyForActivePair(event);
    if (event.action === "signed") {
      const edgeId = frameIndex.edgeIdByEventIndex.get(eventIndex);
      if (edgeId === undefined) {
        continue;
      }
      const previous = activeByPair.get(pairKey);
      if (previous !== undefined) {
        active.delete(previous);
      }
      activeByPair.set(pairKey, edgeId);
      active.add(edgeId);
    } else if (TERMINAL_ACTIONS.has(event.action)) {
      const previous = activeByPair.get(pairKey);
      if (previous !== undefined) {
        active.delete(previous);
        activeByPair.delete(pairKey);
      }
    }
  }

  const activeIndexes = [...active]
    .map((edgeId) => payload.edge_dict[edgeId]?.[0])
    .filter((eventIndex): eventIndex is number => typeof eventIndex === "number");

  const configSignature = strategyConfigSignature(strategyConfig);
  const temporalKey = `${networkTemporalKey(query, maxEdges, strategy, configSignature)}|fi:${playhead ?? ""}`;
  const keepCount = Math.max(maxEdges, Math.min(200, Math.ceil(maxEdges * 1.5)));
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
  const layoutResult = runNetworkLayoutStrategy(strategy, {
    nodeIds: [...nodeIdSet],
    adjacencyByNodeId,
    temporalKey,
    previousState: networkLayoutStateByKey.get(temporalKey),
    strategyConfig
  });
  networkLayoutStateByKey.set(temporalKey, layoutResult.metadata?.state);
  if (networkLayoutStateByKey.size > 40) {
    const first = networkLayoutStateByKey.keys().next().value;
    if (first) {
      networkLayoutStateByKey.delete(first);
    }
  }

  return {
    edgeIndexes,
    layout: layoutResult.layout
  };
}

function computeNetworkEdgeEventIndexes(
  state: WorkerLoadedState,
  query: WorkerQueryState,
  playhead: string | null,
  maxEdges: number,
  strategy: NetworkLayoutStrategy,
  strategyConfig: NetworkLayoutStrategyConfig | undefined
): WorkerNetworkResult {
  const fastPath = computeNetworkEdgeEventIndexesFromFrameIndex(
    state,
    query,
    playhead,
    maxEdges,
    strategy,
    strategyConfig
  );
  if (fastPath) {
    return fastPath;
  }
  return computeNetworkEdgeEventIndexesLegacy(state, query, playhead, maxEdges, strategy, strategyConfig);
}

async function ensureLoadedState(includeFlags: boolean): Promise<WorkerLoadedState> {
  if (loadedState && (!includeFlags || loadedState.includeFlags)) {
    return loadedState;
  }

  if (!loadedState) {
    const basePayload = await loadBasePayload();
    const scoreRanksByDay = normalizeScoreRanksByDay(basePayload.scoreRanksRaw);
    const frameIndex = normalizeFrameIndex(basePayload.frameIndexRaw);
    const payload: LoaderWorkerPayload = {
      ...basePayload,
      allianceFlagsRaw: null,
      flagAssetsRaw: null
    };
    const allEventIndexes = new Array(payload.eventsRaw.length);
    for (let index = 0; index < payload.eventsRaw.length; index += 1) {
      allEventIndexes[index] = index;
    }
    loadedState = {
      payload,
      allEventIndexes,
      includeFlags: false,
      scoreRanksByDay,
      frameIndex
    };
    selectionCache.clear();
    pulseCache.clear();
    networkCache.clear();
    networkLayoutStateByKey.clear();
    incrementalActiveStateByKey.clear();
    frameIndexDayCheckpoint.clear();
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
      const network = computeNetworkEdgeEventIndexes(
        state,
        request.query,
        request.playhead,
        request.maxEdges,
        request.strategy,
        request.strategyConfig
      );
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
