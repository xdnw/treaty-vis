import type {
  LoaderWorkerRequest,
  LoaderWorkerResponse,
  WorkerPulsePoint,
  WorkerQueryState
} from "@/domain/timelapse/workerProtocol";
import { createScoreDayResolver } from "@/domain/timelapse/scoreDay";
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
  action: string;
  treaty_type: string;
  source?: string;
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
const networkCache = new Map<string, Uint32Array>();
const topMembershipCache = new Map<string, Set<number>>();
let loadedState: WorkerLoadedState | null = null;
const TERMINAL_ACTIONS = new Set(["cancelled", "expired", "ended", "inferred_cancelled"]);

function stableHashKey(value: string): number {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return hash >>> 0;
}
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
    const left = leftRaw as { timestamp?: string; event_id?: string };
    const right = rightRaw as { timestamp?: string; event_id?: string };

    const timestampOrder = String(left.timestamp ?? "").localeCompare(String(right.timestamp ?? ""));
    if (timestampOrder !== 0) {
      return timestampOrder;
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

function isInTopXByScore(
  ranksByDay: WorkerScoreRanksByDay,
  resolveRankDay: (timestampOrDay: string) => string | null,
  eventTimestamp: string,
  allianceId: number,
  topX: number
): boolean {
  const resolvedDay = resolveRankDay(eventTimestamp);
  if (!resolvedDay) {
    return false;
  }

  const cacheKey = `${resolvedDay}|${topX}`;
  let members = topMembershipCache.get(cacheKey);
  if (!members) {
    members = new Set<number>();
    const dayRanks = ranksByDay[resolvedDay] ?? {};
    for (const [allianceKey, rankRaw] of Object.entries(dayRanks)) {
      const rank = Number(rankRaw);
      if (!Number.isFinite(rank) || rank > topX) {
        continue;
      }
      const parsedAllianceId = Number(allianceKey);
      if (Number.isFinite(parsedAllianceId)) {
        members.add(parsedAllianceId);
      }
    }
    topMembershipCache.set(cacheKey, members);
  }

  return members.has(allianceId);
}

async function loadOptionalFlagPayload(): Promise<Pick<LoaderWorkerPayload, "allianceFlagsRaw" | "flagAssetsRaw">> {
  const [allianceFlagsRaw, flagAssetsRaw] = await Promise.all([
    fetch("/data/flags.msgpack")
      .then(async (response) => {
        if (!response.ok) {
          console.warn("Optional flags.msgpack fetch failed in worker", response.status);
          return null;
        }
        const body = await response.arrayBuffer();
        return decode(new Uint8Array(body)) as unknown;
      })
      .catch(() => null),
    fetch("/data/flag_assets.msgpack")
      .then(async (response) => {
        if (!response.ok) {
          console.warn("Optional flag_assets.msgpack fetch failed in worker", response.status);
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

function intersectSorted(left: number[], right: number[]): number[] {
  let l = 0;
  let r = 0;
  const next: number[] = [];
  while (l < left.length && r < right.length) {
    if (left[l] === right[r]) {
      next.push(left[l]);
      l += 1;
      r += 1;
      continue;
    }
    if (left[l] < right[r]) {
      l += 1;
    } else {
      r += 1;
    }
  }
  return next;
}

function unionSorted(slices: number[][]): number[] {
  const set = new Set<number>();
  for (const slice of slices) {
    for (const value of slice) {
      set.add(value);
    }
  }
  return [...set].sort((a, b) => a - b);
}

function filterByDimension(values: string[], map: Record<string, number[]>): number[] | null {
  if (values.length === 0) {
    return null;
  }
  return unionSorted(values.map((value) => map[value] ?? []));
}

function filterByAlliances(values: number[], map: Record<string, number[]>): number[] | null {
  if (values.length === 0) {
    return null;
  }
  return unionSorted(values.map((id) => map[String(id)] ?? []));
}

function findStartIndex(events: WorkerEvent[], timestamp: string): number {
  let lo = 0;
  let hi = events.length;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (events[mid].timestamp < timestamp) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

function findEndIndex(events: WorkerEvent[], timestamp: string): number {
  let lo = 0;
  let hi = events.length;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (events[mid].timestamp <= timestamp) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo - 1;
}

function keyForEvent(event: WorkerEvent): string {
  return `${event.pair_min_id ?? ""}:${event.pair_max_id ?? ""}:${event.treaty_type}`;
}

function matchesTextQuery(event: WorkerEvent, query: string): boolean {
  if (!query) {
    return true;
  }
  const normalized = query.toLowerCase();
  const haystack = [
    event.event_id,
    event.action,
    event.treaty_type,
    event.source || "unknown",
    event.from_alliance_name || "",
    event.to_alliance_name || "",
    String(event.from_alliance_id),
    String(event.to_alliance_id)
  ]
    .join(" ")
    .toLowerCase();
  return haystack.includes(normalized);
}

function compareBySort(left: WorkerEvent, right: WorkerEvent, query: WorkerQueryState): number {
  const direction = query.sort.direction === "asc" ? 1 : -1;
  let value = 0;

  if (query.sort.field === "timestamp") {
    value = left.timestamp.localeCompare(right.timestamp);
  } else if (query.sort.field === "action") {
    value = left.action.localeCompare(right.action);
  } else if (query.sort.field === "type") {
    value = left.treaty_type.localeCompare(right.treaty_type);
  } else if (query.sort.field === "from") {
    value = (left.from_alliance_name || String(left.from_alliance_id)).localeCompare(
      right.from_alliance_name || String(right.from_alliance_id)
    );
  } else if (query.sort.field === "to") {
    value = (left.to_alliance_name || String(left.to_alliance_id)).localeCompare(
      right.to_alliance_name || String(right.to_alliance_id)
    );
  } else {
    value = (left.source || "unknown").localeCompare(right.source || "unknown");
  }

  if (value !== 0) {
    return value * direction;
  }
  return left.event_id.localeCompare(right.event_id) * direction;
}

function selectionKey(query: WorkerQueryState): string {
  return [
    query.time.start ?? "",
    query.time.end ?? "",
    query.playback.playhead ?? "",
    String(query.focus.allianceId ?? ""),
    query.focus.edgeKey ?? "",
    query.focus.eventId ?? "",
    query.filters.actions.join(","),
    query.filters.treatyTypes.join(","),
    query.filters.sources.join(","),
    query.filters.alliances.join(","),
    query.filters.includeInferred ? "1" : "0",
    query.filters.includeNoise ? "1" : "0",
    query.filters.evidenceMode,
    String(query.filters.topXByScore ?? ""),
    query.textQuery.trim().toLowerCase(),
    query.sort.field,
    query.sort.direction
  ].join("|");
}

function compareEventChronology(left: WorkerEvent, right: WorkerEvent): number {
  const timestampOrder = left.timestamp.localeCompare(right.timestamp);
  if (timestampOrder !== 0) {
    return timestampOrder;
  }
  return left.event_id.localeCompare(right.event_id);
}

function computeSelectionIndexes(state: WorkerLoadedState, query: WorkerQueryState): Uint32Array {
  const cacheKey = selectionKey(query);
  const cached = selectionCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const events = state.payload.eventsRaw as WorkerEvent[];
  const indices = state.payload.indicesRaw;
  const candidates: number[][] = [state.allEventIndexes];

  const actionSlice = filterByDimension(query.filters.actions, indices.byAction);
  if (actionSlice) {
    candidates.push(actionSlice);
  }

  const typeSlice = filterByDimension(query.filters.treatyTypes, indices.byType);
  if (typeSlice) {
    candidates.push(typeSlice);
  }

  const sourceSlice = filterByDimension(query.filters.sources, indices.bySource);
  if (sourceSlice) {
    candidates.push(sourceSlice);
  }

  const allianceSlice = filterByAlliances(query.filters.alliances, indices.byAlliance);
  if (allianceSlice) {
    candidates.push(allianceSlice);
  }

  candidates.sort((left, right) => left.length - right.length);
  let selected = candidates[0] ?? [];
  for (let index = 1; index < candidates.length; index += 1) {
    selected = intersectSorted(selected, candidates[index]);
  }

  let start = 0;
  let end = events.length - 1;
  if (query.time.start) {
    start = findStartIndex(events, query.time.start);
  }
  if (query.time.end) {
    end = findEndIndex(events, query.time.end);
  }
  if (query.playback.playhead) {
    end = Math.min(end, findEndIndex(events, query.playback.playhead));
  }

  const topX = query.filters.topXByScore ?? null;
  const ranksByDay = normalizeScoreRanksByDay(state.payload.scoreRanksRaw);
  const rankDays = ranksByDay ? Object.keys(ranksByDay).sort((left, right) => left.localeCompare(right)) : [];
  const resolveRankDay = rankDays.length > 0 ? createScoreDayResolver(rankDays) : null;
  const applyTopX = topX !== null && topX > 0 && Boolean(ranksByDay) && Boolean(resolveRankDay);

  const filtered = selected.filter((eventIndex) => {
    if (eventIndex < start || eventIndex > end) {
      return false;
    }
    const event = events[eventIndex];
    if (applyTopX && ranksByDay && resolveRankDay) {
      if (
        !isInTopXByScore(ranksByDay, resolveRankDay, event.timestamp, event.from_alliance_id, topX) ||
        !isInTopXByScore(ranksByDay, resolveRankDay, event.timestamp, event.to_alliance_id, topX)
      ) {
        return false;
      }
    }
    if (!query.filters.includeInferred && event.inferred) {
      return false;
    }
    if (!query.filters.includeNoise && event.noise_filtered) {
      return false;
    }
    if (query.filters.evidenceMode === "one-confirmed" && !(event.grounded_from || event.grounded_to)) {
      return false;
    }
    if (query.filters.evidenceMode === "both-confirmed" && !(event.grounded_from && event.grounded_to)) {
      return false;
    }
    if (!matchesTextQuery(event, query.textQuery.trim())) {
      return false;
    }
    if (
      query.focus.allianceId !== null &&
      event.from_alliance_id !== query.focus.allianceId &&
      event.to_alliance_id !== query.focus.allianceId
    ) {
      return false;
    }
    if (query.focus.edgeKey && keyForEvent(event) !== query.focus.edgeKey) {
      return false;
    }
    if (query.focus.eventId && event.event_id !== query.focus.eventId) {
      return false;
    }
    return true;
  });

  filtered.sort((leftIndex, rightIndex) => compareBySort(events[leftIndex], events[rightIndex], query));
  const result = Uint32Array.from(filtered);
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
  const indexes = computeSelectionIndexes(state, query);
  const dayMap = new Map<string, WorkerPulsePoint>();

  for (let i = 0; i < indexes.length; i += 1) {
    const event = events[indexes[i]];
    if (playhead && event.timestamp > playhead) {
      continue;
    }
    const day = event.timestamp.slice(0, 10);
    const row = dayMap.get(day) ?? { day, signed: 0, terminal: 0, inferred: 0 };
    if (event.action === "signed") {
      row.signed += 1;
    }
    if (TERMINAL_ACTIONS.has(event.action)) {
      row.terminal += 1;
    }
    if (event.inferred) {
      row.inferred += 1;
    }
    dayMap.set(day, row);
  }

  const rows = [...dayMap.values()].sort((a, b) => a.day.localeCompare(b.day));
  if (rows.length <= maxPoints) {
    pulseCache.set(cacheKey, rows);
    if (pulseCache.size > 80) {
      const first = pulseCache.keys().next().value;
      if (first) {
        pulseCache.delete(first);
      }
    }
    return rows;
  }

  const bucketSize = Math.ceil(rows.length / maxPoints);
  const bucketed: WorkerPulsePoint[] = [];
  for (let start = 0; start < rows.length; start += bucketSize) {
    const slice = rows.slice(start, start + bucketSize);
    bucketed.push({
      day: slice[0].day,
      signed: slice.reduce((sum, row) => sum + row.signed, 0),
      terminal: slice.reduce((sum, row) => sum + row.terminal, 0),
      inferred: slice.reduce((sum, row) => sum + row.inferred, 0)
    });
  }

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

function keyForActivePair(event: WorkerEvent): string {
  return `${event.pair_min_id ?? ""}:${event.pair_max_id ?? ""}:${event.treaty_type}`;
}

function selectStableCappedActiveIndexes(
  activeIndexes: number[],
  events: WorkerEvent[],
  activeKeyByIndex: Map<number, string>,
  keepCount: number
): number[] {
  if (activeIndexes.length <= keepCount) {
    return activeIndexes;
  }

  const hashByOffset = new Uint32Array(activeIndexes.length);
  const eventIdByOffset = new Array<string>(activeIndexes.length);
  for (let offset = 0; offset < activeIndexes.length; offset += 1) {
    const eventIndex = activeIndexes[offset];
    const edgeKey = activeKeyByIndex.get(eventIndex) ?? keyForActivePair(events[eventIndex]);
    hashByOffset[offset] = stableHashKey(edgeKey);
    eventIdByOffset[offset] = String(events[eventIndex].event_id);
  }

  const isWorse = (leftOffset: number, rightOffset: number): boolean => {
    const leftHash = hashByOffset[leftOffset];
    const rightHash = hashByOffset[rightOffset];
    if (leftHash !== rightHash) {
      return leftHash > rightHash;
    }
    return eventIdByOffset[leftOffset] > eventIdByOffset[rightOffset];
  };

  const heap: number[] = [];
  const siftUp = (startIndex: number) => {
    let child = startIndex;
    while (child > 0) {
      const parent = Math.floor((child - 1) / 2);
      if (!isWorse(heap[child], heap[parent])) {
        break;
      }
      const next = heap[parent];
      heap[parent] = heap[child];
      heap[child] = next;
      child = parent;
    }
  };

  const siftDown = (startIndex: number) => {
    let parent = startIndex;
    while (true) {
      const left = parent * 2 + 1;
      const right = left + 1;
      let largest = parent;

      if (left < heap.length && isWorse(heap[left], heap[largest])) {
        largest = left;
      }
      if (right < heap.length && isWorse(heap[right], heap[largest])) {
        largest = right;
      }
      if (largest === parent) {
        break;
      }

      const next = heap[parent];
      heap[parent] = heap[largest];
      heap[largest] = next;
      parent = largest;
    }
  };

  for (let offset = 0; offset < activeIndexes.length; offset += 1) {
    if (heap.length < keepCount) {
      heap.push(offset);
      siftUp(heap.length - 1);
      continue;
    }

    const worstSelected = heap[0];
    if (isWorse(offset, worstSelected)) {
      continue;
    }

    heap[0] = offset;
    siftDown(0);
  }

  const selectedLookup = new Set(heap);
  const reduced: number[] = [];
  for (let offset = 0; offset < activeIndexes.length && reduced.length < keepCount; offset += 1) {
    if (selectedLookup.has(offset)) {
      reduced.push(activeIndexes[offset]);
    }
  }
  return reduced;
}

function computeNetworkEdgeEventIndexes(
  state: WorkerLoadedState,
  query: WorkerQueryState,
  playhead: string | null,
  maxEdges: number
): Uint32Array {
  const cacheKey = networkCacheKey(query, playhead, maxEdges);
  const cached = networkCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const events = state.payload.eventsRaw as WorkerEvent[];
  const indexes = [...computeSelectionIndexes(state, query)];
  indexes.sort((leftIndex, rightIndex) => compareEventChronology(events[leftIndex], events[rightIndex]));

  const active = new Map<string, number>();
  for (const eventIndex of indexes) {
    const event = events[eventIndex];
    if (playhead && event.timestamp > playhead) {
      break;
    }

    const key = keyForActivePair(event);
    if (event.action === "signed") {
      active.delete(key);
      active.set(key, eventIndex);
    } else if (TERMINAL_ACTIONS.has(event.action)) {
      active.delete(key);
    }
  }

  const activeIndexes = [...active.values()];
  const activeKeyByIndex = new Map<number, string>();
  for (const [key, eventIndex] of active.entries()) {
    activeKeyByIndex.set(eventIndex, key);
  }
  const keepCount = Math.max(maxEdges, 200);
  const reduced = selectStableCappedActiveIndexes(activeIndexes, events, activeKeyByIndex, keepCount);
  const result = Uint32Array.from(reduced);

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
    topMembershipCache.clear();
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
      const response: LoaderWorkerResponse = {
        kind: "load",
        ok: false,
        error: reason instanceof Error ? reason.message : "Unknown loader worker error"
      };
      workerScope.postMessage(response);
    }
    return;
  }

  if (request.kind === "select") {
    try {
      const state = await ensureLoadedState(request.query.filters.showFlags);
      const indexes = computeSelectionIndexes(state, request.query);
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
      const response: LoaderWorkerResponse = {
        kind: "select",
        ok: false,
        requestId: request.requestId,
        error: reason instanceof Error ? reason.message : "Unknown selection worker error"
      };
      workerScope.postMessage(response);
    }
    return;
  }

  if (request.kind === "network") {
    try {
      const state = await ensureLoadedState(request.query.filters.showFlags);
      const edgeIndexes = computeNetworkEdgeEventIndexes(state, request.query, request.playhead, request.maxEdges);
      const edgeIndexesBuffer = new Uint32Array(edgeIndexes).buffer;
      const response: LoaderWorkerResponse = {
        kind: "network",
        ok: true,
        requestId: request.requestId,
        edgeIndexesBuffer,
        length: edgeIndexes.length
      };
      workerScope.postMessage(response, [edgeIndexesBuffer]);
    } catch (reason) {
      const response: LoaderWorkerResponse = {
        kind: "network",
        ok: false,
        requestId: request.requestId,
        error: reason instanceof Error ? reason.message : "Unknown network worker error"
      };
      workerScope.postMessage(response);
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
    const response: LoaderWorkerResponse = {
      kind: "pulse",
      ok: false,
      requestId: request.requestId,
      error: reason instanceof Error ? reason.message : "Unknown pulse worker error"
    };
    workerScope.postMessage(response);
  }
});
