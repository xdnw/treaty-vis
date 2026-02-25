import type {
  LoaderWorkerRequest,
  LoaderWorkerResponse,
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
const networkCache = new Map<string, Uint32Array>();
let loadedState: WorkerLoadedState | null = null;
const TERMINAL_ACTIONS = new Set(["cancelled", "expired", "ended", "terminated", "termination", "inferred_cancelled"]);
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

function keyForActivePair(event: WorkerEvent): string {
  return keyForEvent(event);
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
  const indexes = [...computeSelectionIndexesCached(state, query)];
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
  const keepCount = Math.max(maxEdges, 200);
  const reducedEvents = selectStableCappedActiveEvents(activeIndexes.map((index) => events[index]), keepCount);
  const reduced = reducedEvents.map((event) => state.payload.indicesRaw.eventIdToIndex[event.event_id]);
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
