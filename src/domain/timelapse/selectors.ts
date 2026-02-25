import type { AllianceScoreRanksByDay, TimelapseEvent } from "@/domain/timelapse/schema";
import {
  buildPulseSeries as buildSharedPulseSeries,
  buildQuerySelectionKey,
  buildTopXMembershipLookup,
  compareEventChronology,
  computeSelectionIndexes,
  keyForEvent,
  selectStableCappedActiveEvents
} from "@/domain/timelapse/queryEngine";
import type { QueryState } from "@/features/filters/filterStore";

const TERMINAL_ACTIONS = new Set(["cancelled", "expired", "ended", "inferred_cancelled"]);

export type TimelapseIndices = {
  allEventIndexes: number[];
  byAction: Record<string, number[]>;
  byType: Record<string, number[]>;
  bySource: Record<string, number[]>;
  byAlliance: Record<string, number[]>;
  eventIdToIndex: Record<string, number>;
  allActions: string[];
  allTypes: string[];
  allSources: string[];
  alliances: Array<{ id: number; name: string; count: number }>;
  minTimestamp: string | null;
  maxTimestamp: string | null;
};

export type TimelapseDerivedData = {
  datasetKey: string;
  events: TimelapseEvent[];
  indices: TimelapseIndices;
  allianceScoreRanksByDay: AllianceScoreRanksByDay | null;
};

export type SelectionResult = {
  indexes: number[];
  events: TimelapseEvent[];
};

export type NetworkEdge = {
  key: string;
  eventId: string;
  sourceId: string;
  targetId: string;
  sourceLabel: string;
  targetLabel: string;
  treatyType: string;
  sourceType: string;
  confidence: string;
};

export type PulsePoint = {
  day: string;
  signed: number;
  terminal: number;
  inferred: number;
};

type SelectorCacheEntry = {
  key: string;
  indexes: number[];
};

const selectorCache = new Map<string, SelectorCacheEntry>();
const networkOrderCache = new WeakMap<number[], number[]>();

function addToRecordList(record: Record<string, number[]>, key: string, index: number): void {
  if (!record[key]) {
    record[key] = [];
  }
  record[key].push(index);
}

export function buildTimelapseIndices(events: TimelapseEvent[]): TimelapseIndices {
  const byAction: Record<string, number[]> = {};
  const byType: Record<string, number[]> = {};
  const bySource: Record<string, number[]> = {};
  const byAlliance: Record<string, number[]> = {};
  const eventIdToIndex: Record<string, number> = {};
  const allianceMeta = new Map<number, { name: string; count: number }>();

  const allEventIndexes: number[] = new Array(events.length);
  for (let index = 0; index < events.length; index += 1) {
    allEventIndexes[index] = index;
    const event = events[index];
    const source = event.source || "unknown";
    addToRecordList(byAction, event.action, index);
    addToRecordList(byType, event.treaty_type, index);
    addToRecordList(bySource, source, index);
    addToRecordList(byAlliance, String(event.from_alliance_id), index);
    if (event.to_alliance_id !== event.from_alliance_id) {
      addToRecordList(byAlliance, String(event.to_alliance_id), index);
    }
    eventIdToIndex[event.event_id] = index;

    const fromMeta = allianceMeta.get(event.from_alliance_id) ?? {
      name: event.from_alliance_name || String(event.from_alliance_id),
      count: 0
    };
    fromMeta.name = fromMeta.name || event.from_alliance_name || String(event.from_alliance_id);
    fromMeta.count += 1;
    allianceMeta.set(event.from_alliance_id, fromMeta);

    const toMeta = allianceMeta.get(event.to_alliance_id) ?? {
      name: event.to_alliance_name || String(event.to_alliance_id),
      count: 0
    };
    toMeta.name = toMeta.name || event.to_alliance_name || String(event.to_alliance_id);
    toMeta.count += 1;
    allianceMeta.set(event.to_alliance_id, toMeta);
  }

  return {
    allEventIndexes,
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
    minTimestamp: events.length > 0 ? events[0].timestamp : null,
    maxTimestamp: events.length > 0 ? events[events.length - 1].timestamp : null
  };
}


function selectionCacheKey(datasetKey: string, query: QueryState): string {
  return `${datasetKey}|${buildQuerySelectionKey(query)}`;
}

export function selectEvents(derived: TimelapseDerivedData, query: QueryState): SelectionResult {
  const canUseCache = derived.datasetKey !== "__loading__";
  const cacheKey = selectionCacheKey(derived.datasetKey, query);
  if (canUseCache) {
    const cacheHit = selectorCache.get(cacheKey);
    if (cacheHit) {
      const cachedIndexes = cacheHit.indexes;
      return {
        indexes: cachedIndexes,
        events: cachedIndexes.map((index) => derived.events[index])
      };
    }
  }

  const { events, indices } = derived;

  const topXByScore = query.filters.topXByScore ?? null;
  const topMembershipLookup =
    topXByScore !== null &&
    topXByScore > 0 &&
    derived.allianceScoreRanksByDay !== null &&
    Object.keys(derived.allianceScoreRanksByDay).length > 0
      ? buildTopXMembershipLookup(derived.allianceScoreRanksByDay, topXByScore)
      : null;

  const sortedIndexes = computeSelectionIndexes({
    events,
    indices,
    query,
    topXMembershipLookup: topMembershipLookup
  });
  const result = {
    indexes: sortedIndexes,
    events: sortedIndexes.map((index) => events[index])
  };

  if (canUseCache) {
    selectorCache.set(cacheKey, { key: cacheKey, indexes: sortedIndexes });
    if (selectorCache.size > 60) {
      const first = selectorCache.keys().next().value;
      if (first) {
        selectorCache.delete(first);
      }
    }
  }

  return result;
}

export function deriveNetworkEdges(
  events: TimelapseEvent[],
  indexes: number[],
  playhead: string | null,
  maxEdges: number
): NetworkEdge[] {
  const active = new Map<string, TimelapseEvent>();
  let chronologicallyOrderedIndexes = networkOrderCache.get(indexes);
  if (!chronologicallyOrderedIndexes) {
    chronologicallyOrderedIndexes = [...indexes].sort((leftIndex, rightIndex) =>
      compareEventChronology(events[leftIndex], events[rightIndex])
    );
    networkOrderCache.set(indexes, chronologicallyOrderedIndexes);
  }

  for (const index of chronologicallyOrderedIndexes) {
    const event = events[index];
    if (playhead && event.timestamp > playhead) {
      break;
    }
    const key = keyForEvent(event);
    if (event.action === "signed") {
      active.delete(key);
      active.set(key, event);
    } else if (TERMINAL_ACTIONS.has(event.action)) {
      active.delete(key);
    }
  }

  const activeEvents = [...active.values()];
  const keepCount = Math.max(maxEdges, 200);
  const reduced = selectStableCappedActiveEvents(activeEvents, keepCount);

  return reduced.map((event) => ({
    key: keyForEvent(event),
    eventId: event.event_id,
    sourceId: String(event.from_alliance_id),
    targetId: String(event.to_alliance_id),
    sourceLabel: event.from_alliance_name || String(event.from_alliance_id),
    targetLabel: event.to_alliance_name || String(event.to_alliance_id),
    treatyType: event.treaty_type,
    sourceType: event.source || "unknown",
    confidence: event.confidence
  }));
}

export function buildPulseSeries(
  events: TimelapseEvent[],
  indexes: number[],
  maxPoints: number,
  playhead: string | null
): PulsePoint[] {
  return buildSharedPulseSeries(events, indexes, maxPoints, playhead);
}

export function countByAction(events: TimelapseEvent[]): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const event of events) {
    counts[event.action] = (counts[event.action] ?? 0) + 1;
  }
  return counts;
}
