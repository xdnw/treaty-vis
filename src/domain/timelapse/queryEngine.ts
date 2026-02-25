import { createScoreDayResolver } from "@/domain/timelapse/scoreDay";

const TERMINAL_ACTIONS = new Set(["cancelled", "expired", "ended", "inferred_cancelled"]);

export type TimelapseSortField = "timestamp" | "action" | "type" | "from" | "to" | "source";
export type TimelapseSortDirection = "asc" | "desc";
export type TimelapseEvidenceMode = "all" | "one-confirmed" | "both-confirmed";

export type TimelapseQueryLike = {
  time: {
    start: string | null;
    end: string | null;
  };
  playback: {
    playhead: string | null;
  };
  focus: {
    allianceId: number | null;
    edgeKey: string | null;
    eventId: string | null;
  };
  filters: {
    alliances: number[];
    treatyTypes: string[];
    actions: string[];
    sources: string[];
    includeInferred: boolean;
    includeNoise: boolean;
    evidenceMode: TimelapseEvidenceMode;
    topXByScore: number | null;
  };
  textQuery: string;
  sort: {
    field: TimelapseSortField;
    direction: TimelapseSortDirection;
  };
};

export type TimelapseEventLike = {
  event_id: string;
  action: string;
  treaty_type: string;
  source?: string | null;
  timestamp: string;
  from_alliance_id: number;
  to_alliance_id: number;
  from_alliance_name?: string;
  to_alliance_name?: string;
  confidence?: string;
  grounded_from?: boolean;
  grounded_to?: boolean;
  inferred?: boolean;
  noise_filtered?: boolean;
  pair_min_id?: number;
  pair_max_id?: number;
};

export type TimelapseIndexMaps = {
  allEventIndexes: number[];
  byAction: Record<string, number[]>;
  byType: Record<string, number[]>;
  bySource: Record<string, number[]>;
  byAlliance: Record<string, number[]>;
};

export type TimelapsePulsePoint = {
  day: string;
  signed: number;
  terminal: number;
  inferred: number;
};

export type AllianceScoreRanksByDayLike = Record<string, Record<string, number>>;

function unionSorted(slices: number[][]): number[] {
  const set = new Set<number>();
  for (const slice of slices) {
    for (const value of slice) {
      set.add(value);
    }
  }
  return [...set].sort((a, b) => a - b);
}

export function intersectSorted(left: number[], right: number[]): number[] {
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

export function filterByDimension(values: string[], map: Record<string, number[]>): number[] | null {
  if (values.length === 0) {
    return null;
  }
  return unionSorted(values.map((value) => map[value] ?? []));
}

export function filterByAlliances(values: number[], map: Record<string, number[]>): number[] | null {
  if (values.length === 0) {
    return null;
  }
  return unionSorted(values.map((id) => map[String(id)] ?? []));
}

export function findStartIndex(events: TimelapseEventLike[], timestamp: string): number {
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

export function findEndIndex(events: TimelapseEventLike[], timestamp: string): number {
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

export function keyForEvent(event: TimelapseEventLike): string {
  return `${event.pair_min_id ?? ""}:${event.pair_max_id ?? ""}:${event.treaty_type}`;
}

export function matchesTextQuery(event: TimelapseEventLike, query: string): boolean {
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

function compareByField(left: TimelapseEventLike, right: TimelapseEventLike, field: TimelapseSortField): number {
  if (field === "timestamp") {
    const byTimestamp = left.timestamp.localeCompare(right.timestamp);
    if (byTimestamp !== 0) {
      return byTimestamp;
    }
  } else if (field === "action") {
    const byAction = left.action.localeCompare(right.action);
    if (byAction !== 0) {
      return byAction;
    }
  } else if (field === "type") {
    const byType = left.treaty_type.localeCompare(right.treaty_type);
    if (byType !== 0) {
      return byType;
    }
  } else if (field === "from") {
    const byFrom = (left.from_alliance_name || String(left.from_alliance_id)).localeCompare(
      right.from_alliance_name || String(right.from_alliance_id)
    );
    if (byFrom !== 0) {
      return byFrom;
    }
  } else if (field === "to") {
    const byTo = (left.to_alliance_name || String(left.to_alliance_id)).localeCompare(
      right.to_alliance_name || String(right.to_alliance_id)
    );
    if (byTo !== 0) {
      return byTo;
    }
  } else {
    const bySource = (left.source || "unknown").localeCompare(right.source || "unknown");
    if (bySource !== 0) {
      return bySource;
    }
  }

  const byEventId = left.event_id.localeCompare(right.event_id);
  if (byEventId !== 0) {
    return byEventId;
  }
  return left.timestamp.localeCompare(right.timestamp);
}

export function compareTimelapseEvents(
  left: TimelapseEventLike,
  right: TimelapseEventLike,
  field: TimelapseSortField,
  direction: TimelapseSortDirection
): number {
  const sign = direction === "asc" ? 1 : -1;
  return compareByField(left, right, field) * sign;
}

export function compareEventChronology(left: TimelapseEventLike, right: TimelapseEventLike): number {
  const timestampOrder = left.timestamp.localeCompare(right.timestamp);
  if (timestampOrder !== 0) {
    return timestampOrder;
  }
  return left.event_id.localeCompare(right.event_id);
}

export function applySort(indexes: number[], events: TimelapseEventLike[], query: TimelapseQueryLike): number[] {
  const sorted = [...indexes];
  sorted.sort((leftIndex, rightIndex) =>
    compareTimelapseEvents(events[leftIndex], events[rightIndex], query.sort.field, query.sort.direction)
  );
  return sorted;
}

export function buildTopXMembershipLookup(
  ranksByDay: AllianceScoreRanksByDayLike,
  topX: number
): ((timestamp: string, allianceId: number) => boolean) | null {
  const rankDays = Object.keys(ranksByDay).sort((left, right) => left.localeCompare(right));
  if (rankDays.length === 0) {
    return null;
  }

  const resolveRankDay = createScoreDayResolver(rankDays);
  const membershipCache = new Map<string, Set<number>>();

  return (timestamp: string, allianceId: number) => {
    const rankDay = resolveRankDay(timestamp);
    if (!rankDay) {
      return false;
    }

    const cacheKey = `${rankDay}|${topX}`;
    let members = membershipCache.get(cacheKey);
    if (!members) {
      members = new Set<number>();
      const rankRow = ranksByDay[rankDay] ?? {};
      for (const [allianceKey, rankRaw] of Object.entries(rankRow)) {
        const rank = Number(rankRaw);
        if (!Number.isFinite(rank) || rank > topX) {
          continue;
        }
        const parsedAllianceId = Number(allianceKey);
        if (Number.isFinite(parsedAllianceId)) {
          members.add(parsedAllianceId);
        }
      }
      membershipCache.set(cacheKey, members);
    }

    return members.has(allianceId);
  };
}

export function buildQuerySelectionKey(query: TimelapseQueryLike): string {
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

export function computeSelectionIndexes(params: {
  events: TimelapseEventLike[];
  indices: TimelapseIndexMaps;
  query: TimelapseQueryLike;
  topXMembershipLookup?: ((timestamp: string, allianceId: number) => boolean) | null;
}): number[] {
  const { events, indices, query, topXMembershipLookup } = params;
  const candidates: number[][] = [indices.allEventIndexes];

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

  const text = query.textQuery.trim();
  const filtered = selected.filter((eventIndex) => {
    if (eventIndex < start || eventIndex > end) {
      return false;
    }

    const event = events[eventIndex];
    if (
      topXMembershipLookup &&
      (!topXMembershipLookup(event.timestamp, event.from_alliance_id) ||
        !topXMembershipLookup(event.timestamp, event.to_alliance_id))
    ) {
      return false;
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
    if (!matchesTextQuery(event, text)) {
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

  return applySort(filtered, events, query);
}

function stableHashKey(value: string): number {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return hash >>> 0;
}

export function selectStableCappedActiveEvents<TEvent extends TimelapseEventLike>(
  activeEvents: TEvent[],
  keepCount: number
): TEvent[] {
  if (activeEvents.length <= keepCount) {
    return activeEvents;
  }

  const hashByIndex = new Uint32Array(activeEvents.length);
  const eventIdByIndex = new Array<string>(activeEvents.length);
  for (let index = 0; index < activeEvents.length; index += 1) {
    hashByIndex[index] = stableHashKey(keyForEvent(activeEvents[index]));
    eventIdByIndex[index] = activeEvents[index].event_id;
  }

  const isWorse = (leftIndex: number, rightIndex: number): boolean => {
    const leftHash = hashByIndex[leftIndex];
    const rightHash = hashByIndex[rightIndex];
    if (leftHash !== rightHash) {
      return leftHash > rightHash;
    }
    return eventIdByIndex[leftIndex] > eventIdByIndex[rightIndex];
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

  for (let index = 0; index < activeEvents.length; index += 1) {
    if (heap.length < keepCount) {
      heap.push(index);
      siftUp(heap.length - 1);
      continue;
    }

    const worstSelected = heap[0];
    if (isWorse(index, worstSelected)) {
      continue;
    }

    heap[0] = index;
    siftDown(0);
  }

  const selectedLookup = new Set(heap);
  const reduced: TEvent[] = [];
  for (let index = 0; index < activeEvents.length && reduced.length < keepCount; index += 1) {
    if (selectedLookup.has(index)) {
      reduced.push(activeEvents[index]);
    }
  }
  return reduced;
}

export function buildPulseSeries(
  events: TimelapseEventLike[],
  indexes: number[],
  maxPoints: number,
  playhead: string | null
): TimelapsePulsePoint[] {
  const dayMap = new Map<string, TimelapsePulsePoint>();
  for (const index of indexes) {
    const event = events[index];
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
    return rows;
  }

  const bucketSize = Math.ceil(rows.length / maxPoints);
  const bucketed: TimelapsePulsePoint[] = [];
  for (let start = 0; start < rows.length; start += bucketSize) {
    const slice = rows.slice(start, start + bucketSize);
    bucketed.push({
      day: slice[0].day,
      signed: slice.reduce((sum, row) => sum + row.signed, 0),
      terminal: slice.reduce((sum, row) => sum + row.terminal, 0),
      inferred: slice.reduce((sum, row) => sum + row.inferred, 0)
    });
  }
  return bucketed;
}
