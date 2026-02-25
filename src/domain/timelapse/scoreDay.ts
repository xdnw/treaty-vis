function toDay(timestampOrDay: string): string {
  return timestampOrDay.length >= 10 ? timestampOrDay.slice(0, 10) : timestampOrDay;
}

export type ScoreRowResolution = {
  day: string | null;
  row: Record<string, number> | null;
  usedFallback: boolean;
};

export function resolveScoreDay(scoreDays: string[], timestampOrDay: string | null): string | null {
  if (scoreDays.length === 0) {
    return null;
  }
  if (!timestampOrDay) {
    return scoreDays[scoreDays.length - 1] ?? null;
  }

  const targetDay = toDay(timestampOrDay);
  let lo = 0;
  let hi = scoreDays.length - 1;
  let best = -1;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (scoreDays[mid] <= targetDay) {
      best = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }

  if (best >= 0) {
    return scoreDays[best] ?? null;
  }
  return scoreDays[0] ?? null;
}

export function createScoreDayResolver(scoreDays: string[]): (timestampOrDay: string) => string | null {
  const cache = new Map<string, string | null>();
  return (timestampOrDay: string) => {
    const targetDay = toDay(timestampOrDay);
    if (cache.has(targetDay)) {
      return cache.get(targetDay) ?? null;
    }
    const resolved = resolveScoreDay(scoreDays, targetDay);
    cache.set(targetDay, resolved);
    return resolved;
  };
}

export function resolveScoreRowForPlayhead(
  scoreByDay: Record<string, Record<string, number>>,
  scoreDays: string[],
  timestampOrDay: string | null
): ScoreRowResolution {
  if (scoreDays.length === 0) {
    return { day: null, row: null, usedFallback: false };
  }

  const requestedDay = timestampOrDay ? toDay(timestampOrDay) : scoreDays[scoreDays.length - 1];
  if (requestedDay in scoreByDay) {
    return {
      day: requestedDay,
      row: scoreByDay[requestedDay],
      usedFallback: false
    };
  }

  const resolvedDay = resolveScoreDay(scoreDays, requestedDay);
  if (!resolvedDay) {
    return { day: null, row: null, usedFallback: false };
  }

  const resolvedRow = scoreByDay[resolvedDay];
  if (resolvedRow) {
    return {
      day: resolvedDay,
      row: resolvedRow,
      usedFallback: resolvedDay !== requestedDay
    };
  }

  const resolvedIndex = scoreDays.indexOf(resolvedDay);
  if (resolvedIndex < 0) {
    return { day: null, row: null, usedFallback: false };
  }

  for (let index = resolvedIndex - 1; index >= 0; index -= 1) {
    const day = scoreDays[index];
    const row = scoreByDay[day];
    if (row) {
      return {
        day,
        row,
        usedFallback: true
      };
    }
  }

  for (let index = resolvedIndex + 1; index < scoreDays.length; index += 1) {
    const day = scoreDays[index];
    const row = scoreByDay[day];
    if (row) {
      return {
        day,
        row,
        usedFallback: true
      };
    }
  }

  return { day: null, row: null, usedFallback: false };
}
