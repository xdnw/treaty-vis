function toDay(timestampOrDay: string): string {
  return timestampOrDay.length >= 10 ? timestampOrDay.slice(0, 10) : timestampOrDay;
}

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
