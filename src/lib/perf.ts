type PerfStats = {
  count: number;
  totalMs: number;
  maxMs: number;
};

type PerfCollector = {
  enabled: boolean;
  mark: (name: string, durationMs: number) => void;
  reset: () => void;
  report: () => Record<string, { count: number; avgMs: number; maxMs: number }>;
  scoreLoads: ScoreLoadAttemptTelemetry[];
};

export type ScoreLoadAttemptTelemetry = {
  requestId: string;
  datasetId: string;
  state:
    | "ready"
    | "error-timeout"
    | "error-http"
    | "error-network"
    | "error-decode"
    | "error-abort"
    | "error-manifest-missing"
    | "error-worker-unavailable"
    | "error-worker-failure";
  elapsedMs: number;
  httpStatus: number | null;
  bytesFetched: number;
  totalBytes: number | null;
  decodeMs: number | null;
  dayCount: number;
  scoredNodeCount: number;
  reasonCode: string | null;
  message: string | null;
  fromCache: boolean;
  at: string;
};

type PerfWindow = Window & {
  __timelapsePerf?: PerfCollector;
};

const DEV_PERF_ENABLED = import.meta.env.DEV;
const stats = new Map<string, PerfStats>();
const noopCollector: PerfCollector = {
  enabled: false,
  mark: () => {},
  reset: () => {},
  report: () => ({}),
  scoreLoads: []
};

function isPerfEnabled(): boolean {
  if (!DEV_PERF_ENABLED || typeof window === "undefined") {
    return false;
  }

  const queryEnabled = new URLSearchParams(window.location.search).get("perf") === "1";
  const envEnabled = import.meta.env.VITE_TIMELAPSE_PERF === "true";
  return queryEnabled || envEnabled;
}

function buildCollector(): PerfCollector {
  const enabled = isPerfEnabled();

  const mark = (name: string, durationMs: number) => {
    if (!enabled || !Number.isFinite(durationMs)) {
      return;
    }
    const current = stats.get(name) ?? { count: 0, totalMs: 0, maxMs: 0 };
    current.count += 1;
    current.totalMs += durationMs;
    current.maxMs = Math.max(current.maxMs, durationMs);
    stats.set(name, current);
  };

  const reset = () => {
    stats.clear();
    collector.scoreLoads.splice(0, collector.scoreLoads.length);
  };

  const report = () => {
    const result: Record<string, { count: number; avgMs: number; maxMs: number }> = {};
    for (const [name, value] of stats.entries()) {
      result[name] = {
        count: value.count,
        avgMs: value.count > 0 ? Number((value.totalMs / value.count).toFixed(2)) : 0,
        maxMs: Number(value.maxMs.toFixed(2))
      };
    }
    return result;
  };

  const collector: PerfCollector = {
    enabled,
    mark,
    reset,
    report,
    scoreLoads: []
  };

  return collector;
}

export function initTimelapsePerfCollector(): PerfCollector {
  if (!DEV_PERF_ENABLED || typeof window === "undefined") {
    return noopCollector;
  }

  const perfWindow = window as PerfWindow;
  if (!perfWindow.__timelapsePerf) {
    perfWindow.__timelapsePerf = buildCollector();
  }

  return perfWindow.__timelapsePerf;
}

export function markTimelapsePerf(name: string, durationMs: number): void {
  initTimelapsePerfCollector().mark(name, durationMs);
}

export function pushScoreLoadAttempt(attempt: Omit<ScoreLoadAttemptTelemetry, "at">): void {
  if (!DEV_PERF_ENABLED || typeof window === "undefined") {
    return;
  }

  const collector = initTimelapsePerfCollector();
  const withTimestamp: ScoreLoadAttemptTelemetry = {
    ...attempt,
    at: new Date().toISOString()
  };
  collector.scoreLoads.push(withTimestamp);
  if (collector.scoreLoads.length > 20) {
    collector.scoreLoads.splice(0, collector.scoreLoads.length - 20);
  }
}

declare global {
  interface Window {
    __timelapsePerf?: PerfCollector;
  }
}
