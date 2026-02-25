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
};

type PerfWindow = Window & {
  __timelapsePerf?: PerfCollector;
};

const stats = new Map<string, PerfStats>();

function isPerfEnabled(): boolean {
  if (typeof window === "undefined") {
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

  return {
    enabled,
    mark,
    reset,
    report
  };
}

export function initTimelapsePerfCollector(): PerfCollector {
  if (typeof window === "undefined") {
    return {
      enabled: false,
      mark: () => {},
      reset: () => {},
      report: () => ({})
    };
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

declare global {
  interface Window {
    __timelapsePerf?: PerfCollector;
  }
}
