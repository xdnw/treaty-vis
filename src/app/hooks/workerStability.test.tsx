// @vitest-environment jsdom

import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { QueryState } from "@/features/filters/filterStore";

const selectTimelapseIndexesMock = vi.fn<
  (query: QueryState) => Promise<Uint32Array>
>();
const selectTimelapsePulseMock = vi.fn<
  (query: QueryState, maxPoints: number, playhead: string | null) => Promise<Array<{ day: string; signed: number; terminal: number; inferred: number }>>
>();
const selectTimelapseNetworkEventIndexesMock = vi.fn<
  (
    query: QueryState,
    playhead: string | null,
    maxEdges: number,
    strategy: "hybrid-backbone" | "fa2line",
    strategyConfig?: Record<string, unknown>
  ) => Promise<{
    edgeEventIndexes: Uint32Array;
    layout: { components: []; communities: []; nodeTargets: [] };
    startedAt: number;
    finishedAt: number;
  }>
>();

vi.mock("@/domain/timelapse/loader", () => ({
  selectTimelapseIndexes: (...args: Parameters<typeof selectTimelapseIndexesMock>) => selectTimelapseIndexesMock(...args),
  selectTimelapsePulse: (...args: Parameters<typeof selectTimelapsePulseMock>) => selectTimelapsePulseMock(...args),
  selectTimelapseNetworkEventIndexes: (...args: Parameters<typeof selectTimelapseNetworkEventIndexesMock>) =>
    selectTimelapseNetworkEventIndexesMock(...args)
}));

import { useTimelapseWorkerSelection } from "@/app/hooks/useTimelapseWorkerSelection";
import { useNetworkWorkerIndexes } from "@/features/network/useNetworkWorkerIndexes";

declare global {
  // eslint-disable-next-line no-var
  var IS_REACT_ACT_ENVIRONMENT: boolean | undefined;
}

globalThis.IS_REACT_ACT_ENVIRONMENT = true;

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
};

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function makeQuery(overrides?: Partial<QueryState>): QueryState {
  return {
    time: { start: null, end: null },
    playback: { playhead: null, isPlaying: false, speed: 1 },
    focus: { allianceId: null, edgeKey: null, eventId: null },
    filters: {
      alliances: [],
      anchoredAllianceIds: [],
      treatyTypes: [],
      actions: [],
      sources: [],
      includeInferred: true,
      includeNoise: true,
      evidenceMode: "all",
      topXByScore: null,
      sizeByScore: false,
      scoreSizeContrast: 1,
      maxNodeRadius: 14,
      showFlags: false
    },
    textQuery: "",
    sort: { field: "timestamp", direction: "desc" },
    ...overrides
  };
}

const TEST_BUNDLE = {
  manifest: null,
  events: [],
  indices: {
    allEventIndexes: [],
    byAction: {},
    byType: {},
    bySource: {},
    byAlliance: {},
    eventIdToIndex: {},
    allActions: [],
    allTypes: [],
    allSources: [],
    alliances: [],
    minTimestamp: null,
    maxTimestamp: null
  },
  summary: {
    generated_at: "2026-02-25T00:00:00.000Z",
    event_count: 0,
    source_counts: {},
    treaty_type_counts: {}
  },
  flags: [],
  allianceFlagsPayload: null,
  flagAssetsPayload: null,
  allianceFlagTimelines: {},
  allianceScoreRanksByDay: null
};

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

type SelectionProbeProps = {
  query: QueryState;
  errorHandler: (message: string) => void;
};

function SelectionProbe({ query, errorHandler }: SelectionProbeProps) {
  const { scopedSelectionIndexes, pulse } = useTimelapseWorkerSelection({
    // Hook only checks truthiness and does not mutate bundle.
    bundle: TEST_BUNDLE as never,
    baseQuery: query,
    onError: errorHandler
  });

  return <div data-testid="selection-state">indexes:{scopedSelectionIndexes.length}|pulse:{pulse.length}</div>;
}

type NetworkProbeProps = {
  query: QueryState;
  playhead: string | null;
};

function NetworkProbe({ query, playhead }: NetworkProbeProps) {
  const { workerEdgeEventIndexes, workerError } = useNetworkWorkerIndexes({
    baseQuery: query,
    playhead,
    maxEdges: 200,
    strategy: "hybrid-backbone"
  });

  return (
    <div data-testid="network-state">
      edges:{workerEdgeEventIndexes?.length ?? 0}|error:{workerError ? "1" : "0"}
    </div>
  );
}

describe("worker hook stability during refresh", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("keeps previous selection and pulse while next worker request is pending", async () => {
    const errorHandler = vi.fn<(message: string) => void>();
    const nextSelection = createDeferred<Uint32Array>();
    const nextPulse = createDeferred<Array<{ day: string; signed: number; terminal: number; inferred: number }>>();

    selectTimelapseIndexesMock.mockResolvedValueOnce(new Uint32Array([1, 2]));
    selectTimelapsePulseMock.mockResolvedValueOnce([{ day: "2026-02-25", signed: 2, terminal: 0, inferred: 0 }]);
    selectTimelapseIndexesMock.mockReturnValueOnce(nextSelection.promise);
    selectTimelapsePulseMock.mockReturnValueOnce(nextPulse.promise);

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    const queryA = makeQuery();
    const queryB = makeQuery({ textQuery: "next" });

    await act(async () => {
      root.render(<SelectionProbe query={queryA} errorHandler={errorHandler} />);
      await flush();
      await flush();
    });

    expect(container.textContent).toContain("indexes:2|pulse:1");

    await act(async () => {
      root.render(<SelectionProbe query={queryB} errorHandler={errorHandler} />);
      await flush();
    });

    // Critical regression guard: pending request should not clear existing rendered state.
    expect(container.textContent).toContain("indexes:2|pulse:1");

    await act(async () => {
      nextSelection.resolve(new Uint32Array([7]));
      nextPulse.resolve([{ day: "2026-02-26", signed: 1, terminal: 1, inferred: 0 }]);
      await flush();
      await flush();
    });

    expect(container.textContent).toContain("indexes:1|pulse:1");
    expect(errorHandler).not.toHaveBeenCalled();

    await act(async () => {
      root.unmount();
    });
  });

  it("keeps previous network edge indexes while next request is pending", async () => {
    const nextEdges = createDeferred<{
      edgeEventIndexes: Uint32Array;
      layout: { components: []; communities: []; nodeTargets: [] };
      startedAt: number;
      finishedAt: number;
    }>();

    selectTimelapseNetworkEventIndexesMock.mockResolvedValueOnce({
      edgeEventIndexes: new Uint32Array([3, 4, 5]),
      layout: { components: [], communities: [], nodeTargets: [] },
      startedAt: 10,
      finishedAt: 12
    });
    selectTimelapseNetworkEventIndexesMock.mockReturnValueOnce(nextEdges.promise);

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    const queryA = makeQuery();
    const queryB = makeQuery({ textQuery: "network-next" });

    await act(async () => {
      root.render(<NetworkProbe query={queryA} playhead={null} />);
      await flush();
      await flush();
    });

    expect(container.textContent).toContain("edges:3|error:0");

    await act(async () => {
      root.render(<NetworkProbe query={queryB} playhead={null} />);
      await flush();
    });

    // Critical regression guard: pending request should not clear visible edges.
    expect(container.textContent).toContain("edges:3|error:0");

    await act(async () => {
      nextEdges.resolve({
        edgeEventIndexes: new Uint32Array([9]),
        layout: { components: [], communities: [], nodeTargets: [] },
        startedAt: 20,
        finishedAt: 22
      });
      await flush();
      await flush();
    });

    expect(container.textContent).toContain("edges:1|error:0");

    await act(async () => {
      root.unmount();
    });
  });

  it("runs only latest pending network snapshot after in-flight completion", async () => {
    const firstDeferred = createDeferred<{
      edgeEventIndexes: Uint32Array;
      layout: { components: []; communities: []; nodeTargets: [] };
      startedAt: number;
      finishedAt: number;
    }>();

    selectTimelapseNetworkEventIndexesMock.mockReturnValueOnce(firstDeferred.promise);
    selectTimelapseNetworkEventIndexesMock.mockResolvedValueOnce({
      edgeEventIndexes: new Uint32Array([42]),
      layout: { components: [], communities: [], nodeTargets: [] },
      startedAt: 30,
      finishedAt: 35
    });

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    const queryA = makeQuery({ textQuery: "a" });
    const queryB = makeQuery({ textQuery: "b" });
    const queryC = makeQuery({ textQuery: "c" });

    await act(async () => {
      root.render(<NetworkProbe query={queryA} playhead={null} />);
      await flush();
    });

    await act(async () => {
      root.render(<NetworkProbe query={queryB} playhead={null} />);
      await flush();
    });

    await act(async () => {
      root.render(<NetworkProbe query={queryC} playhead={null} />);
      await flush();
    });

    await act(async () => {
      firstDeferred.resolve({
        edgeEventIndexes: new Uint32Array([1]),
        layout: { components: [], communities: [], nodeTargets: [] },
        startedAt: 1,
        finishedAt: 3
      });
      await flush();
      await flush();
    });

    expect(selectTimelapseNetworkEventIndexesMock).toHaveBeenCalledTimes(2);
    expect(selectTimelapseNetworkEventIndexesMock.mock.calls[1]?.[0].textQuery).toBe("c");
    expect(container.textContent).toContain("edges:1|error:0");

    await act(async () => {
      await flush();
      await flush();
      await flush();
      await flush();
    });

    expect(container.textContent).toContain("error:0");

    await act(async () => {
      root.unmount();
    });
  });
});
