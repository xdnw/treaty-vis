// @vitest-environment jsdom

import { act } from "react";
import { createRoot } from "react-dom/client";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ScoreLoaderSnapshot, ScoreLoaderState } from "@/domain/timelapse/scoreLoader";

const loadTimelapseBundleMock = vi.fn();
const loadScoreRuntimeMock = vi.fn();

vi.mock("@/domain/timelapse/loader", () => ({
  loadTimelapseBundle: (...args: unknown[]) => loadTimelapseBundleMock(...args),
  resolveAllianceFlagSnapshot: vi.fn(() => null),
  selectTimelapseIndexes: vi.fn(async () => null),
  selectTimelapsePulse: vi.fn(async () => null)
}));

vi.mock("@/domain/timelapse/scoreLoader", () => ({
  loadScoreRuntime: (...args: unknown[]) => loadScoreRuntimeMock(...args)
}));

vi.mock("@/features/filters/FilterBar", () => ({
  FilterBar: () => <div data-testid="filter-bar" />
}));

vi.mock("@/features/filters/PlaybackControls", () => ({
  PlaybackControls: () => <div data-testid="playback-controls" />
}));

vi.mock("@/features/inspector/InspectorView", () => ({
  InspectorView: () => <div data-testid="inspector-view" />
}));

vi.mock("@/features/timeline/TimelineView", () => ({
  TimelineView: () => <div data-testid="timeline-view" />
}));

vi.mock("@/features/network/NetworkAllianceHint", () => ({
  NetworkAllianceHint: () => <div data-testid="network-hint" />
}));

vi.mock("@/features/network/NetworkView", () => ({
  NetworkView: ({ onRetryScoreLoad }: { onRetryScoreLoad: () => void }) => (
    <button type="button" data-testid="retry-score-load" onClick={onRetryScoreLoad}>
      Retry score load
    </button>
  )
}));

import { App } from "@/app/App";

function makeSnapshot(requestId: string, state: ScoreLoaderState): ScoreLoaderSnapshot {
  return {
    requestId,
    datasetId: "dataset-a",
    state,
    startedAtMs: 0,
    atMs: 10,
    elapsedMs: 10,
    stageDurationMs: 10,
    httpStatus: null,
    bytesFetched: 0,
    totalBytes: null,
    decodeMs: null,
    dayCount: 0,
    scoredNodeCount: 0,
    reasonCode: null,
    message: null,
    runtime: null,
    fromCache: false
  };
}

function makeBundle() {
  return {
    manifest: {
      datasetId: "dataset-a",
      generatedAt: "2026-02-25T00:00:00.000Z",
      files: {
        "alliance_scores_v2.msgpack": {
          sizeBytes: 1024,
          sha256: "abc"
        }
      }
    },
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
}

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

describe("App score retry wiring", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    loadTimelapseBundleMock.mockResolvedValue(makeBundle());
    loadScoreRuntimeMock.mockImplementation(async (options: { requestId: string; onEvent?: (snapshot: ScoreLoaderSnapshot) => void }) => {
      const snapshot = makeSnapshot(options.requestId, "ready");
      options.onEvent?.(snapshot);
      return snapshot;
    });
  });

  it("issues one initial load, then retry creates new request id with forceNetwork", async () => {
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    await act(async () => {
      root.render(<App />);
      await flush();
      await flush();
      await flush();
    });

    expect(loadScoreRuntimeMock).toHaveBeenCalledTimes(1);

    const firstCall = loadScoreRuntimeMock.mock.calls[0]?.[0] as { requestId: string; forceNetwork?: boolean };
    expect(firstCall.forceNetwork).toBe(false);

    // Extra flushes should not trigger duplicate first-load attempts for the same dataset+retry key.
    await act(async () => {
      await flush();
      await flush();
    });
    expect(loadScoreRuntimeMock).toHaveBeenCalledTimes(1);

    const retryButton = container.querySelector('[data-testid="retry-score-load"]') as HTMLButtonElement | null;
    expect(retryButton).not.toBeNull();

    await act(async () => {
      retryButton?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      await flush();
      await flush();
    });

    expect(loadScoreRuntimeMock).toHaveBeenCalledTimes(2);

    const secondCall = loadScoreRuntimeMock.mock.calls[1]?.[0] as { requestId: string; forceNetwork?: boolean };
    expect(secondCall.forceNetwork).toBe(true);
    expect(secondCall.requestId).not.toBe(firstCall.requestId);

    await act(async () => {
      root.unmount();
    });
  });
});
