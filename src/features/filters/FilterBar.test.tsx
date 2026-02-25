// @vitest-environment jsdom

import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import type { TimelapseIndices } from "@/domain/timelapse/selectors";
import { FilterBar } from "@/features/filters/FilterBar";
import { serializeQueryState, useFilterStore, type QueryState } from "@/features/filters/filterStore";

declare global {
  var IS_REACT_ACT_ENVIRONMENT: boolean | undefined;
}

globalThis.IS_REACT_ACT_ENVIRONMENT = true;

function makeDefaultQuery(): QueryState {
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
      showFlags: false
    },
    textQuery: "",
    sort: { field: "timestamp", direction: "desc" }
  };
}

function makeNonDefaultQuery(): QueryState {
  return {
    time: { start: "2021-01-01T00:00:00.000Z", end: "2021-02-01T00:00:00.000Z" },
    playback: { playhead: "2021-01-15T00:00:00.000Z", isPlaying: true, speed: 8 },
    focus: { allianceId: 5, edgeKey: "5-10", eventId: "evt-11" },
    filters: {
      alliances: [5, 10],
      anchoredAllianceIds: [10],
      treatyTypes: ["mdp"],
      actions: ["signed"],
      sources: ["archive"],
      includeInferred: false,
      includeNoise: false,
      evidenceMode: "one-confirmed",
      topXByScore: 10,
      sizeByScore: true,
      showFlags: true
    },
    textQuery: "constraint",
    sort: { field: "source", direction: "asc" }
  };
}

const TEST_INDICES: TimelapseIndices = {
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
};

afterEach(() => {
  useFilterStore.setState({ query: makeDefaultQuery() });
  document.body.innerHTML = "";
});

describe("FilterBar reset interaction", () => {
  it("clears all active constraints when clicking Reset Filters", () => {
    useFilterStore.setState({ query: makeNonDefaultQuery() });

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    act(() => {
      root.render(
        <FilterBar indices={TEST_INDICES} timelineTicks={["2021-01-01T00:00:00.000Z"]} hasScoreData hasScoreRankData />
      );
    });

    const resetButton = Array.from(container.querySelectorAll("button")).find(
      (button) => button.textContent?.trim() === "Reset Filters"
    );
    expect(resetButton).toBeTruthy();

    act(() => {
      resetButton?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    });

    expect(useFilterStore.getState().query).toEqual(makeDefaultQuery());
    expect(serializeQueryState(useFilterStore.getState().query)).toBe("");

    act(() => {
      root.unmount();
    });
  });
});

