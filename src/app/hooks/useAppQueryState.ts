import { useMemo } from "react";
import { type FocusSlice, type QueryState, useFilterStore } from "@/features/filters/filterStore";

type AppActions = {
  setTimeRange: (start: string | null, end: string | null) => void;
  setPlayhead: (playhead: string | null) => void;
  setPlaying: (isPlaying: boolean) => void;
  setFocus: (focus: Partial<FocusSlice>) => void;
  clearFocus: () => void;
  resetAll: () => void;
  isNetworkFullscreen: boolean;
  setNetworkFullscreen: (value: boolean) => void;
  setStateFromUrl: (state: Partial<QueryState>) => void;
};

export function useAppQueryState(): { query: QueryState; actions: AppActions } {
  const time = useFilterStore((state) => state.query.time);
  const playback = useFilterStore((state) => state.query.playback);
  const focus = useFilterStore((state) => state.query.focus);
  const filters = useFilterStore((state) => state.query.filters);
  const textQuery = useFilterStore((state) => state.query.textQuery);
  const sort = useFilterStore((state) => state.query.sort);

  const actions = {
    setStateFromUrl: useFilterStore((state) => state.setStateFromUrl),
    setTimeRange: useFilterStore((state) => state.setTimeRange),
    setPlayhead: useFilterStore((state) => state.setPlayhead),
    setPlaying: useFilterStore((state) => state.setPlaying),
    setFocus: useFilterStore((state) => state.setFocus),
    clearFocus: useFilterStore((state) => state.clearFocus),
    resetAll: useFilterStore((state) => state.resetAll),
    isNetworkFullscreen: useFilterStore((state) => state.isNetworkFullscreen),
    setNetworkFullscreen: useFilterStore((state) => state.setNetworkFullscreen)
  };

  const query = useMemo<QueryState>(
    () => ({
      time,
      playback,
      focus,
      filters,
      textQuery,
      sort
    }),
    [filters, focus, playback, sort, textQuery, time]
  );

  return { query, actions };
}
