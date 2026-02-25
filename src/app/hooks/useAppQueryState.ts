import { useMemo } from "react";
import { type FocusSlice, type QueryState, useFilterStore } from "@/features/filters/filterStore";
import { useFilterStoreShallow } from "@/features/filters/useFilterStoreShallow";

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
  const query = useFilterStore((state) => state.query);

  const actions = useFilterStoreShallow<AppActions>(
    (state) => ({
      setStateFromUrl: state.setStateFromUrl,
      setTimeRange: state.setTimeRange,
      setPlayhead: state.setPlayhead,
      setPlaying: state.setPlaying,
      setFocus: state.setFocus,
      clearFocus: state.clearFocus,
      resetAll: state.resetAll,
      isNetworkFullscreen: state.isNetworkFullscreen,
      setNetworkFullscreen: state.setNetworkFullscreen
    })
  );

  return useMemo(() => ({ query, actions }), [actions, query]);
}
