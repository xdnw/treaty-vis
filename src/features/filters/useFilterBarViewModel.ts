import { useMemo } from "react";
import { useFilterStore, type SortDirection, type SortField } from "@/features/filters/filterStore";
import { useFilterStoreShallow } from "@/features/filters/useFilterStoreShallow";

export function useFilterBarViewModel() {
  const query = useFilterStore((state) => state.query);
  const selected = useFilterStoreShallow((state) => ({
    isNetworkFullscreen: state.isNetworkFullscreen,
    setTextQuery: state.setTextQuery,
    setSort: state.setSort,
    setTimeRange: state.setTimeRange,
    toggleAlliance: state.toggleAlliance,
    toggleAction: state.toggleAction,
    toggleTreatyType: state.toggleTreatyType,
    toggleSource: state.toggleSource,
    setIncludeInferred: state.setIncludeInferred,
    setIncludeNoise: state.setIncludeNoise,
    setEvidenceMode: state.setEvidenceMode,
    setTopXByScore: state.setTopXByScore,
    setSizeByScore: state.setSizeByScore,
    setScoreSizeContrast: state.setScoreSizeContrast,
    setMaxNodeRadius: state.setMaxNodeRadius,
    setShowFlags: state.setShowFlags,
    resetFilters: state.clearFilters
  }));

  const actions = useMemo(
    () => ({
      setTextQuery: selected.setTextQuery,
      setSort: selected.setSort,
      setTimeRange: selected.setTimeRange,
      toggleAlliance: selected.toggleAlliance,
      toggleAction: selected.toggleAction,
      toggleTreatyType: selected.toggleTreatyType,
      toggleSource: selected.toggleSource,
      setIncludeInferred: selected.setIncludeInferred,
      setIncludeNoise: selected.setIncludeNoise,
      setEvidenceMode: selected.setEvidenceMode,
      setTopXByScore: selected.setTopXByScore,
      setSizeByScore: selected.setSizeByScore,
      setScoreSizeContrast: selected.setScoreSizeContrast,
      setMaxNodeRadius: selected.setMaxNodeRadius,
      setShowFlags: selected.setShowFlags,
      resetFilters: selected.resetFilters
    }),
    [
      selected.resetFilters,
      selected.setEvidenceMode,
      selected.setIncludeInferred,
      selected.setIncludeNoise,
      selected.setMaxNodeRadius,
      selected.setScoreSizeContrast,
      selected.setShowFlags,
      selected.setSizeByScore,
      selected.setSort,
      selected.setTextQuery,
      selected.setTimeRange,
      selected.setTopXByScore,
      selected.toggleAction,
      selected.toggleAlliance,
      selected.toggleSource,
      selected.toggleTreatyType
    ]
  );

  return {
    query,
    actions,
    isNetworkFullscreen: selected.isNetworkFullscreen,
    casts: {
      toSortField: (value: string) => value as SortField,
      toSortDirection: (value: string) => value as SortDirection
    }
  };
}
