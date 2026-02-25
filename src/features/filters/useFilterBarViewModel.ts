import { useFilterStore, type SortDirection, type SortField } from "@/features/filters/filterStore";

export function useFilterBarViewModel() {
  const query = useFilterStore((state) => state.query);
  const isNetworkFullscreen = useFilterStore((state) => state.isNetworkFullscreen);

  const actions = {
    setTextQuery: useFilterStore((state) => state.setTextQuery),
    setSort: useFilterStore((state) => state.setSort),
    setTimeRange: useFilterStore((state) => state.setTimeRange),
    toggleAlliance: useFilterStore((state) => state.toggleAlliance),
    toggleAction: useFilterStore((state) => state.toggleAction),
    toggleTreatyType: useFilterStore((state) => state.toggleTreatyType),
    toggleSource: useFilterStore((state) => state.toggleSource),
    setIncludeInferred: useFilterStore((state) => state.setIncludeInferred),
    setIncludeNoise: useFilterStore((state) => state.setIncludeNoise),
    setEvidenceMode: useFilterStore((state) => state.setEvidenceMode),
    setTopXByScore: useFilterStore((state) => state.setTopXByScore),
    setSizeByScore: useFilterStore((state) => state.setSizeByScore),
    setScoreSizeContrast: useFilterStore((state) => state.setScoreSizeContrast),
    setMaxNodeRadius: useFilterStore((state) => state.setMaxNodeRadius),
    setShowFlags: useFilterStore((state) => state.setShowFlags),
    resetFilters: useFilterStore((state) => state.clearFilters)
  };

  return {
    query,
    actions,
    isNetworkFullscreen,
    casts: {
      toSortField: (value: string) => value as SortField,
      toSortDirection: (value: string) => value as SortDirection
    }
  };
}
