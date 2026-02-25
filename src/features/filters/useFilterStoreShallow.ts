import { useShallow } from "zustand/react/shallow";
import { useFilterStore, type FilterStore } from "@/features/filters/filterStore";

export function useFilterStoreShallow<T>(selector: (state: FilterStore) => T): T {
  return useFilterStore(useShallow(selector));
}
