import { useEffect, useRef } from "react";
import { deserializeQueryState, serializeQueryState, type QueryState } from "@/features/filters/filterStore";

const MANAGED_QUERY_KEYS = new Set([
  "start",
  "end",
  "playhead",
  "playing",
  "speed",
  "focusAlliance",
  "focusEdge",
  "focusEvent",
  "alliances",
  "anchors",
  "types",
  "actions",
  "sources",
  "includeInferred",
  "includeNoise",
  "evidence",
  "topXByScore",
  "sizeByScore",
  "scoreSizeContrast",
  "maxNodeRadius",
  "showFlags",
  "q",
  "sortField",
  "sortDirection"
]);

function mergeManagedSearch(existingSearch: string, managedSerialized: string): string {
  const existing = new URLSearchParams(existingSearch);
  const next = new URLSearchParams();

  for (const [key, value] of existing.entries()) {
    if (!MANAGED_QUERY_KEYS.has(key)) {
      next.append(key, value);
    }
  }

  const managed = new URLSearchParams(managedSerialized);
  for (const [key, value] of managed.entries()) {
    next.set(key, value);
  }

  return next.toString();
}

export function useUrlStateSync(query: QueryState, setStateFromUrl: (state: Partial<QueryState>) => void): void {
  const urlSyncTimerRef = useRef<number | null>(null);

  useEffect(() => {
    setStateFromUrl(deserializeQueryState(window.location.search));
  }, [setStateFromUrl]);

  useEffect(() => {
    if (urlSyncTimerRef.current !== null) {
      window.clearTimeout(urlSyncTimerRef.current);
      urlSyncTimerRef.current = null;
    }

    if (query.playback.isPlaying) {
      return;
    }

    urlSyncTimerRef.current = window.setTimeout(() => {
      const serialized = serializeQueryState({
        ...query,
        playback: {
          ...query.playback,
          isPlaying: false
        }
      });
      const merged = mergeManagedSearch(window.location.search, serialized);
      const nextSearch = merged ? `?${merged}` : "";
      if (window.location.search !== nextSearch) {
        window.history.replaceState({}, "", `${window.location.pathname}${nextSearch}`);
      }
    }, 180);

    return () => {
      if (urlSyncTimerRef.current !== null) {
        window.clearTimeout(urlSyncTimerRef.current);
        urlSyncTimerRef.current = null;
      }
    };
  }, [query]);
}
