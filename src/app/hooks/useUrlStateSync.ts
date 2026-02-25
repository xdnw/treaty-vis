import { useEffect, useRef } from "react";
import { deserializeQueryState, serializeQueryState, type QueryState } from "@/features/filters/filterStore";

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
      const nextSearch = serialized ? `?${serialized}` : "";
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
