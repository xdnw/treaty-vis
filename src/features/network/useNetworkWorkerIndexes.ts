import { useCallback } from "react";
import { selectTimelapseNetworkEventIndexes } from "@/domain/timelapse/loader";
import type { QueryState } from "@/features/filters/filterStore";
import { useAsyncWorkerRequest } from "@/app/hooks/useAsyncWorkerRequest";

export function useNetworkWorkerIndexes(params: {
  baseQuery: QueryState;
  playhead: string | null;
  maxEdges: number;
}) {
  const { baseQuery, playhead, maxEdges } = params;
  const request = useCallback(
    async () => Array.from(await selectTimelapseNetworkEventIndexes(baseQuery, playhead, maxEdges)),
    [baseQuery, maxEdges, playhead]
  );

  const { data: workerEdgeEventIndexes, error: workerError } = useAsyncWorkerRequest<number[] | null>({
    dependencies: [baseQuery, playhead, maxEdges, request],
    initialData: null as number[] | null,
    request,
    formatError: (reason) => {
      const message = reason instanceof Error ? reason.message : "Unknown network worker error";
      return `[network] Worker pipeline failed: ${message}`;
    }
  });

  return {
    workerEdgeEventIndexes,
    workerError
  };
}
