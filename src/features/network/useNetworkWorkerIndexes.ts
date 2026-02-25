import { useEffect, useRef, useState } from "react";
import { selectTimelapseNetworkEventIndexes } from "@/domain/timelapse/loader";
import type { QueryState } from "@/features/filters/filterStore";

export function useNetworkWorkerIndexes(params: {
  baseQuery: QueryState;
  playhead: string | null;
  maxEdges: number;
}) {
  const { baseQuery, playhead, maxEdges } = params;
  const [workerEdgeEventIndexes, setWorkerEdgeEventIndexes] = useState<number[] | null>(null);
  const [workerError, setWorkerError] = useState<string | null>(null);
  const requestRef = useRef(0);

  useEffect(() => {
    requestRef.current += 1;
    const requestId = requestRef.current;
    setWorkerError(null);

    void selectTimelapseNetworkEventIndexes(baseQuery, playhead, maxEdges)
      .then((workerIndexes) => {
        if (requestRef.current !== requestId) {
          return;
        }
        setWorkerEdgeEventIndexes(Array.from(workerIndexes));
      })
      .catch((reason) => {
        if (requestRef.current !== requestId) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "Unknown network worker error";
        setWorkerError(`[network] Worker pipeline failed: ${message}`);
      });
  }, [baseQuery, maxEdges, playhead]);

  return {
    workerEdgeEventIndexes,
    workerError
  };
}
