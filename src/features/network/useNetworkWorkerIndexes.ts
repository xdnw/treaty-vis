import { useEffect, useRef, useState } from "react";
import { selectTimelapseNetworkEventIndexes } from "@/domain/timelapse/loader";
import type { WorkerNetworkLayout } from "@/domain/timelapse/workerProtocol";
import type { QueryState } from "@/features/filters/filterStore";
import { markTimelapsePerf } from "@/lib/perf";

type WorkerNetworkIndexes = {
  edgeEventIndexes: number[];
  layout: WorkerNetworkLayout;
};

type NetworkRequestLifecycle = {
  requestId: number;
  requestedAt: number;
  startedAt: number;
  finishedAt: number;
};

type RequestSnapshot = {
  requestId: number;
  requestedAt: number;
  baseQuery: QueryState;
  playhead: string | null;
  maxEdges: number;
};

export function useNetworkWorkerIndexes(params: {
  baseQuery: QueryState;
  playhead: string | null;
  maxEdges: number;
}) {
  const { baseQuery, playhead, maxEdges } = params;
  const [workerNetworkIndexes, setWorkerNetworkIndexes] = useState<WorkerNetworkIndexes | null>(null);
  const [workerError, setWorkerError] = useState<string | null>(null);
  const [requestLifecycle, setRequestLifecycle] = useState<NetworkRequestLifecycle | null>(null);
  const requestCounterRef = useRef(0);
  const inFlightRef = useRef(false);
  const pendingRef = useRef<RequestSnapshot | null>(null);
  const latestAcceptedRequestIdRef = useRef(0);
  const isMountedRef = useRef(true);
  isMountedRef.current = true;
  const startRequestRef = useRef<(snapshot: RequestSnapshot) => void>(() => {});

  startRequestRef.current = (snapshot: RequestSnapshot) => {
    const run = async () => {
      inFlightRef.current = true;
      try {
        const response = await selectTimelapseNetworkEventIndexes(snapshot.baseQuery, snapshot.playhead, snapshot.maxEdges);

        if (!isMountedRef.current || snapshot.requestId < latestAcceptedRequestIdRef.current) {
          return;
        }
        latestAcceptedRequestIdRef.current = snapshot.requestId;

        const queueAge = Math.max(0, response.startedAt - snapshot.requestedAt);
        const turnaround = Math.max(0, response.finishedAt - response.startedAt);
        markTimelapsePerf("network.worker.queueAge", queueAge);
        markTimelapsePerf("network.worker.turnaround", turnaround);

        setRequestLifecycle({
          requestId: snapshot.requestId,
          requestedAt: snapshot.requestedAt,
          startedAt: response.startedAt,
          finishedAt: response.finishedAt
        });
        setWorkerNetworkIndexes({
          edgeEventIndexes: Array.from(response.edgeEventIndexes),
          layout: response.layout
        });
        setWorkerError(null);
      } catch (reason) {
        const message = reason instanceof Error ? reason.message : "Unknown network worker error";
        if (!isMountedRef.current || message === "Superseded network request") {
          return;
        }
        setWorkerError(`[network] Worker pipeline failed: ${message}`);
      } finally {
        inFlightRef.current = false;
        if (!isMountedRef.current) {
          return;
        }
        const next = pendingRef.current;
        if (!next) {
          return;
        }
        pendingRef.current = null;
        startRequestRef.current(next);
      }
    };

    void run();
  };

  useEffect(() => {
    return () => {
      isMountedRef.current = false;
      pendingRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!isMountedRef.current) {
      return;
    }

    requestCounterRef.current += 1;
    const snapshot: RequestSnapshot = {
      requestId: requestCounterRef.current,
      requestedAt: performance.now(),
      baseQuery,
      playhead,
      maxEdges
    };

    if (inFlightRef.current) {
      pendingRef.current = snapshot;
    } else {
      startRequestRef.current(snapshot);
    }
  }, [baseQuery, maxEdges, playhead]);

  return {
    workerEdgeEventIndexes: workerNetworkIndexes?.edgeEventIndexes ?? null,
    workerLayout: workerNetworkIndexes?.layout ?? null,
    workerRequestLifecycle: requestLifecycle,
    workerError
  };
}
