import { useCallback } from "react";
import { selectTimelapseIndexes, selectTimelapsePulse, type TimelapseDataBundle } from "@/domain/timelapse/loader";
import type { PulsePoint } from "@/domain/timelapse/selectors";
import type { QueryState } from "@/features/filters/filterStore";
import { useAsyncWorkerRequest } from "@/app/hooks/useAsyncWorkerRequest";

export function useTimelapseWorkerSelection(params: {
  bundle: TimelapseDataBundle | null;
  baseQuery: QueryState;
  onError: (message: string) => void;
}) {
  const { bundle, baseQuery, onError } = params;
  const enabled = Boolean(bundle);
  const selectionRequest = useCallback(
    async () => Array.from(await selectTimelapseIndexes(baseQuery)),
    [baseQuery]
  );
  const pulseRequest = useCallback(
    () => selectTimelapsePulse(baseQuery, 280, null),
    [baseQuery]
  );

  const { data: scopedSelectionIndexes } = useAsyncWorkerRequest<number[]>({
    enabled,
    dependencies: [enabled, baseQuery, selectionRequest],
    initialData: [],
    request: selectionRequest,
    formatError: (reason) => {
      const message = reason instanceof Error ? reason.message : "Unknown worker selection error";
      return `[timelapse] Selection pipeline failed: ${message}`;
    },
    onError
  });

  const { data: pulse } = useAsyncWorkerRequest<PulsePoint[]>({
    enabled,
    dependencies: [enabled, baseQuery, pulseRequest],
    initialData: [],
    request: pulseRequest,
    formatError: (reason) => {
      const message = reason instanceof Error ? reason.message : "Unknown worker pulse error";
      return `[timelapse] Pulse pipeline failed: ${message}`;
    },
    onError
  });

  return {
    scopedSelectionIndexes,
    pulse
  };
}
