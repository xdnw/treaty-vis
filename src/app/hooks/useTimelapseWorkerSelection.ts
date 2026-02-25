import { useEffect, useState } from "react";
import { selectTimelapseIndexes, selectTimelapsePulse, type TimelapseDataBundle } from "@/domain/timelapse/loader";
import type { PulsePoint } from "@/domain/timelapse/selectors";
import type { QueryState } from "@/features/filters/filterStore";

export function useTimelapseWorkerSelection(params: {
  bundle: TimelapseDataBundle | null;
  baseQuery: QueryState;
  onError: (message: string) => void;
}) {
  const { bundle, baseQuery, onError } = params;
  const [scopedSelectionIndexes, setScopedSelectionIndexes] = useState<number[]>([]);
  const [pulse, setPulse] = useState<PulsePoint[]>([]);

  useEffect(() => {
    let cancelled = false;
    if (!bundle) {
      setScopedSelectionIndexes([]);
      return;
    }

    void selectTimelapseIndexes(baseQuery)
      .then((workerIndexes) => {
        if (cancelled) {
          return;
        }
        setScopedSelectionIndexes(Array.from(workerIndexes));
      })
      .catch((reason) => {
        if (cancelled) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "Unknown worker selection error";
        onError(`[timelapse] Selection pipeline failed: ${message}`);
      });

    return () => {
      cancelled = true;
    };
  }, [baseQuery, bundle, onError]);

  useEffect(() => {
    let cancelled = false;
    if (!bundle) {
      setPulse([]);
      return;
    }

    void selectTimelapsePulse(baseQuery, 280, null)
      .then((nextPulse) => {
        if (cancelled) {
          return;
        }
        setPulse(nextPulse);
      })
      .catch((reason) => {
        if (cancelled) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "Unknown worker pulse error";
        onError(`[timelapse] Pulse pipeline failed: ${message}`);
      });

    return () => {
      cancelled = true;
    };
  }, [baseQuery, bundle, onError]);

  return {
    scopedSelectionIndexes,
    pulse
  };
}
