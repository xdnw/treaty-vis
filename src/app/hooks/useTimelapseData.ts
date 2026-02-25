import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { loadTimelapseBundle, type TimelapseDataBundle } from "@/domain/timelapse/loader";
import { loadScoreRuntime, type ScoreLoaderSnapshot } from "@/domain/timelapse/scoreLoader";
import type { AllianceScoresRuntime } from "@/domain/timelapse/schema";

export function useTimelapseData(showFlags: boolean, sizeByScore: boolean) {
  const [bundle, setBundle] = useState<TimelapseDataBundle | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [allianceScores, setAllianceScores] = useState<AllianceScoresRuntime | null>(null);
  const [scoreLoadSnapshot, setScoreLoadSnapshot] = useState<ScoreLoaderSnapshot | null>(null);
  const [scoreRetryNonce, setScoreRetryNonce] = useState(0);
  const scoreLoadRequestRef = useRef(0);
  const scoreAttemptKeyRef = useRef<string | null>(null);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError(null);

    loadTimelapseBundle({ showFlags })
      .then((result) => {
        if (!mounted) {
          return;
        }
        setBundle(result);
      })
      .catch((reason) => {
        if (!mounted) {
          return;
        }
        setError(reason instanceof Error ? reason.message : "Unknown loading error");
      })
      .finally(() => {
        if (mounted) {
          setLoading(false);
        }
      });

    return () => {
      mounted = false;
    };
  }, [showFlags]);

  const scoreFileDeclared = Boolean(bundle?.manifest?.files?.["alliance_scores_v2.msgpack"]);
  const hasScoreRankData = useMemo(() => {
    if (!bundle?.allianceScoreRanksByDay) {
      return false;
    }
    return Object.keys(bundle.allianceScoreRanksByDay).length > 0;
  }, [bundle?.allianceScoreRanksByDay]);

  useEffect(() => {
    if (!bundle) {
      return;
    }

    if (!scoreFileDeclared && hasScoreRankData) {
      console.warn(
        "[timelapse] Score sizing disabled: manifest missing 'alliance_scores_v2.msgpack'. " +
          "Run 'npm run data:sync' after generating score artifacts."
      );
    }
  }, [bundle, hasScoreRankData, scoreFileDeclared]);

  useEffect(() => {
    if (!bundle) {
      return;
    }

    if (sizeByScore && !scoreFileDeclared) {
      console.error(
        "[timelapse] Requested 'sizeByScore=1' but manifest does not declare 'alliance_scores_v2.msgpack'."
      );
    }
  }, [bundle, scoreFileDeclared, sizeByScore]);

  useEffect(() => {
    if (!bundle) {
      return;
    }

    setAllianceScores(null);
    setScoreLoadSnapshot(null);
    setScoreRetryNonce(0);
    scoreAttemptKeyRef.current = null;
  }, [bundle?.manifest?.datasetId]);

  useEffect(() => {
    if (!bundle?.manifest) {
      return;
    }

    if (!scoreFileDeclared) {
      return;
    }

    const attemptKey = `${bundle.manifest.datasetId}:${scoreRetryNonce}`;
    if (scoreAttemptKeyRef.current === attemptKey) {
      return;
    }
    scoreAttemptKeyRef.current = attemptKey;

    let mounted = true;
    scoreLoadRequestRef.current += 1;
    const nextRequestId = `${bundle.manifest.datasetId}:score:${scoreLoadRequestRef.current}:${scoreRetryNonce}`;

    void loadScoreRuntime({
      manifest: bundle.manifest,
      requestId: nextRequestId,
      forceNetwork: scoreRetryNonce > 0,
      onEvent: (snapshot) => {
        if (!mounted || snapshot.requestId !== nextRequestId) {
          return;
        }
        setScoreLoadSnapshot(snapshot);
        if (snapshot.runtime) {
          setAllianceScores(snapshot.runtime);
        }
      }
    });

    return () => {
      mounted = false;
    };
  }, [bundle?.manifest, scoreFileDeclared, scoreRetryNonce]);

  const retryScoreLoad = useCallback(() => {
    setScoreRetryNonce((current) => current + 1);
  }, []);

  return {
    bundle,
    loading,
    error,
    setError,
    allianceScores,
    scoreLoadSnapshot,
    retryScoreLoad,
    scoreFileDeclared,
    hasScoreRankData
  };
}
