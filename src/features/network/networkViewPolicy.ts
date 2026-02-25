import type { QueryState } from "@/features/filters/filterStore";
import type { FlagPressureLevel } from "@/features/network/flagRender";

export const FLAG_PRESSURE_SCORE_ELEVATED_TRIGGER = 5;
export const FLAG_PRESSURE_SCORE_ELEVATED_RECOVER = 1;
export const FLAG_PRESSURE_SCORE_CRITICAL_TRIGGER = 7;
export const FLAG_PRESSURE_SCORE_CRITICAL_RECOVER = 3;

export function derivePressureLevel(score: number, current: FlagPressureLevel): FlagPressureLevel {
  if (current === "none") {
    return score >= FLAG_PRESSURE_SCORE_ELEVATED_TRIGGER ? "elevated" : "none";
  }

  if (current === "elevated") {
    if (score >= FLAG_PRESSURE_SCORE_CRITICAL_TRIGGER) {
      return "critical";
    }
    return score <= FLAG_PRESSURE_SCORE_ELEVATED_RECOVER ? "none" : "elevated";
  }

  return score <= FLAG_PRESSURE_SCORE_CRITICAL_RECOVER ? "elevated" : "critical";
}

type BuildHoverResetKeyOptions = {
  sizeByScore: boolean;
  scoreSizeContrast: number;
  maxNodeRadius: number;
  maxEdges: number;
  allEventsLength: number;
  scopedIndexes: number[];
  playhead?: string | null;
};

export function buildHoverResetKey(baseQuery: QueryState, options: BuildHoverResetKeyOptions): string {
  const scopedFingerprint =
    options.scopedIndexes.length > 0
      ? `${options.scopedIndexes.length}:${options.scopedIndexes[0] ?? ""}:${options.scopedIndexes[options.scopedIndexes.length - 1] ?? ""}`
      : "0";

  return [
    baseQuery.time.start ?? "",
    baseQuery.time.end ?? "",
    [...baseQuery.filters.alliances].sort((left, right) => left - right).join(","),
    [...baseQuery.filters.treatyTypes].sort((left, right) => left.localeCompare(right)).join(","),
    [...baseQuery.filters.actions].sort((left, right) => left.localeCompare(right)).join(","),
    [...baseQuery.filters.sources].sort((left, right) => left.localeCompare(right)).join(","),
    baseQuery.filters.includeInferred ? "1" : "0",
    baseQuery.filters.includeNoise ? "1" : "0",
    baseQuery.filters.evidenceMode,
    options.sizeByScore ? "1" : "0",
    String(options.scoreSizeContrast),
    String(options.maxNodeRadius),
    baseQuery.textQuery.trim().toLowerCase(),
    baseQuery.sort.field,
    baseQuery.sort.direction,
    String(options.maxEdges),
    String(options.allEventsLength),
    scopedFingerprint
  ].join("|");
}

export function shouldExitNetworkFullscreenOnEscape(eventKey: string, isNetworkFullscreen: boolean): boolean {
  return isNetworkFullscreen && eventKey === "Escape";
}

export function shouldForceNodeLabel(
  isFullscreen: boolean,
  forceFullscreenLabels: boolean,
  isPriorityNode: boolean
): boolean {
  if (isFullscreen && forceFullscreenLabels) {
    return true;
  }
  return isPriorityNode;
}

export function quantizePlayheadIndexForAutoplay(index: number, speed: number): number {
  if (!Number.isFinite(index) || index < 0) {
    return 0;
  }
  const normalizedSpeed = Number.isFinite(speed) && speed > 0 ? speed : 1;
  if (normalizedSpeed <= 8) {
    return Math.floor(index);
  }

  const step = normalizedSpeed >= 32 ? 4 : normalizedSpeed >= 16 ? 3 : 2;
  return Math.floor(Math.floor(index) / step) * step;
}