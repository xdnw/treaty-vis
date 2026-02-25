import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Graph from "graphology";
import Sigma from "sigma";
import type { ScoreLoaderSnapshot } from "@/domain/timelapse/scoreLoader";
import type { AllianceFlagSnapshot, AllianceScoresByDay, FlagAssetsPayload, TimelapseEvent } from "@/domain/timelapse/schema";
import { resolveScoreRowForPlayhead } from "@/domain/timelapse/scoreDay";
import { type QueryState, useFilterStore } from "@/features/filters/filterStore";
import { dampPosition, positionForNode, type Point } from "@/features/network/layout";
import {
  FLAG_MAX_SPRITES,
  FLAG_PRESSURE_BUILD_MS,
  FLAG_PRESSURE_REFRESH_MS,
  deriveFlagRenderMode,
  resolveAtlasSprite,
  resolveSpriteNodeIds,
  type FlagPressureLevel,
  type FlagRenderMode
} from "@/features/network/flagRender";
import {
  buildHoverResetKey,
  derivePressureLevel,
  quantizePlayheadIndexForAutoplay,
  shouldForceNodeLabel
} from "@/features/network/networkViewPolicy";
import { pushNetworkFlagDiagnostic } from "@/features/network/networkViewDiagnostics";
import {
  applyScoreContrast,
  colorWithOpacity,
  degreeRadius,
  resolveAllianceScoreWithFallback,
  scoreRadiusWithContrast,
  DEFAULT_MAX_NODE_RADIUS
} from "@/features/network/networkGraphMath";
import { NetworkAllianceHint, type NetworkAllianceHintData } from "@/features/network/NetworkAllianceHint";
import { NetworkViewPanel } from "@/features/network/NetworkViewPanel";
import { EDGE_FALLBACK_COLOR, TREATY_TYPE_STYLES } from "@/features/network/networkViewLegend";
import { useNetworkWorkerIndexes } from "@/features/network/useNetworkWorkerIndexes";
import { markTimelapsePerf } from "@/lib/perf";

const LABEL_TEXT_COLOR = "#1f2937";
const BASE_EDGE_OPACITY = 0.62;
const FOCUSED_ADJACENT_EDGE_OPACITY = 0.84;
const HIGHLIGHTED_EDGE_OPACITY = 0.95;
const NON_ANCHORED_DAMPING = 0.22;
const ZOOM_BAND_ZOOMED_IN_MAX_RATIO = 1.25;
const ZOOM_BAND_MID_MAX_RATIO = 2.35;
const FLAG_SIZE_TO_NODE_RATIO = 1.45;
const FLAG_MIN_DRAW_SIZE = 10;

type ZoomBand = "zoomed-out" | "mid" | "zoomed-in";


type Props = {
  allEvents: TimelapseEvent[];
  scopedIndexes: number[];
  baseQuery: QueryState;
  playhead: string | null;
  focusedAllianceId: number | null;
  focusedEdgeKey: string | null;
  sizeByScore: boolean;
  scoreSizeContrast: number;
  maxNodeRadius: number;
  showFlags: boolean;
  flagAssetsPayload: FlagAssetsPayload | null;
  allianceScoresByDay: AllianceScoresByDay | null;
  allianceScoreDays: string[];
  scoreLoadSnapshot: ScoreLoaderSnapshot | null;
  scoreManifestDeclared: boolean;
  onRetryScoreLoad: () => void;
  resolveAllianceFlagAtPlayhead: (allianceId: number, playhead: string | null) => AllianceFlagSnapshot | null;
  onFocusAlliance: (allianceId: number | null) => void;
  onFocusEdge: (edgeKey: string | null) => void;
  isFullscreen: boolean;
  onEnterFullscreen: () => void;
  onExitFullscreen: () => void;
  forceFullscreenLabels: boolean;
  isPlaying: boolean;
  onFullscreenHintChange?: (hint: NetworkAllianceHintData | null) => void;
};

function calcMaxEdges(width: number, height: number): number {
  const area = Math.max(width * height, 1);
  const adaptive = Math.round(area / 2200);
  return Math.max(240, Math.min(2400, adaptive));
}

function canonicalTreatyTypeKey(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
}
export { applyScoreContrast, scoreRadiusWithContrast, DEFAULT_MAX_NODE_RADIUS };

function deriveZoomBand(cameraRatio: number): ZoomBand {
  if (cameraRatio <= ZOOM_BAND_ZOOMED_IN_MAX_RATIO) {
    return "zoomed-in";
  }
  if (cameraRatio <= ZOOM_BAND_MID_MAX_RATIO) {
    return "mid";
  }
  return "zoomed-out";
}

function abbreviateAllianceLabel(label: string): string {
  const trimmed = label.trim();
  if (trimmed.length <= 16) {
    return trimmed;
  }

  const words = trimmed.split(/\s+/).filter(Boolean);
  if (words.length >= 2) {
    const acronym = words.map((word) => word[0]?.toUpperCase() ?? "").join("");
    if (acronym.length >= 2 && acronym.length <= 8) {
      return acronym;
    }
  }

  return `${trimmed.slice(0, 13).trimEnd()}...`;
}

function displayLabelForBand(fullLabel: string, zoomBand: ZoomBand, priorityLabel: boolean): string | null {
  if (priorityLabel) {
    return fullLabel;
  }
  if (zoomBand === "zoomed-out") {
    return null;
  }
  if (zoomBand === "mid") {
    return abbreviateAllianceLabel(fullLabel);
  }
  return fullLabel;
}

type NodePayload = {
  id: string;
  fullLabel: string;
  isPriorityNode: boolean;
  degree: number;
  counterparties: number;
  treatyCount: number;
  activeTreaties: Array<{
    counterpartyId: string;
    counterpartyLabel: string;
    treatyTypes: string[];
  }>;
  flagKey: string | null;
  score: number | null;
  scoreDay: string | null;
  radius: number;
  highlighted: boolean;
  anchored: boolean;
  flagSprite: { x: number; y: number; w: number; h: number } | null;
  x: number;
  y: number;
};

type EdgePayload = {
  id: string;
  source: string;
  target: string;
  edgeKey: string;
  sourceLabel: string;
  targetLabel: string;
  treatyType: string;
  sourceType: string;
  confidence: string;
  color: string;
  highlighted: boolean;
  adjacentToFocusedAlliance: boolean;
};

type HoverPayload = {
  kind: "alliance";
  allianceId: string;
};

export type ScoreFailureDiagnostic = {
  code: string;
  title: string;
  message: string;
  actionableRetry: boolean;
  details: string | null;
};

export function deriveScoreFailureDiagnostic(params: {
  sizeByScore: boolean;
  scoreManifestDeclared: boolean;
  scoreLoadSnapshot: ScoreLoaderSnapshot | null;
}): ScoreFailureDiagnostic | null {
  const { sizeByScore, scoreManifestDeclared, scoreLoadSnapshot } = params;

  if (sizeByScore && !scoreManifestDeclared) {
    return {
      code: "manifest-missing",
      title: "Score sizing unavailable",
      message: "Manifest does not declare alliance_scores_v2.msgpack.",
      actionableRetry: false,
      details: null
    };
  }

  if (!scoreLoadSnapshot || !scoreLoadSnapshot.state.startsWith("error-")) {
    return null;
  }

  const details = [
    scoreLoadSnapshot.httpStatus === null ? null : `HTTP ${scoreLoadSnapshot.httpStatus}`,
    scoreLoadSnapshot.decodeMs === null ? null : `decode ${Math.round(scoreLoadSnapshot.decodeMs)}ms`,
    `elapsed ${Math.round(scoreLoadSnapshot.elapsedMs)}ms`
  ]
    .filter(Boolean)
    .join(" | ");

  switch (scoreLoadSnapshot.state) {
    case "error-timeout":
      return {
        code: "timeout",
        title: "Score load timed out",
        message: scoreLoadSnapshot.message ?? "Score request timed out.",
        actionableRetry: true,
        details
      };
    case "error-http":
      return {
        code: "http",
        title: "Score artifact HTTP failure",
        message: scoreLoadSnapshot.message ?? "Score artifact request returned a non-OK HTTP status.",
        actionableRetry: true,
        details
      };
    case "error-network":
      return {
        code: "network",
        title: "Score artifact network failure",
        message: scoreLoadSnapshot.message ?? "Network request for score artifact failed.",
        actionableRetry: true,
        details
      };
    case "error-worker-unavailable":
      return {
        code: "worker-unavailable",
        title: "Score decode worker unavailable",
        message: scoreLoadSnapshot.message ?? "Web Worker is unavailable in this environment.",
        actionableRetry: true,
        details
      };
    case "error-worker-failure":
      return {
        code: "worker-failure",
        title: "Score decode worker failed",
        message: scoreLoadSnapshot.message ?? "Worker failed while decoding score payload.",
        actionableRetry: true,
        details
      };
    case "error-decode":
      return {
        code: "decode",
        title: "Score payload decode failure",
        message: scoreLoadSnapshot.message ?? "Score payload could not be decoded.",
        actionableRetry: true,
        details
      };
    case "error-abort":
      return {
        code: "abort",
        title: "Score load aborted",
        message: scoreLoadSnapshot.message ?? "Score request was aborted.",
        actionableRetry: false,
        details
      };
    case "error-manifest-missing":
      return {
        code: "manifest-missing",
        title: "Score sizing unavailable",
        message: scoreLoadSnapshot.message ?? "Manifest does not declare alliance_scores_v2.msgpack.",
        actionableRetry: false,
        details
      };
    default:
      return {
        code: "unknown",
        title: "Score load failed",
        message: scoreLoadSnapshot.message ?? scoreLoadSnapshot.reasonCode ?? "Score loader failed.",
        actionableRetry: true,
        details
      };
  }
}

export function NetworkView({
  allEvents,
  scopedIndexes,
  baseQuery,
  playhead,
  focusedAllianceId,
  focusedEdgeKey,
  sizeByScore,
  scoreSizeContrast,
  maxNodeRadius,
  showFlags,
  flagAssetsPayload,
  allianceScoresByDay,
  allianceScoreDays,
  scoreLoadSnapshot,
  scoreManifestDeclared,
  onRetryScoreLoad,
  resolveAllianceFlagAtPlayhead,
  onFocusAlliance,
  onFocusEdge,
  isFullscreen,
  onEnterFullscreen,
  onExitFullscreen,
  forceFullscreenLabels,
  isPlaying,
  onFullscreenHintChange
}: Props) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const rendererRef = useRef<Sigma | null>(null);
  const graphRef = useRef<Graph | null>(null);
  const focusedAllianceRef = useRef<number | null>(focusedAllianceId);
  const focusedEdgeRef = useRef<string | null>(focusedEdgeKey);
  const onFocusAllianceRef = useRef(onFocusAlliance);
  const onFocusEdgeRef = useRef(onFocusEdge);
  const spriteLayerRef = useRef<HTMLCanvasElement | null>(null);
  const atlasImageRef = useRef<HTMLImageElement | null>(null);
  const framePressureScoreRef = useRef(0);
  const framePressureLevelRef = useRef<FlagPressureLevel>("none");
  const flagDrawMsRef = useRef(0);
  const showFlagsRef = useRef(showFlags);
  const flagAssetsRef = useRef<FlagAssetsPayload | null>(flagAssetsPayload);
  const visibleNodeIdsRef = useRef<Set<string>>(new Set());
  const warnedUnmappedTreatySignatureRef = useRef("");
  const [size, setSize] = useState({ width: 1000, height: 350 });
  const [hovered, setHovered] = useState<HoverPayload | null>(null);
  const [budgetPreset, setBudgetPreset] = useState<"auto" | "500" | "1000" | "2000" | "unlimited">("auto");
  const [cameraRatio, setCameraRatio] = useState(1);
  const [atlasReady, setAtlasReady] = useState(false);
  const [framePressureLevel, setFramePressureLevel] = useState<FlagPressureLevel>("none");
  const refreshFrameRef = useRef<number | null>(null);
  const previousPositionsRef = useRef<Map<string, Point>>(new Map());
  const anchoredAllianceIds = useFilterStore((state) => state.query.filters.anchoredAllianceIds);
  const setAnchoredAllianceIds = useFilterStore((state) => state.setAnchoredAllianceIds);
  const anchoredAllianceIdsRef = useRef<number[]>(anchoredAllianceIds);

  useEffect(() => {
    anchoredAllianceIdsRef.current = anchoredAllianceIds;
  }, [anchoredAllianceIds]);

  const anchoredAllianceIdLookup = useMemo(
    () => new Set(anchoredAllianceIds.map((allianceId) => String(allianceId))),
    [anchoredAllianceIds]
  );

  const zoomBand = useMemo(() => deriveZoomBand(cameraRatio), [cameraRatio]);

  useEffect(() => {
    framePressureLevelRef.current = framePressureLevel;
  }, [framePressureLevel]);

  useEffect(() => {
    showFlagsRef.current = showFlags;
    flagAssetsRef.current = flagAssetsPayload;
  }, [flagAssetsPayload, showFlags]);

  useEffect(() => {
    if (!showFlags || !flagAssetsPayload) {
      atlasImageRef.current = null;
      setAtlasReady(false);
      return;
    }

    const atlasSrc = flagAssetsPayload.atlas.webp || flagAssetsPayload.atlas.png;
    if (!atlasSrc) {
      atlasImageRef.current = null;
      setAtlasReady(false);
      return;
    }

    const image = new Image();
    image.decoding = "async";
    image.onload = () => {
      atlasImageRef.current = image;
      setAtlasReady(true);
    };
    image.onerror = () => {
      atlasImageRef.current = null;
      setAtlasReady(false);
    };
    image.src = atlasSrc;

    return () => {
      image.onload = null;
      image.onerror = null;
    };
  }, [flagAssetsPayload, showFlags]);

  useEffect(() => {
    focusedAllianceRef.current = focusedAllianceId;
    focusedEdgeRef.current = focusedEdgeKey;
    onFocusAllianceRef.current = onFocusAlliance;
    onFocusEdgeRef.current = onFocusEdge;
  }, [focusedAllianceId, focusedEdgeKey, onFocusAlliance, onFocusEdge]);

  useEffect(() => {
    if (!hostRef.current || typeof ResizeObserver === "undefined") {
      return;
    }
    const observer = new ResizeObserver((entries) => {
      const box = entries[0]?.contentRect;
      if (!box) {
        return;
      }
      setSize({ width: box.width, height: box.height });
    });
    observer.observe(hostRef.current);
    return () => observer.disconnect();
  }, []);

  const adaptiveBudget = useMemo(() => calcMaxEdges(size.width, size.height), [size.height, size.width]);
  const maxEdges = useMemo(
    () =>
      budgetPreset === "auto"
        ? adaptiveBudget
        : budgetPreset === "unlimited"
          ? Number.MAX_SAFE_INTEGER
          : Number(budgetPreset),
    [adaptiveBudget, budgetPreset]
  );

  const scheduleRendererRefresh = useCallback(() => {
    if (refreshFrameRef.current !== null) {
      return;
    }

    refreshFrameRef.current = window.requestAnimationFrame(() => {
      refreshFrameRef.current = null;
      rendererRef.current?.refresh();
    });
  }, []);

  const networkSelectionPlayhead = useMemo(() => {
    if (!playhead || !isPlaying || allEvents.length === 0) {
      return playhead;
    }

    let latestIndex = -1;
    for (let index = 0; index < allEvents.length; index += 1) {
      if (allEvents[index].timestamp <= playhead) {
        latestIndex = index;
      } else {
        break;
      }
    }

    if (latestIndex < 0) {
      return playhead;
    }

    const quantizedIndex = quantizePlayheadIndexForAutoplay(latestIndex, baseQuery.playback.speed);
    return allEvents[quantizedIndex]?.timestamp ?? playhead;
  }, [allEvents, baseQuery.playback.speed, isPlaying, playhead]);

  const { workerEdgeEventIndexes, workerError } = useNetworkWorkerIndexes({
    baseQuery,
    playhead: networkSelectionPlayhead,
    maxEdges
  });

  const allianceNameById = useMemo(() => {
    const latestNameById = new Map<string, string>();

    for (const event of allEvents) {
      if (playhead && event.timestamp > playhead) {
        continue;
      }

      const fromName = event.from_alliance_name.trim();
      if (fromName) {
        latestNameById.set(String(event.from_alliance_id), fromName);
      }

      const toName = event.to_alliance_name.trim();
      if (toName) {
        latestNameById.set(String(event.to_alliance_id), toName);
      }
    }

    if (latestNameById.size === 0) {
      for (const event of allEvents) {
        const fromName = event.from_alliance_name.trim();
        if (fromName) {
          latestNameById.set(String(event.from_alliance_id), fromName);
        }

        const toName = event.to_alliance_name.trim();
        if (toName) {
          latestNameById.set(String(event.to_alliance_id), toName);
        }
      }
    }

    const names = new Map<string, string>();
    for (const [id, value] of latestNameById.entries()) {
      names.set(id, value);
    }
    return names;
  }, [allEvents, playhead]);

  const edges = useMemo(() => {
    const rawEdges = (workerEdgeEventIndexes ?? []).map((eventIndex) => {
      const event = allEvents[eventIndex];
      return {
        key: `${event.pair_min_id}:${event.pair_max_id}:${event.treaty_type}`,
        eventId: event.event_id,
        sourceId: String(event.from_alliance_id),
        targetId: String(event.to_alliance_id),
        sourceLabel: event.from_alliance_name || String(event.from_alliance_id),
        targetLabel: event.to_alliance_name || String(event.to_alliance_id),
        treatyType: event.treaty_type,
        sourceType: event.source || "unknown",
        confidence: event.confidence
      };
    });

    return rawEdges.map((edge) => ({
      ...edge,
      sourceLabel: allianceNameById.get(edge.sourceId) ?? edge.sourceLabel,
      targetLabel: allianceNameById.get(edge.targetId) ?? edge.targetLabel
    }));
  }, [allEvents, allianceNameById, workerEdgeEventIndexes]);

  const hoverResetKey = useMemo(() => {
    return buildHoverResetKey(baseQuery, {
      sizeByScore,
      scoreSizeContrast,
      maxNodeRadius,
      maxEdges,
      allEventsLength: allEvents.length,
      scopedIndexes,
      playhead
    });
  }, [
    allEvents.length,
    baseQuery.filters.actions,
    baseQuery.filters.alliances,
    baseQuery.filters.evidenceMode,
    baseQuery.filters.includeInferred,
    baseQuery.filters.includeNoise,
    baseQuery.filters.sources,
    baseQuery.filters.treatyTypes,
    baseQuery.sort.direction,
    baseQuery.sort.field,
    baseQuery.textQuery,
    baseQuery.time.end,
    baseQuery.time.start,
    maxNodeRadius,
    maxEdges,
    playhead,
    scoreSizeContrast,
    scopedIndexes,
    sizeByScore
  ]);

  useEffect(() => {
    setHovered(null);
  }, [hoverResetKey]);

  const graph = useMemo(() => {
    const graphBuildStartedAt = performance.now();
    const nodeMetaById = new Map<string, { degree: number }>();
    const nodeCounterparties = new Map<
      string,
      Map<string, { counterpartyLabel: string; treatyTypes: Set<string> }>
    >();

    const addCounterparty = (nodeId: string, otherId: string, otherLabel: string, treatyType: string) => {
      const counterparties =
        nodeCounterparties.get(nodeId) ?? new Map<string, { counterpartyLabel: string; treatyTypes: Set<string> }>();
      const existing = counterparties.get(otherId) ?? {
        counterpartyLabel: otherLabel || otherId,
        treatyTypes: new Set<string>()
      };
      existing.counterpartyLabel = otherLabel || existing.counterpartyLabel;
      existing.treatyTypes.add(treatyType);
      counterparties.set(otherId, existing);
      nodeCounterparties.set(nodeId, counterparties);
    };

    for (const edge of edges) {
      const source = nodeMetaById.get(edge.sourceId) ?? { degree: 0 };
      source.degree += 1;
      nodeMetaById.set(edge.sourceId, source);

      const target = nodeMetaById.get(edge.targetId) ?? { degree: 0 };
      target.degree += 1;
      nodeMetaById.set(edge.targetId, target);

      addCounterparty(
        edge.sourceId,
        edge.targetId,
        allianceNameById.get(edge.targetId) ?? edge.targetLabel ?? edge.targetId,
        edge.treatyType
      );
      addCounterparty(
        edge.targetId,
        edge.sourceId,
        allianceNameById.get(edge.sourceId) ?? edge.sourceLabel ?? edge.sourceId,
        edge.treatyType
      );
    }

    const nodeIds = [...nodeMetaById.keys()].sort((left, right) => left.localeCompare(right));
    const focusedAlliance = focusedAllianceId === null ? null : String(focusedAllianceId);
    const priorityLabelNodeIds = new Set<string>(anchoredAllianceIdLookup);
    if (focusedAlliance !== null) {
      priorityLabelNodeIds.add(focusedAlliance);
    }
    const flagRenderMode = deriveFlagRenderMode(
      showFlags,
      flagAssetsPayload !== null,
      cameraRatio,
      nodeIds.length,
      framePressureLevel
    );
    const scoreResolution =
      sizeByScore && allianceScoresByDay
        ? resolveScoreRowForPlayhead(allianceScoresByDay, allianceScoreDays, playhead)
        : { day: null, row: null, usedFallback: false };
    const scoreDay = scoreResolution.day;
    const dayScores = scoreResolution.row;

    const scoreByNode = new Map<string, number>();
    let minVisibleScore = Number.POSITIVE_INFINITY;
    let maxVisibleScore = 0;
    if (allianceScoresByDay && dayScores) {
      for (const nodeId of nodeIds) {
        const score = resolveAllianceScoreWithFallback(nodeId, allianceScoresByDay, allianceScoreDays, scoreDay);
        if (typeof score !== "number" || !Number.isFinite(score) || score <= 0) {
          continue;
        }
        scoreByNode.set(nodeId, score);
        if (score < minVisibleScore) {
          minVisibleScore = score;
        }
        if (score > maxVisibleScore) {
          maxVisibleScore = score;
        }
      }
    }
    const useScoreSizing = Boolean(sizeByScore && dayScores && scoreByNode.size > 0 && maxVisibleScore > 0);
    const normalizedMinScore = Number.isFinite(minVisibleScore) ? minVisibleScore : 0;

    const flagImportanceByNodeId = new Map<string, number>();
    for (const nodeId of nodeIds) {
      const degree = nodeMetaById.get(nodeId)?.degree ?? 0;
      const score = scoreByNode.get(nodeId) ?? 0;
      // Weighted score keeps high-signal alliances favored while preserving deterministic fallback.
      flagImportanceByNodeId.set(nodeId, degree * 1_000 + score);
    }

    const flagResolutionNodeIds = resolveSpriteNodeIds(
      flagRenderMode,
      nodeIds,
      focusedAlliance,
      hovered?.allianceId ?? null,
      FLAG_MAX_SPRITES,
      {
        visibleNodeIds: visibleNodeIdsRef.current,
        importanceByNodeId: flagImportanceByNodeId
      }
    );

    const previousPositions = previousPositionsRef.current;
    const nextPositions = new Map(previousPositions);

    const nodes: NodePayload[] = nodeIds.map((id) => {
      const meta = nodeMetaById.get(id)!;
      const deterministicTarget = positionForNode(id);
      const previousPosition = previousPositions.get(id);
      const anchored = anchoredAllianceIdLookup.has(id);
      const pos = anchored
        ? (previousPosition ?? deterministicTarget)
        : previousPosition
          ? dampPosition(previousPosition, deterministicTarget, NON_ANCHORED_DAMPING)
          : deterministicTarget;
      nextPositions.set(id, pos);
      const highlighted = focusedAlliance !== null && focusedAlliance === id;
      const nodeScore = scoreByNode.get(id) ?? null;
      const baseRadius =
        useScoreSizing && nodeScore !== null
          ? scoreRadiusWithContrast(nodeScore, normalizedMinScore, maxVisibleScore, scoreSizeContrast, maxNodeRadius)
          : degreeRadius(meta.degree, maxNodeRadius);
      const counterparties = nodeCounterparties.get(id);
      const activeTreaties = counterparties
        ? [...counterparties.entries()]
            .map(([counterpartyId, value]) => ({
              counterpartyId,
              counterpartyLabel: value.counterpartyLabel,
              treatyTypes: [...value.treatyTypes].sort((left, right) => left.localeCompare(right))
            }))
            .sort(
              (left, right) =>
                left.counterpartyLabel.localeCompare(right.counterpartyLabel) ||
                left.counterpartyId.localeCompare(right.counterpartyId)
            )
        : [];
      const treatyCount = activeTreaties.reduce((sum, treaty) => sum + treaty.treatyTypes.length, 0);

      const numericId = Number(id);
      const flagSnapshot =
        flagRenderMode !== "off" && flagResolutionNodeIds.has(id) && Number.isFinite(numericId)
          ? resolveAllianceFlagAtPlayhead(numericId, playhead)
          : null;
      const spriteLookup = resolveAtlasSprite(flagAssetsPayload, flagSnapshot?.flagKey ?? null);
      const fullLabel = allianceNameById.get(id) ?? id;
      const isPriorityNode = priorityLabelNodeIds.has(id);

      return {
        id,
        fullLabel,
        isPriorityNode,
        degree: meta.degree,
        counterparties: activeTreaties.length,
        treatyCount,
        activeTreaties,
        flagKey: flagSnapshot?.flagKey || null,
        score: nodeScore,
        scoreDay,
        radius: highlighted ? baseRadius + 1.4 : baseRadius,
        highlighted,
        anchored,
        flagSprite: spriteLookup ? { ...spriteLookup.asset } : null,
        x: pos.x,
        y: pos.y
      };
    });

    previousPositionsRef.current = nextPositions;

    const unmappedTreatyTypes = new Set<string>();

    const links: EdgePayload[] = edges.map((edge) => {
      const treatyType = canonicalTreatyTypeKey(edge.treatyType);
      const style = TREATY_TYPE_STYLES[treatyType];
      const color = style?.color ?? EDGE_FALLBACK_COLOR;
      if (!style && treatyType) {
        unmappedTreatyTypes.add(treatyType);
      }
      const highlighted = focusedEdgeKey !== null && focusedEdgeKey === edge.key;
      const linkedToFocusedAlliance =
        focusedAlliance !== null && (edge.sourceId === focusedAlliance || edge.targetId === focusedAlliance);
      return {
        id: edge.eventId,
        source: edge.sourceId,
        target: edge.targetId,
        edgeKey: edge.key,
        sourceLabel: edge.sourceLabel,
        targetLabel: edge.targetLabel,
        treatyType,
        sourceType: edge.sourceType,
        confidence: edge.confidence,
        color,
        highlighted,
        adjacentToFocusedAlliance: linkedToFocusedAlliance
      };
    });

    const graphBuildMs = performance.now() - graphBuildStartedAt;

    return {
      adaptiveBudget,
      maxEdges,
      budgetLabel: budgetPreset === "auto" ? `Auto (${adaptiveBudget})` : budgetPreset === "unlimited" ? "Unlimited" : budgetPreset,
      renderedEdges: links.length,
      nodes,
      links,
      flagRenderMode,
      flagResolvedNodeCount: flagResolutionNodeIds.size,
      spriteNodeIds: nodes.filter((node) => node.flagSprite !== null).map((node) => node.id),
      unmappedTreatyTypes: [...unmappedTreatyTypes].sort((left, right) => left.localeCompare(right)),
      scoreSizingActive: useScoreSizing,
      scoreSizedNodeCount: scoreByNode.size,
      scoreMin: normalizedMinScore,
      scoreMax: maxVisibleScore,
      usedDayFallback: scoreResolution.usedFallback,
      scoreDay,
      graphBuildMs
    };
  }, [
    adaptiveBudget,
    allianceNameById,
    allianceScoresByDay,
    allianceScoreDays,
    anchoredAllianceIdLookup,
    budgetPreset,
    cameraRatio,
    edges,
    framePressureLevel,
    flagAssetsPayload,
    focusedAllianceId,
    focusedEdgeKey,
    maxNodeRadius,
    maxEdges,
    playhead,
    resolveAllianceFlagAtPlayhead,
    scoreSizeContrast,
    showFlags,
    sizeByScore
  ]);

  useEffect(() => {
    if (!import.meta.env.DEV || graph.unmappedTreatyTypes.length === 0) {
      return;
    }

    const signature = graph.unmappedTreatyTypes.join("|");
    if (warnedUnmappedTreatySignatureRef.current === signature) {
      return;
    }
    warnedUnmappedTreatySignatureRef.current = signature;

    console.warn("[NetworkView] Unmapped treaty types in edge color map", {
      count: graph.unmappedTreatyTypes.length,
      types: graph.unmappedTreatyTypes
    });
  }, [graph.unmappedTreatyTypes]);

  useEffect(() => {
    markTimelapsePerf("network.graph.build", graph.graphBuildMs);
    pushNetworkFlagDiagnostic({
      stage: "graph-build",
      ts: Date.now(),
      cameraRatio,
      nodeCount: graph.nodes.length,
      framePressureScore: framePressureScoreRef.current,
      framePressure: framePressureLevel !== "none",
      framePressureLevel,
      mode: graph.flagRenderMode,
      spriteCandidateCount: graph.flagResolvedNodeCount,
      graphBuildMs: Number(graph.graphBuildMs.toFixed(2))
    });
  }, [cameraRatio, framePressureLevel, graph.flagRenderMode, graph.flagResolvedNodeCount, graph.graphBuildMs, graph.nodes.length]);

  useEffect(() => {
    if (!hovered) {
      return;
    }
    const stillVisible = graph.nodes.some((node) => node.id === hovered.allianceId);
    if (!stillVisible) {
      setHovered(null);
    }
  }, [graph.nodes, hovered]);

  const hoveredAlliance = useMemo(() => {
    if (!hovered) {
      return null;
    }
    return graph.nodes.find((node) => node.id === hovered.allianceId) ?? null;
  }, [graph.nodes, hovered]);

  const hoveredFlagKey = useMemo(() => {
    if (!hoveredAlliance || graph.flagRenderMode === "off") {
      return null;
    }
    if (hoveredAlliance.flagKey) {
      return hoveredAlliance.flagKey;
    }
    const numericId = Number(hoveredAlliance.id);
    if (!Number.isFinite(numericId)) {
      return null;
    }
    return resolveAllianceFlagAtPlayhead(numericId, playhead)?.flagKey ?? null;
  }, [graph.flagRenderMode, hoveredAlliance, playhead, resolveAllianceFlagAtPlayhead]);

  useEffect(() => {
    if (!hostRef.current || rendererRef.current) {
      return;
    }

    const graphModel = new Graph({ type: "directed", multi: true, allowSelfLoops: true });
    const renderer = new Sigma(graphModel, hostRef.current, {
      renderEdgeLabels: false,
      labelRenderedSizeThreshold: 0,
      labelColor: {
        attribute: "default",
        color: LABEL_TEXT_COLOR
      },
      minCameraRatio: 0.1,
      maxCameraRatio: 8,
      defaultNodeColor: "#0c8599",
      defaultEdgeColor: EDGE_FALLBACK_COLOR,
      enableEdgeEvents: true,
      minEdgeThickness: 2.4
    });

    const spriteLayer = renderer.createCanvas("flag-sprites", {
      beforeLayer: "mouse",
      style: {
        pointerEvents: "none"
      }
    });
    spriteLayerRef.current = spriteLayer;

    const drawFlagSprites = () => {
      const ctx = spriteLayer.getContext("2d");
      if (!ctx) {
        return;
      }

      const dimensions = renderer.getDimensions();
      const dpr = window.devicePixelRatio || 1;
      const targetWidth = Math.max(1, Math.floor(dimensions.width * dpr));
      const targetHeight = Math.max(1, Math.floor(dimensions.height * dpr));
      if (spriteLayer.width !== targetWidth || spriteLayer.height !== targetHeight) {
        spriteLayer.width = targetWidth;
        spriteLayer.height = targetHeight;
        spriteLayer.style.width = `${dimensions.width}px`;
        spriteLayer.style.height = `${dimensions.height}px`;
      }

      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, dimensions.width, dimensions.height);

      const atlas = atlasImageRef.current;
      if (!atlas || graphRef.current === null || !flagAssetsRef.current || !showFlagsRef.current) {
        flagDrawMsRef.current = 0;
        return;
      }

      const drawStartedAt = performance.now();
      let drawn = 0;
      const visibleNodeIds = new Set<string>();
      for (const nodeId of graphModel.nodes()) {
        const sprite = graphModel.getNodeAttribute(nodeId, "flagSprite") as { x: number; y: number; w: number; h: number } | null;
        const display = renderer.getNodeDisplayData(nodeId);
        if (!display || display.hidden) {
          continue;
        }

        visibleNodeIds.add(nodeId);

        if (drawn >= FLAG_MAX_SPRITES || !sprite) {
          continue;
        }

        const viewport = renderer.framedGraphToViewport({ x: display.x, y: display.y });
        const nodePixelSize = renderer.scaleSize(display.size);
        const drawSize = Math.max(FLAG_MIN_DRAW_SIZE, nodePixelSize * FLAG_SIZE_TO_NODE_RATIO);
        const drawX = viewport.x - drawSize / 2;
        const drawY = viewport.y - drawSize / 2;

        ctx.drawImage(atlas, sprite.x, sprite.y, sprite.w, sprite.h, drawX, drawY, drawSize, drawSize);
        drawn += 1;
      }

      visibleNodeIdsRef.current = visibleNodeIds;

      const drawMs = performance.now() - drawStartedAt;
      flagDrawMsRef.current = Number(drawMs.toFixed(2));
      markTimelapsePerf("network.flagSprites.draw", drawMs);
    };

    renderer.on("afterRender", drawFlagSprites);

    const camera = renderer.getCamera();
    const syncCameraRatio = () => {
      setCameraRatio(camera.getState().ratio);
    };
    camera.on("updated", syncCameraRatio);
    syncCameraRatio();

    renderer.on("clickNode", ({ node, event }: { node: string; event?: { original?: MouseEvent | TouchEvent } }) => {
      const nodeId = Number(node);
      if (!Number.isFinite(nodeId)) {
        return;
      }

      const isShiftToggle =
        event?.original !== undefined && "shiftKey" in event.original && Boolean(event.original.shiftKey);
      if (isShiftToggle) {
        const nextAnchors = new Set(anchoredAllianceIdsRef.current);
        if (nextAnchors.has(nodeId)) {
          nextAnchors.delete(nodeId);
        } else {
          nextAnchors.add(nodeId);
        }
        setAnchoredAllianceIds([...nextAnchors]);
        return;
      }

      onFocusAllianceRef.current(focusedAllianceRef.current === nodeId ? null : nodeId);
    });

    renderer.on("clickEdge", ({ edge }: { edge: string }) => {
      const edgeKey = graphModel.getEdgeAttribute(edge, "edgeKey") as string;
      onFocusEdgeRef.current(focusedEdgeRef.current === edgeKey ? null : edgeKey);
    });

    renderer.on("clickStage", () => {
      onFocusAllianceRef.current(null);
      onFocusEdgeRef.current(null);
    });

    renderer.on("enterNode", ({ node }: { node: string }) => {
      setHovered({
        kind: "alliance",
        allianceId: node
      });
    });

    renderer.on("leaveNode", () => {});

    renderer.on("enterEdge", ({ edge }: { edge: string }) => {
      const sourceNode = graphModel.source(edge);
      if (!sourceNode) {
        return;
      }
      setHovered({ kind: "alliance", allianceId: String(sourceNode) });
    });

    renderer.on("leaveEdge", () => {});

    graphRef.current = graphModel;
    rendererRef.current = renderer;

    return () => {
      if (refreshFrameRef.current !== null) {
        window.cancelAnimationFrame(refreshFrameRef.current);
        refreshFrameRef.current = null;
      }
      if (typeof (camera as { off?: (event: "updated", handler: () => void) => void }).off === "function") {
        (camera as { off: (event: "updated", handler: () => void) => void }).off("updated", syncCameraRatio);
      }
      renderer.off("afterRender", drawFlagSprites);
      renderer.kill();
      spriteLayerRef.current = null;
      rendererRef.current = null;
      graphRef.current = null;
    };
  }, [setAnchoredAllianceIds]);

  useEffect(() => {
    const renderer = rendererRef.current;
    if (!renderer) {
      return;
    }

    const labelThreshold = zoomBand === "zoomed-out" ? 8 : zoomBand === "mid" ? 3 : 0;
    const labelSize = zoomBand === "mid" ? 12 : 14;
    renderer.setSetting("labelRenderedSizeThreshold", labelThreshold);
    renderer.setSetting("labelSize", labelSize);
    scheduleRendererRefresh();
  }, [scheduleRendererRefresh, zoomBand]);

  useEffect(() => {
    const renderer = rendererRef.current;
    if (!renderer) {
      return;
    }
    scheduleRendererRefresh();
  }, [atlasReady, scheduleRendererRefresh]);

  useEffect(() => {
    const renderer = rendererRef.current;
    const graphModel = graphRef.current;
    if (!renderer || !graphModel) {
      return;
    }

    for (const node of graph.nodes) {
      if (!graphModel.hasNode(node.id)) {
        continue;
      }

      const isHovered = hovered?.allianceId === node.id;
      const forceLabel = shouldForceNodeLabel(
        isFullscreen,
        forceFullscreenLabels,
        node.isPriorityNode || isHovered
      );
      const displayLabel = displayLabelForBand(node.fullLabel, zoomBand, forceLabel);

      if (displayLabel) {
        graphModel.setNodeAttribute(node.id, "label", displayLabel);
      } else {
        graphModel.removeNodeAttribute(node.id, "label");
      }

      if (forceLabel) {
        graphModel.setNodeAttribute(node.id, "forceLabel", true);
      } else {
        graphModel.removeNodeAttribute(node.id, "forceLabel");
      }
    }

    scheduleRendererRefresh();
  }, [forceFullscreenLabels, graph.nodes, hovered?.allianceId, isFullscreen, scheduleRendererRefresh, zoomBand]);

  useEffect(() => {
    const renderer = rendererRef.current;
    const graphModel = graphRef.current;
    if (!renderer || !graphModel) {
      return;
    }

    graphModel.clear();

    for (const node of graph.nodes) {
      const forceLabel = shouldForceNodeLabel(isFullscreen, forceFullscreenLabels, node.isPriorityNode);
      const displayLabel = displayLabelForBand(node.fullLabel, zoomBand, forceLabel);
      const nodeAttributes: Record<string, unknown> = {
        x: node.x,
        y: node.y,
        fullLabel: node.fullLabel,
        isPriorityNode: node.isPriorityNode,
        treatyCount: node.treatyCount,
        counterparties: node.counterparties,
        size: node.radius,
        score: node.score,
        scoreDay: node.scoreDay,
        color: node.highlighted ? "#2b8a3e" : node.anchored ? "#d9480f" : "#0c8599"
      };
      if (displayLabel) {
        nodeAttributes.label = displayLabel;
      }
      if (forceLabel) {
        nodeAttributes.forceLabel = true;
      }
      if (graph.flagRenderMode !== "off" && node.flagSprite) {
        nodeAttributes.flagSprite = node.flagSprite;
      }
      graphModel.addNode(node.id, nodeAttributes);
    }

    for (const edge of graph.links) {
      if (!graphModel.hasNode(edge.source) || !graphModel.hasNode(edge.target)) {
        continue;
      }
      const edgeOpacity = edge.highlighted
        ? HIGHLIGHTED_EDGE_OPACITY
        : edge.adjacentToFocusedAlliance
          ? FOCUSED_ADJACENT_EDGE_OPACITY
          : BASE_EDGE_OPACITY;
      graphModel.addDirectedEdgeWithKey(edge.id, edge.source, edge.target, {
        size: edge.highlighted ? 3.2 : edge.adjacentToFocusedAlliance ? 2 : 1.2,
        color: colorWithOpacity(edge.color, edgeOpacity),
        zIndex: edge.highlighted ? 3 : edge.adjacentToFocusedAlliance ? 2 : 1,
        edgeKey: edge.edgeKey,
        sourceLabel: edge.sourceLabel,
        targetLabel: edge.targetLabel,
        treatyType: edge.treatyType,
        sourceType: edge.sourceType,
        confidence: edge.confidence
      });
    }

    scheduleRendererRefresh();
    const refreshMs = flagDrawMsRef.current;
    markTimelapsePerf("network.renderer.refresh", refreshMs);

    const overBudget = refreshMs > FLAG_PRESSURE_REFRESH_MS || graph.graphBuildMs > FLAG_PRESSURE_BUILD_MS;
    framePressureScoreRef.current += overBudget ? 1 : -1;
    framePressureScoreRef.current = Math.max(-12, Math.min(12, framePressureScoreRef.current));

    const nextPressureLevel = derivePressureLevel(framePressureScoreRef.current, framePressureLevelRef.current);
    if (nextPressureLevel !== framePressureLevelRef.current) {
      setFramePressureLevel(nextPressureLevel);
    }

    pushNetworkFlagDiagnostic({
      stage: "refresh",
      ts: Date.now(),
      cameraRatio,
      nodeCount: graph.nodes.length,
      framePressureScore: framePressureScoreRef.current,
      framePressure: nextPressureLevel !== "none",
      framePressureLevel: nextPressureLevel,
      mode: graph.flagRenderMode,
      spriteCandidateCount: graph.flagResolvedNodeCount,
      refreshMs: Number(refreshMs.toFixed(2)),
      graphBuildMs: Number(graph.graphBuildMs.toFixed(2))
    });
  }, [
    cameraRatio,
    forceFullscreenLabels,
    graph.flagRenderMode,
    graph.flagResolvedNodeCount,
    graph.graphBuildMs,
    graph.links,
    graph.nodes,
    isFullscreen,
    scheduleRendererRefresh,
    zoomBand
  ]);

  useEffect(() => {
    const renderer = rendererRef.current;
    if (!renderer) {
      return;
    }
    renderer.resize();
    scheduleRendererRefresh();
  }, [scheduleRendererRefresh, size.height, size.width]);

  const focusedDetail = useMemo(() => {
    if (focusedAllianceId === null) {
      return null;
    }
    const node = graph.nodes.find((item) => item.id === String(focusedAllianceId));
    if (!node) {
      return `Focused alliance ${focusedAllianceId} is outside the current network scope.`;
    }
    return `Focused Alliance: ${node.fullLabel} | Treaties: ${node.treatyCount} | Counterparties: ${node.counterparties}`;
  }, [focusedAllianceId, graph.nodes]);

  const hoveredHintData = useMemo<NetworkAllianceHintData | null>(() => {
    if (!hoveredAlliance) {
      return null;
    }
    return {
      id: hoveredAlliance.id,
      fullLabel: hoveredAlliance.fullLabel,
      score: hoveredAlliance.score,
      scoreDay: hoveredAlliance.scoreDay,
      treatyCount: hoveredAlliance.treatyCount,
      counterparties: hoveredAlliance.counterparties,
      flagKey: hoveredFlagKey,
      activeTreaties: hoveredAlliance.activeTreaties
    };
  }, [hoveredAlliance, hoveredFlagKey]);

  useEffect(() => {
    if (!onFullscreenHintChange) {
      return;
    }
    if (!isFullscreen) {
      onFullscreenHintChange(null);
      return;
    }
    onFullscreenHintChange(hoveredHintData);
  }, [hoveredHintData, isFullscreen, onFullscreenHintChange]);

  const scoreStatusRows = useMemo(
    () =>
      deriveScoreFailureDiagnostic({
        sizeByScore,
        scoreManifestDeclared,
        scoreLoadSnapshot
      }),
    [scoreLoadSnapshot, scoreManifestDeclared, sizeByScore]
  );

  return (
    <section className={isFullscreen ? "panel relative h-full w-full rounded-xl p-2 md:p-3" : "panel p-4"}>
      <NetworkViewPanel
        isFullscreen={isFullscreen}
        graph={graph}
        budgetPreset={budgetPreset}
        anchoredCount={anchoredAllianceIds.length}
        onBudgetChange={setBudgetPreset}
        onClearAnchors={() => setAnchoredAllianceIds([])}
        onEnterFullscreen={onEnterFullscreen}
        onExitFullscreen={onExitFullscreen}
        scoreStatusRows={scoreStatusRows}
        onRetryScoreLoad={onRetryScoreLoad}
      />
      {!isFullscreen && workerError ? (
        <div className="mb-2 rounded border border-rose-200 bg-rose-50 px-2 py-2 text-[11px] text-rose-900">{workerError}</div>
      ) : null}
      <div
        ref={hostRef}
        className={isFullscreen ? "h-full overflow-hidden rounded-xl border border-slate-200" : "h-[350px] overflow-hidden rounded-xl border border-slate-200"}
      />
      {!isFullscreen && hoveredHintData ? (
        <NetworkAllianceHint hint={hoveredHintData} flagAssetsPayload={flagAssetsPayload} className="mt-2 rounded-md border border-slate-300 bg-slate-50 p-3 text-xs text-slate-700" />
      ) : null}
      {!isFullscreen && focusedDetail ? <div className="mt-2 text-xs text-slate-700">{focusedDetail}</div> : null}
      {!isFullscreen ? <p className="mt-2 text-xs text-muted">
        LOD budget controls rendered edge count to keep interaction responsive on large graphs.
      </p> : null}
      {!isFullscreen ? <p className="text-xs text-muted">Shift+click a node to toggle an anchor while keeping regular click-to-focus behavior.</p> : null}
    </section>
  );
}
