import { useEffect, useMemo, useRef, useState } from "react";
import Graph from "graphology";
import Sigma from "sigma";
import type { AllianceFlagSnapshot, AllianceScoresByDay, FlagAssetsPayload, TimelapseEvent } from "@/domain/timelapse/schema";
import { selectTimelapseNetworkEventIndexes } from "@/domain/timelapse/loader";
import { deriveNetworkEdges } from "@/domain/timelapse/selectors";
import { type QueryState, useFilterStore } from "@/features/filters/filterStore";
import { dampPosition, positionForNode, type Point } from "@/features/network/layout";
import {
  FLAG_MAX_SPRITES,
  FLAG_PRESSURE_BUILD_MS,
  FLAG_PRESSURE_REFRESH_MS,
  deriveFlagRenderMode,
  resolveAtlasSprite,
  resolveSpriteNodeIds,
  type FlagRenderMode
} from "@/features/network/flagRender";
import { markTimelapsePerf } from "@/lib/perf";

const TREATY_EDGE_COLORS: Record<string, string> = {
  MDP: "#0b7285",
  ODP: "#5f3dc4",
  NAP: "#2b8a3e",
  PIAT: "#364fc7",
  protectorate: "#d9480f"
};

const LABEL_TEXT_COLOR = "#1f2937";
const EDGE_FALLBACK_COLOR = "#7f8ca3";
const MIN_NODE_RADIUS = 5;
const MAX_NODE_RADIUS = 12;
const NON_ANCHORED_DAMPING = 0.22;
const ZOOM_BAND_ZOOMED_IN_MAX_RATIO = 1.25;
const ZOOM_BAND_MID_MAX_RATIO = 2.35;
const FLAG_SIZE_TO_NODE_RATIO = 1.45;
const FLAG_MIN_DRAW_SIZE = 10;
const FLAG_PRESSURE_SCORE_TRIGGER = 3;
const FLAG_PRESSURE_SCORE_RECOVER = 0;

type ZoomBand = "zoomed-out" | "mid" | "zoomed-in";

type EdgeLegendItem = {
  key: string;
  label: string;
  color: string;
};

const EDGE_LEGEND_ITEMS: EdgeLegendItem[] = [
  ...Object.entries(TREATY_EDGE_COLORS).map(([key, color]) => ({
    key,
    color,
    label: key === "protectorate" ? "Protectorate" : key
  })),
  {
    key: "unknown",
    label: "Unknown / other",
    color: EDGE_FALLBACK_COLOR
  }
];

type Props = {
  allEvents: TimelapseEvent[];
  scopedIndexes: number[];
  baseQuery: QueryState;
  playhead: string | null;
  focusedAllianceId: number | null;
  focusedEdgeKey: string | null;
  sizeByScore: boolean;
  showFlags: boolean;
  flagAssetsPayload: FlagAssetsPayload | null;
  allianceScoresByDay: AllianceScoresByDay | null;
  resolveAllianceFlagAtPlayhead: (allianceId: number, playhead: string | null) => AllianceFlagSnapshot | null;
  onFocusAlliance: (allianceId: number | null) => void;
  onFocusEdge: (edgeKey: string | null) => void;
};

function calcMaxEdges(width: number, height: number): number {
  const area = Math.max(width * height, 1);
  const adaptive = Math.round(area / 2200);
  return Math.max(240, Math.min(2400, adaptive));
}

function clampRadius(value: number): number {
  return Math.max(MIN_NODE_RADIUS, Math.min(MAX_NODE_RADIUS, value));
}

function degreeRadius(degree: number): number {
  return clampRadius(3 + Math.log2(degree + 1) * 1.2);
}

function scoreRadius(score: number, maxScore: number): number {
  if (score <= 0 || maxScore <= 0) {
    return MIN_NODE_RADIUS;
  }
  const normalized = Math.sqrt(score / maxScore);
  return clampRadius(MIN_NODE_RADIUS + normalized * (MAX_NODE_RADIUS - MIN_NODE_RADIUS));
}

function resolveScoreDay(scoreDays: string[], playhead: string | null): string | null {
  if (scoreDays.length === 0) {
    return null;
  }
  if (!playhead) {
    return scoreDays[scoreDays.length - 1] ?? null;
  }

  const targetDay = playhead.slice(0, 10);
  let lo = 0;
  let hi = scoreDays.length - 1;
  let best = -1;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (scoreDays[mid] <= targetDay) {
      best = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  if (best >= 0) {
    return scoreDays[best] ?? null;
  }
  return scoreDays[0] ?? null;
}

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
  displayLabel: string | null;
  forceLabel: boolean;
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

type GraphPerfState = {
  graphBuildMs: number;
  refreshMs: number;
  flagDrawMs: number;
};

type FlagSpriteProps = {
  allianceLabel: string;
  flagKey: string;
  flagAssetsPayload: FlagAssetsPayload;
};

function FlagSprite({ allianceLabel, flagKey, flagAssetsPayload }: FlagSpriteProps) {
  const asset = flagAssetsPayload.assets[flagKey];
  if (!asset) {
    return <div className="text-slate-500">Atlas key not found: {flagKey}</div>;
  }

  const atlas = flagAssetsPayload.atlas;
  const fallbackSrc = atlas.png || atlas.webp;

  return (
    <div
      className="inline-block overflow-hidden rounded border border-slate-300"
      style={{ width: asset.w, height: asset.h }}
      aria-label={`${allianceLabel} flag`}
      title={flagKey}
    >
      <picture>
        <source srcSet={atlas.webp} type="image/webp" />
        <img
          src={fallbackSrc}
          alt={`${allianceLabel} flag`}
          loading="lazy"
          className="block"
          style={{
            width: atlas.width,
            height: atlas.height,
            maxWidth: "none",
            maxHeight: "none",
            transform: `translate(-${asset.x}px, -${asset.y}px)`
          }}
        />
      </picture>
    </div>
  );
}

export function NetworkView({
  allEvents,
  scopedIndexes,
  baseQuery,
  playhead,
  focusedAllianceId,
  focusedEdgeKey,
  sizeByScore,
  showFlags,
  flagAssetsPayload,
  allianceScoresByDay,
  resolveAllianceFlagAtPlayhead,
  onFocusAlliance,
  onFocusEdge
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
  const framePressureRef = useRef(false);
  const flagDrawMsRef = useRef(0);
  const showFlagsRef = useRef(showFlags);
  const flagAssetsRef = useRef<FlagAssetsPayload | null>(flagAssetsPayload);
  const [size, setSize] = useState({ width: 1000, height: 350 });
  const [hovered, setHovered] = useState<HoverPayload | null>(null);
  const [budgetPreset, setBudgetPreset] = useState<"auto" | "500" | "1000" | "2000" | "unlimited">("auto");
  const [cameraRatio, setCameraRatio] = useState(1);
  const [perf, setPerf] = useState<GraphPerfState>({ graphBuildMs: 0, refreshMs: 0, flagDrawMs: 0 });
  const [atlasReady, setAtlasReady] = useState(false);
  const [framePressure, setFramePressure] = useState(false);
  const [workerEdgeEventIndexes, setWorkerEdgeEventIndexes] = useState<number[] | null>(null);
  const networkRequestRef = useRef(0);
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
    framePressureRef.current = framePressure;
  }, [framePressure]);

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

  useEffect(() => {
    networkRequestRef.current += 1;
    const requestId = networkRequestRef.current;
    setWorkerEdgeEventIndexes(null);

    void selectTimelapseNetworkEventIndexes(baseQuery, playhead, maxEdges).then((workerIndexes) => {
      if (networkRequestRef.current !== requestId) {
        return;
      }
      if (workerIndexes) {
        setWorkerEdgeEventIndexes(Array.from(workerIndexes));
      }
    });
  }, [baseQuery, maxEdges, playhead]);

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
    const rawEdges =
      workerEdgeEventIndexes !== null
        ? workerEdgeEventIndexes.map((eventIndex) => {
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
          })
        : deriveNetworkEdges(allEvents, scopedIndexes, playhead, maxEdges);

    return rawEdges.map((edge) => ({
      ...edge,
      sourceLabel: allianceNameById.get(edge.sourceId) ?? edge.sourceLabel,
      targetLabel: allianceNameById.get(edge.targetId) ?? edge.targetLabel
    }));
  }, [allEvents, allianceNameById, maxEdges, playhead, scopedIndexes, workerEdgeEventIndexes]);

  const hoverResetKey = useMemo(() => {
    const scopedFingerprint =
      scopedIndexes.length > 0
        ? `${scopedIndexes.length}:${scopedIndexes[0] ?? ""}:${scopedIndexes[scopedIndexes.length - 1] ?? ""}`
        : "0";
    return [
      playhead ?? "",
      baseQuery.time.start ?? "",
      baseQuery.time.end ?? "",
      [...baseQuery.filters.alliances].sort((left, right) => left - right).join(","),
      [...baseQuery.filters.treatyTypes].sort((left, right) => left.localeCompare(right)).join(","),
      [...baseQuery.filters.actions].sort((left, right) => left.localeCompare(right)).join(","),
      [...baseQuery.filters.sources].sort((left, right) => left.localeCompare(right)).join(","),
      baseQuery.filters.includeInferred ? "1" : "0",
      baseQuery.filters.includeNoise ? "1" : "0",
      baseQuery.filters.evidenceMode,
      sizeByScore ? "1" : "0",
      baseQuery.textQuery.trim().toLowerCase(),
      baseQuery.sort.field,
      baseQuery.sort.direction,
      String(maxEdges),
      String(allEvents.length),
      scopedFingerprint
    ].join("|");
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
    maxEdges,
    playhead,
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
    if (hovered?.allianceId) {
      priorityLabelNodeIds.add(hovered.allianceId);
    }
    const flagRenderMode = deriveFlagRenderMode(
      showFlags,
      flagAssetsPayload !== null,
      cameraRatio,
      nodeIds.length,
      framePressure
    );
    const scoreDays = allianceScoresByDay ? Object.keys(allianceScoresByDay).sort((a, b) => a.localeCompare(b)) : [];
    const scoreDay = sizeByScore ? resolveScoreDay(scoreDays, playhead) : null;
    const dayScores = scoreDay && allianceScoresByDay ? allianceScoresByDay[scoreDay] ?? null : null;

    const scoreByNode = new Map<string, number>();
    let maxVisibleScore = 0;
    if (dayScores) {
      for (const nodeId of nodeIds) {
        const score = dayScores[nodeId];
        if (typeof score !== "number" || !Number.isFinite(score) || score <= 0) {
          continue;
        }
        scoreByNode.set(nodeId, score);
        if (score > maxVisibleScore) {
          maxVisibleScore = score;
        }
      }
    }
    const useScoreSizing = Boolean(sizeByScore && dayScores && maxVisibleScore > 0);

    const flagResolutionNodeIds = resolveSpriteNodeIds(
      flagRenderMode,
      nodeIds,
      focusedAlliance,
      hovered?.allianceId ?? null,
      FLAG_MAX_SPRITES
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
      const baseRadius = useScoreSizing && nodeScore !== null ? scoreRadius(nodeScore, maxVisibleScore) : degreeRadius(meta.degree);
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
      const forceLabel = priorityLabelNodeIds.has(id);
      const displayLabel = displayLabelForBand(fullLabel, zoomBand, forceLabel);

      return {
        id,
        fullLabel,
        displayLabel,
        forceLabel,
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

    const links: EdgePayload[] = edges.map((edge) => {
      const color = TREATY_EDGE_COLORS[edge.treatyType] ?? EDGE_FALLBACK_COLOR;
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
        treatyType: edge.treatyType,
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
      scoreSizingActive: useScoreSizing,
      scoreDay,
      graphBuildMs
    };
  }, [
    adaptiveBudget,
    allianceNameById,
    allianceScoresByDay,
    anchoredAllianceIdLookup,
    budgetPreset,
    cameraRatio,
    edges,
    framePressure,
    flagAssetsPayload,
    focusedAllianceId,
    focusedEdgeKey,
    hovered?.allianceId,
    maxEdges,
    playhead,
    resolveAllianceFlagAtPlayhead,
    showFlags,
    sizeByScore,
    zoomBand
  ]);

  useEffect(() => {
    markTimelapsePerf("network.graph.build", graph.graphBuildMs);
    setPerf((current) => ({ ...current, graphBuildMs: Number(graph.graphBuildMs.toFixed(2)) }));
  }, [graph.graphBuildMs]);

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
      for (const nodeId of graphModel.nodes()) {
        if (drawn >= FLAG_MAX_SPRITES) {
          break;
        }

        const sprite = graphModel.getNodeAttribute(nodeId, "flagSprite") as { x: number; y: number; w: number; h: number } | null;
        if (!sprite) {
          continue;
        }

        const display = renderer.getNodeDisplayData(nodeId);
        if (!display || display.hidden) {
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
    renderer.refresh();
  }, [zoomBand]);

  useEffect(() => {
    const renderer = rendererRef.current;
    if (!renderer) {
      return;
    }
    renderer.refresh();
  }, [atlasReady]);

  useEffect(() => {
    const renderer = rendererRef.current;
    const graphModel = graphRef.current;
    if (!renderer || !graphModel) {
      return;
    }

    graphModel.clear();

    for (const node of graph.nodes) {
      const nodeAttributes: Record<string, unknown> = {
        x: node.x,
        y: node.y,
        fullLabel: node.fullLabel,
        treatyCount: node.treatyCount,
        counterparties: node.counterparties,
        size: node.radius,
        score: node.score,
        scoreDay: node.scoreDay,
        color: node.highlighted ? "#2b8a3e" : node.anchored ? "#d9480f" : "#0c8599"
      };
      if (node.displayLabel) {
        nodeAttributes.label = node.displayLabel;
      }
      if (node.forceLabel) {
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
      graphModel.addDirectedEdgeWithKey(edge.id, edge.source, edge.target, {
        size: edge.highlighted ? 3 : 1.2,
        color: edge.highlighted || edge.adjacentToFocusedAlliance ? edge.color : "#c7cfdd",
        edgeKey: edge.edgeKey,
        sourceLabel: edge.sourceLabel,
        targetLabel: edge.targetLabel,
        treatyType: edge.treatyType,
        sourceType: edge.sourceType,
        confidence: edge.confidence
      });
    }

    const refreshStartedAt = performance.now();
    renderer.refresh();
    const refreshMs = performance.now() - refreshStartedAt;
    markTimelapsePerf("network.renderer.refresh", refreshMs);

    const overBudget = refreshMs > FLAG_PRESSURE_REFRESH_MS || graph.graphBuildMs > FLAG_PRESSURE_BUILD_MS;
    framePressureScoreRef.current += overBudget ? 1 : -1;
    framePressureScoreRef.current = Math.max(-8, Math.min(8, framePressureScoreRef.current));

    if (!framePressureRef.current && framePressureScoreRef.current >= FLAG_PRESSURE_SCORE_TRIGGER) {
      setFramePressure(true);
    } else if (framePressureRef.current && framePressureScoreRef.current <= FLAG_PRESSURE_SCORE_RECOVER) {
      setFramePressure(false);
    }

    setPerf((current) => ({
      ...current,
      refreshMs: Number(refreshMs.toFixed(2)),
      flagDrawMs: Number(flagDrawMsRef.current.toFixed(2))
    }));
  }, [graph.flagRenderMode, graph.graphBuildMs, graph.links, graph.nodes]);

  useEffect(() => {
    const renderer = rendererRef.current;
    if (!renderer) {
      return;
    }
    renderer.resize();
    renderer.refresh();
  }, [size.height, size.width]);

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

  return (
    <section className="panel p-4">
      <header className="mb-2 flex items-center justify-between">
        <h2 className="text-lg">Network Explorer</h2>
        <span className="text-xs text-muted">
          {graph.renderedEdges} edges / {graph.budgetLabel} LOD budget
        </span>
      </header>
      <div className="mb-3 grid gap-2 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-start">
        <div className="flex flex-wrap items-center gap-2 text-xs text-muted">
          <label htmlFor="lod-budget">LOD budget</label>
          <select
            id="lod-budget"
            className="rounded border border-slate-300 px-1 py-0.5"
            value={budgetPreset}
            onChange={(event) => setBudgetPreset(event.target.value as typeof budgetPreset)}
          >
            <option value="auto">Auto ({graph.adaptiveBudget})</option>
            <option value="500">500</option>
            <option value="1000">1000</option>
            <option value="2000">2000</option>
            <option value="unlimited">Unlimited</option>
          </select>
          <span className="ml-1">Anchored: {anchoredAllianceIds.length}</span>
          <button
            type="button"
            className="rounded border border-slate-300 px-1 py-0.5 disabled:cursor-not-allowed disabled:opacity-60"
            onClick={() => setAnchoredAllianceIds([])}
            disabled={anchoredAllianceIds.length === 0}
          >
            Clear anchors
          </button>
        </div>
        <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-[11px] text-slate-700">
          <div className="uppercase tracking-wide text-slate-500">Edge legend</div>
          <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1">
            {EDGE_LEGEND_ITEMS.map((item) => (
              <div key={item.key} className="flex items-center gap-1">
                <span
                  aria-hidden="true"
                  className="inline-block h-2.5 w-2.5 rounded-sm"
                  style={{ backgroundColor: item.color }}
                />
                <span>{item.label}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
      <div className="mb-2 flex items-center justify-between text-xs text-muted">
        <span>State at {playhead ?? "latest"}</span>
        <span>Rendering {graph.renderedEdges} edges with full detail</span>
      </div>
      <div className="mb-2 text-xs text-muted">
        Node sizing: {graph.scoreSizingActive ? `score (${graph.scoreDay ?? "n/a"})` : "degree"}
      </div>
      <div className="mb-2 text-[11px] text-muted">
        Labels: {zoomBand} | Flags: {graph.flagRenderMode} | camera ratio: {cameraRatio.toFixed(2)} | resolved sprites: {graph.flagResolvedNodeCount}
      </div>
      <div className="mb-2 text-[11px] text-muted">
        Perf: build {perf.graphBuildMs.toFixed(2)} ms | refresh {perf.refreshMs.toFixed(2)} ms | sprite draw {perf.flagDrawMs.toFixed(2)} ms | pressure {framePressure ? "on" : "off"}
      </div>
      <div ref={hostRef} className="h-[350px] overflow-hidden rounded-xl border border-slate-200" />
      {hoveredAlliance ? (
        <div className="mt-2 rounded-md border border-slate-300 bg-slate-50 p-3 text-xs text-slate-700">
          <div className="text-[11px] uppercase tracking-wide text-slate-500">Alliance</div>
          <div className="text-sm font-semibold text-slate-900">{hoveredAlliance.fullLabel}</div>

          <div className="mt-2 text-[11px] uppercase tracking-wide text-slate-500">Score</div>
          <div>
            {typeof hoveredAlliance.score === "number" && Number.isFinite(hoveredAlliance.score)
              ? `${hoveredAlliance.score.toLocaleString(undefined, { maximumFractionDigits: 2 })}${hoveredAlliance.scoreDay ? ` (${hoveredAlliance.scoreDay})` : ""}`
              : "n/a"}
          </div>

          <div className="mt-2 text-[11px] uppercase tracking-wide text-slate-500">Flag</div>
          {graph.flagRenderMode === "off" ? (
            <div className="text-slate-500">flags disabled</div>
          ) : hoveredFlagKey && flagAssetsPayload ? (
            <FlagSprite
              allianceLabel={hoveredAlliance.fullLabel}
              flagKey={hoveredFlagKey}
              flagAssetsPayload={flagAssetsPayload}
            />
          ) : (
            <div className="text-slate-500">No atlas key available at current playhead.</div>
          )}

          <div className="mt-2 text-[11px] uppercase tracking-wide text-slate-500">Active Treaties / Counterparties</div>
          <div className="mb-1 text-slate-600">
            {hoveredAlliance.treatyCount} treaties across {hoveredAlliance.counterparties} counterparties
          </div>
          {hoveredAlliance.activeTreaties.length > 0 ? (
            <ul className="space-y-1">
              {hoveredAlliance.activeTreaties.map((treaty) => (
                <li key={`${hoveredAlliance.id}:${treaty.counterpartyId}`} className="leading-tight">
                  <span className="font-medium">{treaty.counterpartyLabel}</span>: {treaty.treatyTypes.join(", ")}
                </li>
              ))}
            </ul>
          ) : (
            <div>none</div>
          )}
        </div>
      ) : null}
      {focusedDetail ? <div className="mt-2 text-xs text-slate-700">{focusedDetail}</div> : null}
      <p className="mt-2 text-xs text-muted">
        LOD budget controls rendered edge count to keep interaction responsive on large graphs.
      </p>
      <p className="text-xs text-muted">Shift+click a node to toggle an anchor while keeping regular click-to-focus behavior.</p>
    </section>
  );
}
