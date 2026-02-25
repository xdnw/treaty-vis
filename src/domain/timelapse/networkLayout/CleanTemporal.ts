import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutStrategyDefinition } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyDefinition";
import type { NetworkLayoutInput, NetworkLayoutOutput } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";
import type { WorkerCommunityTarget, WorkerComponentTarget, WorkerNodeTarget } from "@/domain/timelapse/workerProtocol";

const TAU = Math.PI * 2;
const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5));

const DEFAULT_NODE_SPACING = 8;
const DEFAULT_STABILITY = 0.8;
const DEFAULT_QUALITY = 1.0;

// Spatial hash packing constants (unique numeric key for (cx, cy) within a huge range)
const GRID_OFFSET = 1_048_576; // 2^20
const GRID_SCALE = 2_097_153; // 2*offset + 1

const EPS = 1e-6;

const compareString = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value));

type SnapshotComponent = {
  componentId: string;
  nodeIds: string[];
  anchorX: number;
  anchorY: number;
  radius?: number;
};

type SnapshotCommunity = {
  communityId: string;
  componentId: string;
  nodeIds: string[];
  anchorX: number;
  anchorY: number;
  radius?: number;
};

type LayoutSnapshot = {
  version: 2;
  components: SnapshotComponent[];
  communities: SnapshotCommunity[];
  nodePositions: Record<string, { x: number; y: number }>;
};

type SnapshotIndex = {
  componentByNodeId: Map<string, string>;
  communityByNodeId: Map<string, string>;
  componentSizeById: Map<string, number>;
  communitySizeById: Map<string, number>;
  componentAnchorById: Map<string, { x: number; y: number }>;
  communityAnchorById: Map<string, { x: number; y: number }>;
  communityComponentById: Map<string, string>;
  nodePositions: Record<string, { x: number; y: number }>;
};

type CommunityPlan = {
  communityId: string;
  componentId: string;
  nodeIds: string[]; // sorted
  radius: number;
  anchorX: number;
  anchorY: number;
};

type ComponentPlan = {
  componentId: string;
  nodeIds: string[]; // sorted
  communities: CommunityPlan[];
  radius: number;
  anchorX: number;
  anchorY: number;
};

type BucketStore = {
  buckets: Map<number, number[]>;
  usedKeys: number[];
  pool: number[][];
};

export class CleanTemporalLayoutAlgorithm implements INetworkLayoutAlgorithm {
  public readonly strategy = "clean-temporal" as const;

  public run(input: NetworkLayoutInput): NetworkLayoutOutput {
    const quality = resolveNumberConfig(input.strategyConfig, "quality", DEFAULT_QUALITY, 0.5, 1.5);
    const stability = resolveNumberConfig(input.strategyConfig, "stability", DEFAULT_STABILITY, 0, 0.95);
    const nodeSpacing = resolveNumberConfig(input.strategyConfig, "nodeSpacing", DEFAULT_NODE_SPACING, 4, 20);

    const nodeIdsSorted = [...input.nodeIds].sort(compareString);

    // Pre-hash node ids once for deterministic sampling and jitter.
    const nodeHashById = new Map<string, number>();
    for (let i = 0; i < nodeIdsSorted.length; i += 1) {
      const nodeId = nodeIdsSorted[i];
      nodeHashById.set(nodeId, this.hashId(nodeId));
    }

    const previousSnapshot = this.readSnapshot(input.previousState);
    const previousIndex = this.indexSnapshot(previousSnapshot);

    // Connected components.
    const rawComponents = this.computeConnectedComponents(nodeIdsSorted, input.adjacencyByNodeId);

    // Stable component IDs.
    const componentsAssigned = this.assignStableComponentIds(rawComponents, previousIndex);

    // Build component + community plans.
    const componentPlans: ComponentPlan[] = [];
    for (let cIndex = 0; cIndex < componentsAssigned.length; cIndex += 1) {
      const component = componentsAssigned[cIndex];

      const communitiesRaw = this.detectCommunitiesLabelPropagation({
        componentId: component.componentId,
        nodeIds: component.nodeIds,
        adjacencyByNodeId: input.adjacencyByNodeId,
        previousIndex
      });

      const communitiesAssigned = this.assignStableCommunityIds(component.componentId, communitiesRaw, previousIndex);

      const communityPlans: CommunityPlan[] = [];
      for (let i = 0; i < communitiesAssigned.length; i += 1) {
        const community = communitiesAssigned[i];
        communityPlans.push({
          communityId: community.communityId,
          componentId: component.componentId,
          nodeIds: community.nodeIds,
          radius: this.computeCommunityRadius(community.nodeIds.length, nodeSpacing),
          anchorX: 0,
          anchorY: 0
        });
      }

      communityPlans.sort(
        (a, b) =>
          b.nodeIds.length - a.nodeIds.length || compareString(a.communityId, b.communityId)
      );

      componentPlans.push({
        componentId: component.componentId,
        nodeIds: component.nodeIds,
        communities: communityPlans,
        radius: this.computeComponentRadius(communityPlans, nodeSpacing),
        anchorX: 0,
        anchorY: 0
      });
    }

    componentPlans.sort(
      (a, b) => b.nodeIds.length - a.nodeIds.length || compareString(a.componentId, b.componentId)
    );

    // 1) Pack component anchors (rough, based on estimated radii).
    this.layoutComponentAnchors(componentPlans, previousIndex, nodeSpacing, quality, {
      useCurrentAsStart: false
    });

    // 2) Layout community anchors inside each component.
    for (let i = 0; i < componentPlans.length; i += 1) {
      this.layoutCommunityAnchors({
        component: componentPlans[i],
        adjacencyByNodeId: input.adjacencyByNodeId,
        previousIndex,
        nodeHashById,
        nodeSpacing,
        quality
      });
    }

    // 3) Refine component radii from actual community extents, repack, then translate communities by component delta.
    const anchorsBeforeSecondPack = new Map<string, { x: number; y: number }>();
    for (let i = 0; i < componentPlans.length; i += 1) {
      const c = componentPlans[i];
      anchorsBeforeSecondPack.set(c.componentId, { x: c.anchorX, y: c.anchorY });
    }

    this.refineComponentRadiiFromCommunities(componentPlans, nodeSpacing);

    this.layoutComponentAnchors(componentPlans, previousIndex, nodeSpacing, quality, {
      useCurrentAsStart: true
    });

    for (let i = 0; i < componentPlans.length; i += 1) {
      const component = componentPlans[i];
      const before = anchorsBeforeSecondPack.get(component.componentId);
      if (!before) continue;

      const dx = component.anchorX - before.x;
      const dy = component.anchorY - before.y;
      if (Math.abs(dx) < 1e-9 && Math.abs(dy) < 1e-9) continue;

      for (let j = 0; j < component.communities.length; j += 1) {
        component.communities[j].anchorX += dx;
        component.communities[j].anchorY += dy;
      }
    }

    // 4) Layout nodes inside each community (anchored, collision-aware, temporally stable).
    const nextNodePositions: Record<string, { x: number; y: number }> = Object.create(null);
    const nodeMetaById = new Map<string, { componentId: string; communityId: string; anchorX: number; anchorY: number }>();

    for (let i = 0; i < componentPlans.length; i += 1) {
      const component = componentPlans[i];
      for (let j = 0; j < component.communities.length; j += 1) {
        const community = component.communities[j];

        const { nodeIds, x, y } = this.layoutNodesInCommunity({
          componentId: component.componentId,
          communityId: community.communityId,
          nodeIds: community.nodeIds,
          anchorX: community.anchorX,
          anchorY: community.anchorY,
          communityRadius: community.radius,
          nodeSpacing,
          quality,
          stability,
          adjacencyByNodeId: input.adjacencyByNodeId,
          nodeHashById,
          previousIndex
        });

        for (let k = 0; k < nodeIds.length; k += 1) {
          const nodeId = nodeIds[k];
          nextNodePositions[nodeId] = { x: x[k], y: y[k] };
          nodeMetaById.set(nodeId, {
            componentId: component.componentId,
            communityId: community.communityId,
            anchorX: community.anchorX,
            anchorY: community.anchorY
          });
        }
      }
    }

    // Build final targets + snapshot.
    const componentTargets: WorkerComponentTarget[] = [];
    const communityTargets: WorkerCommunityTarget[] = [];

    const nextSnapshot: LayoutSnapshot = {
      version: 2,
      components: [],
      communities: [],
      nodePositions: nextNodePositions
    };

    for (let i = 0; i < componentPlans.length; i += 1) {
      const component = componentPlans[i];
      componentTargets.push({
        componentId: component.componentId,
        nodeIds: component.nodeIds,
        anchorX: component.anchorX,
        anchorY: component.anchorY
      });

      nextSnapshot.components.push({
        componentId: component.componentId,
        nodeIds: component.nodeIds,
        anchorX: component.anchorX,
        anchorY: component.anchorY,
        radius: component.radius
      });

      for (let j = 0; j < component.communities.length; j += 1) {
        const community = component.communities[j];
        communityTargets.push({
          communityId: community.communityId,
          componentId: component.componentId,
          nodeIds: community.nodeIds,
          anchorX: community.anchorX,
          anchorY: community.anchorY
        });

        nextSnapshot.communities.push({
          communityId: community.communityId,
          componentId: component.componentId,
          nodeIds: community.nodeIds,
          anchorX: community.anchorX,
          anchorY: community.anchorY,
          radius: community.radius
        });
      }
    }

    // Node targets (iterate sorted for stable output order).
    const nodeTargets: WorkerNodeTarget[] = [];
    for (let i = 0; i < nodeIdsSorted.length; i += 1) {
      const nodeId = nodeIdsSorted[i];
      const pos = nextNodePositions[nodeId] ?? previousIndex.nodePositions[nodeId] ?? { x: 0, y: 0 };
      const meta = nodeMetaById.get(nodeId);

      // (Should always exist; fallbacks keep layout resilient to partial state.)
      const componentId = meta?.componentId ?? previousIndex.componentByNodeId.get(nodeId) ?? "component:unknown";
      const communityId = meta?.communityId ?? previousIndex.communityByNodeId.get(nodeId) ?? `${componentId}:community:unknown`;
      const anchorX = meta?.anchorX ?? pos.x;
      const anchorY = meta?.anchorY ?? pos.y;

      let neighborX = 0;
      let neighborY = 0;
      let neighborCount = 0;

      const neighbors = input.adjacencyByNodeId.get(nodeId);
      if (neighbors) {
        for (const neighborId of neighbors) {
          const p = nextNodePositions[neighborId];
          if (!p) continue;
          neighborX += p.x;
          neighborY += p.y;
          neighborCount += 1;
        }
      }

      if (neighborCount === 0) {
        neighborX = pos.x;
        neighborY = pos.y;
      } else {
        neighborX /= neighborCount;
        neighborY /= neighborCount;
      }

      nodeTargets.push({
        nodeId,
        componentId,
        communityId,
        targetX: pos.x,
        targetY: pos.y,
        neighborX,
        neighborY,
        anchorX,
        anchorY
      });
    }

    return {
      layout: {
        components: componentTargets,
        communities: communityTargets,
        nodeTargets
      },
      metadata: {
        state: nextSnapshot
      }
    };
  }

  // -----------------------
  // Snapshot parsing/index
  // -----------------------

  private readSnapshot(value: unknown): LayoutSnapshot | undefined {
    if (!value || typeof value !== "object") {
      return undefined;
    }
    const v = value as Record<string, unknown>;

    const componentsRaw = v.components;
    const communitiesRaw = v.communities;
    const nodePositionsRaw = v.nodePositions;

    if (!Array.isArray(componentsRaw) || !Array.isArray(communitiesRaw) || !nodePositionsRaw || typeof nodePositionsRaw !== "object") {
      return undefined;
    }

    const components: SnapshotComponent[] = [];
    for (let i = 0; i < componentsRaw.length; i += 1) {
      const entry = componentsRaw[i];
      if (!entry || typeof entry !== "object") continue;

      const e = entry as Record<string, unknown>;
      const componentId = typeof e.componentId === "string" ? e.componentId : null;
      const anchorX = Number(e.anchorX);
      const anchorY = Number(e.anchorY);
      const radius = e.radius == null ? undefined : Number(e.radius);

      if (!componentId || !Number.isFinite(anchorX) || !Number.isFinite(anchorY)) continue;

      const nodeIds =
        this.readStringArray(e.nodeIds) ??
        this.readStringArray(e.members) ??
        [];

      components.push({
        componentId,
        nodeIds,
        anchorX,
        anchorY,
        radius: Number.isFinite(radius ?? NaN) ? radius : undefined
      });
    }

    const communities: SnapshotCommunity[] = [];
    for (let i = 0; i < communitiesRaw.length; i += 1) {
      const entry = communitiesRaw[i];
      if (!entry || typeof entry !== "object") continue;

      const e = entry as Record<string, unknown>;
      const communityId = typeof e.communityId === "string" ? e.communityId : null;
      const componentId = typeof e.componentId === "string" ? e.componentId : null;
      const anchorX = Number(e.anchorX);
      const anchorY = Number(e.anchorY);
      const radius = e.radius == null ? undefined : Number(e.radius);

      if (!communityId || !componentId || !Number.isFinite(anchorX) || !Number.isFinite(anchorY)) continue;

      const nodeIds =
        this.readStringArray(e.nodeIds) ??
        this.readStringArray(e.members) ??
        [];

      communities.push({
        communityId,
        componentId,
        nodeIds,
        anchorX,
        anchorY,
        radius: Number.isFinite(radius ?? NaN) ? radius : undefined
      });
    }

    return {
      version: 2,
      components,
      communities,
      nodePositions: this.toPointRecord(nodePositionsRaw)
    };
  }

  private readStringArray(value: unknown): string[] | null {
    if (!value) return null;

    const out: string[] = [];
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i += 1) {
        if (typeof value[i] === "string") out.push(value[i] as string);
      }
      return out;
    }

    if (value instanceof Set) {
      for (const entry of value) {
        if (typeof entry === "string") out.push(entry);
      }
      return out;
    }

    return null;
  }

  private toPointRecord(value: unknown): Record<string, { x: number; y: number }> {
    if (!value || typeof value !== "object") {
      return Object.create(null);
    }

    const positions: Record<string, { x: number; y: number }> = Object.create(null);
    for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
      if (!raw || typeof raw !== "object") continue;
      const point = raw as { x?: unknown; y?: unknown };
      const x = Number(point.x);
      const y = Number(point.y);
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
      positions[key] = { x, y };
    }
    return positions;
  }

  private indexSnapshot(snapshot: LayoutSnapshot | undefined): SnapshotIndex {
    const componentByNodeId = new Map<string, string>();
    const communityByNodeId = new Map<string, string>();
    const componentSizeById = new Map<string, number>();
    const communitySizeById = new Map<string, number>();
    const componentAnchorById = new Map<string, { x: number; y: number }>();
    const communityAnchorById = new Map<string, { x: number; y: number }>();
    const communityComponentById = new Map<string, string>();

    if (!snapshot) {
      return {
        componentByNodeId,
        communityByNodeId,
        componentSizeById,
        communitySizeById,
        componentAnchorById,
        communityAnchorById,
        communityComponentById,
        nodePositions: Object.create(null)
      };
    }

    for (let i = 0; i < snapshot.components.length; i += 1) {
      const c = snapshot.components[i];
      componentSizeById.set(c.componentId, c.nodeIds.length);
      componentAnchorById.set(c.componentId, { x: c.anchorX, y: c.anchorY });
      for (let j = 0; j < c.nodeIds.length; j += 1) {
        componentByNodeId.set(c.nodeIds[j], c.componentId);
      }
    }

    for (let i = 0; i < snapshot.communities.length; i += 1) {
      const c = snapshot.communities[i];
      communitySizeById.set(c.communityId, c.nodeIds.length);
      communityAnchorById.set(c.communityId, { x: c.anchorX, y: c.anchorY });
      communityComponentById.set(c.communityId, c.componentId);
      for (let j = 0; j < c.nodeIds.length; j += 1) {
        communityByNodeId.set(c.nodeIds[j], c.communityId);
      }
    }

    return {
      componentByNodeId,
      communityByNodeId,
      componentSizeById,
      communitySizeById,
      componentAnchorById,
      communityAnchorById,
      communityComponentById,
      nodePositions: snapshot.nodePositions ?? Object.create(null)
    };
  }

  // -----------------------
  // Hash / IDs
  // -----------------------

  private hashId(id: string): number {
    // FNV-1a 32-bit
    let hash = 2166136261;
    for (let i = 0; i < id.length; i += 1) {
      hash ^= id.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return hash >>> 0;
  }

  private buildComponentId(componentNodeIds: string[]): string {
    const head = componentNodeIds.slice(0, 10).join(",");
    return `component:${componentNodeIds.length}:${this.hashId(head).toString(36)}`;
  }

  private buildCommunityId(componentId: string, communityNodeIds: string[]): string {
    const head = communityNodeIds.slice(0, 10).join(",");
    return `${componentId}:community:${communityNodeIds.length}:${this.hashId(head).toString(36)}`;
  }

  // -----------------------
  // Graph partitioning
  // -----------------------

  private computeConnectedComponents(
    nodeIdsSorted: string[],
    adjacencyByNodeId: Map<string, Set<string>>
  ): string[][] {
    const visited = new Set<string>();
    const components: string[][] = [];

    for (let i = 0; i < nodeIdsSorted.length; i += 1) {
      const root = nodeIdsSorted[i];
      if (visited.has(root)) continue;

      const queue: string[] = [root];
      let q = 0;
      visited.add(root);

      const members: string[] = [];
      while (q < queue.length) {
        const current = queue[q++];
        members.push(current);

        const neighbors = adjacencyByNodeId.get(current);
        if (!neighbors) continue;

        for (const neighbor of neighbors) {
          if (visited.has(neighbor)) continue;
          visited.add(neighbor);
          queue.push(neighbor);
        }
      }

      members.sort(compareString);
      components.push(members);
    }

    components.sort((a, b) => b.length - a.length || compareString(a[0] ?? "", b[0] ?? ""));
    return components;
  }

  private assignStableComponentIds(
    rawComponents: string[][],
    previous: SnapshotIndex
  ): Array<{ componentId: string; nodeIds: string[] }> {
    const claimed = new Set<string>();
    const assigned: Array<{ componentId: string; nodeIds: string[] }> = [];

    for (let i = 0; i < rawComponents.length; i += 1) {
      const nodeIds = rawComponents[i];
      const counts = new Map<string, number>();

      for (let j = 0; j < nodeIds.length; j += 1) {
        const prevId = previous.componentByNodeId.get(nodeIds[j]);
        if (!prevId) continue;
        counts.set(prevId, (counts.get(prevId) ?? 0) + 1);
      }

      let bestId: string | null = null;
      let bestOverlap = 0;
      let bestRatio = -1;

      for (const [prevId, overlap] of counts.entries()) {
        if (claimed.has(prevId)) continue;
        const prevSize = previous.componentSizeById.get(prevId) ?? 0;
        const ratio = overlap / Math.max(1, Math.max(prevSize, nodeIds.length));

        if (
          overlap > bestOverlap ||
          (overlap === bestOverlap && ratio > bestRatio) ||
          (overlap === bestOverlap && ratio === bestRatio && bestId !== null && compareString(prevId, bestId) < 0)
        ) {
          bestId = prevId;
          bestOverlap = overlap;
          bestRatio = ratio;
        }
      }

      const componentId = bestId ?? this.buildComponentId(nodeIds);
      if (bestId) claimed.add(bestId);
      assigned.push({ componentId, nodeIds });
    }

    assigned.sort((a, b) => b.nodeIds.length - a.nodeIds.length || compareString(a.componentId, b.componentId));
    return assigned;
  }

  private detectCommunitiesLabelPropagation(params: {
    componentId: string;
    nodeIds: string[];
    adjacencyByNodeId: Map<string, Set<string>>;
    previousIndex: SnapshotIndex;
  }): string[][] {
    const { componentId, nodeIds, adjacencyByNodeId, previousIndex } = params;
    if (nodeIds.length <= 1) return nodeIds.length === 1 ? [nodeIds] : [];

    const sortedNodeIds = [...nodeIds].sort(compareString);
    const nodeIndex = new Map<string, number>();
    for (let i = 0; i < sortedNodeIds.length; i += 1) nodeIndex.set(sortedNodeIds[i], i);

    // Initialize with previous community IDs when available (stabilizes over time).
    const labels = new Map<string, string>();
    for (let i = 0; i < sortedNodeIds.length; i += 1) {
      const nodeId = sortedNodeIds[i];
      const prevCommunityId = previousIndex.communityByNodeId.get(nodeId);
      const prevCommunityComponentId = prevCommunityId ? previousIndex.communityComponentById.get(prevCommunityId) : null;
      if (prevCommunityId && prevCommunityComponentId === componentId) {
        labels.set(nodeId, prevCommunityId);
      } else {
        labels.set(nodeId, nodeId);
      }
    }

    const n = sortedNodeIds.length;
    const maxIterations = clamp(Math.ceil(Math.log2(n + 1)) + 6, 6, 18);
    const inertia = 1.15; // self-label bias: improves temporal stability / avoids oscillation

    const freq = new Map<string, number>();

    for (let iter = 0; iter < maxIterations; iter += 1) {
      let changed = false;

      for (let i = 0; i < sortedNodeIds.length; i += 1) {
        const nodeId = sortedNodeIds[i];
        const neighbors = adjacencyByNodeId.get(nodeId);
        if (!neighbors || neighbors.size === 0) continue;

        freq.clear();

        for (const neighborId of neighbors) {
          if (!nodeIndex.has(neighborId)) continue;
          const label = labels.get(neighborId) ?? neighborId;
          freq.set(label, (freq.get(label) ?? 0) + 1);
        }

        const currentLabel = labels.get(nodeId) ?? nodeId;
        freq.set(currentLabel, (freq.get(currentLabel) ?? 0) + inertia);

        let bestLabel = currentLabel;
        let bestScore = -Infinity;

        for (const [label, score] of freq.entries()) {
          if (score > bestScore || (score === bestScore && compareString(label, bestLabel) < 0)) {
            bestLabel = label;
            bestScore = score;
          }
        }

        if (bestLabel !== currentLabel) {
          labels.set(nodeId, bestLabel);
          changed = true;
        }
      }

      if (!changed) break;
    }

    const byLabel = new Map<string, string[]>();
    for (let i = 0; i < sortedNodeIds.length; i += 1) {
      const nodeId = sortedNodeIds[i];
      const label = labels.get(nodeId) ?? nodeId;
      const arr = byLabel.get(label);
      if (arr) arr.push(nodeId);
      else byLabel.set(label, [nodeId]);
    }

    const communities = [...byLabel.values()];
    for (let i = 0; i < communities.length; i += 1) communities[i].sort(compareString);
    communities.sort((a, b) => b.length - a.length || compareString(a[0] ?? "", b[0] ?? ""));
    return communities;
  }

  private assignStableCommunityIds(
    componentId: string,
    rawCommunities: string[][],
    previous: SnapshotIndex
  ): Array<{ communityId: string; nodeIds: string[] }> {
    const claimed = new Set<string>();
    const assigned: Array<{ communityId: string; nodeIds: string[] }> = [];

    for (let i = 0; i < rawCommunities.length; i += 1) {
      const nodeIds = rawCommunities[i];

      const counts = new Map<string, number>();
      for (let j = 0; j < nodeIds.length; j += 1) {
        const prevCommunityId = previous.communityByNodeId.get(nodeIds[j]);
        if (!prevCommunityId) continue;
        if (previous.communityComponentById.get(prevCommunityId) !== componentId) continue;
        counts.set(prevCommunityId, (counts.get(prevCommunityId) ?? 0) + 1);
      }

      let bestId: string | null = null;
      let bestOverlap = 0;
      let bestRatio = -1;

      for (const [prevId, overlap] of counts.entries()) {
        if (claimed.has(prevId)) continue;
        const prevSize = previous.communitySizeById.get(prevId) ?? 0;
        const ratio = overlap / Math.max(1, Math.max(prevSize, nodeIds.length));

        if (
          overlap > bestOverlap ||
          (overlap === bestOverlap && ratio > bestRatio) ||
          (overlap === bestOverlap && ratio === bestRatio && bestId !== null && compareString(prevId, bestId) < 0)
        ) {
          bestId = prevId;
          bestOverlap = overlap;
          bestRatio = ratio;
        }
      }

      const communityId = bestId ?? this.buildCommunityId(componentId, nodeIds);
      if (bestId) claimed.add(bestId);
      assigned.push({ communityId, nodeIds });
    }

    assigned.sort((a, b) => b.nodeIds.length - a.nodeIds.length || compareString(a.communityId, b.communityId));
    return assigned;
  }

  // -----------------------
  // Radii / geometry
  // -----------------------

  private computeCommunityRadius(nodeCount: number, nodeSpacing: number): number {
    // Compact but safe radius for collision-free packing + containment.
    // r ≈ nodeSpacing * (2 + 0.8*sqrt(n))
    const n = Math.max(1, nodeCount);
    return nodeSpacing * (2 + 0.8 * Math.sqrt(n));
  }

  private computeComponentRadius(communities: CommunityPlan[], nodeSpacing: number): number {
    if (communities.length === 0) return nodeSpacing * 10;

    let sumSq = 0;
    for (let i = 0; i < communities.length; i += 1) {
      const r = communities[i].radius;
      sumSq += r * r;
    }

    // Packing factor + modest margin.
    const packingFactor = 1.25;
    const margin = nodeSpacing * 4;
    return Math.sqrt(sumSq * packingFactor) + margin;
  }

  private refineComponentRadiiFromCommunities(components: ComponentPlan[], nodeSpacing: number): void {
    const margin = nodeSpacing * 4;

    for (let i = 0; i < components.length; i += 1) {
      const component = components[i];

      let maxExtent = nodeSpacing * 6;
      for (let j = 0; j < component.communities.length; j += 1) {
        const community = component.communities[j];
        const dx = community.anchorX - component.anchorX;
        const dy = community.anchorY - component.anchorY;
        const extent = Math.sqrt(dx * dx + dy * dy) + community.radius;
        if (extent > maxExtent) maxExtent = extent;
      }

      // Allow shrinking to reduce unnecessary gaps, but keep a margin.
      component.radius = Math.max(nodeSpacing * 8, maxExtent + margin);
    }
  }

  // -----------------------
  // Component packing
  // -----------------------

  private layoutComponentAnchors(
    components: ComponentPlan[],
    previous: SnapshotIndex,
    nodeSpacing: number,
    quality: number,
    options: { useCurrentAsStart: boolean }
  ): void {
    if (components.length === 0) return;
    if (components.length === 1) {
      // Keep single component near previous anchor (if any), else center.
      const only = components[0];
      const prev = previous.componentAnchorById.get(only.componentId);
      only.anchorX = prev?.x ?? 0;
      only.anchorY = prev?.y ?? 0;
      return;
    }

    let centerX = 0;
    let centerY = 0;
    if (previous.componentAnchorById.size > 0) {
      let count = 0;
      for (const anchor of previous.componentAnchorById.values()) {
        centerX += anchor.x;
        centerY += anchor.y;
        count += 1;
      }
      centerX /= Math.max(1, count);
      centerY /= Math.max(1, count);
    }

    const n = components.length;
    const x = new Float64Array(n);
    const y = new Float64Array(n);
    const r = new Float64Array(n);
    const prefX = new Float64Array(n);
    const prefY = new Float64Array(n);

    for (let i = 0; i < n; i += 1) {
      const comp = components[i];
      r[i] = comp.radius;

      const prevAnchor = previous.componentAnchorById.get(comp.componentId);

      let preferredX = NaN;
      let preferredY = NaN;

      if (prevAnchor) {
        preferredX = prevAnchor.x;
        preferredY = prevAnchor.y;
      } else {
        // New component: try barycenter of previous node positions, else deterministic spiral.
        let sx = 0;
        let sy = 0;
        let sc = 0;
        for (let k = 0; k < comp.nodeIds.length; k += 1) {
          const pos = previous.nodePositions[comp.nodeIds[k]];
          if (!pos) continue;
          sx += pos.x;
          sy += pos.y;
          sc += 1;
        }

        if (sc > 0) {
          preferredX = sx / sc;
          preferredY = sy / sc;
        } else {
          const seed = this.hashId(comp.componentId);
          const angle = ((seed % 360) / 360) * TAU;
          const rad = nodeSpacing * (10 + Math.sqrt(comp.nodeIds.length) * 3.2);
          preferredX = centerX + Math.cos(angle) * rad;
          preferredY = centerY + Math.sin(angle) * rad;
        }
      }

      prefX[i] = preferredX;
      prefY[i] = preferredY;

      if (options.useCurrentAsStart && Number.isFinite(comp.anchorX) && Number.isFinite(comp.anchorY)) {
        x[i] = comp.anchorX;
        y[i] = comp.anchorY;
      } else {
        x[i] = preferredX;
        y[i] = preferredY;
      }
    }

    // A compact, stable pack: collision + mild centering + pinning to preferred.
    const iterations = this.adaptiveIterations(Math.round(22 * quality), n, 10, 50);
    const padding = nodeSpacing * 5;
    const maxStep = nodeSpacing * 6;

    this.relaxCirclePacking({
      x,
      y,
      r,
      preferredX: prefX,
      preferredY: prefY,
      iterations,
      padding,
      centerX,
      centerY,
      pinStrength: 0.08,
      centerStrength: 0.006,
      maxStep
    });

    // Align whole layout translation to previous anchors to reduce drift.
    let sumDx = 0;
    let sumDy = 0;
    let count = 0;
    for (let i = 0; i < n; i += 1) {
      const prevAnchor = previous.componentAnchorById.get(components[i].componentId);
      if (!prevAnchor) continue;
      sumDx += prevAnchor.x - x[i];
      sumDy += prevAnchor.y - y[i];
      count += 1;
    }
    if (count > 0) {
      const dx = sumDx / count;
      const dy = sumDy / count;
      for (let i = 0; i < n; i += 1) {
        x[i] += dx;
        y[i] += dy;
      }
    }

    for (let i = 0; i < n; i += 1) {
      components[i].anchorX = x[i];
      components[i].anchorY = y[i];
    }
  }

  private relaxCirclePacking(params: {
    x: Float64Array;
    y: Float64Array;
    r: Float64Array;
    preferredX: Float64Array;
    preferredY: Float64Array;
    iterations: number;
    padding: number;
    centerX: number;
    centerY: number;
    pinStrength: number;
    centerStrength: number;
    maxStep: number;
  }): void {
    const { x, y, r, preferredX, preferredY, iterations, padding, centerX, centerY, pinStrength, centerStrength, maxStep } =
      params;

    const n = x.length;
    if (n <= 1) return;

    const dx = new Float64Array(n);
    const dy = new Float64Array(n);
    const mass = new Float64Array(n);

    let maxRadius = 0;
    for (let i = 0; i < n; i += 1) {
      const ri = r[i];
      mass[i] = ri * ri;
      if (ri > maxRadius) maxRadius = ri;
    }

    const useGrid = n > 320;
    const gridSize = Math.max(1, maxRadius + padding);

    const store: BucketStore = {
      buckets: new Map<number, number[]>(),
      usedKeys: [],
      pool: []
    };

    for (let iter = 0; iter < iterations; iter += 1) {
      dx.fill(0);
      dy.fill(0);

      const t = iterations <= 1 ? 0 : iter / (iterations - 1);
      const alpha = 0.25 + 0.75 * (1 - t); // keep some movement late for convergence
      const stepCap = maxStep * alpha;

      // Pin + centering.
      for (let i = 0; i < n; i += 1) {
        dx[i] += (preferredX[i] - x[i]) * pinStrength;
        dy[i] += (preferredY[i] - y[i]) * pinStrength;
        dx[i] += (centerX - x[i]) * centerStrength;
        dy[i] += (centerY - y[i]) * centerStrength;
      }

      // Collisions.
      if (!useGrid) {
        for (let i = 0; i < n; i += 1) {
          for (let j = i + 1; j < n; j += 1) {
            this.applyCircleCollision({
              i,
              j,
              x,
              y,
              r,
              dx,
              dy,
              mass,
              padding,
              iterSeed: iter
            });
          }
        }
      } else {
        this.bucketClear(store);

        for (let i = 0; i < n; i += 1) {
          const cx = Math.floor(x[i] / gridSize);
          const cy = Math.floor(y[i] / gridSize);
          const key = (cx + GRID_OFFSET) * GRID_SCALE + (cy + GRID_OFFSET);
          this.bucketPush(store, key, i);
        }

        for (let i = 0; i < n; i += 1) {
          const cx = Math.floor(x[i] / gridSize);
          const cy = Math.floor(y[i] / gridSize);

          for (let ox = -2; ox <= 2; ox += 1) {
            for (let oy = -2; oy <= 2; oy += 1) {
              const key = (cx + ox + GRID_OFFSET) * GRID_SCALE + (cy + oy + GRID_OFFSET);
              const bucket = store.buckets.get(key);
              if (!bucket) continue;

              for (let b = 0; b < bucket.length; b += 1) {
                const j = bucket[b];
                if (j <= i) continue;
                this.applyCircleCollision({
                  i,
                  j,
                  x,
                  y,
                  r,
                  dx,
                  dy,
                  mass,
                  padding,
                  iterSeed: iter
                });
              }
            }
          }
        }

        this.bucketClear(store);
      }

      // Integrate.
      for (let i = 0; i < n; i += 1) {
        let sx = dx[i] * alpha;
        let sy = dy[i] * alpha;

        const speed2 = sx * sx + sy * sy;
        if (speed2 > stepCap * stepCap) {
          const inv = stepCap / Math.sqrt(speed2);
          sx *= inv;
          sy *= inv;
        }

        x[i] += sx;
        y[i] += sy;
      }
    }
  }

  private applyCircleCollision(params: {
    i: number;
    j: number;
    x: Float64Array;
    y: Float64Array;
    r: Float64Array;
    dx: Float64Array;
    dy: Float64Array;
    mass: Float64Array;
    padding: number;
    iterSeed: number;
  }): void {
    const { i, j, x, y, r, dx, dy, mass, padding, iterSeed } = params;

    let vx = x[j] - x[i];
    let vy = y[j] - y[i];
    let dist2 = vx * vx + vy * vy;

    if (dist2 < EPS) {
      const a = ((i * 997 + j * 991 + iterSeed * 37) % 360) * (TAU / 360);
      vx = Math.cos(a);
      vy = Math.sin(a);
      dist2 = 1;
    }

    const dist = Math.sqrt(dist2);
    const minDist = r[i] + r[j] + padding;
    if (dist >= minDist) return;

    const overlap = minDist - dist;
    const ux = vx / dist;
    const uy = vy / dist;

    // Larger circles move less.
    const mi = mass[i];
    const mj = mass[j];
    const invTotal = 1 / Math.max(EPS, mi + mj);

    const wi = mj * invTotal;
    const wj = mi * invTotal;

    dx[i] -= ux * overlap * wi;
    dy[i] -= uy * overlap * wi;
    dx[j] += ux * overlap * wj;
    dy[j] += uy * overlap * wj;
  }

  // -----------------------
  // Community anchor layout
  // -----------------------

  private layoutCommunityAnchors(params: {
    component: ComponentPlan;
    adjacencyByNodeId: Map<string, Set<string>>;
    previousIndex: SnapshotIndex;
    nodeHashById: Map<string, number>;
    nodeSpacing: number;
    quality: number;
  }): void {
    const { component, adjacencyByNodeId, previousIndex, nodeHashById, nodeSpacing, quality } = params;

    const communities = component.communities;
    if (communities.length === 0) return;

    if (communities.length === 1) {
      communities[0].anchorX = component.anchorX;
      communities[0].anchorY = component.anchorY;
      return;
    }

    const cCount = communities.length;

    // Map node -> community index for cross-community edge weights.
    const communityIndexByNodeId = new Map<string, number>();
    for (let ci = 0; ci < cCount; ci += 1) {
      const comm = communities[ci];
      for (let k = 0; k < comm.nodeIds.length; k += 1) {
        communityIndexByNodeId.set(comm.nodeIds[k], ci);
      }
    }

    // Cross-community edge weights (symmetric count is OK; we normalize later).
    const weights = new Map<number, number>();
    for (let i = 0; i < component.nodeIds.length; i += 1) {
      const nodeId = component.nodeIds[i];
      const ci = communityIndexByNodeId.get(nodeId);
      if (ci == null) continue;

      const neighbors = adjacencyByNodeId.get(nodeId);
      if (!neighbors) continue;

      for (const neighborId of neighbors) {
        const cj = communityIndexByNodeId.get(neighborId);
        if (cj == null || cj === ci) continue;
        const a = ci < cj ? ci : cj;
        const b = ci < cj ? cj : ci;
        const key = a * cCount + b;
        weights.set(key, (weights.get(key) ?? 0) + 1);
      }
    }

    // Build weighted edge list.
    const edgesA: number[] = [];
    const edgesB: number[] = [];
    const edgesW: number[] = [];
    let maxW = 0;

    for (const [key, w] of weights.entries()) {
      if (w > maxW) maxW = w;
      const a = Math.floor(key / cCount);
      const b = key - a * cCount;
      edgesA.push(a);
      edgesB.push(b);
      edgesW.push(w);
    }

    // Initialize positions.
    const x = new Float64Array(cCount);
    const y = new Float64Array(cCount);
    const r = new Float64Array(cCount);
    const prefX = new Float64Array(cCount);
    const prefY = new Float64Array(cCount);

    const prevComponentAnchor = previousIndex.componentAnchorById.get(component.componentId);
    const componentDeltaX = prevComponentAnchor ? component.anchorX - prevComponentAnchor.x : 0;
    const componentDeltaY = prevComponentAnchor ? component.anchorY - prevComponentAnchor.y : 0;

    let maxRadius = 0;
    for (let i = 0; i < cCount; i += 1) {
      const community = communities[i];
      r[i] = community.radius;
      if (community.radius > maxRadius) maxRadius = community.radius;

      const prevAnchor = previousIndex.communityAnchorById.get(community.communityId);

      let px = NaN;
      let py = NaN;

      if (prevAnchor) {
        // Translate by component movement to preserve relative arrangement.
        px = prevAnchor.x + componentDeltaX;
        py = prevAnchor.y + componentDeltaY;
      } else {
        // New community: try barycenter of previous node positions (also translated by component delta).
        let sx = 0;
        let sy = 0;
        let sc = 0;
        for (let k = 0; k < community.nodeIds.length; k += 1) {
          const pos = previousIndex.nodePositions[community.nodeIds[k]];
          if (!pos) continue;
          sx += pos.x;
          sy += pos.y;
          sc += 1;
        }

        if (sc > 0) {
          px = sx / sc + componentDeltaX;
          py = sy / sc + componentDeltaY;
        } else {
          // Deterministic spiral around component anchor.
          const seed = this.hashId(community.communityId);
          const angle = (seed % 10_000) * (TAU / 10_000) + i * GOLDEN_ANGLE;
          const step = maxRadius * 0.85 + nodeSpacing * 8;
          const rad = step * Math.sqrt(i + 1);
          px = component.anchorX + Math.cos(angle) * rad;
          py = component.anchorY + Math.sin(angle) * rad;
        }
      }

      prefX[i] = px;
      prefY[i] = py;
      x[i] = px;
      y[i] = py;
    }

    const iterations = this.adaptiveIterations(Math.round(26 * quality), cCount, 10, 60);
    const padding = nodeSpacing * 3;
    const maxStep = nodeSpacing * 8;

    this.relaxCommunityCircles({
      x,
      y,
      r,
      preferredX: prefX,
      preferredY: prefY,
      iterations,
      padding,
      centerX: component.anchorX,
      centerY: component.anchorY,
      pinStrength: 0.09,
      centerStrength: 0.02,
      maxStep,
      edgesA,
      edgesB,
      edgesW,
      maxW,
      linkDistance: nodeSpacing * 10,
      edgeStrength: 0.022 * quality
    });

    for (let i = 0; i < cCount; i += 1) {
      communities[i].anchorX = x[i];
      communities[i].anchorY = y[i];
    }
  }

  private relaxCommunityCircles(params: {
    x: Float64Array;
    y: Float64Array;
    r: Float64Array;
    preferredX: Float64Array;
    preferredY: Float64Array;
    iterations: number;
    padding: number;
    centerX: number;
    centerY: number;
    pinStrength: number;
    centerStrength: number;
    maxStep: number;
    edgesA: number[];
    edgesB: number[];
    edgesW: number[];
    maxW: number;
    linkDistance: number;
    edgeStrength: number;
  }): void {
    const {
      x,
      y,
      r,
      preferredX,
      preferredY,
      iterations,
      padding,
      centerX,
      centerY,
      pinStrength,
      centerStrength,
      maxStep,
      edgesA,
      edgesB,
      edgesW,
      maxW,
      linkDistance,
      edgeStrength
    } = params;

    const n = x.length;
    if (n <= 1) return;

    const dx = new Float64Array(n);
    const dy = new Float64Array(n);
    const mass = new Float64Array(n);

    let maxRadius = 0;
    for (let i = 0; i < n; i += 1) {
      const ri = r[i];
      mass[i] = ri * ri;
      if (ri > maxRadius) maxRadius = ri;
    }

    const store: BucketStore = {
      buckets: new Map<number, number[]>(),
      usedKeys: [],
      pool: []
    };

    const gridSize = Math.max(1, maxRadius + padding);

    const logMaxW = maxW > 0 ? Math.log1p(maxW) : 1;

    for (let iter = 0; iter < iterations; iter += 1) {
      dx.fill(0);
      dy.fill(0);

      const t = iterations <= 1 ? 0 : iter / (iterations - 1);
      const alpha = 0.25 + 0.75 * (1 - t);
      const stepCap = maxStep * alpha;

      // Pin + gravity to component anchor.
      for (let i = 0; i < n; i += 1) {
        dx[i] += (preferredX[i] - x[i]) * pinStrength;
        dy[i] += (preferredY[i] - y[i]) * pinStrength;
        dx[i] += (centerX - x[i]) * centerStrength;
        dy[i] += (centerY - y[i]) * centerStrength;
      }

      // Edge attraction between communities (weighted spring).
      for (let e = 0; e < edgesA.length; e += 1) {
        const a = edgesA[e];
        const b = edgesB[e];
        const w = edgesW[e];

        let vx = x[b] - x[a];
        let vy = y[b] - y[a];
        let dist2 = vx * vx + vy * vy;
        if (dist2 < EPS) {
          const seed = (a * 911 + b * 353 + iter * 37) % 360;
          const ang = seed * (TAU / 360);
          vx = Math.cos(ang);
          vy = Math.sin(ang);
          dist2 = 1;
        }
        const dist = Math.sqrt(dist2);

        const wNorm = maxW > 0 ? Math.log1p(w) / logMaxW : 0;
        const desired = r[a] + r[b] + linkDistance * (1.25 - 0.9 * wNorm);
        const force = (dist - desired) * edgeStrength * wNorm;

        const ux = vx / dist;
        const uy = vy / dist;

        dx[a] += ux * force;
        dy[a] += uy * force;
        dx[b] -= ux * force;
        dy[b] -= uy * force;
      }

      // Collisions via grid.
      this.bucketClear(store);

      for (let i = 0; i < n; i += 1) {
        const cx = Math.floor(x[i] / gridSize);
        const cy = Math.floor(y[i] / gridSize);
        const key = (cx + GRID_OFFSET) * GRID_SCALE + (cy + GRID_OFFSET);
        this.bucketPush(store, key, i);
      }

      for (let i = 0; i < n; i += 1) {
        const cx = Math.floor(x[i] / gridSize);
        const cy = Math.floor(y[i] / gridSize);

        for (let ox = -2; ox <= 2; ox += 1) {
          for (let oy = -2; oy <= 2; oy += 1) {
            const key = (cx + ox + GRID_OFFSET) * GRID_SCALE + (cy + oy + GRID_OFFSET);
            const bucket = store.buckets.get(key);
            if (!bucket) continue;

            for (let bi = 0; bi < bucket.length; bi += 1) {
              const j = bucket[bi];
              if (j <= i) continue;
              this.applyCircleCollision({
                i,
                j,
                x,
                y,
                r,
                dx,
                dy,
                mass,
                padding,
                iterSeed: iter
              });
            }
          }
        }
      }

      this.bucketClear(store);

      // Integrate.
      for (let i = 0; i < n; i += 1) {
        let sx = dx[i] * alpha;
        let sy = dy[i] * alpha;
        const speed2 = sx * sx + sy * sy;
        if (speed2 > stepCap * stepCap) {
          const inv = stepCap / Math.sqrt(speed2);
          sx *= inv;
          sy *= inv;
        }
        x[i] += sx;
        y[i] += sy;
      }
    }
  }

  // -----------------------
  // Node layout (per community)
  // -----------------------

  private layoutNodesInCommunity(params: {
    componentId: string;
    communityId: string;
    nodeIds: string[];
    anchorX: number;
    anchorY: number;
    communityRadius: number;
    nodeSpacing: number;
    quality: number;
    stability: number;
    adjacencyByNodeId: Map<string, Set<string>>;
    nodeHashById: Map<string, number>;
    previousIndex: SnapshotIndex;
  }): { nodeIds: string[]; x: Float64Array; y: Float64Array } {
    const {
      communityId,
      nodeIds,
      anchorX,
      anchorY,
      communityRadius,
      nodeSpacing,
      quality,
      stability,
      adjacencyByNodeId,
      nodeHashById,
      previousIndex
    } = params;

    const n = nodeIds.length;
    const x = new Float64Array(n);
    const y = new Float64Array(n);

    if (n === 0) return { nodeIds, x, y };

    // Use previous community anchor to preserve relative coordinates when anchors move.
    const prevAnchor = previousIndex.communityAnchorById.get(communityId);
    const anchorDeltaX = prevAnchor ? anchorX - prevAnchor.x : 0;
    const anchorDeltaY = prevAnchor ? anchorY - prevAnchor.y : 0;

    // Baseline (previous positions translated by anchor delta), used for temporal blending + movement caps.
    const baselineX = new Float64Array(n);
    const baselineY = new Float64Array(n);
    const hasBaseline = new Uint8Array(n);

    // Local index for membership tests.
    const localIndex = new Map<string, number>();
    for (let i = 0; i < n; i += 1) localIndex.set(nodeIds[i], i);

    const angleOffset = ((this.hashId(communityId) % 10_000) / 10_000) * TAU;
    const seedContainRadius = Math.max(nodeSpacing * 3, communityRadius - nodeSpacing * 2);

    // Seed positions.
    for (let i = 0; i < n; i += 1) {
      const nodeId = nodeIds[i];
      const prevPos = previousIndex.nodePositions[nodeId];

      if (prevPos) {
        const bx = prevPos.x + anchorDeltaX;
        const by = prevPos.y + anchorDeltaY;

        baselineX[i] = bx;
        baselineY[i] = by;
        hasBaseline[i] = 1;

        x[i] = bx;
        y[i] = by;
        continue;
      }

      // Try to spawn near existing neighbors (from previous positions).
      let sx = 0;
      let sy = 0;
      let sc = 0;

      const neighbors = adjacencyByNodeId.get(nodeId);
      if (neighbors) {
        for (const neighborId of neighbors) {
          const p = previousIndex.nodePositions[neighborId];
          if (!p) continue;
          sx += p.x + anchorDeltaX;
          sy += p.y + anchorDeltaY;
          sc += 1;
          if (sc >= 6) break; // limit cost
        }
      }

      if (sc > 0) {
        const nx = sx / sc;
        const ny = sy / sc;

        // Clamp inside community circle for cleanliness.
        let dx0 = nx - anchorX;
        let dy0 = ny - anchorY;
        const d = Math.sqrt(dx0 * dx0 + dy0 * dy0);
        if (d > seedContainRadius) {
          const inv = seedContainRadius / Math.max(EPS, d);
          dx0 *= inv;
          dy0 *= inv;
        }

        x[i] = anchorX + dx0;
        y[i] = anchorY + dy0;
        continue;
      }

      // Deterministic phyllotaxis seed (compact, uniform, avoids initial overlaps).
      const t = (i + 0.5) / Math.max(1, n);
      const rad = Math.sqrt(t) * seedContainRadius;
      const nodeSeed = nodeHashById.get(nodeId) ?? 0;
      const jitter = ((nodeSeed % 1024) / 1024 - 0.5) * (nodeSpacing * 0.12);
      const ang = i * GOLDEN_ANGLE + angleOffset + jitter * 0.01;

      x[i] = anchorX + Math.cos(ang) * rad;
      y[i] = anchorY + Math.sin(ang) * rad;
    }

    // Neighbor sampling (for edge forces): bounded for performance.
    let maxNeighbors =
      n <= 60 ? 28 :
      n <= 200 ? 18 :
      n <= 900 ? 12 :
      n <= 2500 ? 8 : 6;
    maxNeighbors = clamp(Math.round(maxNeighbors * (0.7 + 0.6 * quality)), 4, 40);

    const offsets = new Int32Array(n + 1);
    const neighborsFlat: number[] = [];

    // Reuse small buffers for streaming top-k selection (no per-node allocations).
    const bestIdx = new Int32Array(maxNeighbors);
    const bestScore = new Uint32Array(maxNeighbors);

    for (let i = 0; i < n; i += 1) {
      offsets[i] = neighborsFlat.length;

      const nodeId = nodeIds[i];
      const nodeSeed = (nodeHashById.get(nodeId) ?? this.hashId(nodeId)) >>> 0;
      const neighbors = adjacencyByNodeId.get(nodeId);

      if (!neighbors || neighbors.size === 0) continue;

      let bestCount = 0;
      let worstPos = -1;
      let worstScore = 0;
      let worstIdx = 0;

      for (const neighborId of neighbors) {
        const j = localIndex.get(neighborId);
        if (j == null || j === i) continue;

        const score = ((nodeHashById.get(neighborId) ?? this.hashId(neighborId)) ^ nodeSeed) >>> 0;

        if (bestCount < maxNeighbors) {
          bestIdx[bestCount] = j;
          bestScore[bestCount] = score;
          bestCount += 1;

          if (bestCount === maxNeighbors) {
            // Find initial worst.
            worstPos = 0;
            worstScore = bestScore[0];
            worstIdx = bestIdx[0];
            for (let t = 1; t < maxNeighbors; t += 1) {
              const s = bestScore[t];
              const idx = bestIdx[t];
              if (s > worstScore || (s === worstScore && idx > worstIdx)) {
                worstPos = t;
                worstScore = s;
                worstIdx = idx;
              }
            }
          }
          continue;
        }

        // Replace worst if candidate is better in lexicographic (score, idx).
        if (score < worstScore || (score === worstScore && j < worstIdx)) {
          bestIdx[worstPos] = j;
          bestScore[worstPos] = score;

          // Recompute worst (k is small).
          worstPos = 0;
          worstScore = bestScore[0];
          worstIdx = bestIdx[0];
          for (let t = 1; t < maxNeighbors; t += 1) {
            const s = bestScore[t];
            const idx = bestIdx[t];
            if (s > worstScore || (s === worstScore && idx > worstIdx)) {
              worstPos = t;
              worstScore = s;
              worstIdx = idx;
            }
          }
        }
      }

      // Insertion sort selected neighbors by (score, idx) for determinism.
      for (let a = 1; a < bestCount; a += 1) {
        const s = bestScore[a];
        const idx = bestIdx[a];
        let b = a - 1;
        while (
          b >= 0 &&
          (bestScore[b] > s || (bestScore[b] === s && bestIdx[b] > idx))
        ) {
          bestScore[b + 1] = bestScore[b];
          bestIdx[b + 1] = bestIdx[b];
          b -= 1;
        }
        bestScore[b + 1] = s;
        bestIdx[b + 1] = idx;
      }

      for (let t = 0; t < bestCount; t += 1) {
        neighborsFlat.push(bestIdx[t]);
      }
    }
    offsets[n] = neighborsFlat.length;

    // Force simulation parameters (adaptive for size, tuned for cleanliness + performance).
    const iterations = this.adaptiveIterations(Math.round(26 * quality), n, 6, 55);

    const minDist = nodeSpacing * 0.9;
    const repulsionCutoff =
      nodeSpacing * (n > 4000 ? 4.6 : n > 1500 ? 5.2 : 6.0);

    const gridSize = Math.max(1, repulsionCutoff);
    const edgeLength = nodeSpacing * 2.6;

    const edgeStrength = 0.020 * quality;
    const gravityStrength = 0.010 * quality;
    const repulsionStrength = nodeSpacing * 0.22 * (0.75 + 0.25 * quality);
    const collisionStrength = nodeSpacing * 0.18 * (0.85 + 0.15 * quality);

    const containmentRadius = Math.max(nodeSpacing * 3, communityRadius - nodeSpacing * 2);
    const containmentStrength = 0.028 * quality;

    const maxStepBase = nodeSpacing * (0.85 + 0.35 * quality);

    const fx = new Float64Array(n);
    const fy = new Float64Array(n);

    const store: BucketStore = {
      buckets: new Map<number, number[]>(),
      usedKeys: [],
      pool: []
    };

    for (let iter = 0; iter < iterations; iter += 1) {
      fx.fill(0);
      fy.fill(0);

      const t = iterations <= 1 ? 0 : iter / (iterations - 1);
      const alpha = 0.18 + 0.82 * (1 - t); // cooling
      const stepCap = maxStepBase * alpha;

      // Gravity + containment.
      for (let i = 0; i < n; i += 1) {
        const ax = anchorX - x[i];
        const ay = anchorY - y[i];
        fx[i] += ax * gravityStrength;
        fy[i] += ay * gravityStrength;

        const dist = Math.sqrt(ax * ax + ay * ay);
        if (dist > containmentRadius) {
          const over = dist - containmentRadius;
          const inv = 1 / Math.max(EPS, dist);
          fx[i] += ax * inv * over * containmentStrength;
          fy[i] += ay * inv * over * containmentStrength;
        }
      }

      // Spring edges (sampled neighbors).
      for (let i = 0; i < n; i += 1) {
        const start = offsets[i];
        const end = offsets[i + 1];

        for (let p = start; p < end; p += 1) {
          const j = neighborsFlat[p];
          if (j < 0 || j >= n) continue;

          let vx = x[j] - x[i];
          let vy = y[j] - y[i];
          let dist2 = vx * vx + vy * vy;

          if (dist2 < EPS) {
            const seed = ((i * 911 + j * 353 + iter * 37) % 360) * (TAU / 360);
            vx = Math.cos(seed);
            vy = Math.sin(seed);
            dist2 = 1;
          }

          const dist = Math.sqrt(dist2);
          const ux = vx / dist;
          const uy = vy / dist;

          const force = (dist - edgeLength) * edgeStrength;
          fx[i] += ux * force;
          fy[i] += uy * force;
          fx[j] -= ux * force;
          fy[j] -= uy * force;
        }
      }

      // Repulsion + collision via spatial hash grid.
      this.bucketClear(store);
      for (let i = 0; i < n; i += 1) {
        const cx = Math.floor(x[i] / gridSize);
        const cy = Math.floor(y[i] / gridSize);
        const key = (cx + GRID_OFFSET) * GRID_SCALE + (cy + GRID_OFFSET);
        this.bucketPush(store, key, i);
      }

      for (let i = 0; i < n; i += 1) {
        const cx = Math.floor(x[i] / gridSize);
        const cy = Math.floor(y[i] / gridSize);

        for (let ox = -1; ox <= 1; ox += 1) {
          for (let oy = -1; oy <= 1; oy += 1) {
            const key = (cx + ox + GRID_OFFSET) * GRID_SCALE + (cy + oy + GRID_OFFSET);
            const bucket = store.buckets.get(key);
            if (!bucket) continue;

            for (let bi = 0; bi < bucket.length; bi += 1) {
              const j = bucket[bi];
              if (j <= i) continue;

              let vx = x[i] - x[j];
              let vy = y[i] - y[j];
              let dist2 = vx * vx + vy * vy;

              if (dist2 < EPS) {
                const seed = ((i * 997 + j * 991 + iter * 37) % 360) * (TAU / 360);
                vx = Math.cos(seed);
                vy = Math.sin(seed);
                dist2 = 1;
              }

              const dist = Math.sqrt(dist2);
              if (dist > repulsionCutoff) continue;

              const ux = vx / dist;
              const uy = vy / dist;

              const t = (repulsionCutoff - dist) / repulsionCutoff;
              const repulse = t * repulsionStrength;

              fx[i] += ux * repulse;
              fy[i] += uy * repulse;
              fx[j] -= ux * repulse;
              fy[j] -= uy * repulse;

              if (dist < minDist) {
                const overlap = (minDist - dist) / Math.max(EPS, minDist);
                const push = overlap * collisionStrength;

                fx[i] += ux * push;
                fy[i] += uy * push;
                fx[j] -= ux * push;
                fy[j] -= uy * push;
              }
            }
          }
        }
      }

      this.bucketClear(store);

      // Integrate (capped).
      for (let i = 0; i < n; i += 1) {
        let sx = fx[i] * alpha;
        let sy = fy[i] * alpha;

        const speed2 = sx * sx + sy * sy;
        if (speed2 > stepCap * stepCap) {
          const inv = stepCap / Math.sqrt(speed2);
          sx *= inv;
          sy *= inv;
        }

        x[i] += sx;
        y[i] += sy;
      }
    }

    // Align rotation to previous frame in local (anchor-relative) coordinates to prevent "teleporty" rotations.
    if (prevAnchor) {
      let sumRe = 0;
      let sumIm = 0;
      let matched = 0;

      for (let i = 0; i < n; i += 1) {
        if (!hasBaseline[i]) continue;

        const ax = x[i] - anchorX;
        const ay = y[i] - anchorY;

        // Previous local vector (use original prev position relative to prev anchor).
        const nodeId = nodeIds[i];
        const prevPos = previousIndex.nodePositions[nodeId];
        if (!prevPos) continue;

        const bx = prevPos.x - prevAnchor.x;
        const by = prevPos.y - prevAnchor.y;

        sumRe += bx * ax + by * ay;
        sumIm += by * ax - bx * ay;
        matched += 1;
      }

      if (matched >= 6 && (sumRe * sumRe + sumIm * sumIm) > EPS) {
        const angle = Math.atan2(sumIm, sumRe);
        const cos = Math.cos(angle);
        const sin = Math.sin(angle);

        for (let i = 0; i < n; i += 1) {
          const ax = x[i] - anchorX;
          const ay = y[i] - anchorY;
          x[i] = anchorX + ax * cos - ay * sin;
          y[i] = anchorY + ax * sin + ay * cos;
        }
      }
    }

    // Temporal blending + movement cap relative to translated baseline (prevents teleport).
    const blendT = 1 - stability;
    const maxMove = nodeSpacing * 6;

    for (let i = 0; i < n; i += 1) {
      let fx0 = x[i];
      let fy0 = y[i];

      if (hasBaseline[i]) {
        const bx = baselineX[i];
        const by = baselineY[i];

        fx0 = bx + (fx0 - bx) * blendT;
        fy0 = by + (fy0 - by) * blendT;

        const dxm = fx0 - bx;
        const dym = fy0 - by;
        const dist = Math.sqrt(dxm * dxm + dym * dym);
        if (dist > maxMove) {
          const inv = maxMove / Math.max(EPS, dist);
          fx0 = bx + dxm * inv;
          fy0 = by + dym * inv;
        }
      }

      // Final containment clamp (keeps communities tight and avoids overlaps across communities).
      const ax = fx0 - anchorX;
      const ay = fy0 - anchorY;
      const dist = Math.sqrt(ax * ax + ay * ay);
      if (dist > containmentRadius) {
        const inv = containmentRadius / Math.max(EPS, dist);
        fx0 = anchorX + ax * inv;
        fy0 = anchorY + ay * inv;
      }

      x[i] = fx0;
      y[i] = fy0;
    }

    // Tiny post-pass collision relaxation (fixes overlaps that blending might reintroduce).
    this.postRelaxNodeCollisions({
      x,
      y,
      anchorX,
      anchorY,
      nodeSpacing,
      containmentRadius,
      iterations: n <= 1200 ? 2 : 1
    });

    return { nodeIds, x, y };
  }

  private postRelaxNodeCollisions(params: {
    x: Float64Array;
    y: Float64Array;
    anchorX: number;
    anchorY: number;
    nodeSpacing: number;
    containmentRadius: number;
    iterations: number;
  }): void {
    const { x, y, anchorX, anchorY, nodeSpacing, containmentRadius, iterations } = params;
    const n = x.length;
    if (n <= 1 || iterations <= 0) return;

    const minDist = nodeSpacing * 0.9;
    const gridSize = Math.max(1, minDist * 2.0);
    const maxStep = nodeSpacing * 0.55;

    const fx = new Float64Array(n);
    const fy = new Float64Array(n);

    const store: BucketStore = {
      buckets: new Map<number, number[]>(),
      usedKeys: [],
      pool: []
    };

    for (let iter = 0; iter < iterations; iter += 1) {
      fx.fill(0);
      fy.fill(0);

      this.bucketClear(store);
      for (let i = 0; i < n; i += 1) {
        const cx = Math.floor(x[i] / gridSize);
        const cy = Math.floor(y[i] / gridSize);
        const key = (cx + GRID_OFFSET) * GRID_SCALE + (cy + GRID_OFFSET);
        this.bucketPush(store, key, i);
      }

      for (let i = 0; i < n; i += 1) {
        const cx = Math.floor(x[i] / gridSize);
        const cy = Math.floor(y[i] / gridSize);

        for (let ox = -1; ox <= 1; ox += 1) {
          for (let oy = -1; oy <= 1; oy += 1) {
            const key = (cx + ox + GRID_OFFSET) * GRID_SCALE + (cy + oy + GRID_OFFSET);
            const bucket = store.buckets.get(key);
            if (!bucket) continue;

            for (let bi = 0; bi < bucket.length; bi += 1) {
              const j = bucket[bi];
              if (j <= i) continue;

              let vx = x[i] - x[j];
              let vy = y[i] - y[j];
              let dist2 = vx * vx + vy * vy;

              if (dist2 < EPS) {
                const seed = ((i * 997 + j * 991 + iter * 37) % 360) * (TAU / 360);
                vx = Math.cos(seed);
                vy = Math.sin(seed);
                dist2 = 1;
              }

              const dist = Math.sqrt(dist2);
              if (dist >= minDist) continue;

              const overlap = minDist - dist;
              const ux = vx / dist;
              const uy = vy / dist;

              fx[i] += ux * overlap * 0.5;
              fy[i] += uy * overlap * 0.5;
              fx[j] -= ux * overlap * 0.5;
              fy[j] -= uy * overlap * 0.5;
            }
          }
        }
      }

      this.bucketClear(store);

      for (let i = 0; i < n; i += 1) {
        let sx = fx[i];
        let sy = fy[i];

        const speed2 = sx * sx + sy * sy;
        if (speed2 > maxStep * maxStep) {
          const inv = maxStep / Math.sqrt(speed2);
          sx *= inv;
          sy *= inv;
        }

        let nx = x[i] + sx;
        let ny = y[i] + sy;

        // keep inside containment
        const ax = nx - anchorX;
        const ay = ny - anchorY;
        const dist = Math.sqrt(ax * ax + ay * ay);
        if (dist > containmentRadius) {
          const inv = containmentRadius / Math.max(EPS, dist);
          nx = anchorX + ax * inv;
          ny = anchorY + ay * inv;
        }

        x[i] = nx;
        y[i] = ny;
      }
    }
  }

  // -----------------------
  // Adaptive iteration helper
  // -----------------------

  private adaptiveIterations(base: number, size: number, min: number, max: number): number {
    let factor = 1.0;

    if (size <= 40) factor = 1.25;
    else if (size <= 120) factor = 1.1;
    else if (size <= 450) factor = 1.0;
    else if (size <= 1200) factor = 0.75;
    else if (size <= 4000) factor = 0.55;
    else factor = 0.40;

    return clamp(Math.round(base * factor), min, max);
  }

  // -----------------------
  // BucketStore (reuses arrays to reduce GC in tight loops)
  // -----------------------

  private bucketPush(store: BucketStore, key: number, value: number): void {
    let arr = store.buckets.get(key);
    if (!arr) {
      arr = store.pool.pop() ?? [];
      store.buckets.set(key, arr);
      store.usedKeys.push(key);
    }
    arr.push(value);
  }

  private bucketClear(store: BucketStore): void {
    for (let i = 0; i < store.usedKeys.length; i += 1) {
      const key = store.usedKeys[i];
      const arr = store.buckets.get(key);
      if (arr) {
        arr.length = 0;
        store.pool.push(arr);
        store.buckets.delete(key);
      }
    }
    store.usedKeys.length = 0;
  }
}

export const CLEAN_TEMPORAL_STRATEGY_DEFINITION: NetworkLayoutStrategyDefinition = {
  strategy: "clean-temporal",
  label: "Clean Temporal",
  fields: [
    { key: "quality", label: "Quality", min: 0.5, max: 1.5, step: 0.05 },
    { key: "stability", label: "Stability", min: 0, max: 0.95, step: 0.05 },
    { key: "nodeSpacing", label: "Node Spacing", min: 4, max: 20, step: 1 }
  ],
  createInitialConfig: () => ({
    quality: DEFAULT_QUALITY,
    stability: DEFAULT_STABILITY,
    nodeSpacing: DEFAULT_NODE_SPACING
  }),
  summarizeConfig: (config) => {
    const quality = Number(config.quality ?? DEFAULT_QUALITY);
    const stability = Number(config.stability ?? DEFAULT_STABILITY);
    const nodeSpacing = Number(config.nodeSpacing ?? DEFAULT_NODE_SPACING);
    return `q=${quality.toFixed(2)} stab=${stability.toFixed(2)} space=${nodeSpacing}`;
  },
  createAlgorithm: () => new CleanTemporalLayoutAlgorithm()
};