import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutStrategyDefinition } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyDefinition";
import type { NetworkLayoutInput, NetworkLayoutOutput } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";
import type { WorkerCommunityTarget, WorkerComponentTarget, WorkerNodeTarget } from "@/domain/timelapse/workerProtocol";

const TAU = Math.PI * 2;
const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5));
const EPS = 1e-9;

const DEFAULT_NODE_SPACING = 8;
const DEFAULT_STABILITY = 0.8;
const DEFAULT_QUALITY = 1.0;

// Spatial hash packing: unique integer key for grid cell (cx, cy)
const GRID_OFFSET = 1_048_576; // 2^20
const GRID_SCALE  = 2_097_153; // 2*offset + 1

const compareString = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, v));

// ──────────────────────────────────────────────────────────────────────────────
// Types (snapshot & plan)
// ──────────────────────────────────────────────────────────────────────────────

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
  nodeIds: string[];
  radius: number;
  anchorX: number;
  anchorY: number;
};

type ComponentPlan = {
  componentId: string;
  nodeIds: string[];
  communities: CommunityPlan[];
  radius: number;
  anchorX: number;
  anchorY: number;
};

type GridStore = {
  buckets: Map<number, number[]>;
  usedKeys: number[];
  pool: number[][];
};

// ──────────────────────────────────────────────────────────────────────────────
// Algorithm
// ──────────────────────────────────────────────────────────────────────────────

/**
 * StrictTemporalLayoutAlgorithm
 *
 * Design goals:
 *   1. Non-tangled graphs — achieved via BFS-ring node layout: hub nodes in the
 *      centre, their neighbours on the next ring, etc.  Connected nodes are
 *      always on adjacent rings so edges are short and radial; no force loop
 *      can push them through unrelated clusters.
 *   2. Temporal stability — achieved by treating the BFS-ring positions as
 *      *targets* and lerping the actual rendered positions toward them each
 *      frame.  Community and component anchors are similarly held at their
 *      previous positions and only pushed apart when they physically overlap.
 *
 * Force simulation is kept only as a final collision-resolution post-pass
 * (1–3 iterations), which converges immediately.
 */
export class StrictTemporalLayoutAlgorithm implements INetworkLayoutAlgorithm {
  public readonly strategy = "strict-temporal" as const;

  // ────────────────────────────────────────────────────────────────────────────
  // Main entry point
  // ────────────────────────────────────────────────────────────────────────────

  public run(input: NetworkLayoutInput): NetworkLayoutOutput {
    const quality    = resolveNumberConfig(input.strategyConfig, "quality",      DEFAULT_QUALITY,      0.5, 1.5);
    const stability  = resolveNumberConfig(input.strategyConfig, "stability",    DEFAULT_STABILITY,    0,   0.95);
    const nodeSpacing = resolveNumberConfig(input.strategyConfig, "nodeSpacing", DEFAULT_NODE_SPACING, 4,   20);

    const nodeIdsSorted = [...input.nodeIds].sort(compareString);

    const nodeHashById = new Map<string, number>();
    for (const nodeId of nodeIdsSorted) nodeHashById.set(nodeId, this.hashId(nodeId));

    const previousSnapshot = this.readSnapshot(input.previousState);
    const previousIndex    = this.indexSnapshot(previousSnapshot);

    // ── 1. Partition into connected components ────────────────────────────────
    const rawComponents       = this.computeConnectedComponents(nodeIdsSorted, input.adjacencyByNodeId);
    const componentsAssigned  = this.assignStableComponentIds(rawComponents, previousIndex);

    // ── 2. Detect communities + build plans ──────────────────────────────────
    const componentPlans: ComponentPlan[] = [];

    for (const component of componentsAssigned) {
      const communitiesRaw      = this.detectCommunitiesLabelPropagation({
        componentId: component.componentId,
        nodeIds: component.nodeIds,
        adjacencyByNodeId: input.adjacencyByNodeId,
        previousIndex
      });
      const communitiesAssigned = this.assignStableCommunityIds(
        component.componentId, communitiesRaw, previousIndex
      );

      const communityPlans: CommunityPlan[] = communitiesAssigned.map(c => ({
        communityId: c.communityId,
        componentId: component.componentId,
        nodeIds:     c.nodeIds,
        radius:      this.computeCommunityRadius(c.nodeIds.length, nodeSpacing),
        anchorX:     0,
        anchorY:     0
      }));
      communityPlans.sort(
        (a, b) => b.nodeIds.length - a.nodeIds.length || compareString(a.communityId, b.communityId)
      );

      componentPlans.push({
        componentId: component.componentId,
        nodeIds:     component.nodeIds,
        communities: communityPlans,
        radius:      this.computeComponentRadius(communityPlans, nodeSpacing),
        anchorX:     0,
        anchorY:     0
      });
    }

    componentPlans.sort(
      (a, b) => b.nodeIds.length - a.nodeIds.length || compareString(a.componentId, b.componentId)
    );

    // ── 3. Place component anchors (start from prev, separate overlaps) ───────
    this.placeComponentAnchors(componentPlans, previousIndex, nodeSpacing);

    // ── 4. Place community anchors inside each component ─────────────────────
    for (const component of componentPlans) {
      this.placeCommunityAnchors(component, previousIndex, nodeSpacing);
    }

    // ── 5. Layout nodes inside each community ────────────────────────────────
    const nextNodePositions: Record<string, { x: number; y: number }> = Object.create(null);
    const nodeMetaById = new Map<string, {
      componentId: string;
      communityId: string;
      anchorX: number;
      anchorY: number;
    }>();

    for (const component of componentPlans) {
      for (const community of component.communities) {
        const { nodeIds: ids, x, y } = this.layoutNodesInCommunity({
          communityId:     community.communityId,
          nodeIds:         community.nodeIds,
          anchorX:         community.anchorX,
          anchorY:         community.anchorY,
          communityRadius: community.radius,
          nodeSpacing,
          stability,
          quality,
          adjacencyByNodeId: input.adjacencyByNodeId,
          nodeHashById,
          previousIndex
        });

        for (let k = 0; k < ids.length; k++) {
          nextNodePositions[ids[k]] = { x: x[k], y: y[k] };
          nodeMetaById.set(ids[k], {
            componentId: component.componentId,
            communityId: community.communityId,
            anchorX:     community.anchorX,
            anchorY:     community.anchorY
          });
        }
      }
    }

    // ── 6. Build output targets + next snapshot ───────────────────────────────
    const componentTargets: WorkerComponentTarget[] = [];
    const communityTargets: WorkerCommunityTarget[] = [];
    const nextSnapshot: LayoutSnapshot = {
      version:       2,
      components:    [],
      communities:   [],
      nodePositions: nextNodePositions
    };

    for (const component of componentPlans) {
      componentTargets.push({
        componentId: component.componentId,
        nodeIds:     component.nodeIds,
        anchorX:     component.anchorX,
        anchorY:     component.anchorY
      });
      nextSnapshot.components.push({
        componentId: component.componentId,
        nodeIds:     component.nodeIds,
        anchorX:     component.anchorX,
        anchorY:     component.anchorY,
        radius:      component.radius
      });

      for (const community of component.communities) {
        communityTargets.push({
          communityId: community.communityId,
          componentId: component.componentId,
          nodeIds:     community.nodeIds,
          anchorX:     community.anchorX,
          anchorY:     community.anchorY
        });
        nextSnapshot.communities.push({
          communityId: community.communityId,
          componentId: component.componentId,
          nodeIds:     community.nodeIds,
          anchorX:     community.anchorX,
          anchorY:     community.anchorY,
          radius:      community.radius
        });
      }
    }

    // Node targets (sorted for stable output order)
    const nodeTargets: WorkerNodeTarget[] = [];

    for (const nodeId of nodeIdsSorted) {
      const pos    = nextNodePositions[nodeId] ?? previousIndex.nodePositions[nodeId] ?? { x: 0, y: 0 };
      const meta   = nodeMetaById.get(nodeId);
      const componentId = meta?.componentId ?? previousIndex.componentByNodeId.get(nodeId) ?? "component:unknown";
      const communityId = meta?.communityId ?? previousIndex.communityByNodeId.get(nodeId) ?? `${componentId}:community:unknown`;
      const anchorX     = meta?.anchorX ?? pos.x;
      const anchorY     = meta?.anchorY ?? pos.y;

      let neighborX = 0, neighborY = 0, neighborCount = 0;
      const neighbors = input.adjacencyByNodeId.get(nodeId);
      if (neighbors) {
        for (const neighborId of neighbors) {
          const p = nextNodePositions[neighborId];
          if (!p) continue;
          neighborX += p.x;
          neighborY += p.y;
          neighborCount++;
        }
      }
      if (neighborCount === 0) { neighborX = pos.x; neighborY = pos.y; }
      else { neighborX /= neighborCount; neighborY /= neighborCount; }

      nodeTargets.push({
        nodeId, componentId, communityId,
        targetX:   pos.x,
        targetY:   pos.y,
        neighborX, neighborY,
        anchorX,   anchorY
      });
    }

    return {
      layout: {
        components:  componentTargets,
        communities: communityTargets,
        nodeTargets
      },
      metadata: { state: nextSnapshot }
    };
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Snapshot I/O  (unchanged from original)
  // ────────────────────────────────────────────────────────────────────────────

  private readSnapshot(value: unknown): LayoutSnapshot | undefined {
    if (!value || typeof value !== "object") return undefined;
    const v = value as Record<string, unknown>;

    if (!Array.isArray(v.components) || !Array.isArray(v.communities) ||
        !v.nodePositions || typeof v.nodePositions !== "object") return undefined;

    const components: SnapshotComponent[] = [];
    for (const entry of v.components as unknown[]) {
      if (!entry || typeof entry !== "object") continue;
      const e = entry as Record<string, unknown>;
      const componentId = typeof e.componentId === "string" ? e.componentId : null;
      const anchorX = Number(e.anchorX), anchorY = Number(e.anchorY);
      if (!componentId || !Number.isFinite(anchorX) || !Number.isFinite(anchorY)) continue;
      const nodeIds = this.readStringArray(e.nodeIds) ?? this.readStringArray(e.members) ?? [];
      const radius  = e.radius == null ? undefined : Number(e.radius);
      components.push({ componentId, nodeIds, anchorX, anchorY, radius: Number.isFinite(radius ?? NaN) ? radius : undefined });
    }

    const communities: SnapshotCommunity[] = [];
    for (const entry of v.communities as unknown[]) {
      if (!entry || typeof entry !== "object") continue;
      const e = entry as Record<string, unknown>;
      const communityId = typeof e.communityId === "string" ? e.communityId : null;
      const componentId = typeof e.componentId === "string" ? e.componentId : null;
      const anchorX = Number(e.anchorX), anchorY = Number(e.anchorY);
      if (!communityId || !componentId || !Number.isFinite(anchorX) || !Number.isFinite(anchorY)) continue;
      const nodeIds = this.readStringArray(e.nodeIds) ?? this.readStringArray(e.members) ?? [];
      const radius  = e.radius == null ? undefined : Number(e.radius);
      communities.push({ communityId, componentId, nodeIds, anchorX, anchorY, radius: Number.isFinite(radius ?? NaN) ? radius : undefined });
    }

    return { version: 2, components, communities, nodePositions: this.toPointRecord(v.nodePositions) };
  }

  private readStringArray(value: unknown): string[] | null {
    if (!value) return null;
    const out: string[] = [];
    if (Array.isArray(value)) {
      for (const item of value) if (typeof item === "string") out.push(item);
      return out;
    }
    if (value instanceof Set) {
      for (const item of value) if (typeof item === "string") out.push(item);
      return out;
    }
    return null;
  }

  private toPointRecord(value: unknown): Record<string, { x: number; y: number }> {
    if (!value || typeof value !== "object") return Object.create(null);
    const out: Record<string, { x: number; y: number }> = Object.create(null);
    for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
      if (!raw || typeof raw !== "object") continue;
      const pt = raw as { x?: unknown; y?: unknown };
      const x = Number(pt.x), y = Number(pt.y);
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
      out[key] = { x, y };
    }
    return out;
  }

  private indexSnapshot(snapshot: LayoutSnapshot | undefined): SnapshotIndex {
    const componentByNodeId    = new Map<string, string>();
    const communityByNodeId    = new Map<string, string>();
    const componentSizeById    = new Map<string, number>();
    const communitySizeById    = new Map<string, number>();
    const componentAnchorById  = new Map<string, { x: number; y: number }>();
    const communityAnchorById  = new Map<string, { x: number; y: number }>();
    const communityComponentById = new Map<string, string>();

    if (!snapshot) return {
      componentByNodeId, communityByNodeId,
      componentSizeById, communitySizeById,
      componentAnchorById, communityAnchorById,
      communityComponentById,
      nodePositions: Object.create(null)
    };

    for (const c of snapshot.components) {
      componentSizeById.set(c.componentId, c.nodeIds.length);
      componentAnchorById.set(c.componentId, { x: c.anchorX, y: c.anchorY });
      for (const nodeId of c.nodeIds) componentByNodeId.set(nodeId, c.componentId);
    }

    for (const c of snapshot.communities) {
      communitySizeById.set(c.communityId, c.nodeIds.length);
      communityAnchorById.set(c.communityId, { x: c.anchorX, y: c.anchorY });
      communityComponentById.set(c.communityId, c.componentId);
      for (const nodeId of c.nodeIds) communityByNodeId.set(nodeId, c.communityId);
    }

    return {
      componentByNodeId, communityByNodeId,
      componentSizeById, communitySizeById,
      componentAnchorById, communityAnchorById,
      communityComponentById,
      nodePositions: snapshot.nodePositions ?? Object.create(null)
    };
  }

  // ────────────────────────────────────────────────────────────────────────────
  // ID utilities  (unchanged from original)
  // ────────────────────────────────────────────────────────────────────────────

  private hashId(id: string): number {
    let h = 2166136261;
    for (let i = 0; i < id.length; i++) { h ^= id.charCodeAt(i); h = Math.imul(h, 16777619); }
    return h >>> 0;
  }

  private buildComponentId(nodeIds: string[]): string {
    return `component:${nodeIds.length}:${this.hashId(nodeIds.slice(0, 10).join(",")).toString(36)}`;
  }

  private buildCommunityId(componentId: string, nodeIds: string[]): string {
    return `${componentId}:community:${nodeIds.length}:${this.hashId(nodeIds.slice(0, 10).join(",")).toString(36)}`;
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Graph partitioning  (unchanged from original)
  // ────────────────────────────────────────────────────────────────────────────

  private computeConnectedComponents(
    nodeIdsSorted: string[],
    adjacencyByNodeId: Map<string, Set<string>>
  ): string[][] {
    const visited = new Set<string>();
    const components: string[][] = [];

    for (const root of nodeIdsSorted) {
      if (visited.has(root)) continue;
      const queue: string[] = [root];
      visited.add(root);
      let qi = 0;
      const members: string[] = [];
      while (qi < queue.length) {
        const cur = queue[qi++];
        members.push(cur);
        const neighbors = adjacencyByNodeId.get(cur);
        if (!neighbors) continue;
        for (const nb of neighbors) {
          if (visited.has(nb)) continue;
          visited.add(nb);
          queue.push(nb);
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
    const claimed  = new Set<string>();
    const assigned: Array<{ componentId: string; nodeIds: string[] }> = [];

    for (const nodeIds of rawComponents) {
      const counts = new Map<string, number>();
      for (const nodeId of nodeIds) {
        const prevId = previous.componentByNodeId.get(nodeId);
        if (prevId) counts.set(prevId, (counts.get(prevId) ?? 0) + 1);
      }

      let bestId = null as string | null, bestOverlap = 0, bestRatio = -1;
      for (const [prevId, overlap] of counts) {
        if (claimed.has(prevId)) continue;
        const ratio = overlap / Math.max(1, Math.max(previous.componentSizeById.get(prevId) ?? 0, nodeIds.length));
        if (overlap > bestOverlap || (overlap === bestOverlap && ratio > bestRatio) ||
            (overlap === bestOverlap && ratio === bestRatio && bestId !== null && compareString(prevId, bestId) < 0)) {
          bestId = prevId; bestOverlap = overlap; bestRatio = ratio;
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

    const sorted = [...nodeIds].sort(compareString);
    const nodeIndex = new Map<string, number>();
    for (let i = 0; i < sorted.length; i++) nodeIndex.set(sorted[i], i);

    // Seed with previous community IDs when available (stabilises across frames)
    const labels = new Map<string, string>();
    for (const nodeId of sorted) {
      const prevCommunityId = previousIndex.communityByNodeId.get(nodeId);
      const prevCompId      = prevCommunityId ? previousIndex.communityComponentById.get(prevCommunityId) : null;
      labels.set(nodeId, (prevCommunityId && prevCompId === componentId) ? prevCommunityId : nodeId);
    }

    const n = sorted.length;
    const maxIterations = clamp(Math.ceil(Math.log2(n + 1)) + 6, 6, 18);
    const inertia = 1.15; // self-label bias for stability
    const freq    = new Map<string, number>();

    for (let iter = 0; iter < maxIterations; iter++) {
      let changed = false;
      for (const nodeId of sorted) {
        const neighbors = adjacencyByNodeId.get(nodeId);
        if (!neighbors || neighbors.size === 0) continue;

        freq.clear();
        for (const nb of neighbors) {
          if (!nodeIndex.has(nb)) continue;
          const label = labels.get(nb) ?? nb;
          freq.set(label, (freq.get(label) ?? 0) + 1);
        }

        const current = labels.get(nodeId) ?? nodeId;
        freq.set(current, (freq.get(current) ?? 0) + inertia);

        let best = current, bestScore = -Infinity;
        for (const [label, score] of freq) {
          if (score > bestScore || (score === bestScore && compareString(label, best) < 0)) {
            best = label; bestScore = score;
          }
        }

        if (best !== current) { labels.set(nodeId, best); changed = true; }
      }
      if (!changed) break;
    }

    const byLabel = new Map<string, string[]>();
    for (const nodeId of sorted) {
      const label = labels.get(nodeId) ?? nodeId;
      const arr   = byLabel.get(label);
      if (arr) arr.push(nodeId); else byLabel.set(label, [nodeId]);
    }

    const communities = [...byLabel.values()];
    for (const c of communities) c.sort(compareString);
    communities.sort((a, b) => b.length - a.length || compareString(a[0] ?? "", b[0] ?? ""));
    return communities;
  }

  private assignStableCommunityIds(
    componentId: string,
    rawCommunities: string[][],
    previous: SnapshotIndex
  ): Array<{ communityId: string; nodeIds: string[] }> {
    const claimed  = new Set<string>();
    const assigned: Array<{ communityId: string; nodeIds: string[] }> = [];

    for (const nodeIds of rawCommunities) {
      const counts = new Map<string, number>();
      for (const nodeId of nodeIds) {
        const prevId = previous.communityByNodeId.get(nodeId);
        if (!prevId || previous.communityComponentById.get(prevId) !== componentId) continue;
        counts.set(prevId, (counts.get(prevId) ?? 0) + 1);
      }

      let bestId = null as string | null, bestOverlap = 0, bestRatio = -1;
      for (const [prevId, overlap] of counts) {
        if (claimed.has(prevId)) continue;
        const ratio = overlap / Math.max(1, Math.max(previous.communitySizeById.get(prevId) ?? 0, nodeIds.length));
        if (overlap > bestOverlap || (overlap === bestOverlap && ratio > bestRatio) ||
            (overlap === bestOverlap && ratio === bestRatio && bestId !== null && compareString(prevId, bestId) < 0)) {
          bestId = prevId; bestOverlap = overlap; bestRatio = ratio;
        }
      }

      const communityId = bestId ?? this.buildCommunityId(componentId, nodeIds);
      if (bestId) claimed.add(bestId);
      assigned.push({ communityId, nodeIds });
    }

    assigned.sort((a, b) => b.nodeIds.length - a.nodeIds.length || compareString(a.communityId, b.communityId));
    return assigned;
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Radius estimation
  // ────────────────────────────────────────────────────────────────────────────

  private computeCommunityRadius(nodeCount: number, nodeSpacing: number): number {
    // Generous estimate so BFS rings fit even for star-like topologies
    const n = Math.max(1, nodeCount);
    return nodeSpacing * (2.5 + 1.3 * Math.sqrt(n));
  }

  private computeComponentRadius(communities: CommunityPlan[], nodeSpacing: number): number {
    if (communities.length === 0) return nodeSpacing * 10;
    let sumSq = 0;
    for (const c of communities) sumSq += c.radius * c.radius;
    return Math.sqrt(sumSq * 1.3) + nodeSpacing * 5;
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Component anchor placement
  // ────────────────────────────────────────────────────────────────────────────

  /**
   * Starts every component at its previous anchor (maximum stability) and only
   * pushes components apart when they physically overlap.  A global translation
   * correction is applied afterwards so the whole layout doesn't drift.
   */
  private placeComponentAnchors(
    components: ComponentPlan[],
    previous: SnapshotIndex,
    nodeSpacing: number
  ): void {
    const n = components.length;
    if (n === 0) return;

    if (n === 1) {
      const only = components[0];
      const prev = previous.componentAnchorById.get(only.componentId);
      only.anchorX = prev?.x ?? 0;
      only.anchorY = prev?.y ?? 0;
      return;
    }

    // Reference centre: mean of all known previous anchors
    let refX = 0, refY = 0, refN = 0;
    for (const { componentId } of components) {
      const prev = previous.componentAnchorById.get(componentId);
      if (prev) { refX += prev.x; refY += prev.y; refN++; }
    }
    if (refN > 0) { refX /= refN; refY /= refN; }

    const x = new Float64Array(n);
    const y = new Float64Array(n);

    for (let i = 0; i < n; i++) {
      const comp = components[i];
      const prev = previous.componentAnchorById.get(comp.componentId);
      if (prev) {
        x[i] = prev.x;
        y[i] = prev.y;
      } else {
        // New component: deterministic golden-angle spiral around the reference centre
        const angle = i * GOLDEN_ANGLE;
        const rad   = nodeSpacing * 14 * Math.sqrt(i + 1);
        x[i] = refX + Math.cos(angle) * rad;
        y[i] = refY + Math.sin(angle) * rad;
      }
    }

    // Separation pass: push overlapping circles apart (weighted by component size)
    const padding = nodeSpacing * 4;
    for (let iter = 0; iter < 80; iter++) {
      let anyOverlap = false;
      for (let i = 0; i < n; i++) {
        for (let j = i + 1; j < n; j++) {
          const dx = x[j] - x[i], dy = y[j] - y[i];
          const d2  = dx * dx + dy * dy;
          const min = components[i].radius + components[j].radius + padding;
          if (d2 >= min * min) continue;
          anyOverlap = true;
          const d   = Math.sqrt(d2) || EPS;
          const ovl = (min - d) * 0.55;
          const ux  = dx / d, uy = dy / d;
          const si  = components[i].nodeIds.length;
          const sj  = components[j].nodeIds.length;
          const tot = si + sj;
          x[i] -= ux * ovl * sj / tot;
          y[i] -= uy * ovl * sj / tot;
          x[j] += ux * ovl * si / tot;
          y[j] += uy * ovl * si / tot;
        }
      }
      if (!anyOverlap) break;
    }

    // Align translation to minimise drift vs. previous anchors
    let sdx = 0, sdy = 0, sc = 0;
    for (let i = 0; i < n; i++) {
      const prev = previous.componentAnchorById.get(components[i].componentId);
      if (!prev) continue;
      sdx += prev.x - x[i];
      sdy += prev.y - y[i];
      sc++;
    }
    if (sc > 0) {
      const tdx = sdx / sc, tdy = sdy / sc;
      for (let i = 0; i < n; i++) { x[i] += tdx; y[i] += tdy; }
    }

    for (let i = 0; i < n; i++) {
      components[i].anchorX = x[i];
      components[i].anchorY = y[i];
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Community anchor placement
  // ────────────────────────────────────────────────────────────────────────────

  /**
   * Starts communities at their previous positions translated by the component's
   * movement delta, then separates overlaps.  Never uses spring forces — only
   * repulsion — so communities stay near where they were.
   */
  private placeCommunityAnchors(
    component: ComponentPlan,
    previous: SnapshotIndex,
    nodeSpacing: number
  ): void {
    const communities = component.communities;
    if (communities.length === 0) return;

    if (communities.length === 1) {
      communities[0].anchorX = component.anchorX;
      communities[0].anchorY = component.anchorY;
      return;
    }

    const cN = communities.length;
    const x  = new Float64Array(cN);
    const y  = new Float64Array(cN);

    // How much the component anchor moved this frame
    const prevComp = previous.componentAnchorById.get(component.componentId);
    const cdx = prevComp ? component.anchorX - prevComp.x : 0;
    const cdy = prevComp ? component.anchorY - prevComp.y : 0;

    for (let i = 0; i < cN; i++) {
      const comm = communities[i];
      const prev = previous.communityAnchorById.get(comm.communityId);
      if (prev) {
        // Translate by component movement delta to preserve relative arrangement
        x[i] = prev.x + cdx;
        y[i] = prev.y + cdy;
      } else {
        // New community: golden-angle spiral around the component anchor
        const angle = i * GOLDEN_ANGLE;
        const rad   = Math.max(communities[0].radius * 0.6, nodeSpacing * 5) * Math.sqrt(i + 1);
        x[i] = component.anchorX + Math.cos(angle) * rad;
        y[i] = component.anchorY + Math.sin(angle) * rad;
      }
    }

    // Separation pass
    const padding = nodeSpacing * 2;
    for (let iter = 0; iter < 80; iter++) {
      let anyOverlap = false;
      for (let i = 0; i < cN; i++) {
        for (let j = i + 1; j < cN; j++) {
          const dx = x[j] - x[i], dy = y[j] - y[i];
          const d2  = dx * dx + dy * dy;
          const min = communities[i].radius + communities[j].radius + padding;
          if (d2 >= min * min) continue;
          anyOverlap = true;
          const d   = Math.sqrt(d2) || EPS;
          const ovl = (min - d) * 0.55;
          const ux  = dx / d, uy = dy / d;
          const si  = communities[i].nodeIds.length;
          const sj  = communities[j].nodeIds.length;
          const tot = si + sj;
          x[i] -= ux * ovl * sj / tot;
          y[i] -= uy * ovl * sj / tot;
          x[j] += ux * ovl * si / tot;
          y[j] += uy * ovl * si / tot;
        }
      }
      if (!anyOverlap) break;
    }

    for (let i = 0; i < cN; i++) {
      communities[i].anchorX = x[i];
      communities[i].anchorY = y[i];
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Node layout  (BFS-ring targets + temporal lerp)
  // ────────────────────────────────────────────────────────────────────────────

  private layoutNodesInCommunity(params: {
    communityId: string;
    nodeIds: string[];
    anchorX: number;
    anchorY: number;
    communityRadius: number;
    nodeSpacing: number;
    stability: number;
    quality: number;
    adjacencyByNodeId: Map<string, Set<string>>;
    nodeHashById: Map<string, number>;
    previousIndex: SnapshotIndex;
  }): { nodeIds: string[]; x: Float64Array; y: Float64Array } {
    const {
      communityId, nodeIds, anchorX, anchorY, communityRadius,
      nodeSpacing, stability, quality,
      adjacencyByNodeId, nodeHashById, previousIndex
    } = params;
    const n = nodeIds.length;
    const x = new Float64Array(n);
    const y = new Float64Array(n);
    if (n === 0) return { nodeIds, x, y };

    const localIndex = new Map<string, number>();
    for (let i = 0; i < n; i++) localIndex.set(nodeIds[i], i);

    // ── a) BFS-ring target positions ─────────────────────────────────────────
    const targetX = new Float64Array(n);
    const targetY = new Float64Array(n);
    this.computeBFSRingLayout({
      nodeIds, localIndex, anchorX, anchorY,
      communityRadius, nodeSpacing, adjacencyByNodeId, nodeHashById,
      targetX, targetY
    });

    // ── b) Rotate targets to align with previous frame (prevents spin drift) ─
    const prevAnchor = previousIndex.communityAnchorById.get(communityId);
    if (prevAnchor) {
      this.alignTargetRotation({
        nodeIds, targetX, targetY,
        anchorX, anchorY, prevAnchor, previousIndex
      });
    }

    // ── c) Temporal lerp: prev → target ──────────────────────────────────────
    const anchorDx    = prevAnchor ? anchorX - prevAnchor.x : 0;
    const anchorDy    = prevAnchor ? anchorY - prevAnchor.y : 0;
    const blendFactor = 1 - stability; // how much to move toward target each frame

    for (let i = 0; i < n; i++) {
      const prevPos = previousIndex.nodePositions[nodeIds[i]];
      if (prevPos && stability > 0) {
        // Translate previous position by anchor movement so nodes don't drift
        // when the whole community moves
        const px = prevPos.x + anchorDx;
        const py = prevPos.y + anchorDy;
        x[i] = px + (targetX[i] - px) * blendFactor;
        y[i] = py + (targetY[i] - py) * blendFactor;
      } else {
        // No history → place directly at target
        x[i] = targetX[i];
        y[i] = targetY[i];
      }
    }

    // ── d) Soft containment (clamp to community radius) ───────────────────────
    // Allow a small overflow so nodes near the boundary aren't crushed together
    const containR = communityRadius * 1.08;
    for (let i = 0; i < n; i++) {
      const ax = x[i] - anchorX, ay = y[i] - anchorY;
      const d  = Math.sqrt(ax * ax + ay * ay);
      if (d > containR) {
        const inv = containR / d;
        x[i] = anchorX + ax * inv;
        y[i] = anchorY + ay * inv;
      }
    }

    // ── e) Light collision resolution ────────────────────────────────────────
    // Only needed to fix overlaps introduced by the lerp or containment clamp.
    // The BFS ring layout itself is already non-overlapping.
    if (n > 1) {
      const colIters = n <= 300 ? Math.round(3 * quality) : n <= 1000 ? Math.round(2 * quality) : 1;
      this.resolveNodeCollisions({ x, y, anchorX, anchorY, nodeSpacing, containR, n, iterations: colIters });
    }

    return { nodeIds, x, y };
  }

  // ────────────────────────────────────────────────────────────────────────────
  // BFS-ring layout
  // ────────────────────────────────────────────────────────────────────────────

  /**
   * Places nodes on concentric rings anchored at (anchorX, anchorY).
   *
   * Ring 0 = highest-degree node (hub) at the centre.
   * Ring k = all neighbours of ring k-1 not already assigned.
   *
   * Within each ring, nodes are sorted by their parent's angle so that edges
   * from parent to child are radial and never cross each other.
   *
   * Ring radii are adaptive: large enough to prevent intra-ring overlaps even
   * for star graphs where ring 1 may contain hundreds of children.
   */
  private computeBFSRingLayout(params: {
    nodeIds: string[];
    localIndex: Map<string, number>;
    anchorX: number;
    anchorY: number;
    communityRadius: number;
    nodeSpacing: number;
    adjacencyByNodeId: Map<string, Set<string>>;
    nodeHashById: Map<string, number>;
    targetX: Float64Array;
    targetY: Float64Array;
  }): void {
    const { nodeIds, localIndex, anchorX, anchorY, communityRadius, nodeSpacing,
            adjacencyByNodeId, nodeHashById, targetX, targetY } = params;
    const n = nodeIds.length;

    if (n === 1) { targetX[0] = anchorX; targetY[0] = anchorY; return; }

    // ── Find hub node (highest local degree; tie-break by nodeId) ─────────────
    let centerIdx = 0, maxDeg = -1;
    for (let i = 0; i < n; i++) {
      let deg = 0;
      const nb = adjacencyByNodeId.get(nodeIds[i]);
      if (nb) for (const nbId of nb) if (localIndex.has(nbId)) deg++;
      if (deg > maxDeg || (deg === maxDeg && compareString(nodeIds[i], nodeIds[centerIdx]) < 0)) {
        maxDeg = deg; centerIdx = i;
      }
    }

    // ── BFS to assign ring levels ──────────────────────────────────────────────
    const ring      = new Int32Array(n).fill(-1);
    const parentIdx = new Int32Array(n).fill(-1);
    const bfsQ: number[] = [centerIdx];
    ring[centerIdx]  = 0;
    let head = 0, maxRing = 0;

    while (head < bfsQ.length) {
      const i = bfsQ[head++];
      const r = ring[i];
      const nb = adjacencyByNodeId.get(nodeIds[i]);
      if (nb) {
        for (const nbId of nb) {
          const j = localIndex.get(nbId);
          if (j == null || ring[j] !== -1) continue;
          ring[j] = r + 1;
          parentIdx[j] = i;
          if (r + 1 > maxRing) maxRing = r + 1;
          bfsQ.push(j);
        }
      }
    }

    // Nodes not reachable from hub (should be rare; means disconnected subgraph
    // within a community which label-propagation should prevent)
    let hasUnreached = false;
    for (let i = 0; i < n; i++) {
      if (ring[i] === -1) { ring[i] = maxRing + 1; hasUnreached = true; }
    }
    const numRings = maxRing + (hasUnreached ? 2 : 1);

    // ── Group indices by ring ─────────────────────────────────────────────────
    const byRing: number[][] = [];
    for (let i = 0; i < n; i++) {
      const r = ring[i];
      while (byRing.length <= r) byRing.push([]);
      byRing[r].push(i);
    }

    // ── Compute ring radii ────────────────────────────────────────────────────
    // Each ring must be wide enough that adjacent nodes don't overlap.
    // r_k = max(proportional, enough-for-node-count, prev_ring + min_gap)
    const usableR = Math.max(nodeSpacing * 2, communityRadius - nodeSpacing);
    const baseStep = numRings > 1 ? usableR / (numRings - 0.5) : usableR;
    const ringR    = new Float64Array(numRings);
    let prevR = 0;

    for (let r = 1; r < numRings; r++) {
      const count     = byRing[r]?.length ?? 0;
      // Minimum circumference to fit `count` nodes spaced nodeSpacing apart
      const minByCount = count > 1 ? count * nodeSpacing * 1.15 / (2 * Math.PI) : 0;
      const minByPrev  = prevR + nodeSpacing * 1.5;
      const natural    = r * baseStep;
      ringR[r] = Math.max(natural, minByCount, minByPrev);
      prevR    = ringR[r];
    }

    // ── Place ring 0 (hub) at anchor ──────────────────────────────────────────
    targetX[centerIdx] = anchorX;
    targetY[centerIdx] = anchorY;

    // Deterministic angle offset based on hub's hash (consistent across frames)
    const angleOffset = ((nodeHashById.get(nodeIds[centerIdx]) ?? 0) % 10000) / 10000 * TAU;

    // ── Place each subsequent ring ─────────────────────────────────────────────
    for (let r = 1; r < byRing.length; r++) {
      const indices = byRing[r];
      if (!indices || indices.length === 0) continue;

      const radius = ringR[r];
      const count  = indices.length;

      // Sort by parent's angle so tree edges are radial and non-crossing
      const sorted = indices.map(i => {
        const pi = parentIdx[i];
        let parentAngle: number;
        if (pi >= 0) {
          const px = targetX[pi] - anchorX;
          const py = targetY[pi] - anchorY;
          parentAngle = Math.sqrt(px * px + py * py) > EPS ? Math.atan2(py, px) : 0;
        } else {
          parentAngle = ((nodeHashById.get(nodeIds[i]) ?? 0) % 10000) / 10000 * TAU;
        }
        return { i, parentAngle };
      });

      sorted.sort((a, b) => {
        const d = a.parentAngle - b.parentAngle;
        return Math.abs(d) > 1e-12 ? d : compareString(nodeIds[a.i], nodeIds[b.i]);
      });

      // Spread evenly around the ring, starting from mean parent angle
      const startAngle = sorted[0].parentAngle + angleOffset;
      for (let k = 0; k < count; k++) {
        const angle     = startAngle + (k / count) * TAU;
        const { i }     = sorted[k];
        targetX[i] = anchorX + Math.cos(angle) * radius;
        targetY[i] = anchorY + Math.sin(angle) * radius;
      }
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Rotation alignment
  // ────────────────────────────────────────────────────────────────────────────

  /**
   * Finds the 2-D rotation that best aligns the BFS-ring target positions with
   * the previous frame's positions and applies it to targetX/targetY.
   *
   * This prevents the ring from spinning between frames, which would otherwise
   * cause all nodes to orbit the anchor even when nothing structural changed.
   *
   * Math: θ = atan2( Σ(tx·py − ty·px), Σ(tx·px + ty·py) )
   * where (tx,ty) = target relative to anchor, (px,py) = prev relative to prev anchor.
   */
  private alignTargetRotation(params: {
    nodeIds: string[];
    targetX: Float64Array;
    targetY: Float64Array;
    anchorX: number;
    anchorY: number;
    prevAnchor: { x: number; y: number };
    previousIndex: SnapshotIndex;
  }): void {
    const { nodeIds, targetX, targetY, anchorX, anchorY, prevAnchor, previousIndex } = params;
    const n = nodeIds.length;

    let sumRe = 0, sumIm = 0, count = 0;
    for (let i = 0; i < n; i++) {
      const prevPos = previousIndex.nodePositions[nodeIds[i]];
      if (!prevPos) continue;

      const tx = targetX[i] - anchorX,      ty = targetY[i] - anchorY;
      const px = prevPos.x  - prevAnchor.x, py = prevPos.y  - prevAnchor.y;

      // Skip nodes near the anchor (centre node) — they carry no rotation signal
      if (tx * tx + ty * ty < EPS || px * px + py * py < EPS) continue;

      sumRe += tx * px + ty * py;
      sumIm += tx * py - ty * px;
      count++;
    }

    if (count < 2 || sumRe * sumRe + sumIm * sumIm < EPS) return;

    const angle = Math.atan2(sumIm, sumRe);
    if (Math.abs(angle) < 0.008) return; // negligible — don't perturb

    const cos = Math.cos(angle), sin = Math.sin(angle);
    for (let i = 0; i < n; i++) {
      const tx = targetX[i] - anchorX, ty = targetY[i] - anchorY;
      targetX[i] = anchorX + tx * cos - ty * sin;
      targetY[i] = anchorY + tx * sin + ty * cos;
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Collision resolution
  // ────────────────────────────────────────────────────────────────────────────

  private resolveNodeCollisions(params: {
    x: Float64Array;
    y: Float64Array;
    anchorX: number;
    anchorY: number;
    nodeSpacing: number;
    containR: number;
    n: number;
    iterations: number;
  }): void {
    const { x, y, anchorX, anchorY, nodeSpacing, containR, n, iterations } = params;
    const minDist = nodeSpacing * 0.85;

    if (n <= 400) {
      // O(n²) — fast for small communities
      for (let iter = 0; iter < iterations; iter++) {
        for (let i = 0; i < n; i++) {
          for (let j = i + 1; j < n; j++) {
            let dx = x[i] - x[j], dy = y[i] - y[j];
            const d2 = dx * dx + dy * dy;
            if (d2 >= minDist * minDist) continue;
            const d  = Math.sqrt(d2) || EPS;
            const push = (minDist - d) * 0.5;
            dx /= d; dy /= d;
            x[i] += dx * push; y[i] += dy * push;
            x[j] -= dx * push; y[j] -= dy * push;
          }
        }
        this.clampToContainment(x, y, anchorX, anchorY, containR, n);
      }
    } else {
      // Grid-based O(n) for large communities
      const gridSize = minDist * 2;
      const store: GridStore = { buckets: new Map(), usedKeys: [], pool: [] };
      const fx = new Float64Array(n), fy = new Float64Array(n);

      for (let iter = 0; iter < iterations; iter++) {
        fx.fill(0); fy.fill(0);
        this.gridClear(store);

        for (let i = 0; i < n; i++) {
          const key = (Math.floor(x[i] / gridSize) + GRID_OFFSET) * GRID_SCALE
                    + (Math.floor(y[i] / gridSize) + GRID_OFFSET);
          this.gridPush(store, key, i);
        }

        for (let i = 0; i < n; i++) {
          const cx = Math.floor(x[i] / gridSize);
          const cy = Math.floor(y[i] / gridSize);
          for (let ox = -1; ox <= 1; ox++) {
            for (let oy = -1; oy <= 1; oy++) {
              const bucket = store.buckets.get(
                (cx + ox + GRID_OFFSET) * GRID_SCALE + (cy + oy + GRID_OFFSET)
              );
              if (!bucket) continue;
              for (const j of bucket) {
                if (j <= i) continue;
                let dx = x[i] - x[j], dy = y[i] - y[j];
                const d2 = dx * dx + dy * dy;
                if (d2 >= minDist * minDist) continue;
                const d  = Math.sqrt(d2) || EPS;
                const push = (minDist - d) * 0.5;
                dx /= d; dy /= d;
                fx[i] += dx * push; fy[i] += dy * push;
                fx[j] -= dx * push; fy[j] -= dy * push;
              }
            }
          }
        }

        this.gridClear(store);
        for (let i = 0; i < n; i++) { x[i] += fx[i]; y[i] += fy[i]; }
        this.clampToContainment(x, y, anchorX, anchorY, containR, n);
      }
    }
  }

  private clampToContainment(
    x: Float64Array, y: Float64Array,
    anchorX: number, anchorY: number,
    containR: number, n: number
  ): void {
    for (let i = 0; i < n; i++) {
      const ax = x[i] - anchorX, ay = y[i] - anchorY;
      const d  = Math.sqrt(ax * ax + ay * ay);
      if (d > containR) { const inv = containR / d; x[i] = anchorX + ax * inv; y[i] = anchorY + ay * inv; }
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Grid store utilities
  // ────────────────────────────────────────────────────────────────────────────

  private gridPush(store: GridStore, key: number, val: number): void {
    let arr = store.buckets.get(key);
    if (!arr) { arr = store.pool.pop() ?? []; store.buckets.set(key, arr); store.usedKeys.push(key); }
    arr.push(val);
  }

  private gridClear(store: GridStore): void {
    for (const key of store.usedKeys) {
      const arr = store.buckets.get(key);
      if (arr) { arr.length = 0; store.pool.push(arr); store.buckets.delete(key); }
    }
    store.usedKeys.length = 0;
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// Strategy definition  (unchanged from original)
// ──────────────────────────────────────────────────────────────────────────────

export const STRICT_TEMPORAL_STRATEGY_DEFINITION: NetworkLayoutStrategyDefinition = {
  strategy: "strict-temporal",
  label:    "Strict Temporal",
  fields: [
    { key: "quality",     label: "Quality",      min: 0.5, max: 1.5,  step: 0.05 },
    { key: "stability",   label: "Stability",    min: 0,   max: 0.95, step: 0.05 },
    { key: "nodeSpacing", label: "Node Spacing", min: 4,   max: 20,   step: 1    }
  ],
  createInitialConfig: () => ({
    quality:     DEFAULT_QUALITY,
    stability:   DEFAULT_STABILITY,
    nodeSpacing: DEFAULT_NODE_SPACING
  }),
  summarizeConfig: (config) => {
    const q  = Number(config.quality     ?? DEFAULT_QUALITY);
    const s  = Number(config.stability   ?? DEFAULT_STABILITY);
    const sp = Number(config.nodeSpacing ?? DEFAULT_NODE_SPACING);
    return `q=${q.toFixed(2)} stab=${s.toFixed(2)} space=${sp}`;
  },
  createAlgorithm: () => new StrictTemporalLayoutAlgorithm()
};