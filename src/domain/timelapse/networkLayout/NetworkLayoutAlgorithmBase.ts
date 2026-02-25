import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutInput, NetworkLayoutOutput } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";
import type { WorkerCommunityTarget, WorkerComponentTarget, WorkerNodeTarget } from "@/domain/timelapse/workerProtocol";

/**
 * Shared network layout orchestration pipeline.
 *
 * Responsibilities owned here:
 * - Snapshot read/index and stable component/community identity assignment.
 * - Component/community anchor planning and overlap resolution.
 * - Worker layout target assembly and metadata snapshot output.
 *
 * Strategy modules should only implement `layoutNodesInCommunity`.
 */

export const TAU = Math.PI * 2;
export const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5));
export const EPS = 1e-9;

const GRID_OFFSET = 1_048_576;
const GRID_SCALE = 2_097_153;

export type SnapshotComponent = {
  componentId: string;
  nodeIds: string[];
  anchorX: number;
  anchorY: number;
  radius?: number;
};

export type SnapshotCommunity = {
  communityId: string;
  componentId: string;
  nodeIds: string[];
  anchorX: number;
  anchorY: number;
  radius?: number;
};

export type LayoutSnapshot = {
  version: 2;
  components: SnapshotComponent[];
  communities: SnapshotCommunity[];
  nodePositions: Record<string, { x: number; y: number }>;
};

export type SnapshotIndex = {
  componentByNodeId: Map<string, string>;
  communityByNodeId: Map<string, string>;
  componentSizeById: Map<string, number>;
  communitySizeById: Map<string, number>;
  componentAnchorById: Map<string, { x: number; y: number }>;
  communityAnchorById: Map<string, { x: number; y: number }>;
  communityComponentById: Map<string, string>;
  nodePositions: Record<string, { x: number; y: number }>;
};

export type CommunityPlan = {
  communityId: string;
  componentId: string;
  nodeIds: string[];
  radius: number;
  anchorX: number;
  anchorY: number;
};

export type ComponentPlan = {
  componentId: string;
  nodeIds: string[];
  communities: CommunityPlan[];
  radius: number;
  anchorX: number;
  anchorY: number;
};

export type NodeLayoutParams = {
  communityId: string;
  componentId: string;
  nodeIds: string[];
  anchorX: number;
  anchorY: number;
  communityRadius: number;
  nodeSpacing: number;
  stability: number;
  quality: number;
  strategyConfig: Record<string, unknown>;
  adjacencyByNodeId: Map<string, Set<string>>;
  nodeHashById: Map<string, number>;
  previousIndex: SnapshotIndex;
};

export type NodeLayoutResult = {
  nodeIds: string[];
  x: Float64Array;
  y: Float64Array;
};

export type GridStore = {
  buckets: Map<number, number[]>;
  usedKeys: number[];
  pool: number[][];
};

export const compareString = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
export const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, v));

export abstract class NetworkLayoutAlgorithmBase implements INetworkLayoutAlgorithm {
  public abstract readonly strategy: string;

  protected abstract layoutNodesInCommunity(params: NodeLayoutParams): NodeLayoutResult;

  public run(input: NetworkLayoutInput): NetworkLayoutOutput {
    const quality = resolveNumberConfig(input.strategyConfig, "quality", 1.0, 0.5, 1.5);
    const stability = resolveNumberConfig(input.strategyConfig, "stability", 0.8, 0, 0.95);
    const nodeSpacing = resolveNumberConfig(input.strategyConfig, "nodeSpacing", 8, 4, 20);

    const nodeIdsSorted = [...input.nodeIds].sort(compareString);

    const nodeHashById = new Map<string, number>();
    for (const id of nodeIdsSorted) {
      nodeHashById.set(id, this.hashId(id));
    }

    const previousSnapshot = this.readSnapshot(input.previousState);
    const previousIndex = this.indexSnapshot(previousSnapshot);

    const rawComponents = this.computeConnectedComponents(nodeIdsSorted, input.adjacencyByNodeId);
    const componentsAssigned = this.assignStableComponentIds(rawComponents, previousIndex);

    const componentPlans: ComponentPlan[] = [];
    for (const component of componentsAssigned) {
      const commRaw = this.detectCommunitiesLabelPropagation({
        componentId: component.componentId,
        nodeIds: component.nodeIds,
        adjacencyByNodeId: input.adjacencyByNodeId,
        previousIndex
      });
      const commAssigned = this.assignStableCommunityIds(component.componentId, commRaw, previousIndex);

      const communityPlans: CommunityPlan[] = commAssigned.map((c) => ({
        communityId: c.communityId,
        componentId: component.componentId,
        nodeIds: c.nodeIds,
        radius: this.computeCommunityRadius(c.nodeIds.length, nodeSpacing),
        anchorX: 0,
        anchorY: 0
      }));
      communityPlans.sort((a, b) => b.nodeIds.length - a.nodeIds.length || compareString(a.communityId, b.communityId));

      componentPlans.push({
        componentId: component.componentId,
        nodeIds: component.nodeIds,
        communities: communityPlans,
        radius: this.computeComponentRadius(communityPlans, nodeSpacing),
        anchorX: 0,
        anchorY: 0
      });
    }

    componentPlans.sort((a, b) => b.nodeIds.length - a.nodeIds.length || compareString(a.componentId, b.componentId));

    this.placeComponentAnchors(componentPlans, previousIndex, nodeSpacing);

    for (const component of componentPlans) {
      this.placeCommunityAnchors(component, previousIndex, nodeSpacing);
    }

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
          communityId: community.communityId,
          componentId: component.componentId,
          nodeIds: community.nodeIds,
          anchorX: community.anchorX,
          anchorY: community.anchorY,
          communityRadius: community.radius,
          nodeSpacing,
          stability,
          quality,
          strategyConfig: input.strategyConfig as Record<string, unknown>,
          adjacencyByNodeId: input.adjacencyByNodeId,
          nodeHashById,
          previousIndex
        });

        for (let k = 0; k < ids.length; k++) {
          nextNodePositions[ids[k]] = { x: x[k], y: y[k] };
          nodeMetaById.set(ids[k], {
            componentId: component.componentId,
            communityId: community.communityId,
            anchorX: community.anchorX,
            anchorY: community.anchorY
          });
        }
      }
    }

    const componentTargets: WorkerComponentTarget[] = [];
    const communityTargets: WorkerCommunityTarget[] = [];
    const nextSnapshot: LayoutSnapshot = {
      version: 2,
      components: [],
      communities: [],
      nodePositions: nextNodePositions
    };

    for (const component of componentPlans) {
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

      for (const community of component.communities) {
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

    const nodeTargets: WorkerNodeTarget[] = [];
    for (const nodeId of nodeIdsSorted) {
      const pos = nextNodePositions[nodeId] ?? previousIndex.nodePositions[nodeId] ?? { x: 0, y: 0 };
      const meta = nodeMetaById.get(nodeId);
      const componentId = meta?.componentId ?? previousIndex.componentByNodeId.get(nodeId) ?? "component:unknown";
      const communityId = meta?.communityId ?? previousIndex.communityByNodeId.get(nodeId) ?? `${componentId}:community:unknown`;
      const anchorX = meta?.anchorX ?? pos.x;
      const anchorY = meta?.anchorY ?? pos.y;

      let nbX = 0;
      let nbY = 0;
      let nbN = 0;
      const nbs = input.adjacencyByNodeId.get(nodeId);
      if (nbs) {
        for (const nb of nbs) {
          const p = nextNodePositions[nb];
          if (p) {
            nbX += p.x;
            nbY += p.y;
            nbN++;
          }
        }
      }
      if (nbN === 0) {
        nbX = pos.x;
        nbY = pos.y;
      } else {
        nbX /= nbN;
        nbY /= nbN;
      }

      nodeTargets.push({
        nodeId,
        componentId,
        communityId,
        targetX: pos.x,
        targetY: pos.y,
        neighborX: nbX,
        neighborY: nbY,
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
      metadata: { state: nextSnapshot }
    };
  }

  protected readSnapshot(value: unknown): LayoutSnapshot | undefined {
    if (!value || typeof value !== "object") {
      return undefined;
    }
    const v = value as Record<string, unknown>;
    if (!Array.isArray(v.components) || !Array.isArray(v.communities) || !v.nodePositions || typeof v.nodePositions !== "object") {
      return undefined;
    }

    const components: SnapshotComponent[] = [];
    for (const raw of v.components as unknown[]) {
      if (!raw || typeof raw !== "object") {
        continue;
      }
      const e = raw as Record<string, unknown>;
      const componentId = typeof e.componentId === "string" ? e.componentId : null;
      const anchorX = Number(e.anchorX);
      const anchorY = Number(e.anchorY);
      if (!componentId || !Number.isFinite(anchorX) || !Number.isFinite(anchorY)) {
        continue;
      }
      const nodeIds = this.readStringArray(e.nodeIds) ?? this.readStringArray(e.members) ?? [];
      const radius = e.radius == null ? undefined : Number(e.radius);
      components.push({
        componentId,
        nodeIds,
        anchorX,
        anchorY,
        radius: Number.isFinite(radius ?? NaN) ? radius : undefined
      });
    }

    const communities: SnapshotCommunity[] = [];
    for (const raw of v.communities as unknown[]) {
      if (!raw || typeof raw !== "object") {
        continue;
      }
      const e = raw as Record<string, unknown>;
      const communityId = typeof e.communityId === "string" ? e.communityId : null;
      const componentId = typeof e.componentId === "string" ? e.componentId : null;
      const anchorX = Number(e.anchorX);
      const anchorY = Number(e.anchorY);
      if (!communityId || !componentId || !Number.isFinite(anchorX) || !Number.isFinite(anchorY)) {
        continue;
      }
      const nodeIds = this.readStringArray(e.nodeIds) ?? this.readStringArray(e.members) ?? [];
      const radius = e.radius == null ? undefined : Number(e.radius);
      communities.push({
        communityId,
        componentId,
        nodeIds,
        anchorX,
        anchorY,
        radius: Number.isFinite(radius ?? NaN) ? radius : undefined
      });
    }

    return { version: 2, components, communities, nodePositions: this.toPointRecord(v.nodePositions) };
  }

  protected readStringArray(value: unknown): string[] | null {
    if (!value) {
      return null;
    }
    const out: string[] = [];
    if (Array.isArray(value)) {
      for (const item of value) {
        if (typeof item === "string") {
          out.push(item);
        }
      }
      return out;
    }
    if (value instanceof Set) {
      for (const item of value) {
        if (typeof item === "string") {
          out.push(item);
        }
      }
      return out;
    }
    return null;
  }

  protected toPointRecord(value: unknown): Record<string, { x: number; y: number }> {
    if (!value || typeof value !== "object") {
      return Object.create(null);
    }
    const out: Record<string, { x: number; y: number }> = Object.create(null);
    for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
      if (!raw || typeof raw !== "object") {
        continue;
      }
      const pt = raw as { x?: unknown; y?: unknown };
      const x = Number(pt.x);
      const y = Number(pt.y);
      if (!Number.isFinite(x) || !Number.isFinite(y)) {
        continue;
      }
      out[key] = { x, y };
    }
    return out;
  }

  protected indexSnapshot(snapshot: LayoutSnapshot | undefined): SnapshotIndex {
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

    for (const c of snapshot.components) {
      componentSizeById.set(c.componentId, c.nodeIds.length);
      componentAnchorById.set(c.componentId, { x: c.anchorX, y: c.anchorY });
      for (const id of c.nodeIds) {
        componentByNodeId.set(id, c.componentId);
      }
    }
    for (const c of snapshot.communities) {
      communitySizeById.set(c.communityId, c.nodeIds.length);
      communityAnchorById.set(c.communityId, { x: c.anchorX, y: c.anchorY });
      communityComponentById.set(c.communityId, c.componentId);
      for (const id of c.nodeIds) {
        communityByNodeId.set(id, c.communityId);
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

  protected hashId(id: string): number {
    let h = 2166136261;
    for (let i = 0; i < id.length; i++) {
      h ^= id.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    return h >>> 0;
  }

  protected buildComponentId(nodeIds: string[]): string {
    return `component:${nodeIds.length}:${this.hashId(nodeIds.slice(0, 10).join(",")).toString(36)}`;
  }

  protected buildCommunityId(componentId: string, nodeIds: string[]): string {
    return `${componentId}:community:${nodeIds.length}:${this.hashId(nodeIds.slice(0, 10).join(",")).toString(36)}`;
  }

  protected computeConnectedComponents(
    nodeIdsSorted: string[],
    adjacencyByNodeId: Map<string, Set<string>>
  ): string[][] {
    const visited = new Set<string>();
    const components: string[][] = [];
    for (const root of nodeIdsSorted) {
      if (visited.has(root)) {
        continue;
      }
      const queue: string[] = [root];
      visited.add(root);
      let qi = 0;
      const members: string[] = [];
      while (qi < queue.length) {
        const cur = queue[qi++];
        members.push(cur);
        const nbs = adjacencyByNodeId.get(cur);
        if (!nbs) {
          continue;
        }
        for (const nb of nbs) {
          if (!visited.has(nb)) {
            visited.add(nb);
            queue.push(nb);
          }
        }
      }
      members.sort(compareString);
      components.push(members);
    }
    components.sort((a, b) => b.length - a.length || compareString(a[0] ?? "", b[0] ?? ""));
    return components;
  }

  protected assignStableComponentIds(
    rawComponents: string[][],
    previous: SnapshotIndex
  ): Array<{ componentId: string; nodeIds: string[] }> {
    const claimed = new Set<string>();
    const assigned: Array<{ componentId: string; nodeIds: string[] }> = [];
    for (const nodeIds of rawComponents) {
      const counts = new Map<string, number>();
      for (const id of nodeIds) {
        const p = previous.componentByNodeId.get(id);
        if (p) {
          counts.set(p, (counts.get(p) ?? 0) + 1);
        }
      }
      let bestId = null as string | null;
      let bestOverlap = 0;
      let bestRatio = -1;
      for (const [prevId, overlap] of counts) {
        if (claimed.has(prevId)) {
          continue;
        }
        const ratio = overlap / Math.max(1, Math.max(previous.componentSizeById.get(prevId) ?? 0, nodeIds.length));
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
      if (bestId) {
        claimed.add(bestId);
      }
      assigned.push({ componentId, nodeIds });
    }
    assigned.sort((a, b) => b.nodeIds.length - a.nodeIds.length || compareString(a.componentId, b.componentId));
    return assigned;
  }

  protected detectCommunitiesLabelPropagation(params: {
    componentId: string;
    nodeIds: string[];
    adjacencyByNodeId: Map<string, Set<string>>;
    previousIndex: SnapshotIndex;
  }): string[][] {
    const { componentId, nodeIds, adjacencyByNodeId, previousIndex } = params;
    if (nodeIds.length <= 1) {
      return nodeIds.length === 1 ? [nodeIds] : [];
    }

    const sorted = [...nodeIds].sort(compareString);
    const nodeIndex = new Map<string, number>();
    for (let i = 0; i < sorted.length; i++) {
      nodeIndex.set(sorted[i], i);
    }

    const labels = new Map<string, string>();
    for (const id of sorted) {
      const prevCommunityId = previousIndex.communityByNodeId.get(id);
      const prevCompId = prevCommunityId ? previousIndex.communityComponentById.get(prevCommunityId) : null;
      labels.set(id, prevCommunityId && prevCompId === componentId ? prevCommunityId : id);
    }

    const maxIter = clamp(Math.ceil(Math.log2(sorted.length + 1)) + 6, 6, 18);
    const freq = new Map<string, number>();

    for (let iter = 0; iter < maxIter; iter++) {
      let changed = false;
      for (const id of sorted) {
        const nbs = adjacencyByNodeId.get(id);
        if (!nbs || nbs.size === 0) {
          continue;
        }
        freq.clear();
        for (const nb of nbs) {
          if (!nodeIndex.has(nb)) {
            continue;
          }
          const l = labels.get(nb) ?? nb;
          freq.set(l, (freq.get(l) ?? 0) + 1);
        }
        const current = labels.get(id) ?? id;
        freq.set(current, (freq.get(current) ?? 0) + 1.15);
        let best = current;
        let bestScore = -Infinity;
        for (const [l, s] of freq) {
          if (s > bestScore || (s === bestScore && compareString(l, best) < 0)) {
            best = l;
            bestScore = s;
          }
        }
        if (best !== current) {
          labels.set(id, best);
          changed = true;
        }
      }
      if (!changed) {
        break;
      }
    }

    const byLabel = new Map<string, string[]>();
    for (const id of sorted) {
      const l = labels.get(id) ?? id;
      const arr = byLabel.get(l);
      if (arr) {
        arr.push(id);
      } else {
        byLabel.set(l, [id]);
      }
    }
    const communities = [...byLabel.values()];
    for (const c of communities) {
      c.sort(compareString);
    }
    communities.sort((a, b) => b.length - a.length || compareString(a[0] ?? "", b[0] ?? ""));
    return communities;
  }

  protected assignStableCommunityIds(
    componentId: string,
    rawCommunities: string[][],
    previous: SnapshotIndex
  ): Array<{ communityId: string; nodeIds: string[] }> {
    const claimed = new Set<string>();
    const assigned: Array<{ communityId: string; nodeIds: string[] }> = [];
    for (const nodeIds of rawCommunities) {
      const counts = new Map<string, number>();
      for (const id of nodeIds) {
        const p = previous.communityByNodeId.get(id);
        if (!p || previous.communityComponentById.get(p) !== componentId) {
          continue;
        }
        counts.set(p, (counts.get(p) ?? 0) + 1);
      }
      let bestId = null as string | null;
      let bestOverlap = 0;
      let bestRatio = -1;
      for (const [prevId, overlap] of counts) {
        if (claimed.has(prevId)) {
          continue;
        }
        const ratio = overlap / Math.max(1, Math.max(previous.communitySizeById.get(prevId) ?? 0, nodeIds.length));
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
      if (bestId) {
        claimed.add(bestId);
      }
      assigned.push({ communityId, nodeIds });
    }
    assigned.sort((a, b) => b.nodeIds.length - a.nodeIds.length || compareString(a.communityId, b.communityId));
    return assigned;
  }

  protected computeCommunityRadius(nodeCount: number, nodeSpacing: number): number {
    return nodeSpacing * (2.5 + 1.3 * Math.sqrt(Math.max(1, nodeCount)));
  }

  protected computeComponentRadius(communities: CommunityPlan[], nodeSpacing: number): number {
    if (communities.length === 0) {
      return nodeSpacing * 10;
    }
    let sumSq = 0;
    for (const c of communities) {
      sumSq += c.radius * c.radius;
    }
    return Math.sqrt(sumSq * 1.3) + nodeSpacing * 5;
  }

  protected placeComponentAnchors(
    components: ComponentPlan[],
    previous: SnapshotIndex,
    nodeSpacing: number
  ): void {
    const n = components.length;
    if (n === 0) {
      return;
    }
    if (n === 1) {
      const prev = previous.componentAnchorById.get(components[0].componentId);
      components[0].anchorX = prev?.x ?? 0;
      components[0].anchorY = prev?.y ?? 0;
      return;
    }

    const x = new Float64Array(n);
    const y = new Float64Array(n);
    for (let i = 0; i < n; i++) {
      const prev = previous.componentAnchorById.get(components[i].componentId);
      if (prev) {
        x[i] = prev.x;
        y[i] = prev.y;
      } else {
        const angle = i * GOLDEN_ANGLE;
        const rad = nodeSpacing * 14 * Math.sqrt(i + 1);
        x[i] = Math.cos(angle) * rad;
        y[i] = Math.sin(angle) * rad;
      }
    }

    const padding = nodeSpacing * 4;
    for (let iter = 0; iter < 80; iter++) {
      let anyOverlap = false;
      for (let i = 0; i < n; i++) {
        for (let j = i + 1; j < n; j++) {
          const dx = x[j] - x[i];
          const dy = y[j] - y[i];
          const d2 = dx * dx + dy * dy;
          const min = components[i].radius + components[j].radius + padding;
          if (d2 >= min * min) {
            continue;
          }
          anyOverlap = true;
          const d = Math.sqrt(d2) || EPS;
          const ovl = (min - d) * 0.55;
          const ux = dx / d;
          const uy = dy / d;
          const si = components[i].nodeIds.length;
          const sj = components[j].nodeIds.length;
          const tot = si + sj;
          x[i] -= ux * ovl * sj / tot;
          y[i] -= uy * ovl * sj / tot;
          x[j] += ux * ovl * si / tot;
          y[j] += uy * ovl * si / tot;
        }
      }
      if (!anyOverlap) {
        break;
      }
    }

    let sdx = 0;
    let sdy = 0;
    let sc = 0;
    for (let i = 0; i < n; i++) {
      const prev = previous.componentAnchorById.get(components[i].componentId);
      if (!prev) {
        continue;
      }
      sdx += prev.x - x[i];
      sdy += prev.y - y[i];
      sc++;
    }
    if (sc > 0) {
      const tdx = sdx / sc;
      const tdy = sdy / sc;
      for (let i = 0; i < n; i++) {
        x[i] += tdx;
        y[i] += tdy;
      }
    }

    for (let i = 0; i < n; i++) {
      components[i].anchorX = x[i];
      components[i].anchorY = y[i];
    }
  }

  protected placeCommunityAnchors(
    component: ComponentPlan,
    previous: SnapshotIndex,
    nodeSpacing: number
  ): void {
    const communities = component.communities;
    if (communities.length === 0) {
      return;
    }
    if (communities.length === 1) {
      communities[0].anchorX = component.anchorX;
      communities[0].anchorY = component.anchorY;
      return;
    }

    const cN = communities.length;
    const x = new Float64Array(cN);
    const y = new Float64Array(cN);

    const prevComp = previous.componentAnchorById.get(component.componentId);
    const cdx = prevComp ? component.anchorX - prevComp.x : 0;
    const cdy = prevComp ? component.anchorY - prevComp.y : 0;

    for (let i = 0; i < cN; i++) {
      const prev = previous.communityAnchorById.get(communities[i].communityId);
      if (prev) {
        x[i] = prev.x + cdx;
        y[i] = prev.y + cdy;
      } else {
        const angle = i * GOLDEN_ANGLE;
        const rad = Math.max(communities[0].radius * 0.6, nodeSpacing * 5) * Math.sqrt(i + 1);
        x[i] = component.anchorX + Math.cos(angle) * rad;
        y[i] = component.anchorY + Math.sin(angle) * rad;
      }
    }

    const padding = nodeSpacing * 2;
    for (let iter = 0; iter < 80; iter++) {
      let anyOverlap = false;
      for (let i = 0; i < cN; i++) {
        for (let j = i + 1; j < cN; j++) {
          const dx = x[j] - x[i];
          const dy = y[j] - y[i];
          const d2 = dx * dx + dy * dy;
          const min = communities[i].radius + communities[j].radius + padding;
          if (d2 >= min * min) {
            continue;
          }
          anyOverlap = true;
          const d = Math.sqrt(d2) || EPS;
          const ovl = (min - d) * 0.55;
          const ux = dx / d;
          const uy = dy / d;
          const si = communities[i].nodeIds.length;
          const sj = communities[j].nodeIds.length;
          const tot = si + sj;
          x[i] -= ux * ovl * sj / tot;
          y[i] -= uy * ovl * sj / tot;
          x[j] += ux * ovl * si / tot;
          y[j] += uy * ovl * si / tot;
        }
      }
      if (!anyOverlap) {
        break;
      }
    }

    for (let i = 0; i < cN; i++) {
      communities[i].anchorX = x[i];
      communities[i].anchorY = y[i];
    }
  }

  protected gridPush(store: GridStore, key: number, val: number): void {
    let arr = store.buckets.get(key);
    if (!arr) {
      arr = store.pool.pop() ?? [];
      store.buckets.set(key, arr);
      store.usedKeys.push(key);
    }
    arr.push(val);
  }

  protected gridClear(store: GridStore): void {
    for (const key of store.usedKeys) {
      const arr = store.buckets.get(key);
      if (arr) {
        arr.length = 0;
        store.pool.push(arr);
        store.buckets.delete(key);
      }
    }
    store.usedKeys.length = 0;
  }

  protected gridKey(xi: number, yi: number, cellSize: number): number {
    const cx = Math.floor(xi / cellSize);
    const cy = Math.floor(yi / cellSize);
    return (cx + GRID_OFFSET) * GRID_SCALE + (cy + GRID_OFFSET);
  }

  protected clampToContainment(
    x: Float64Array,
    y: Float64Array,
    anchorX: number,
    anchorY: number,
    containR: number,
    n: number
  ): void {
    for (let i = 0; i < n; i++) {
      const ax = x[i] - anchorX;
      const ay = y[i] - anchorY;
      const d = Math.sqrt(ax * ax + ay * ay);
      if (d > containR) {
        const inv = containR / d;
        x[i] = anchorX + ax * inv;
        y[i] = anchorY + ay * inv;
      }
    }
  }
}
