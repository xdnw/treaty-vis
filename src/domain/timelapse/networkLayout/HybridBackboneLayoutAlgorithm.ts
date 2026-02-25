import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutStrategyDefinition } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyDefinition";
import type { NetworkLayoutInput, NetworkLayoutOutput } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";
import type { WorkerCommunityTarget, WorkerComponentTarget, WorkerNodeTarget } from "@/domain/timelapse/workerProtocol";

const TAU = Math.PI * 2;
const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5));
const NETWORK_REFINEMENT_ITERATIONS = 4;

type ComponentSnapshot = {
  componentId: string;
  members: Set<string>;
  anchorX: number;
  anchorY: number;
};

type CommunitySnapshot = {
  communityId: string;
  componentId: string;
  members: Set<string>;
  anchorX: number;
  anchorY: number;
};

type LayoutSnapshot = {
  components: ComponentSnapshot[];
  communities: CommunitySnapshot[];
  nodePositions: Record<string, { x: number; y: number }>;
};

export class HybridBackboneLayoutAlgorithm implements INetworkLayoutAlgorithm {
  public readonly strategy = "hybrid-backbone" as const;

  public run(input: NetworkLayoutInput): NetworkLayoutOutput {
    const refinementIterations = Math.floor(
      resolveNumberConfig(input.strategyConfig, "refinementIterations", NETWORK_REFINEMENT_ITERATIONS, 2, 20)
    );
    const communityPlacementScale = resolveNumberConfig(input.strategyConfig, "communityPlacementScale", 1, 0.4, 2.4);

    const sortedAdjacency = this.buildSortedAdjacency(input.nodeIds, input.adjacencyByNodeId);
    const componentsRaw = this.computeConnectedComponents(input.nodeIds, sortedAdjacency);

    const previousSnapshot = this.asLayoutSnapshot(input.previousState);
    const components = this.assignStableComponentIds(componentsRaw, previousSnapshot);
    const componentAnchors = this.resolveComponentAnchors(components, previousSnapshot, communityPlacementScale);

    const componentTargets: WorkerComponentTarget[] = [];
    const communityTargets: WorkerCommunityTarget[] = [];
    const nodeTargets: WorkerNodeTarget[] = [];

    const nextSnapshot: LayoutSnapshot = { components: [], communities: [], nodePositions: {} };

    for (const component of components) {
      const componentAnchor = componentAnchors.get(component.componentId) ?? { x: 0, y: 0 };
      
      let deltaComponentX = 0;
      let deltaComponentY = 0;
      if (previousSnapshot) {
        for (const prev of previousSnapshot.components) {
          if (prev.componentId === component.componentId) {
            deltaComponentX = componentAnchor.x - prev.anchorX;
            deltaComponentY = componentAnchor.y - prev.anchorY;
            break;
          }
        }
      }

      componentTargets.push({
        componentId: component.componentId,
        nodeIds: component.nodeIds,
        anchorX: componentAnchor.x,
        anchorY: componentAnchor.y
      });

      nextSnapshot.components.push({
        componentId: component.componentId,
        members: new Set(component.nodeIds),
        anchorX: componentAnchor.x,
        anchorY: componentAnchor.y
      });

      const communitiesRaw = this.computeCommunities(component.nodeIds, sortedAdjacency, previousSnapshot);
      const communities = this.assignStableCommunityIds(
        component.componentId,
        communitiesRaw,
        previousSnapshot
      );
      
      const communityAnchors = new Map<string, { x: number; y: number }>();
      const componentCommunityTargets: WorkerCommunityTarget[] = [];
      const communityRadii: number[] = [];
      
      for (const community of communities) {
        const communityNodeIds = community.nodeIds;
        communityRadii.push(7 + Math.sqrt(communityNodeIds.length) * 3.6);
      }
      
      const largestCommunityRadius = communityRadii.length > 0 ? Math.max(...communityRadii) : 0;
      const communityPlacementRadius =
        (12 + largestCommunityRadius * 1.4 + Math.sqrt(communities.length) * 7) * communityPlacementScale;

      for (let index = 0; index < communities.length; index += 1) {
        const community = communities[index];
        const communityNodeIds = community.nodeIds;
        const communityId = community.communityId;
        const angleSeed = this.hashId(`${communityId}:angle`) % 360;
        const angle = communities.length <= 1 ? 0 : (((index * 53 + angleSeed) % 360) / 360) * TAU;
        
        const previousAnchor = this.resolvePreviousCommunityAnchor(previousSnapshot, communityId);
        
        const idealX = componentAnchor.x + Math.cos(angle) * communityPlacementRadius;
        const idealY = componentAnchor.y + Math.sin(angle) * communityPlacementRadius;
        let anchorX = idealX;
        let anchorY = idealY;

        if (previousAnchor) {
          const shiftedX = previousAnchor.x + deltaComponentX;
          const shiftedY = previousAnchor.y + deltaComponentY;
          // Smoothly interpolate 15% towards ideal to self-correct layout over time without jumping
          anchorX = shiftedX + (idealX - shiftedX) * 0.15;
          anchorY = shiftedY + (idealY - shiftedY) * 0.15;
        }

        const anchor = { x: anchorX, y: anchorY };
        communityAnchors.set(communityId, anchor);

        const target: WorkerCommunityTarget = {
          communityId,
          componentId: component.componentId,
          nodeIds: communityNodeIds,
          anchorX: anchor.x,
          anchorY: anchor.y
        };
        componentCommunityTargets.push(target);
        communityTargets.push(target);
        nextSnapshot.communities.push({
          communityId,
          componentId: component.componentId,
          members: new Set(communityNodeIds),
          anchorX: anchor.x,
          anchorY: anchor.y
        });
      }

      for (const community of componentCommunityTargets) {
        const anchor = communityAnchors.get(community.communityId)!;
        const initialPositions = new Map<string, { x: number; y: number }>();
        
        for (const nodeId of community.nodeIds) {
          const prevPos = previousSnapshot?.nodePositions[nodeId];
          if (prevPos) {
            let prevAnchorX = 0;
            let prevAnchorY = 0;
            let foundPrevAnchor = false;
            if (previousSnapshot) {
              for (const prevComm of previousSnapshot.communities) {
                if (prevComm.members.has(nodeId)) {
                  prevAnchorX = prevComm.anchorX;
                  prevAnchorY = prevComm.anchorY;
                  foundPrevAnchor = true;
                  break;
                }
              }
            }
            if (foundPrevAnchor) {
              // Lock into relative positioning so communities can fluidly shift screen locations
              initialPositions.set(nodeId, {
                x: anchor.x + (prevPos.x - prevAnchorX),
                y: anchor.y + (prevPos.y - prevAnchorY)
              });
            } else {
              initialPositions.set(nodeId, prevPos);
            }
          }
        }

        const refined = this.refineCommunityTargets({
          communityNodeIds: community.nodeIds,
          sortedAdjacencyByNodeId: sortedAdjacency,
          initialPositions,
          anchorX: anchor.x,
          anchorY: anchor.y,
          componentId: component.componentId,
          communityId: community.communityId,
          refinementIterations: Math.max(2, refinementIterations)
        });
        
        for (const nodeTarget of refined) {
          nodeTargets.push(nodeTarget);
          nextSnapshot.nodePositions[nodeTarget.nodeId] = { x: nodeTarget.targetX, y: nodeTarget.targetY };
        }
      }
    }

    nodeTargets.sort((left, right) => left.nodeId.localeCompare(right.nodeId));
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

  private asLayoutSnapshot(value: unknown): LayoutSnapshot | undefined {
    if (!value || typeof value !== "object") {
      return undefined;
    }
    const candidate = value as { components?: unknown[] };
    if (!Array.isArray(candidate.components)) {
      return undefined;
    }

    const components: ComponentSnapshot[] = [];
    for (const raw of candidate.components) {
      if (!raw || typeof raw !== "object") {
        continue;
      }
      const entry = raw as { componentId?: unknown; members?: unknown; anchorX?: unknown; anchorY?: unknown };
      const componentId = typeof entry.componentId === "string" ? entry.componentId : null;
      const anchorX = Number(entry.anchorX);
      const anchorY = Number(entry.anchorY);
      if (!componentId || !Number.isFinite(anchorX) || !Number.isFinite(anchorY)) {
        continue;
      }

      const members = new Set<string>();
      if (entry.members instanceof Set) {
        for (const member of entry.members) {
          if (typeof member === "string") {
            members.add(member);
          }
        }
      }

      components.push({
        componentId,
        members,
        anchorX,
        anchorY
      });
    }

    return {
      components,
      communities: this.toCommunitySnapshots((value as { communities?: unknown }).communities),
      nodePositions: this.toPointRecord((value as { nodePositions?: unknown }).nodePositions)
    };
  }

  private toPointRecord(value: unknown): Record<string, { x: number; y: number }> {
    if (!value || typeof value !== "object") {
      return {};
    }

    const positions: Record<string, { x: number; y: number }> = {};
    for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
      if (!raw || typeof raw !== "object") {
        continue;
      }
      const point = raw as { x?: unknown; y?: unknown };
      const x = Number(point.x);
      const y = Number(point.y);
      if (!Number.isFinite(x) || !Number.isFinite(y)) {
        continue;
      }
      positions[key] = { x, y };
    }
    return positions;
  }

  private toCommunitySnapshots(value: unknown): CommunitySnapshot[] {
    if (!Array.isArray(value)) {
      return [];
    }

    const snapshots: CommunitySnapshot[] = [];
    for (const raw of value) {
      if (!raw || typeof raw !== "object") {
        continue;
      }
      const entry = raw as {
        communityId?: unknown;
        componentId?: unknown;
        members?: unknown;
        anchorX?: unknown;
        anchorY?: unknown;
      };
      if (typeof entry.communityId !== "string" || typeof entry.componentId !== "string") {
        continue;
      }
      const anchorX = Number(entry.anchorX);
      const anchorY = Number(entry.anchorY);
      if (!Number.isFinite(anchorX) || !Number.isFinite(anchorY)) {
        continue;
      }

      const members = new Set<string>();
      if (Array.isArray(entry.members)) {
        for (const member of entry.members) {
          if (typeof member === "string") {
            members.add(member);
          }
        }
      } else if (entry.members instanceof Set) {
        for (const member of entry.members) {
          if (typeof member === "string") {
            members.add(member);
          }
        }
      }

      snapshots.push({
        communityId: entry.communityId,
        componentId: entry.componentId,
        members,
        anchorX,
        anchorY
      });
    }
    return snapshots;
  }

  private hashId(id: string): number {
    let hash = 2166136261;
    for (let index = 0; index < id.length; index += 1) {
      hash ^= id.charCodeAt(index);
      hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
    }
    return hash >>> 0;
  }

  private buildSortedAdjacency(
    nodeIds: string[],
    adjacencyByNodeId: Map<string, Set<string>>
  ): Map<string, string[]> {
    const sorted = new Map<string, string[]>();
    for (const nodeId of nodeIds) {
      const neighbors = adjacencyByNodeId.get(nodeId);
      if (!neighbors || neighbors.size === 0) {
        sorted.set(nodeId, []);
        continue;
      }
      sorted.set(
        nodeId,
        [...neighbors].sort((left, right) => left.localeCompare(right))
      );
    }
    return sorted;
  }

  private computeConnectedComponents(
    nodeIds: string[],
    sortedAdjacencyByNodeId: Map<string, string[]>
  ): Array<{ nodeIds: string[] }> {
    const visited = new Set<string>();
    const components: Array<{ nodeIds: string[] }> = [];

    const sortedNodeIds = [...nodeIds].sort((left, right) => left.localeCompare(right));
    for (const rootId of sortedNodeIds) {
      if (visited.has(rootId)) {
        continue;
      }

      const queue = [rootId];
      const members: string[] = [];
      visited.add(rootId);

      while (queue.length > 0) {
        const current = queue.shift()!;
        members.push(current);
        const neighbors = sortedAdjacencyByNodeId.get(current);
        if (!neighbors || neighbors.length === 0) {
          continue;
        }
        for (const neighbor of neighbors) {
          if (visited.has(neighbor)) {
            continue;
          }
          visited.add(neighbor);
          queue.push(neighbor);
        }
      }

      members.sort((left, right) => left.localeCompare(right));
      components.push({ nodeIds: members });
    }

    components.sort(
      (left, right) => right.nodeIds.length - left.nodeIds.length || left.nodeIds[0].localeCompare(right.nodeIds[0])
    );
    return components;
  }

  private computeCommunities(
    componentNodeIds: string[], 
    sortedAdjacencyByNodeId: Map<string, string[]>,
    previousSnapshot: LayoutSnapshot | undefined
  ): string[][] {
    const labels = new Map<string, string>();
    const sortedNodeIds = [...componentNodeIds].sort((left, right) => left.localeCompare(right));
    
    // Warm-start labels to heavily prevent component/community jittering
    if (previousSnapshot) {
      const nodeToPrevCommunity = new Map<string, string>();
      for (const comm of previousSnapshot.communities) {
        for (const member of comm.members) {
          nodeToPrevCommunity.set(member, comm.communityId);
        }
      }
      for (const nodeId of sortedNodeIds) {
        labels.set(nodeId, nodeToPrevCommunity.get(nodeId) ?? nodeId);
      }
    } else {
      for (const nodeId of sortedNodeIds) {
        labels.set(nodeId, nodeId);
      }
    }

    const maxIterations = Math.max(2, Math.min(10, Math.ceil(Math.log2(componentNodeIds.length + 1)) + 2));
    for (let iteration = 0; iteration < maxIterations; iteration += 1) {
      let changed = false;
      for (const nodeId of sortedNodeIds) {
        const neighbors = sortedAdjacencyByNodeId.get(nodeId);
        if (!neighbors || neighbors.length === 0) {
          continue;
        }

        const frequencies = new Map<string, number>();
        for (const neighborId of neighbors) {
          const label = labels.get(neighborId) ?? neighborId;
          frequencies.set(label, (frequencies.get(label) ?? 0) + 1);
        }

        let bestLabel = labels.get(nodeId) ?? nodeId;
        let bestScore = -1;
        for (const [label, score] of frequencies.entries()) {
          if (score > bestScore || (score === bestScore && label.localeCompare(bestLabel) < 0)) {
            bestLabel = label;
            bestScore = score;
          }
        }

        if (bestLabel !== labels.get(nodeId)) {
          labels.set(nodeId, bestLabel);
          changed = true;
        }
      }

      if (!changed) {
        break;
      }
    }

    const byLabel = new Map<string, string[]>();
    for (const nodeId of sortedNodeIds) {
      const label = labels.get(nodeId) ?? nodeId;
      const members = byLabel.get(label) ?? [];
      members.push(nodeId);
      byLabel.set(label, members);
    }

    const communities = [...byLabel.values()].map((members) => members.sort((left, right) => left.localeCompare(right)));
    communities.sort((left, right) => right.length - left.length || left[0].localeCompare(right[0]));
    return communities;
  }

  private buildComponentId(componentNodeIds: string[]): string {
    const head = componentNodeIds.slice(0, 8).join(",");
    return `component:${componentNodeIds.length}:${this.hashId(head).toString(36)}`;
  }

  private assignStableComponentIds(
    components: Array<{ nodeIds: string[] }>,
    previousSnapshot: LayoutSnapshot | undefined
  ): Array<{ componentId: string; nodeIds: string[]; weight: number }> {
    const previous = previousSnapshot?.components ?? [];
    const claimed = new Set<string>();

    const assigned = components.map((component) => {
      const members = new Set(component.nodeIds);
      let best: ComponentSnapshot | null = null;
      let bestOverlap = 0;
      let bestRatio = -1;

      for (const snapshot of previous) {
        if (claimed.has(snapshot.componentId)) {
          continue;
        }

        let overlap = 0;
        for (const member of members) {
          if (snapshot.members.has(member)) {
            overlap += 1;
          }
        }
        if (overlap === 0) {
          continue;
        }

        const overlapRatio = overlap / Math.max(1, Math.max(members.size, snapshot.members.size));
        if (
          overlap > bestOverlap ||
          (overlap === bestOverlap && overlapRatio > bestRatio) ||
          (overlap === bestOverlap && overlapRatio === bestRatio && best && snapshot.componentId.localeCompare(best.componentId) < 0)
        ) {
          best = snapshot;
          bestOverlap = overlap;
          bestRatio = overlapRatio;
        }
      }

      const componentId = best ? best.componentId : this.buildComponentId(component.nodeIds);
      if (best) {
        claimed.add(best.componentId);
      }

      return {
        componentId,
        nodeIds: component.nodeIds,
        weight: component.nodeIds.length
      };
    });

    assigned.sort((left, right) => right.weight - left.weight || left.componentId.localeCompare(right.componentId));
    return assigned;
  }

  private assignStableCommunityIds(
    componentId: string,
    communities: string[][],
    previousSnapshot: LayoutSnapshot | undefined
  ): Array<{ communityId: string; nodeIds: string[] }> {
    const previous = (previousSnapshot?.communities ?? []).filter((entry) => entry.componentId === componentId);
    const claimed = new Set<string>();

    return communities
      .map((nodeIds, index) => {
        const members = new Set(nodeIds);
        let best: CommunitySnapshot | null = null;
        let bestOverlap = 0;
        let bestRatio = -1;

        for (const snapshot of previous) {
          if (claimed.has(snapshot.communityId)) {
            continue;
          }

          let overlap = 0;
          for (const member of members) {
            if (snapshot.members.has(member)) {
              overlap += 1;
            }
          }
          if (overlap === 0) {
            continue;
          }

          const overlapRatio = overlap / Math.max(1, Math.max(members.size, snapshot.members.size));
          if (
            overlap > bestOverlap ||
            (overlap === bestOverlap && overlapRatio > bestRatio) ||
            (overlap === bestOverlap && overlapRatio === bestRatio && best && snapshot.communityId.localeCompare(best.communityId) < 0)
          ) {
            best = snapshot;
            bestOverlap = overlap;
            bestRatio = overlapRatio;
          }
        }

        if (best) {
          claimed.add(best.communityId);
          return {
            communityId: best.communityId,
            nodeIds
          };
        }

        const seed = nodeIds.slice(0, 6).join(",");
        return {
          communityId: `${componentId}:community:${index}:${this.hashId(seed).toString(36)}`,
          nodeIds
        };
      })
      .sort((left, right) => right.nodeIds.length - left.nodeIds.length || left.communityId.localeCompare(right.communityId));
  }

  private resolvePreviousCommunityAnchor(
    previousSnapshot: LayoutSnapshot | undefined,
    communityId: string
  ): { x: number; y: number } | null {
    if (!previousSnapshot) {
      return null;
    }
    for (const community of previousSnapshot.communities) {
      if (community.communityId === communityId) {
        return { x: community.anchorX, y: community.anchorY };
      }
    }
    return null;
  }

  private componentRadius(weight: number): number {
    return 30 + Math.sqrt(Math.max(weight, 1)) * 10;
  }

  private isAnchorPlacementFree(
    placed: Array<{ x: number; y: number; radius: number }>,
    candidateX: number,
    candidateY: number,
    candidateRadius: number
  ): boolean {
    for (const existing of placed) {
      const dx = candidateX - existing.x;
      const dy = candidateY - existing.y;
      const minDistance = existing.radius + candidateRadius + 10;
      if (dx * dx + dy * dy < minDistance * minDistance) {
        return false;
      }
    }
    return true;
  }

  private placeAnchorNearPreferred(
    preferred: { x: number; y: number },
    radius: number,
    placed: Array<{ x: number; y: number; radius: number }>,
    seedOffset: number
  ): { x: number; y: number } {
    if (this.isAnchorPlacementFree(placed, preferred.x, preferred.y, radius)) {
      return preferred;
    }

    const ringCount = 18;
    for (let ring = 1; ring <= 28; ring += 1) {
      const ringDistance = ring * (radius * 0.9 + 8);
      for (let step = 0; step < ringCount; step += 1) {
        const angle = ((step + seedOffset) / ringCount) * TAU;
        const candidateX = preferred.x + Math.cos(angle) * ringDistance;
        const candidateY = preferred.y + Math.sin(angle) * ringDistance;
        if (this.isAnchorPlacementFree(placed, candidateX, candidateY, radius)) {
          return { x: candidateX, y: candidateY };
        }
      }
    }

    const fallbackRadius = 40 + seedOffset * 11;
    const fallbackAngle = seedOffset * GOLDEN_ANGLE;
    return {
      x: preferred.x + Math.cos(fallbackAngle) * fallbackRadius,
      y: preferred.y + Math.sin(fallbackAngle) * fallbackRadius
    };
  }

  private resolveComponentAnchors(
    components: Array<{ componentId: string; nodeIds: string[]; weight: number }>,
    previousSnapshot: LayoutSnapshot | undefined,
    communityPlacementScale: number
  ): Map<string, { x: number; y: number }> {
    const anchors = new Map<string, { x: number; y: number; radius: number }>();
    const claimedPrevious = new Set<string>();
    const placedAnchors: Array<{ x: number; y: number; radius: number }> = [];
    const componentsSorted = [...components].sort(
      (left, right) => right.weight - left.weight || left.componentId.localeCompare(right.componentId)
    );

    for (const component of componentsSorted) {
      const targetRadius = this.componentRadius(component.weight) * Math.max(1, communityPlacementScale);
      const deterministicSeed = this.hashId(component.componentId) % 31;
      let preferred = { x: 0, y: 0 };
      let hasPreferred = false;

      if (!previousSnapshot || previousSnapshot.components.length === 0) {
        const ringRadius = 24 + Math.sqrt(component.weight) * 7;
        const ringAngle = deterministicSeed * GOLDEN_ANGLE;
        preferred = {
          x: Math.cos(ringAngle) * ringRadius,
          y: Math.sin(ringAngle) * ringRadius
        };
        hasPreferred = true;
      } else {
        const currentMembers = new Set(component.nodeIds);
        let best: ComponentSnapshot | null = null;
        let bestOverlap = -1;
        let bestOverlapRatio = -1;

        for (const previous of previousSnapshot.components) {
          if (claimedPrevious.has(previous.componentId)) {
            continue;
          }

          let overlap = 0;
          for (const nodeId of currentMembers) {
            if (previous.members.has(nodeId)) {
              overlap += 1;
            }
          }

          const overlapRatio = overlap / Math.max(1, Math.max(currentMembers.size, previous.members.size));
          if (
            overlap > bestOverlap ||
            (overlap === bestOverlap && overlapRatio > bestOverlapRatio) ||
            (overlap === bestOverlap && overlapRatio === bestOverlapRatio && best !== null && previous.componentId.localeCompare(best.componentId) < 0)
          ) {
            best = previous;
            bestOverlap = overlap;
            bestOverlapRatio = overlapRatio;
          }
        }

        if (best && bestOverlap > 0) {
          preferred = { x: best.anchorX, y: best.anchorY };
          hasPreferred = true;
          claimedPrevious.add(best.componentId);
        }
      }

      if (!hasPreferred) {
        const ringRadius = 24 + Math.sqrt(component.weight) * 7;
        const ringAngle = deterministicSeed * GOLDEN_ANGLE;
        preferred = {
          x: Math.cos(ringAngle) * ringRadius,
          y: Math.sin(ringAngle) * ringRadius
        };
      }

      const packed = this.placeAnchorNearPreferred(preferred, targetRadius, placedAnchors, deterministicSeed);
      const placed = { x: packed.x, y: packed.y, radius: targetRadius };
      placedAnchors.push(placed);
      anchors.set(component.componentId, placed);
    }

    return new Map([...anchors.entries()].map(([componentId, anchor]) => [componentId, { x: anchor.x, y: anchor.y }]));
  }

  private refineCommunityTargets(params: {
    communityNodeIds: string[];
    sortedAdjacencyByNodeId: Map<string, string[]>;
    initialPositions: Map<string, { x: number; y: number }>;
    anchorX: number;
    anchorY: number;
    componentId: string;
    communityId: string;
    refinementIterations: number;
  }): WorkerNodeTarget[] {
    const {
      communityNodeIds,
      sortedAdjacencyByNodeId,
      initialPositions,
      anchorX,
      anchorY,
      componentId,
      communityId,
      refinementIterations
    } = params;
    
    const sortedNodeIds = [...communityNodeIds].sort((left, right) => left.localeCompare(right));
    const nodeIdSet = new Set(sortedNodeIds);
    const positions = new Map<string, { x: number; y: number }>();
    const seedRadius = Math.max(8, Math.sqrt(sortedNodeIds.length) * 3.2);

    for (let index = 0; index < sortedNodeIds.length; index += 1) {
      const nodeId = sortedNodeIds[index];
      const initial = initialPositions.get(nodeId);
      if (initial) {
        positions.set(nodeId, { x: initial.x, y: initial.y });
        continue;
      }
      const seed = this.hashId(`${communityId}:${nodeId}`);
      const angleOffset = ((seed % 360) / 360) * TAU;
      const angle = ((index / Math.max(sortedNodeIds.length, 1)) * TAU + angleOffset) % TAU;
      const radius = seedRadius * (0.55 + (((seed >>> 8) % 1000) / 1000) * 0.7);
      positions.set(nodeId, {
        x: anchorX + Math.cos(angle) * radius,
        y: anchorY + Math.sin(angle) * radius
      });
    }

    const neighborAttract = 0.2;
    const anchorPull = 0.04;
    const repulsionStrength = 1.15;
    const maxStep = Math.max(4.2, seedRadius * 0.15);
    const repulsionCutoff = Math.max(18, seedRadius * 1.8);
    
    // Ensure grid is wide enough to capture all overlapping repulsions
    const gridSize = Math.max(12, repulsionCutoff); 
    const minNodeGap = 2.6;

    for (let iteration = 0; iteration < refinementIterations; iteration += 1) {
      const nextPositions = new Map<string, { x: number; y: number }>();
      const bucket = new Map<string, string[]>();

      for (const nodeId of sortedNodeIds) {
        const current = positions.get(nodeId)!;
        const bucketX = Math.floor(current.x / gridSize);
        const bucketY = Math.floor(current.y / gridSize);
        const key = `${bucketX}:${bucketY}`;
        const row = bucket.get(key) ?? [];
        row.push(nodeId);
        bucket.set(key, row);
      }

      for (const nodeId of sortedNodeIds) {
        const current = positions.get(nodeId)!;
        const neighbors = sortedAdjacencyByNodeId.get(nodeId);
        let attractX = 0;
        let attractY = 0;
        let attractCount = 0;

        if (neighbors && neighbors.length > 0) {
          for (const neighborId of neighbors) {
            if (!nodeIdSet.has(neighborId)) {
              continue;
            }
            const neighborPosition = positions.get(neighborId);
            if (!neighborPosition) {
              continue;
            }
            attractX += neighborPosition.x;
            attractY += neighborPosition.y;
            attractCount += 1;
          }
        }

        let velocityX = 0;
        let velocityY = 0;
        if (attractCount > 0) {
          const meanX = attractX / attractCount;
          const meanY = attractY / attractCount;
          velocityX += (meanX - current.x) * neighborAttract;
          velocityY += (meanY - current.y) * neighborAttract;
        }
        velocityX += (anchorX - current.x) * anchorPull;
        velocityY += (anchorY - current.y) * anchorPull;

        const bucketX = Math.floor(current.x / gridSize);
        const bucketY = Math.floor(current.y / gridSize);
        for (let offsetX = -1; offsetX <= 1; offsetX += 1) {
          for (let offsetY = -1; offsetY <= 1; offsetY += 1) {
            const key = `${bucketX + offsetX}:${bucketY + offsetY}`;
            const candidates = bucket.get(key);
            if (!candidates) {
              continue;
            }
            for (const candidateId of candidates) {
              if (candidateId === nodeId) {
                continue;
              }
              const other = positions.get(candidateId)!;
              const dx = current.x - other.x;
              const dy = current.y - other.y;
              const d2 = dx * dx + dy * dy;
              
              if (d2 <= 0.0001) {
                // Determine a safe, pseudo-random but highly stable displacement avoiding node tangling 
                const jx = (this.hashId(nodeId + candidateId) % 100) / 50 - 1;
                const jy = (this.hashId(candidateId + nodeId) % 100) / 50 - 1;
                velocityX += jx * 0.5;
                velocityY += jy * 0.5;
                continue;
              }
              if (d2 > repulsionCutoff * repulsionCutoff) {
                continue;
              }
              const distance = Math.sqrt(d2);
              const scale = ((repulsionCutoff - distance) / repulsionCutoff) * repulsionStrength;
              velocityX += (dx / distance) * scale;
              velocityY += (dy / distance) * scale;

              if (distance < minNodeGap) {
                const overlap = (minNodeGap - distance) / minNodeGap;
                velocityX += (dx / distance) * overlap * 0.9;
                velocityY += (dy / distance) * overlap * 0.9;
              }
            }
          }
        }

        const speed = Math.sqrt(velocityX * velocityX + velocityY * velocityY);
        if (speed > maxStep && speed > 0) {
          velocityX = (velocityX / speed) * maxStep;
          velocityY = (velocityY / speed) * maxStep;
        }

        nextPositions.set(nodeId, {
          x: current.x + velocityX,
          y: current.y + velocityY
        });
      }

      positions.clear();
      for (const [nodeId, next] of nextPositions.entries()) {
        positions.set(nodeId, next);
      }
    }

    const refined: WorkerNodeTarget[] = [];
    for (const nodeId of sortedNodeIds) {
      const finalPosition = positions.get(nodeId)!;
      const neighbors = sortedAdjacencyByNodeId.get(nodeId);
      let neighborX = 0;
      let neighborY = 0;
      let count = 0;
      if (neighbors && neighbors.length > 0) {
        for (const neighborId of neighbors) {
          if (!nodeIdSet.has(neighborId)) {
            continue;
          }
          const neighborPosition = positions.get(neighborId);
          if (!neighborPosition) {
            continue;
          }
          neighborX += neighborPosition.x;
          neighborY += neighborPosition.y;
          count += 1;
        }
      }

      if (count === 0) {
        neighborX = finalPosition.x;
        neighborY = finalPosition.y;
      } else {
        neighborX /= count;
        neighborY /= count;
      }

      refined.push({
        nodeId,
        componentId,
        communityId,
        targetX: finalPosition.x,
        targetY: finalPosition.y,
        neighborX,
        neighborY,
        anchorX,
        anchorY
      });
    }

    return refined;
  }
}

export const HYBRID_BACKBONE_STRATEGY_DEFINITION: NetworkLayoutStrategyDefinition = {
  strategy: "hybrid-backbone",
  label: "Hybrid Backbone",
  fields: [
    { key: "refinementIterations", label: "Refine Iterations", min: 2, max: 20, step: 1 },
    { key: "communityPlacementScale", label: "Community Spread", min: 0.4, max: 2.4, step: 0.05 }
  ],
  createInitialConfig: () => ({
    refinementIterations: 4,
    communityPlacementScale: 1
  }),
  summarizeConfig: (config) => {
    const iterations = Number(config.refinementIterations ?? 4);
    const spread = Number(config.communityPlacementScale ?? 1);
    return `iter=${iterations} spread=${spread.toFixed(2)}`;
  },
  createAlgorithm: () => new HybridBackboneLayoutAlgorithm()
};