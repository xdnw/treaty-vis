import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutInput, NetworkLayoutOutput } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";
import type { WorkerCommunityTarget, WorkerComponentTarget, WorkerNodeTarget } from "@/domain/timelapse/workerProtocol";

const TAU = Math.PI * 2;
const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5));
const NETWORK_REFINEMENT_ITERATIONS = 6;

type ComponentSnapshot = {
  componentId: string;
  members: Set<string>;
  anchorX: number;
  anchorY: number;
};

type LayoutSnapshot = {
  components: ComponentSnapshot[];
};

export class HybridBackboneLayoutAlgorithm implements INetworkLayoutAlgorithm {
  public readonly strategy = "hybrid-backbone" as const;

  public run(input: NetworkLayoutInput): NetworkLayoutOutput {
    const refinementIterations = Math.floor(
      resolveNumberConfig(input.strategyConfig, "refinementIterations", NETWORK_REFINEMENT_ITERATIONS, 2, 20)
    );
    const communityPlacementScale = resolveNumberConfig(input.strategyConfig, "communityPlacementScale", 1, 0.4, 2.4);

    const componentsRaw = this.computeConnectedComponents(input.nodeIds, input.adjacencyByNodeId);
    const components = componentsRaw.map((component) => ({
      componentId: this.buildComponentId(component.nodeIds),
      nodeIds: component.nodeIds,
      weight: component.nodeIds.length
    }));

    const previousSnapshot = this.asLayoutSnapshot(input.previousState);
    const componentAnchors = this.resolveComponentAnchors(components, previousSnapshot);

    const componentTargets: WorkerComponentTarget[] = [];
    const communityTargets: WorkerCommunityTarget[] = [];
    const nodeTargets: WorkerNodeTarget[] = [];

    const nextSnapshot: LayoutSnapshot = { components: [] };

    for (const component of components) {
      const componentAnchor = componentAnchors.get(component.componentId) ?? { x: 0, y: 0 };
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

      const communities = this.computeCommunities(component.nodeIds, input.adjacencyByNodeId);
      const communityAnchors = new Map<string, { x: number; y: number }>();
      const componentCommunityTargets: WorkerCommunityTarget[] = [];
      const communityRadii: number[] = [];
      for (const communityNodeIds of communities) {
        communityRadii.push(7 + Math.sqrt(communityNodeIds.length) * 3.6);
      }
      const largestCommunityRadius = communityRadii.length > 0 ? Math.max(...communityRadii) : 0;
      const communityPlacementRadius =
        (12 + largestCommunityRadius * 1.4 + Math.sqrt(communities.length) * 7) * communityPlacementScale;

      for (let index = 0; index < communities.length; index += 1) {
        const communityNodeIds = communities[index];
        const communityId = `${component.componentId}:community:${index}:${communityNodeIds[0] ?? "none"}`;
        const angleSeed = this.hashId(`${communityId}:angle`) % 360;
        const angle = communities.length <= 1 ? 0 : (((index * 53 + angleSeed) % 360) / 360) * TAU;
        const anchor = {
          x: componentAnchor.x + Math.cos(angle) * communityPlacementRadius,
          y: componentAnchor.y + Math.sin(angle) * communityPlacementRadius
        };
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
      }

      for (const community of componentCommunityTargets) {
        const anchor = communityAnchors.get(community.communityId)!;
        const refined = this.refineCommunityTargets({
          communityNodeIds: community.nodeIds,
          adjacencyByNodeId: input.adjacencyByNodeId,
          anchorX: anchor.x,
          anchorY: anchor.y,
          componentId: component.componentId,
          communityId: community.communityId,
          refinementIterations
        });
        for (const nodeTarget of refined) {
          nodeTargets.push(nodeTarget);
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

    return { components };
  }

  private hashId(id: string): number {
    let hash = 2166136261;
    for (let index = 0; index < id.length; index += 1) {
      hash ^= id.charCodeAt(index);
      hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
    }
    return hash >>> 0;
  }

  private computeConnectedComponents(
    nodeIds: string[],
    adjacencyByNodeId: Map<string, Set<string>>
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
        const neighbors = adjacencyByNodeId.get(current);
        if (!neighbors) {
          continue;
        }
        const sortedNeighbors = [...neighbors].sort((left, right) => left.localeCompare(right));
        for (const neighbor of sortedNeighbors) {
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

  private computeCommunities(componentNodeIds: string[], adjacencyByNodeId: Map<string, Set<string>>): string[][] {
    const labels = new Map<string, string>();
    const sortedNodeIds = [...componentNodeIds].sort((left, right) => left.localeCompare(right));
    for (const nodeId of sortedNodeIds) {
      labels.set(nodeId, nodeId);
    }

    const maxIterations = Math.max(2, Math.min(10, Math.ceil(Math.log2(componentNodeIds.length + 1)) + 2));
    for (let iteration = 0; iteration < maxIterations; iteration += 1) {
      let changed = false;
      for (const nodeId of sortedNodeIds) {
        const neighbors = adjacencyByNodeId.get(nodeId);
        if (!neighbors || neighbors.size === 0) {
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
    return `component:${componentNodeIds[0] ?? "none"}:${componentNodeIds.length}`;
  }

  private componentRadius(weight: number): number {
    return 12 + Math.sqrt(Math.max(weight, 1)) * 4.5;
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
    previousSnapshot: LayoutSnapshot | undefined
  ): Map<string, { x: number; y: number }> {
    const anchors = new Map<string, { x: number; y: number; radius: number }>();
    const claimedPrevious = new Set<string>();
    const placedAnchors: Array<{ x: number; y: number; radius: number }> = [];
    const componentsSorted = [...components].sort(
      (left, right) => right.weight - left.weight || left.componentId.localeCompare(right.componentId)
    );

    for (const component of componentsSorted) {
      const targetRadius = this.componentRadius(component.weight);
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
    adjacencyByNodeId: Map<string, Set<string>>;
    anchorX: number;
    anchorY: number;
    componentId: string;
    communityId: string;
    refinementIterations: number;
  }): WorkerNodeTarget[] {
    const { communityNodeIds, adjacencyByNodeId, anchorX, anchorY, componentId, communityId, refinementIterations } = params;
    const sortedNodeIds = [...communityNodeIds].sort((left, right) => left.localeCompare(right));
    const nodeIdSet = new Set(sortedNodeIds);
    const positions = new Map<string, { x: number; y: number }>();
    const seedRadius = Math.max(8, Math.sqrt(sortedNodeIds.length) * 3.2);

    for (let index = 0; index < sortedNodeIds.length; index += 1) {
      const nodeId = sortedNodeIds[index];
      const seed = this.hashId(`${communityId}:${nodeId}`);
      const angleOffset = ((seed % 360) / 360) * TAU;
      const angle = ((index / Math.max(sortedNodeIds.length, 1)) * TAU + angleOffset) % TAU;
      const radius = seedRadius * (0.55 + (((seed >>> 8) % 1000) / 1000) * 0.7);
      positions.set(nodeId, {
        x: anchorX + Math.cos(angle) * radius,
        y: anchorY + Math.sin(angle) * radius
      });
    }

    const gridSize = 12;
    const neighborAttract = 0.2;
    const anchorPull = 0.04;
    const repulsionStrength = 1.15;
    const maxStep = 4.2;
    const repulsionCutoff = Math.max(18, seedRadius * 1.8);
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
        const neighbors = adjacencyByNodeId.get(nodeId);
        let attractX = 0;
        let attractY = 0;
        let attractCount = 0;

        if (neighbors && neighbors.size > 0) {
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
              if (d2 <= 0.0001 || d2 > repulsionCutoff * repulsionCutoff) {
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
      const neighbors = adjacencyByNodeId.get(nodeId);
      let neighborX = 0;
      let neighborY = 0;
      let count = 0;
      if (neighbors && neighbors.size > 0) {
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
