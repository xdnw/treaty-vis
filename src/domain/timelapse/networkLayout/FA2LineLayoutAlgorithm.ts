import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutStrategyDefinition } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyDefinition";
import type { NetworkLayoutInput, NetworkLayoutOutput } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";
import type { WorkerCommunityTarget, WorkerComponentTarget, WorkerNodeTarget } from "@/domain/timelapse/workerProtocol";

const TAU = Math.PI * 2;
const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5));

type Point = { x: number; y: number };

type ComponentSnapshot = {
  componentId: string;
  members: Set<string>;
  anchorX: number;
  anchorY: number;
};

type FA2LineState = {
  anchors: Record<string, Point>;
  nodePositions: Record<string, Point>;
  components: ComponentSnapshot[];
};

export class FA2LineLayoutAlgorithm implements INetworkLayoutAlgorithm {
  public readonly strategy = "fa2line" as const;

  public run(input: NetworkLayoutInput): NetworkLayoutOutput {
    const previousState = this.asState(input.previousState);
    const sortedAdjacency = this.buildSortedAdjacency(input.nodeIds, input.adjacencyByNodeId);
    const componentsRaw = this.computeConnectedComponents(input.nodeIds, sortedAdjacency);
    const components = this.assignStableComponentIds(componentsRaw, previousState);

    const componentTargets: WorkerComponentTarget[] = [];
    const communityTargets: WorkerCommunityTarget[] = [];
    const nodeTargets: WorkerNodeTarget[] = [];

    const nextState: FA2LineState = {
      anchors: {},
      nodePositions: {},
      components: []
    };

    for (let componentIndex = 0; componentIndex < components.length; componentIndex += 1) {
      const component = components[componentIndex];
      const componentId = component.componentId;
      const anchor = this.resolveComponentAnchor(componentId, component.nodeIds.length, componentIndex, previousState);
      nextState.anchors[componentId] = anchor;
      nextState.components.push({
        componentId,
        members: new Set(component.nodeIds),
        anchorX: anchor.x,
        anchorY: anchor.y
      });

      componentTargets.push({
        componentId,
        nodeIds: component.nodeIds,
        anchorX: anchor.x,
        anchorY: anchor.y
      });

      const communityId = `${componentId}:community:0`;
      communityTargets.push({
        communityId,
        componentId,
        nodeIds: component.nodeIds,
        anchorX: anchor.x,
        anchorY: anchor.y
      });

      const initial = this.seedPositions(component.nodeIds, communityId, anchor, previousState);
      const solved = this.solveComponentPositions(component.nodeIds, sortedAdjacency, anchor, initial, input);
      for (const [nodeId, position] of solved.entries()) {
        nextState.nodePositions[nodeId] = position;
      }

      const lookup = new Set(component.nodeIds);
      for (const nodeId of component.nodeIds) {
        const position = solved.get(nodeId) ?? anchor;
        const neighbors = sortedAdjacency.get(nodeId) ?? [];

        let neighborX = 0;
        let neighborY = 0;
        let count = 0;
        for (const neighborId of neighbors) {
          if (!lookup.has(neighborId)) {
            continue;
          }
          const neighbor = solved.get(neighborId);
          if (!neighbor) {
            continue;
          }
          neighborX += neighbor.x;
          neighborY += neighbor.y;
          count += 1;
        }

        if (count === 0) {
          neighborX = position.x;
          neighborY = position.y;
        } else {
          neighborX /= count;
          neighborY /= count;
        }

        nodeTargets.push({
          nodeId,
          componentId,
          communityId,
          targetX: position.x,
          targetY: position.y,
          neighborX,
          neighborY,
          anchorX: anchor.x,
          anchorY: anchor.y
        });
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
        state: nextState
      }
    };
  }

  private asState(value: unknown): FA2LineState {
    if (!value || typeof value !== "object") {
      return { anchors: {}, nodePositions: {}, components: [] };
    }
    const candidate = value as { anchors?: unknown; nodePositions?: unknown; components?: unknown };

    return {
      anchors: this.toPointRecord(candidate.anchors),
      nodePositions: this.toPointRecord(candidate.nodePositions),
      components: this.toComponentSnapshots(candidate.components)
    };
  }

  private toComponentSnapshots(value: unknown): ComponentSnapshot[] {
    if (!Array.isArray(value)) {
      return [];
    }

    const snapshots: ComponentSnapshot[] = [];
    for (const raw of value) {
      if (!raw || typeof raw !== "object") {
        continue;
      }
      const candidate = raw as {
        componentId?: unknown;
        members?: unknown;
        anchorX?: unknown;
        anchorY?: unknown;
      };
      if (typeof candidate.componentId !== "string") {
        continue;
      }
      const anchorX = Number(candidate.anchorX);
      const anchorY = Number(candidate.anchorY);
      if (!Number.isFinite(anchorX) || !Number.isFinite(anchorY)) {
        continue;
      }

      const members = new Set<string>();
      if (Array.isArray(candidate.members)) {
        for (const member of candidate.members) {
          if (typeof member === "string") {
            members.add(member);
          }
        }
      } else if (candidate.members instanceof Set) {
        for (const member of candidate.members) {
          if (typeof member === "string") {
            members.add(member);
          }
        }
      }

      snapshots.push({
        componentId: candidate.componentId,
        members,
        anchorX,
        anchorY
      });
    }

    return snapshots;
  }

  private toPointRecord(value: unknown): Record<string, Point> {
    if (!value || typeof value !== "object") {
      return {};
    }

    const record: Record<string, Point> = {};
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
      record[key] = { x, y };
    }
    return record;
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

  private buildComponentId(componentNodeIds: string[]): string {
    const head = componentNodeIds.slice(0, 8).join(",");
    return `component:${componentNodeIds.length}:${this.hashId(head).toString(36)}`;
  }

  private assignStableComponentIds(
    components: Array<{ nodeIds: string[] }>,
    previousState: FA2LineState
  ): Array<{ componentId: string; nodeIds: string[] }> {
    const previous = previousState.components;
    if (previous.length === 0) {
      return components.map((component) => ({
        componentId: this.buildComponentId(component.nodeIds),
        nodeIds: component.nodeIds
      }));
    }

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

        const ratio = overlap / Math.max(1, Math.max(members.size, snapshot.members.size));
        if (
          overlap > bestOverlap ||
          (overlap === bestOverlap && ratio > bestRatio) ||
          (overlap === bestOverlap && ratio === bestRatio && best && snapshot.componentId.localeCompare(best.componentId) < 0)
        ) {
          best = snapshot;
          bestOverlap = overlap;
          bestRatio = ratio;
        }
      }

      if (best) {
        claimed.add(best.componentId);
        return {
          componentId: best.componentId,
          nodeIds: component.nodeIds
        };
      }

      return {
        componentId: this.buildComponentId(component.nodeIds),
        nodeIds: component.nodeIds
      };
    });

    assigned.sort(
      (left, right) => right.nodeIds.length - left.nodeIds.length || left.componentId.localeCompare(right.componentId)
    );
    return assigned;
  }

  private resolveComponentAnchor(
    componentId: string,
    componentWeight: number,
    componentIndex: number,
    previousState: FA2LineState
  ): Point {
    const previous = previousState.anchors[componentId];
    if (previous) {
      return previous;
    }

    const ringRadius = 18 + Math.sqrt(Math.max(componentWeight, 1)) * 8;
    const deterministicSeed = (this.hashId(componentId) % 29) + componentIndex;
    const ringAngle = deterministicSeed * GOLDEN_ANGLE;
    return {
      x: Math.cos(ringAngle) * ringRadius,
      y: Math.sin(ringAngle) * ringRadius
    };
  }

  private seedPositions(
    nodeIds: string[],
    communityId: string,
    anchor: Point,
    previousState: FA2LineState
  ): Map<string, Point> {
    const positions = new Map<string, Point>();
    const radius = Math.max(9, Math.sqrt(nodeIds.length) * 4);

    for (let index = 0; index < nodeIds.length; index += 1) {
      const nodeId = nodeIds[index];
      const previous = previousState.nodePositions[nodeId];
      if (previous) {
        positions.set(nodeId, previous);
        continue;
      }

      const seed = this.hashId(`${communityId}:${nodeId}`);
      const angleOffset = ((seed % 360) / 360) * TAU;
      const angle = ((index / Math.max(nodeIds.length, 1)) * TAU + angleOffset) % TAU;
      const distance = radius * (0.5 + (((seed >>> 9) % 1000) / 1000) * 0.75);
      positions.set(nodeId, {
        x: anchor.x + Math.cos(angle) * distance,
        y: anchor.y + Math.sin(angle) * distance
      });
    }

    return positions;
  }

  private solveComponentPositions(
    nodeIds: string[],
    sortedAdjacencyByNodeId: Map<string, string[]>,
    anchor: Point,
    seeded: Map<string, Point>,
    input: NetworkLayoutInput
  ): Map<string, Point> {
    const positions = new Map<string, Point>();
    for (const [nodeId, point] of seeded.entries()) {
      positions.set(nodeId, { x: point.x, y: point.y });
    }

    const nodeSet = new Set(nodeIds);
    const defaultIterations = Math.min(64, Math.max(18, Math.floor(Math.sqrt(nodeIds.length) * 7)));
    const configuredIterations = Math.floor(
      resolveNumberConfig(input.strategyConfig, "iterations", defaultIterations, 12, 180)
    );
    const adaptiveIterationCap = nodeIds.length >= 800 ? 24 : nodeIds.length >= 500 ? 32 : nodeIds.length >= 250 ? 44 : 64;
    const iterations = Math.max(12, Math.min(configuredIterations, adaptiveIterationCap));
    const repulsionStrength = resolveNumberConfig(input.strategyConfig, "repulsionStrength", 14, 2, 60);
    const attractionStrength = resolveNumberConfig(input.strategyConfig, "attractionStrength", 0.08, 0.01, 0.4);
    const gravityStrength = resolveNumberConfig(input.strategyConfig, "gravityStrength", 0.02, 0.001, 0.2);
    const jitterScale = 0.05;

    for (let iteration = 0; iteration < iterations; iteration += 1) {
      const cooling = 1 - iteration / Math.max(iterations, 1);
      const maxStep = 1.2 + cooling * 3.4;
      const next = new Map<string, Point>();

      for (let index = 0; index < nodeIds.length; index += 1) {
        const nodeId = nodeIds[index];
        const current = positions.get(nodeId)!;
        let forceX = (anchor.x - current.x) * gravityStrength;
        let forceY = (anchor.y - current.y) * gravityStrength;

        const neighbors = sortedAdjacencyByNodeId.get(nodeId) ?? [];
        for (const neighborId of neighbors) {
          if (!nodeSet.has(neighborId)) {
            continue;
          }
          const neighbor = positions.get(neighborId);
          if (!neighbor) {
            continue;
          }
          const dx = neighbor.x - current.x;
          const dy = neighbor.y - current.y;
          forceX += dx * attractionStrength;
          forceY += dy * attractionStrength;
        }

        // Bound repulsion work by sampling deterministic peers around each index.
        const sampleCount = Math.min(16, Math.max(4, Math.floor(Math.sqrt(nodeIds.length))));
        for (let offset = 1; offset <= sampleCount; offset += 1) {
          const sampledIndex = (index + offset * 7) % nodeIds.length;
          const otherId = nodeIds[sampledIndex];
          if (otherId === nodeId) {
            continue;
          }
          const other = positions.get(otherId);
          if (!other) {
            continue;
          }
          const dx = current.x - other.x;
          const dy = current.y - other.y;
          const d2 = Math.max(0.25, dx * dx + dy * dy);
          const invDistance = 1 / Math.sqrt(d2);
          const repulse = repulsionStrength / d2;
          forceX += dx * invDistance * repulse;
          forceY += dy * invDistance * repulse;
        }

        const jitterSeed = this.hashId(`${nodeId}:${iteration}`);
        forceX += ((((jitterSeed & 1023) / 1023) - 0.5) * jitterScale) * cooling;
        forceY += (((((jitterSeed >>> 10) & 1023) / 1023) - 0.5) * jitterScale) * cooling;

        const speed = Math.hypot(forceX, forceY);
        if (speed > maxStep && speed > 0) {
          forceX = (forceX / speed) * maxStep;
          forceY = (forceY / speed) * maxStep;
        }

        next.set(nodeId, {
          x: current.x + forceX,
          y: current.y + forceY
        });
      }

      positions.clear();
      for (const [nodeId, point] of next.entries()) {
        positions.set(nodeId, point);
      }
    }

    return positions;
  }
}

export const FA2_LINE_STRATEGY_DEFINITION: NetworkLayoutStrategyDefinition = {
  strategy: "fa2line",
  label: "FA2 Line",
  fields: [
    { key: "iterations", label: "Iterations", min: 12, max: 180, step: 1 },
    { key: "attractionStrength", label: "Attraction", min: 0.01, max: 0.4, step: 0.01 },
    { key: "repulsionStrength", label: "Repulsion", min: 2, max: 60, step: 1 },
    { key: "gravityStrength", label: "Gravity", min: 0.001, max: 0.2, step: 0.001 }
  ],
  createInitialConfig: () => ({
    iterations: 32,
    attractionStrength: 0.08,
    repulsionStrength: 14,
    gravityStrength: 0.02
  }),
  summarizeConfig: (config) => {
    const iterations = Number(config.iterations ?? 32);
    const gravity = Number(config.gravityStrength ?? 0.02);
    return `iter=${iterations} gravity=${gravity.toFixed(3)}`;
  },
  createAlgorithm: () => new FA2LineLayoutAlgorithm()
};
