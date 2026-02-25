import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutInput, NetworkLayoutOutput } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";
import type { WorkerCommunityTarget, WorkerComponentTarget, WorkerNodeTarget } from "@/domain/timelapse/workerProtocol";

const TAU = Math.PI * 2;
const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5));

type Point = { x: number; y: number };

type FA2LineState = {
  anchors: Record<string, Point>;
  nodePositions: Record<string, Point>;
};

export class FA2LineLayoutAlgorithm implements INetworkLayoutAlgorithm {
  public readonly strategy = "fa2line" as const;

  public run(input: NetworkLayoutInput): NetworkLayoutOutput {
    const previousState = this.asState(input.previousState);
    const components = this.computeConnectedComponents(input.nodeIds, input.adjacencyByNodeId);

    const componentTargets: WorkerComponentTarget[] = [];
    const communityTargets: WorkerCommunityTarget[] = [];
    const nodeTargets: WorkerNodeTarget[] = [];

    const nextState: FA2LineState = {
      anchors: {},
      nodePositions: {}
    };

    for (let componentIndex = 0; componentIndex < components.length; componentIndex += 1) {
      const component = components[componentIndex];
      const componentId = this.buildComponentId(component.nodeIds);
      const anchor = this.resolveComponentAnchor(componentId, component.nodeIds.length, componentIndex, previousState);
      nextState.anchors[componentId] = anchor;

      componentTargets.push({
        componentId,
        nodeIds: component.nodeIds,
        anchorX: anchor.x,
        anchorY: anchor.y
      });

      const communityId = `${componentId}:community:0:${component.nodeIds[0] ?? "none"}`;
      communityTargets.push({
        communityId,
        componentId,
        nodeIds: component.nodeIds,
        anchorX: anchor.x,
        anchorY: anchor.y
      });

      const initial = this.seedPositions(component.nodeIds, communityId, anchor, previousState);
      const solved = this.solveComponentPositions(component.nodeIds, input.adjacencyByNodeId, anchor, initial, input);
      for (const [nodeId, position] of solved.entries()) {
        nextState.nodePositions[nodeId] = position;
      }

      const lookup = new Set(component.nodeIds);
      for (const nodeId of component.nodeIds) {
        const position = solved.get(nodeId) ?? anchor;
        const neighbors = input.adjacencyByNodeId.get(nodeId) ?? new Set<string>();

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
      return { anchors: {}, nodePositions: {} };
    }
    const candidate = value as { anchors?: unknown; nodePositions?: unknown };

    return {
      anchors: this.toPointRecord(candidate.anchors),
      nodePositions: this.toPointRecord(candidate.nodePositions)
    };
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

  private buildComponentId(componentNodeIds: string[]): string {
    return `component:${componentNodeIds[0] ?? "none"}:${componentNodeIds.length}`;
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
    adjacencyByNodeId: Map<string, Set<string>>,
    anchor: Point,
    seeded: Map<string, Point>,
    input: NetworkLayoutInput
  ): Map<string, Point> {
    const positions = new Map<string, Point>();
    for (const [nodeId, point] of seeded.entries()) {
      positions.set(nodeId, { x: point.x, y: point.y });
    }

    const nodeSet = new Set(nodeIds);
    const defaultIterations = Math.min(90, Math.max(24, Math.floor(Math.sqrt(nodeIds.length) * 10)));
    const iterations = Math.floor(resolveNumberConfig(input.strategyConfig, "iterations", defaultIterations, 12, 180));
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

        const neighbors = adjacencyByNodeId.get(nodeId) ?? new Set<string>();
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
