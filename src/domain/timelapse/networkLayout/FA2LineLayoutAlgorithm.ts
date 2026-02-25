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

    for (const component of components) {
      const componentId = component.componentId;
      const anchor = this.resolveComponentAnchor(componentId, component.nodeIds.length, previousState);
      
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
          if (!lookup.has(neighborId)) continue;
          const neighbor = solved.get(neighborId);
          if (!neighbor) continue;
          
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
    if (!Array.isArray(value)) return [];

    const snapshots: ComponentSnapshot[] = [];
    for (const raw of value) {
      if (!raw || typeof raw !== "object") continue;
      
      const candidate = raw as any;
      if (typeof candidate.componentId !== "string") continue;
      
      const anchorX = Number(candidate.anchorX);
      const anchorY = Number(candidate.anchorY);
      if (!Number.isFinite(anchorX) || !Number.isFinite(anchorY)) continue;

      const members = new Set<string>();
      if (Array.isArray(candidate.members) || candidate.members instanceof Set) {
        for (const member of candidate.members) {
          if (typeof member === "string") members.add(member);
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
    if (!value || typeof value !== "object") return {};

    const record: Record<string, Point> = {};
    for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
      if (!raw || typeof raw !== "object") continue;
      
      const point = raw as { x?: unknown; y?: unknown };
      const x = Number(point.x);
      const y = Number(point.y);
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
      
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
      if (visited.has(rootId)) continue;

      const queue = [rootId];
      const members: string[] = [];
      visited.add(rootId);

      while (queue.length > 0) {
        const current = queue.shift()!;
        members.push(current);
        const neighbors = sortedAdjacencyByNodeId.get(current);
        if (!neighbors || neighbors.length === 0) continue;
        
        for (const neighbor of neighbors) {
          if (visited.has(neighbor)) continue;
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
        if (claimed.has(snapshot.componentId)) continue;
        
        let overlap = 0;
        for (const member of members) {
          if (snapshot.members.has(member)) overlap += 1;
        }
        if (overlap === 0) continue;

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
    previousState: FA2LineState
  ): Point {
    const previous = previousState.anchors[componentId];
    if (previous) {
      return previous;
    }

    const ringRadius = 18 + Math.sqrt(Math.max(componentWeight, 1)) * 12;
    const deterministicSeed = this.hashId(componentId);
    const ringAngle = (deterministicSeed % 1000) * GOLDEN_ANGLE;
    
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
    const radius = Math.max(5, Math.sqrt(nodeIds.length) * 3);

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
      const distance = radius * (0.2 + (((seed >>> 9) % 1000) / 1000) * 0.8);
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
    const nodeCount = nodeIds.length;
    
    // Flattened Typed Arrays for maximum memory locality & CPU speed
    const x = new Float32Array(nodeCount);
    const y = new Float32Array(nodeCount);
    // Crucial for stability: Velocities act as memory of previous movement (Inertia)
    const vx = new Float32Array(nodeCount);
    const vy = new Float32Array(nodeCount);

    const nodeIdToIndex = new Map<string, number>();
    for (let i = 0; i < nodeCount; i++) {
      const id = nodeIds[i];
      const pos = seeded.get(id) ?? anchor;
      x[i] = pos.x;
      y[i] = pos.y;
      nodeIdToIndex.set(id, i);
    }

    // Pre-calculate edges (O(E) iteration)
    const edges: number[] = [];
    for (let i = 0; i < nodeCount; i++) {
      const u = nodeIds[i];
      const neighbors = sortedAdjacencyByNodeId.get(u);
      if (neighbors) {
        for (const v of neighbors) {
          const j = nodeIdToIndex.get(v);
          if (j !== undefined && i < j) edges.push(i, j);
        }
      }
    }

    const iterations = Math.floor(resolveNumberConfig(input.strategyConfig, "iterations", 150, 20, 500));
    const repulsionStrength = resolveNumberConfig(input.strategyConfig, "repulsionStrength", 100, 10, 2000);
    const attractionStrength = resolveNumberConfig(input.strategyConfig, "attractionStrength", 0.05, 0.001, 0.5);
    const gravityStrength = resolveNumberConfig(input.strategyConfig, "gravityStrength", 0.05, 0.001, 0.5);

    // Simulated Annealing parameters
    let alpha = 1.0; 
    const alphaMin = 0.001;
    // Decay factor ensures 'alpha' hits exactly 0.001 on the final iteration
    const alphaDecay = 1 - Math.pow(alphaMin, 1 / Math.max(iterations, 1)); 
    const velocityDecay = 0.6; // High friction absorbs kinetic energy, totally eliminating bouncing

    for (let iter = 0; iter < iterations; iter++) {
      // 1. Repulsion (O(N^2) Math.sqrt removed, natively fast in JS engines)
      for (let i = 0; i < nodeCount; i++) {
        for (let j = i + 1; j < nodeCount; j++) {
          let dx = x[i] - x[j];
          let dy = y[i] - y[j];
          let distSq = dx * dx + dy * dy;

          // Deterministic jitter completely prevents overlap explosions without using Math.random()
          if (distSq === 0) {
            dx = ((i * 17 + j * 31) % 71) * 0.01 - 0.35;
            dy = ((i * 23 + j * 47) % 71) * 0.01 - 0.35;
            distSq = dx * dx + dy * dy;
          }

          // Softening limit: Prevents forces from exploding to infinity when nodes are very close
          if (distSq < 1.0) distSq = 1.0;

          const force = (repulsionStrength * alpha) / distSq;
          const fx = dx * force;
          const fy = dy * force;

          vx[i] += fx;
          vy[i] += fy;
          vx[j] -= fx;
          vy[j] -= fy;
        }
      }

      // 2. Attraction (O(E))
      for (let k = 0; k < edges.length; k += 2) {
        const i = edges[k];
        const j = edges[k + 1];

        const dx = x[i] - x[j];
        const dy = y[i] - y[j];

        const force = attractionStrength * alpha;
        const fx = dx * force;
        const fy = dy * force;

        vx[i] -= fx;
        vy[i] -= fy;
        vx[j] += fx;
        vy[j] += fy;
      }

      // 3. Gravity & Application (O(N))
      for (let i = 0; i < nodeCount; i++) {
        const dx = anchor.x - x[i];
        const dy = anchor.y - y[i];
        
        // Gravity pulls gently to anchor point to keep component shapes round
        vx[i] += dx * gravityStrength * alpha;
        vy[i] += dy * gravityStrength * alpha;

        // Apply friction and commit to final positions
        vx[i] *= velocityDecay;
        vy[i] *= velocityDecay;

        x[i] += vx[i];
        y[i] += vy[i];
      }

      alpha *= (1 - alphaDecay); // Temperature cools down uniformly
    }

    const result = new Map<string, Point>();
    for (let i = 0; i < nodeCount; i++) {
      result.set(nodeIds[i], { x: x[i], y: y[i] });
    }
    return result;
  }
}

export const FA2_LINE_STRATEGY_DEFINITION: NetworkLayoutStrategyDefinition = {
  strategy: "fa2line",
  label: "FA2 Line",
  fields: [
    { key: "iterations", label: "Iterations", min: 20, max: 500, step: 10 },
    { key: "attractionStrength", label: "Attraction", min: 0.001, max: 0.5, step: 0.001 },
    { key: "repulsionStrength", label: "Repulsion", min: 10, max: 2000, step: 10 },
    { key: "gravityStrength", label: "Gravity", min: 0.001, max: 0.5, step: 0.001 }
  ],
  createInitialConfig: () => ({
    iterations: 150, // More iterations allowed since velocity loops are extremely fast
    attractionStrength: 0.05,
    repulsionStrength: 100,
    gravityStrength: 0.05
  }),
  summarizeConfig: (config) => {
    const iterations = Number(config.iterations ?? 150);
    return `iter=${iterations}`;
  },
  createAlgorithm: () => new FA2LineLayoutAlgorithm()
};