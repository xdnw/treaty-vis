import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutStrategyDefinition } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyDefinition";
import type { NetworkLayoutInput, NetworkLayoutOutput } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";
import type { WorkerCommunityTarget, WorkerComponentTarget, WorkerNodeTarget } from "@/domain/timelapse/workerProtocol";

// --- Constants ---
const DEFAULT_QUALITY = 1.0;
const DEFAULT_STABILITY = 0.85; // High default stability for "Time-lapse" feel
const DEFAULT_NODE_SPACING = 10;

const EPSILON = 0.0001;
const GRAVITY_CONSTANT = 0.05;

type Point = { x: number; y: number };

type SimulationNode = {
  id: string;
  x: number;
  y: number;
  vx: number;
  vy: number;
  fx: number;
  fy: number;
  prevX?: number; // Snapshot position
  prevY?: number; // Snapshot position
  isNew: boolean;
  componentId: string;
  mass: number;
};

type SimulationEdge = {
  source: number; // Index in nodes array
  target: number; // Index in nodes array
  weight: number;
};

type LayoutSnapshot = {
  version: 3;
  nodePositions: Record<string, Point>;
};

export class CleanTemporalLayoutAlgorithm implements INetworkLayoutAlgorithm {
  public readonly strategy = "clean-temporal" as const;

  public run(input: NetworkLayoutInput): NetworkLayoutOutput {
    // 1. Resolve Config
    const quality = resolveNumberConfig(input.strategyConfig, "quality", DEFAULT_QUALITY, 0.1, 2.0);
    const stability = resolveNumberConfig(input.strategyConfig, "stability", DEFAULT_STABILITY, 0.0, 1.0);
    const nodeSpacing = resolveNumberConfig(input.strategyConfig, "nodeSpacing", DEFAULT_NODE_SPACING, 5, 50);

    // Derived simulation parameters
    const iterations = Math.round(150 * quality); 
    const repulsionRadius = nodeSpacing * 4;
    const optimalEdgeLength = nodeSpacing * 1.5;
    
    // Scale tether strength: 0.0 -> 0.0 force, 0.95 -> Strong force. 
    // We treat 1.0 effectively as "Frozen".
    const tetherStrength = stability * 0.15; 

    // 2. Parse Previous State (Snapshot)
    const prevPositions = this.parseSnapshot(input.previousState);

    // 3. Identify Connected Components (for rough grouping)
    // We use this to keep disconnected islands somewhat near each other but chemically distinct.
    const nodeIds = input.nodeIds;
    const adjacency = input.adjacencyByNodeId;
    const components = this.computeConnectedComponents(nodeIds, adjacency);
    const componentIdByNodeId = new Map<string, string>();
    for (const comp of components) {
      const cId = `comp:${comp[0]}`; // Simple ID based on first node
      for (const nId of comp) componentIdByNodeId.set(nId, cId);
    }

    // 4. Initialize Simulation Nodes
    const nodes: SimulationNode[] = [];
    const nodeIndexById = new Map<string, number>();

    // Sort for deterministic order
    const sortedNodeIds = [...nodeIds].sort();

    for (let i = 0; i < sortedNodeIds.length; i++) {
      const id = sortedNodeIds[i];
      const prev = prevPositions.get(id);
      
      let x = 0;
      let y = 0;
      let isNew = true;

      if (prev) {
        x = prev.x;
        y = prev.y;
        isNew = false;
      } else {
        // Smart Placement: Barycenter of existing neighbors
        // If a node is new, place it exactly between its connected neighbors that exist.
        // This prevents "flying in from (0,0)" which causes massive tangling.
        const neighbors = adjacency.get(id);
        let sx = 0, sy = 0, count = 0;
        if (neighbors) {
          for (const nId of neighbors) {
            const nPos = prevPositions.get(nId);
            if (nPos) {
              sx += nPos.x;
              sy += nPos.y;
              count++;
            }
          }
        }
        
        if (count > 0) {
          x = sx / count;
          y = sy / count;
          // Add tiny jitter to prevent stacking exactly on top
          x += (Math.random() - 0.5) * nodeSpacing * 0.1;
          y += (Math.random() - 0.5) * nodeSpacing * 0.1;
        } else {
          // Totally isolated new node? Spiral placement or near origin
          // We spread them out slightly so they don't explode
          const angle = i * 0.1; 
          const radius = nodeSpacing * 5; 
          x = Math.cos(angle) * radius;
          y = Math.sin(angle) * radius;
        }
      }

      nodes.push({
        id,
        x,
        y,
        vx: 0,
        vy: 0,
        fx: 0,
        fy: 0,
        prevX: prev?.x,
        prevY: prev?.y,
        isNew,
        componentId: componentIdByNodeId.get(id) || "unknown",
        mass: 1 + (adjacency.get(id)?.size || 0) * 0.5, // Hubs are heavier
      });
      nodeIndexById.set(id, i);
    }

    // 5. Build Edges
    const edges: SimulationEdge[] = [];
    for (const sourceId of sortedNodeIds) {
      const sourceIdx = nodeIndexById.get(sourceId);
      if (sourceIdx === undefined) continue;
      
      const neighbors = adjacency.get(sourceId);
      if (!neighbors) continue;

      for (const targetId of neighbors) {
        const targetIdx = nodeIndexById.get(targetId);
        // Add edge if target exists and we haven't added this pair yet (undirected)
        if (targetIdx !== undefined && sourceIdx < targetIdx) {
          edges.push({ source: sourceIdx, target: targetIdx, weight: 1 });
        }
      }
    }

    // 6. Run Simulation
    // We use a high-friction environment to ensure stability. 
    // We don't want oscillation.
    const dt = 0.5; // Time step
    const friction = 0.85; 

    // Grid for spatial optimization of repulsion (O(N) instead of O(N^2))
    const gridSize = Math.max(20, repulsionRadius);
    
    for (let iter = 0; iter < iterations; iter++) {
      // Cooling factor: Start hot (move fast) to untangle, cool down to freeze.
      // If stability is high, we start cooler.
      const alpha = (1 - (iter / iterations)) * (1.0 - (stability * 0.5));

      // Reset Forces
      for (let i = 0; i < nodes.length; i++) {
        nodes[i].fx = 0;
        nodes[i].fy = 0;
      }

      // A. Edge Springs (Attraction)
      for (const edge of edges) {
        const u = nodes[edge.source];
        const v = nodes[edge.target];
        
        const dx = v.x - u.x;
        const dy = v.y - u.y;
        let dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < EPSILON) dist = EPSILON;

        // Hooke's Law with optimal length
        // We want the edge to be 'optimalEdgeLength'
        const displacement = dist - optimalEdgeLength;
        const k = 0.05 * alpha; // stiffness
        
        const force = k * displacement;
        const fx = (dx / dist) * force;
        const fy = (dy / dist) * force;

        u.fx += fx;
        u.fy += fy;
        v.fx -= fx;
        v.fy -= fy;
      }

      // B. N-Body Repulsion (Collision) via Spatial Grid
      // Simple bucket hashing
      const grid = new Map<string, number[]>();
      for (let i = 0; i < nodes.length; i++) {
        const gx = Math.floor(nodes[i].x / gridSize);
        const gy = Math.floor(nodes[i].y / gridSize);
        const key = `${gx},${gy}`;
        const bucket = grid.get(key);
        if (bucket) bucket.push(i);
        else grid.set(key, [i]);
      }

      for (let i = 0; i < nodes.length; i++) {
        const n1 = nodes[i];
        const gx = Math.floor(n1.x / gridSize);
        const gy = Math.floor(n1.y / gridSize);

        // Check 3x3 grid neighborhood
        for (let ox = -1; ox <= 1; ox++) {
          for (let oy = -1; oy <= 1; oy++) {
            const key = `${gx + ox},${gy + oy}`;
            const bucket = grid.get(key);
            if (!bucket) continue;

            for (const j of bucket) {
              if (i === j) continue;
              const n2 = nodes[j];

              const dx = n1.x - n2.x;
              const dy = n1.y - n2.y;
              let distSq = dx * dx + dy * dy;
              
              // Soft repulsion range
              if (distSq > repulsionRadius * repulsionRadius) continue;
              if (distSq < EPSILON) {
                 distSq = EPSILON; // Prevent singularity
                 // Jitter to break symmetry
                 n1.fx += Math.random(); 
                 n1.fy += Math.random();
              }

              const dist = Math.sqrt(distSq);
              // Force falls off with distance
              const force = (nodeSpacing * 15 * alpha) / dist; 
              
              const fx = (dx / dist) * force;
              const fy = (dy / dist) * force;

              n1.fx += fx;
              n1.fy += fy;
            }
          }
        }
      }

      // C. Global Gravity (Keep center of mass near 0,0)
      for (let i = 0; i < nodes.length; i++) {
        const n = nodes[i];
        // Weak pull to center
        n.fx -= n.x * GRAVITY_CONSTANT * alpha;
        n.fy -= n.y * GRAVITY_CONSTANT * alpha;
      }

      // D. Temporal Tether (The Stability logic)
      if (tetherStrength > 0) {
        for (let i = 0; i < nodes.length; i++) {
          const n = nodes[i];
          if (n.prevX !== undefined && n.prevY !== undefined) {
            // Pull towards previous frame position
            // Force increases with distance (Rubber band)
            const dx = n.prevX - n.x;
            const dy = n.prevY - n.y;
            
            // If it's a new node, no tether (let it find its place).
            // If it's an old node, strong tether.
            if (!n.isNew) {
               n.fx += dx * tetherStrength;
               n.fy += dy * tetherStrength;
            }
          }
        }
      }

      // E. Integration (Verlet-ish)
      for (let i = 0; i < nodes.length; i++) {
        const n = nodes[i];
        
        // Cap max force to prevent explosion
        const maxF = 100;
        n.fx = Math.max(-maxF, Math.min(maxF, n.fx));
        n.fy = Math.max(-maxF, Math.min(maxF, n.fy));

        n.vx = (n.vx + n.fx * dt) * friction;
        n.vy = (n.vy + n.fy * dt) * friction;

        n.x += n.vx * dt;
        n.y += n.vy * dt;
      }
    }

    // 7. Output Preparation
    const nodeTargets: WorkerNodeTarget[] = [];
    const positionsRecord: Record<string, Point> = {};
    const componentTargets: WorkerComponentTarget[] = [];
    const communityTargets: WorkerCommunityTarget[] = []; // Not using sub-communities in this algo, but protocol requires valid object

    // Compute component centers for "anchors"
    // This helps the camera or UI know where the "center" of a group is.
    const componentCenters = new Map<string, { x: number; y: number; count: number }>();
    for (const n of nodes) {
      if (!componentCenters.has(n.componentId)) {
        componentCenters.set(n.componentId, { x: 0, y: 0, count: 0 });
      }
      const c = componentCenters.get(n.componentId)!;
      c.x += n.x;
      c.y += n.y;
      c.count++;
    }

    for (const [cId, c] of componentCenters) {
      c.x /= c.count;
      c.y /= c.count;
      
      // We also emit a dummy component target for visualizers that need it
      // The strategy doesn't explicitly optimize for these, but we infer them.
      componentTargets.push({
        componentId: cId,
        nodeIds: [], // We don't track the list explicitly here to save time, unless required
        anchorX: c.x,
        anchorY: c.y,
      });
    }

    for (const n of nodes) {
      positionsRecord[n.id] = { x: n.x, y: n.y };
      
      const center = componentCenters.get(n.componentId) || { x: n.x, y: n.y };

      // Infer average neighbor position for smoothing
      let nx = 0, ny = 0, nc = 0;
      const neighbors = adjacency.get(n.id);
      if (neighbors) {
        for (const nid of neighbors) {
          const neighborIdx = nodeIndexById.get(nid);
          if (neighborIdx !== undefined) {
             nx += nodes[neighborIdx].x;
             ny += nodes[neighborIdx].y;
             nc++;
          }
        }
      }
      if (nc === 0) { nx = n.x; ny = n.y; }
      else { nx /= nc; ny /= nc; }

      nodeTargets.push({
        nodeId: n.id,
        targetX: n.x,
        targetY: n.y,
        componentId: n.componentId,
        communityId: "default", // We simplified community out
        anchorX: center.x,
        anchorY: center.y,
        neighborX: nx,
        neighborY: ny,
      });
    }

    return {
      layout: {
        components: componentTargets,
        communities: communityTargets,
        nodeTargets,
      },
      metadata: {
        state: {
          version: 3,
          nodePositions: positionsRecord
        }
      }
    };
  }

  // --- Helpers ---

  private parseSnapshot(snapshot: unknown): Map<string, Point> {
    const map = new Map<string, Point>();
    if (!snapshot || typeof snapshot !== 'object') return map;
    
    // Handle version 2 or 3 inputs
    const s = snapshot as any;
    const positions = s.nodePositions;
    
    if (positions && typeof positions === 'object') {
      for (const [key, val] of Object.entries(positions)) {
        const p = val as any;
        if (typeof p.x === 'number' && typeof p.y === 'number') {
          map.set(key, { x: p.x, y: p.y });
        }
      }
    }
    return map;
  }

  private computeConnectedComponents(
    nodeIds: string[],
    adjacency: Map<string, Set<string>>
  ): string[][] {
    const visited = new Set<string>();
    const components: string[][] = [];

    for (const node of nodeIds) {
      if (visited.has(node)) continue;
      const component: string[] = [];
      const queue = [node];
      visited.add(node);

      while (queue.length > 0) {
        const curr = queue.pop()!;
        component.push(curr);
        const neighbors = adjacency.get(curr);
        if (neighbors) {
          for (const n of neighbors) {
            if (!visited.has(n)) {
              visited.add(n);
              queue.push(n);
            }
          }
        }
      }
      components.push(component);
    }
    // Sort largest components first
    components.sort((a, b) => b.length - a.length);
    return components;
  }
}

export const CLEAN_TEMPORAL_STRATEGY_DEFINITION: NetworkLayoutStrategyDefinition = {
  strategy: "clean-temporal",
  label: "Clean Temporal",
  fields: [
    { 
      key: "quality", 
      label: "Simulation Steps", 
      min: 0.1, 
      max: 2.0, 
      step: 0.1,
    },
    { 
      key: "stability", 
      label: "Stability", 
      min: 0, 
      max: 1.0, 
      step: 0.05,
    },
    { 
      key: "nodeSpacing", 
      label: "Node Spacing", 
      min: 5, 
      max: 50, 
      step: 1,
    }
  ],
  createInitialConfig: () => ({
    quality: DEFAULT_QUALITY,
    stability: DEFAULT_STABILITY,
    nodeSpacing: DEFAULT_NODE_SPACING
  }),
  summarizeConfig: (config) => {
    return `Q:${config.quality} S:${config.stability} Sp:${config.nodeSpacing}`;
  },
  createAlgorithm: () => new CleanTemporalLayoutAlgorithm()
};