import type { NetworkLayoutStrategyDefinition } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyDefinition";
import {
  NetworkLayoutAlgorithmBase,
  type NodeLayoutParams,
  type NodeLayoutResult,
  EPS,
  TAU,
  compareString
} from "@/domain/timelapse/networkLayout/NetworkLayoutAlgorithmBase";

const DEFAULT_NODE_SPACING = 8;
const DEFAULT_STABILITY = 0.8;
const DEFAULT_QUALITY = 1.0;

export class StrictTemporalLayoutAlgorithm extends NetworkLayoutAlgorithmBase {
  public readonly strategy = "strict-temporal" as const;

  protected layoutNodesInCommunity(params: NodeLayoutParams): NodeLayoutResult {
    const {
      communityId,
      nodeIds,
      anchorX,
      anchorY,
      communityRadius,
      nodeSpacing,
      stability,
      quality,
      adjacencyByNodeId,
      nodeHashById,
      previousIndex
    } = params;

    const n = nodeIds.length;
    const x = new Float64Array(n);
    const y = new Float64Array(n);
    if (n === 0) {
      return { nodeIds, x, y };
    }

    const localIndex = new Map<string, number>();
    for (let i = 0; i < n; i++) {
      localIndex.set(nodeIds[i], i);
    }

    const targetX = new Float64Array(n);
    const targetY = new Float64Array(n);
    this.computeBFSRingLayout({
      nodeIds,
      localIndex,
      anchorX,
      anchorY,
      communityRadius,
      nodeSpacing,
      adjacencyByNodeId,
      nodeHashById,
      targetX,
      targetY
    });

    const prevAnchor = previousIndex.communityAnchorById.get(communityId);
    if (prevAnchor) {
      this.alignTargetRotation({
        nodeIds,
        targetX,
        targetY,
        anchorX,
        anchorY,
        prevAnchor,
        previousIndex
      });
    }

    const anchorDx = prevAnchor ? anchorX - prevAnchor.x : 0;
    const anchorDy = prevAnchor ? anchorY - prevAnchor.y : 0;
    const blendFactor = 1 - stability;

    for (let i = 0; i < n; i++) {
      const prevPos = previousIndex.nodePositions[nodeIds[i]];
      if (prevPos && stability > 0) {
        const px = prevPos.x + anchorDx;
        const py = prevPos.y + anchorDy;
        x[i] = px + (targetX[i] - px) * blendFactor;
        y[i] = py + (targetY[i] - py) * blendFactor;
      } else {
        x[i] = targetX[i];
        y[i] = targetY[i];
      }
    }

    const containR = communityRadius * 1.08;
    this.clampToContainment(x, y, anchorX, anchorY, containR, n);

    if (n > 1) {
      const colIters = n <= 300 ? Math.round(3 * quality) : n <= 1000 ? Math.round(2 * quality) : 1;
      this.resolveNodeCollisions({ x, y, anchorX, anchorY, nodeSpacing, containR, n, iterations: colIters });
    }

    return { nodeIds, x, y };
  }

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
    const {
      nodeIds,
      localIndex,
      anchorX,
      anchorY,
      communityRadius,
      nodeSpacing,
      adjacencyByNodeId,
      nodeHashById,
      targetX,
      targetY
    } = params;
    const n = nodeIds.length;

    if (n === 1) {
      targetX[0] = anchorX;
      targetY[0] = anchorY;
      return;
    }

    let centerIdx = 0;
    let maxDeg = -1;
    for (let i = 0; i < n; i++) {
      let deg = 0;
      const nb = adjacencyByNodeId.get(nodeIds[i]);
      if (nb) {
        for (const nbId of nb) {
          if (localIndex.has(nbId)) {
            deg++;
          }
        }
      }
      if (deg > maxDeg || (deg === maxDeg && compareString(nodeIds[i], nodeIds[centerIdx]) < 0)) {
        maxDeg = deg;
        centerIdx = i;
      }
    }

    const ring = new Int32Array(n).fill(-1);
    const parentIdx = new Int32Array(n).fill(-1);
    const bfsQ: number[] = [centerIdx];
    ring[centerIdx] = 0;
    let head = 0;
    let maxRing = 0;

    while (head < bfsQ.length) {
      const i = bfsQ[head++];
      const r = ring[i];
      const nb = adjacencyByNodeId.get(nodeIds[i]);
      if (nb) {
        for (const nbId of nb) {
          const j = localIndex.get(nbId);
          if (j == null || ring[j] !== -1) {
            continue;
          }
          ring[j] = r + 1;
          parentIdx[j] = i;
          if (r + 1 > maxRing) {
            maxRing = r + 1;
          }
          bfsQ.push(j);
        }
      }
    }

    let hasUnreached = false;
    for (let i = 0; i < n; i++) {
      if (ring[i] === -1) {
        ring[i] = maxRing + 1;
        hasUnreached = true;
      }
    }
    const numRings = maxRing + (hasUnreached ? 2 : 1);

    const byRing: number[][] = [];
    for (let i = 0; i < n; i++) {
      const r = ring[i];
      while (byRing.length <= r) {
        byRing.push([]);
      }
      byRing[r].push(i);
    }

    const usableR = Math.max(nodeSpacing * 2, communityRadius - nodeSpacing);
    const baseStep = numRings > 1 ? usableR / (numRings - 0.5) : usableR;
    const ringR = new Float64Array(numRings);
    let prevR = 0;

    for (let r = 1; r < numRings; r++) {
      const count = byRing[r]?.length ?? 0;
      const minByCount = count > 1 ? (count * nodeSpacing * 1.15) / (2 * Math.PI) : 0;
      const minByPrev = prevR + nodeSpacing * 1.5;
      const natural = r * baseStep;
      ringR[r] = Math.max(natural, minByCount, minByPrev);
      prevR = ringR[r];
    }

    targetX[centerIdx] = anchorX;
    targetY[centerIdx] = anchorY;

    const angleOffset = ((nodeHashById.get(nodeIds[centerIdx]) ?? 0) % 10000 / 10000) * TAU;

    for (let r = 1; r < byRing.length; r++) {
      const indices = byRing[r];
      if (!indices || indices.length === 0) {
        continue;
      }

      const radius = ringR[r];
      const count = indices.length;

      const sorted = indices.map((i) => {
        const pi = parentIdx[i];
        let parentAngle: number;
        if (pi >= 0) {
          const px = targetX[pi] - anchorX;
          const py = targetY[pi] - anchorY;
          parentAngle = Math.sqrt(px * px + py * py) > EPS ? Math.atan2(py, px) : 0;
        } else {
          parentAngle = ((nodeHashById.get(nodeIds[i]) ?? 0) % 10000 / 10000) * TAU;
        }
        return { i, parentAngle };
      });

      sorted.sort((a, b) => {
        const d = a.parentAngle - b.parentAngle;
        return Math.abs(d) > 1e-12 ? d : compareString(nodeIds[a.i], nodeIds[b.i]);
      });

      const startAngle = sorted[0].parentAngle + angleOffset;
      for (let k = 0; k < count; k++) {
        const angle = startAngle + (k / count) * TAU;
        const { i } = sorted[k];
        targetX[i] = anchorX + Math.cos(angle) * radius;
        targetY[i] = anchorY + Math.sin(angle) * radius;
      }
    }
  }

  private alignTargetRotation(params: {
    nodeIds: string[];
    targetX: Float64Array;
    targetY: Float64Array;
    anchorX: number;
    anchorY: number;
    prevAnchor: { x: number; y: number };
    previousIndex: NodeLayoutParams["previousIndex"];
  }): void {
    const { nodeIds, targetX, targetY, anchorX, anchorY, prevAnchor, previousIndex } = params;
    const n = nodeIds.length;

    let sumRe = 0;
    let sumIm = 0;
    let count = 0;
    for (let i = 0; i < n; i++) {
      const prevPos = previousIndex.nodePositions[nodeIds[i]];
      if (!prevPos) {
        continue;
      }

      const tx = targetX[i] - anchorX;
      const ty = targetY[i] - anchorY;
      const px = prevPos.x - prevAnchor.x;
      const py = prevPos.y - prevAnchor.y;

      if (tx * tx + ty * ty < EPS || px * px + py * py < EPS) {
        continue;
      }

      sumRe += tx * px + ty * py;
      sumIm += tx * py - ty * px;
      count++;
    }

    if (count < 2 || sumRe * sumRe + sumIm * sumIm < EPS) {
      return;
    }

    const angle = Math.atan2(sumIm, sumRe);
    if (Math.abs(angle) < 0.008) {
      return;
    }

    const cos = Math.cos(angle);
    const sin = Math.sin(angle);
    for (let i = 0; i < n; i++) {
      const tx = targetX[i] - anchorX;
      const ty = targetY[i] - anchorY;
      targetX[i] = anchorX + tx * cos - ty * sin;
      targetY[i] = anchorY + tx * sin + ty * cos;
    }
  }

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
      for (let iter = 0; iter < iterations; iter++) {
        for (let i = 0; i < n; i++) {
          for (let j = i + 1; j < n; j++) {
            let dx = x[i] - x[j];
            let dy = y[i] - y[j];
            const d2 = dx * dx + dy * dy;
            if (d2 >= minDist * minDist) {
              continue;
            }
            const d = Math.sqrt(d2) || EPS;
            const push = (minDist - d) * 0.5;
            dx /= d;
            dy /= d;
            x[i] += dx * push;
            y[i] += dy * push;
            x[j] -= dx * push;
            y[j] -= dy * push;
          }
        }
        this.clampToContainment(x, y, anchorX, anchorY, containR, n);
      }
    } else {
      const gridSize = minDist * 2;
      const store = this.makeGridStore();
      const fx = new Float64Array(n);
      const fy = new Float64Array(n);

      for (let iter = 0; iter < iterations; iter++) {
        fx.fill(0);
        fy.fill(0);
        this.gridClear(store);

        for (let i = 0; i < n; i++) {
          this.gridPush(store, this.gridKey(x[i], y[i], gridSize), i);
        }

        for (let i = 0; i < n; i++) {
          const cx = Math.floor(x[i] / gridSize);
          const cy = Math.floor(y[i] / gridSize);
          for (let ox = -1; ox <= 1; ox++) {
            for (let oy = -1; oy <= 1; oy++) {
              const bucket = store.buckets.get(this.gridKey((cx + ox) * gridSize, (cy + oy) * gridSize, gridSize));
              if (!bucket) {
                continue;
              }
              for (const j of bucket) {
                if (j <= i) {
                  continue;
                }
                let dx = x[i] - x[j];
                let dy = y[i] - y[j];
                const d2 = dx * dx + dy * dy;
                if (d2 >= minDist * minDist) {
                  continue;
                }
                const d = Math.sqrt(d2) || EPS;
                const push = (minDist - d) * 0.5;
                dx /= d;
                dy /= d;
                fx[i] += dx * push;
                fy[i] += dy * push;
                fx[j] -= dx * push;
                fy[j] -= dy * push;
              }
            }
          }
        }

        for (let i = 0; i < n; i++) {
          x[i] += fx[i];
          y[i] += fy[i];
        }
        this.clampToContainment(x, y, anchorX, anchorY, containR, n);
      }
    }
  }

  private makeGridStore(): import("@/domain/timelapse/networkLayout/NetworkLayoutAlgorithmBase").GridStore {
    return { buckets: new Map(), usedKeys: [], pool: [] };
  }
}

export const STRICT_TEMPORAL_STRATEGY_DEFINITION: NetworkLayoutStrategyDefinition = {
  strategy: "strict-temporal",
  label: "Strict Temporal",
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
    const q = Number(config.quality ?? DEFAULT_QUALITY);
    const s = Number(config.stability ?? DEFAULT_STABILITY);
    const sp = Number(config.nodeSpacing ?? DEFAULT_NODE_SPACING);
    return `q=${q.toFixed(2)} stab=${s.toFixed(2)} space=${sp}`;
  },
  createAlgorithm: () => new StrictTemporalLayoutAlgorithm()
};
