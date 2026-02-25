import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutStrategyDefinition } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyDefinition";
import {
  NetworkLayoutAlgorithmBase,
  type NodeLayoutParams,
  type NodeLayoutResult,
  EPS,
  clamp
} from "@/domain/timelapse/networkLayout/NetworkLayoutAlgorithmBase";

const DEFAULT_QUALITY = 1.0;
const DEFAULT_STABILITY = 0.8;
const DEFAULT_NODE_SPACING = 8;
const DEFAULT_ANCHOR_WEIGHT = 1.2;
const DEFAULT_MAX_HOPS = 5;

export class StressMajorizationLayoutAlgorithm extends NetworkLayoutAlgorithmBase {
  public readonly strategy = "stress-majorization" as const;

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
      strategyConfig,
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

    const anchorWeight = resolveNumberConfig(strategyConfig, "anchorWeight", DEFAULT_ANCHOR_WEIGHT, 0.0, 4.0);
    const maxHops = Math.round(resolveNumberConfig(strategyConfig, "maxHops", DEFAULT_MAX_HOPS, 2, 10));
    const iters = clamp(
      Math.round(12 * quality * (n <= 60 ? 1.5 : n <= 250 ? 1.1 : n <= 800 ? 0.8 : 0.6)),
      3,
      25
    );

    const localIndex = new Map<string, number>();
    for (let i = 0; i < n; i++) {
      localIndex.set(nodeIds[i], i);
    }

    const prevAnchor = previousIndex.communityAnchorById.get(communityId);
    const anchorDx = prevAnchor ? anchorX - prevAnchor.x : 0;
    const anchorDy = prevAnchor ? anchorY - prevAnchor.y : 0;
    const containR = communityRadius * 1.05;
    const seedR = Math.max(nodeSpacing * 3, communityRadius - nodeSpacing * 2);

    const prevX = new Float64Array(n);
    const prevY = new Float64Array(n);
    const hasPrev = new Uint8Array(n);

    for (let i = 0; i < n; i++) {
      const prevPos = previousIndex.nodePositions[nodeIds[i]];
      if (prevPos) {
        x[i] = prevPos.x + anchorDx;
        y[i] = prevPos.y + anchorDy;
        prevX[i] = x[i];
        prevY[i] = y[i];
        hasPrev[i] = 1;
        const ax = x[i] - anchorX;
        const ay = y[i] - anchorY;
        const d = Math.sqrt(ax * ax + ay * ay);
        if (d > containR) {
          x[i] = anchorX + (ax * containR) / d;
          y[i] = anchorY + (ay * containR) / d;
        }
      } else {
        let sx = 0;
        let sy = 0;
        let sc = 0;
        const nbs = adjacencyByNodeId.get(nodeIds[i]);
        if (nbs) {
          for (const nb of nbs) {
            const j = localIndex.get(nb);
            if (j != null && hasPrev[j]) {
              sx += x[j];
              sy += y[j];
              sc++;
              if (sc >= 4) {
                break;
              }
            }
          }
        }
        if (sc > 0) {
          x[i] = sx / sc;
          y[i] = sy / sc;
        } else {
          const hash = nodeHashById.get(nodeIds[i]) ?? i;
          const ang = i * Math.PI * (3 - Math.sqrt(5)) + ((hash % 10000) / 10000) * 2 * Math.PI;
          const rad = Math.sqrt((i + 0.5) / Math.max(1, n)) * seedR;
          x[i] = anchorX + Math.cos(ang) * rad;
          y[i] = anchorY + Math.sin(ang) * rad;
        }
        prevX[i] = x[i];
        prevY[i] = y[i];
      }
    }

    const targetIdeal = nodeSpacing * 2.2;

    const neighbourCount = new Int32Array(n);
    const bfsQueue = new Int32Array(n + 4);
    const bfsDist = new Int32Array(n);

    for (let src = 0; src < n; src++) {
      bfsDist.fill(-1);
      bfsDist[src] = 0;
      bfsQueue[0] = src;
      let head = 0;
      let tail = 1;
      while (head < tail) {
        const cur = bfsQueue[head++];
        const d = bfsDist[cur];
        if (d >= maxHops) {
          continue;
        }
        const nbs = adjacencyByNodeId.get(nodeIds[cur]);
        if (!nbs) {
          continue;
        }
        for (const nb of nbs) {
          const j = localIndex.get(nb);
          if (j == null || bfsDist[j] !== -1) {
            continue;
          }
          bfsDist[j] = d + 1;
          bfsQueue[tail++] = j;
          neighbourCount[src]++;
        }
      }
    }

    const offsets = new Int32Array(n + 1);
    for (let i = 0; i < n; i++) {
      offsets[i + 1] = offsets[i] + neighbourCount[i];
    }
    const total = offsets[n];
    const distJ = new Int32Array(total);
    const distD = new Float64Array(total);
    const distW = new Float64Array(total);
    const lw = new Float64Array(n);

    const fillPos = new Int32Array(n);
    for (let src = 0; src < n; src++) {
      bfsDist.fill(-1);
      bfsDist[src] = 0;
      bfsQueue[0] = src;
      let head = 0;
      let tail = 1;
      while (head < tail) {
        const cur = bfsQueue[head++];
        const d = bfsDist[cur];
        if (d >= maxHops) {
          continue;
        }
        const nbs = adjacencyByNodeId.get(nodeIds[cur]);
        if (!nbs) {
          continue;
        }
        for (const nb of nbs) {
          const j = localIndex.get(nb);
          if (j == null || bfsDist[j] !== -1) {
            continue;
          }
          bfsDist[j] = d + 1;
          bfsQueue[tail++] = j;
          const dij = (d + 1) * targetIdeal;
          const wij = 1 / (dij * dij + EPS);
          const pos = offsets[src] + fillPos[src];
          distJ[pos] = j;
          distD[pos] = dij;
          distW[pos] = wij;
          fillPos[src]++;
          lw[src] += wij;
        }
      }
    }

    const nx = new Float64Array(n);
    const ny = new Float64Array(n);

    for (let iter = 0; iter < iters; iter++) {
      for (let i = 0; i < n; i++) {
        const start = offsets[i];
        const end = offsets[i + 1];
        let sumX = 0;
        let sumY = 0;

        for (let p = start; p < end; p++) {
          const j = distJ[p];
          const dij = distD[p];
          const wij = distW[p];

          let dx = x[i] - x[j];
          let dy = y[i] - y[j];
          let d = Math.sqrt(dx * dx + dy * dy);
          if (d < EPS) {
            const ang = ((i * 997 + j * 991 + iter * 37) % 360) * ((2 * Math.PI) / 360);
            dx = Math.cos(ang) * 0.01;
            dy = Math.sin(ang) * 0.01;
            d = 0.01;
          }

          sumX += wij * (x[j] + dij * (dx / d));
          sumY += wij * (y[j] + dij * (dy / d));
        }

        const aw = anchorWeight * (hasPrev[i] ? 1.0 : 0.3);
        const denom = lw[i] + aw;

        if (denom < EPS) {
          nx[i] = x[i];
          ny[i] = y[i];
        } else {
          nx[i] = (sumX + aw * prevX[i]) / denom;
          ny[i] = (sumY + aw * prevY[i]) / denom;
        }
      }

      for (let i = 0; i < n; i++) {
        x[i] = nx[i];
        y[i] = ny[i];
      }
      this.clampToContainment(x, y, anchorX, anchorY, containR, n);
    }

    const gravityStrength = 0.06;
    for (let i = 0; i < n; i++) {
      x[i] += (anchorX - x[i]) * gravityStrength;
      y[i] += (anchorY - y[i]) * gravityStrength;
    }

    const extraBlend = stability * 0.35;
    if (extraBlend > 0.01 && prevAnchor) {
      for (let i = 0; i < n; i++) {
        if (!hasPrev[i]) {
          continue;
        }
        x[i] = prevX[i] + (x[i] - prevX[i]) * (1 - extraBlend);
        y[i] = prevY[i] + (y[i] - prevY[i]) * (1 - extraBlend);
      }
    }

    this.clampToContainment(x, y, anchorX, anchorY, containR, n);
    if (n > 1 && n <= 800) {
      this.resolveCollisions(x, y, anchorX, anchorY, nodeSpacing, containR, n);
    }

    return { nodeIds, x, y };
  }

  private resolveCollisions(
    x: Float64Array,
    y: Float64Array,
    anchorX: number,
    anchorY: number,
    nodeSpacing: number,
    containR: number,
    n: number
  ): void {
    const minD = nodeSpacing * 0.88;
    const iters = n <= 200 ? 2 : 1;

    for (let iter = 0; iter < iters; iter++) {
      if (n <= 350) {
        for (let i = 0; i < n; i++) {
          for (let j = i + 1; j < n; j++) {
            let dx = x[i] - x[j];
            let dy = y[i] - y[j];
            const d2 = dx * dx + dy * dy;
            if (d2 >= minD * minD) {
              continue;
            }
            const d = Math.sqrt(d2) || EPS;
            const push = (minD - d) * 0.5;
            dx /= d;
            dy /= d;
            x[i] += dx * push;
            y[i] += dy * push;
            x[j] -= dx * push;
            y[j] -= dy * push;
          }
        }
      } else {
        const cellSize = minD * 2;
        const store = this.makeGridStore();
        for (let i = 0; i < n; i++) {
          this.gridPush(store, this.gridKey(x[i], y[i], cellSize), i);
        }
        const fx = new Float64Array(n);
        const fy = new Float64Array(n);
        for (let i = 0; i < n; i++) {
          const cx = Math.floor(x[i] / cellSize);
          const cy = Math.floor(y[i] / cellSize);
          for (let ox = -1; ox <= 1; ox++) {
            for (let oy = -1; oy <= 1; oy++) {
              const key = this.gridKey((cx + ox) * cellSize, (cy + oy) * cellSize, cellSize);
              const b = store.buckets.get(key);
              if (!b) {
                continue;
              }
              for (const j of b) {
                if (j <= i) {
                  continue;
                }
                let dx = x[i] - x[j];
                let dy = y[i] - y[j];
                const d2 = dx * dx + dy * dy;
                if (d2 >= minD * minD) {
                  continue;
                }
                const d = Math.sqrt(d2) || EPS;
                const push = (minD - d) * 0.5;
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
        this.gridClear(store);
        for (let i = 0; i < n; i++) {
          x[i] += fx[i];
          y[i] += fy[i];
        }
      }
      this.clampToContainment(x, y, anchorX, anchorY, containR, n);
    }
  }

  private makeGridStore(): import("@/domain/timelapse/networkLayout/NetworkLayoutAlgorithmBase").GridStore {
    return { buckets: new Map(), usedKeys: [], pool: [] };
  }
}

export const STRESS_MAJORIZATION_STRATEGY_DEFINITION: NetworkLayoutStrategyDefinition = {
  strategy: "stress-majorization",
  label: "Stress Majorization",
  fields: [
    { key: "quality", label: "Quality", min: 0.5, max: 1.5, step: 0.05 },
    { key: "stability", label: "Stability", min: 0, max: 0.95, step: 0.05 },
    { key: "nodeSpacing", label: "Node Spacing", min: 4, max: 20, step: 1 },
    { key: "anchorWeight", label: "Anchor Weight", min: 0.0, max: 4.0, step: 0.1 },
    { key: "maxHops", label: "Max Hops", min: 2, max: 10, step: 1 }
  ],
  createInitialConfig: () => ({
    quality: DEFAULT_QUALITY,
    stability: DEFAULT_STABILITY,
    nodeSpacing: DEFAULT_NODE_SPACING,
    anchorWeight: DEFAULT_ANCHOR_WEIGHT,
    maxHops: DEFAULT_MAX_HOPS
  }),
  summarizeConfig: (c) =>
    `q=${Number(c.quality ?? DEFAULT_QUALITY).toFixed(2)} ` +
    `stab=${Number(c.stability ?? DEFAULT_STABILITY).toFixed(2)} ` +
    `anc=${Number(c.anchorWeight ?? DEFAULT_ANCHOR_WEIGHT).toFixed(1)} ` +
    `hops=${Number(c.maxHops ?? DEFAULT_MAX_HOPS)}`,
  createAlgorithm: () => new StressMajorizationLayoutAlgorithm()
};
