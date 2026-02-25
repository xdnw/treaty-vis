import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutStrategyDefinition } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyDefinition";
import {
  NetworkLayoutAlgorithmBase,
  type NodeLayoutParams,
  type NodeLayoutResult,
  type SnapshotIndex,
  EPS,
  TAU,
  clamp,
  compareString
} from "@/domain/timelapse/networkLayout/NetworkLayoutAlgorithmBase";

const DEFAULT_QUALITY = 1.0;
const DEFAULT_STABILITY = 0.8;
const DEFAULT_NODE_SPACING = 8;
const DEFAULT_SWEEP_PASSES = 4;

export class RadialSugiyamaLayoutAlgorithm extends NetworkLayoutAlgorithmBase {
  public readonly strategy = "radial-sugiyama" as const;

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
    if (n === 1) {
      x[0] = anchorX;
      y[0] = anchorY;
      return { nodeIds, x, y };
    }

    const sweepPasses = Math.round(
      resolveNumberConfig(strategyConfig, "sweepPasses", DEFAULT_SWEEP_PASSES, 1, 8) * quality
    );

    const localIndex = new Map<string, number>();
    for (let i = 0; i < n; i++) {
      localIndex.set(nodeIds[i], i);
    }

    let hubIdx = 0;
    let maxDeg = -1;
    for (let i = 0; i < n; i++) {
      let deg = 0;
      const nbs = adjacencyByNodeId.get(nodeIds[i]);
      if (nbs) {
        for (const nb of nbs) {
          if (localIndex.has(nb)) {
            deg++;
          }
        }
      }
      if (deg > maxDeg || (deg === maxDeg && compareString(nodeIds[i], nodeIds[hubIdx]) < 0)) {
        maxDeg = deg;
        hubIdx = i;
      }
    }

    const layer = new Int32Array(n).fill(-1);
    const parentIdx = new Int32Array(n).fill(-1);
    const bfsQ = new Int32Array(n);
    layer[hubIdx] = 0;
    bfsQ[0] = hubIdx;
    let head = 0;
    let tail = 1;
    let maxLayer = 0;

    while (head < tail) {
      const cur = bfsQ[head++];
      const lay = layer[cur];
      const nbs = adjacencyByNodeId.get(nodeIds[cur]);
      if (!nbs) {
        continue;
      }
      for (const nb of nbs) {
        const j = localIndex.get(nb);
        if (j == null || layer[j] !== -1) {
          continue;
        }
        layer[j] = lay + 1;
        parentIdx[j] = cur;
        if (lay + 1 > maxLayer) {
          maxLayer = lay + 1;
        }
        bfsQ[tail++] = j;
      }
    }

    for (let i = 0; i < n; i++) {
      if (layer[i] === -1) {
        layer[i] = maxLayer + 1;
      }
    }
    const numLayers = maxLayer + 1;

    const rings: number[][] = [];
    for (let r = 0; r <= maxLayer + 1; r++) {
      rings.push([]);
    }
    for (let i = 0; i < n; i++) {
      rings[layer[i]].push(i);
    }

    const usableR = Math.max(nodeSpacing * 2, communityRadius - nodeSpacing);
    const ringR = new Float64Array(numLayers);
    ringR[0] = 0;
    let prevR = 0;
    for (let r = 1; r < numLayers; r++) {
      const count = rings[r]?.length ?? 0;
      const minByN = count > 1 ? (count * nodeSpacing * 1.2) / (2 * Math.PI) : nodeSpacing;
      const minByPrev = prevR + nodeSpacing * 1.6;
      const natural = (r / numLayers) * usableR;
      ringR[r] = Math.max(natural, minByN, minByPrev);
      prevR = ringR[r];
    }

    const ringPos = new Array<Float64Array>(numLayers);
    for (let r = 0; r < numLayers; r++) {
      ringPos[r] = new Float64Array(rings[r].length);
      for (let k = 0; k < rings[r].length; k++) {
        ringPos[r][k] = k;
      }
    }
    const posInRing = new Float64Array(n);
    for (let r = 0; r < numLayers; r++) {
      for (let k = 0; k < rings[r].length; k++) {
        posInRing[rings[r][k]] = ringPos[r][k];
      }
    }

    const startAngle = ((nodeHashById.get(nodeIds[hubIdx]) ?? this.hashId(nodeIds[hubIdx])) % 10000 / 10000) * TAU;

    const nodeAngle = new Float64Array(n);
    const updateAngles = () => {
      nodeAngle[hubIdx] = 0;
      for (let r = 1; r < numLayers; r++) {
        const ring = rings[r];
        const count = ring.length;
        if (count === 0) {
          continue;
        }
        for (let k = 0; k < count; k++) {
          nodeAngle[ring[k]] = startAngle + (k / count) * TAU;
        }
      }
    };
    updateAngles();

    for (let pass = 0; pass < sweepPasses; pass++) {
      for (let r = 1; r < numLayers; r++) {
        const ring = rings[r];
        if (ring.length <= 1) {
          continue;
        }
        this.barycentricSort(ring, r, adjacencyByNodeId, nodeIds, localIndex, layer, nodeAngle, startAngle);
        for (let k = 0; k < ring.length; k++) {
          nodeAngle[ring[k]] = startAngle + (k / ring.length) * TAU;
        }
      }

      for (let r = numLayers - 2; r >= 1; r--) {
        const ring = rings[r];
        if (ring.length <= 1) {
          continue;
        }
        this.barycentricSort(ring, r, adjacencyByNodeId, nodeIds, localIndex, layer, nodeAngle, startAngle);
        for (let k = 0; k < ring.length; k++) {
          nodeAngle[ring[k]] = startAngle + (k / ring.length) * TAU;
        }
      }

      if (pass === sweepPasses - 1 || n <= 60) {
        for (let r = 1; r < numLayers; r++) {
          const ring = rings[r];
          if (ring.length <= 2) {
            continue;
          }
          this.adjacentSwapPass(ring, r, adjacencyByNodeId, nodeIds, localIndex, layer, nodeAngle, startAngle);
          for (let k = 0; k < ring.length; k++) {
            nodeAngle[ring[k]] = startAngle + (k / ring.length) * TAU;
          }
        }
      }
    }

    const targetX = new Float64Array(n);
    const targetY = new Float64Array(n);
    targetX[hubIdx] = anchorX;
    targetY[hubIdx] = anchorY;

    for (let r = 1; r < numLayers; r++) {
      const ring = rings[r];
      const count = ring.length;
      for (let k = 0; k < count; k++) {
        const i = ring[k];
        const ang = startAngle + (k / count) * TAU;
        targetX[i] = anchorX + Math.cos(ang) * ringR[r];
        targetY[i] = anchorY + Math.sin(ang) * ringR[r];
      }
    }

    const prevAnchor = previousIndex.communityAnchorById.get(communityId);
    if (prevAnchor) {
      this.alignRotation(targetX, targetY, nodeIds, anchorX, anchorY, prevAnchor, previousIndex, n);
    }

    const anchorDx = prevAnchor ? anchorX - prevAnchor.x : 0;
    const anchorDy = prevAnchor ? anchorY - prevAnchor.y : 0;
    const blendT = 1 - stability;
    const containR = communityRadius * 1.05;

    for (let i = 0; i < n; i++) {
      const prevPos = previousIndex.nodePositions[nodeIds[i]];
      if (prevPos && stability > 0) {
        const px = prevPos.x + anchorDx;
        const py = prevPos.y + anchorDy;
        x[i] = px + (targetX[i] - px) * blendT;
        y[i] = py + (targetY[i] - py) * blendT;
      } else {
        x[i] = targetX[i];
        y[i] = targetY[i];
      }
    }

    this.clampToContainment(x, y, anchorX, anchorY, containR, n);
    return { nodeIds, x, y };
  }

  private barycentricSort(
    ring: number[],
    r: number,
    adjacencyByNodeId: Map<string, Set<string>>,
    nodeIds: string[],
    localIndex: Map<string, number>,
    layer: Int32Array,
    nodeAngle: Float64Array,
    startAngle: number
  ): void {
    const count = ring.length;
    if (count <= 1) {
      return;
    }

    const bary = new Float64Array(count);

    for (let k = 0; k < count; k++) {
      const i = ring[k];
      const nbs = adjacencyByNodeId.get(nodeIds[i]);
      let sumSin = 0;
      let sumCos = 0;
      let sc = 0;

      if (nbs) {
        for (const nb of nbs) {
          const j = localIndex.get(nb);
          if (j == null || layer[j] === r) {
            continue;
          }
          const ang = nodeAngle[j];
          sumCos += Math.cos(ang);
          sumSin += Math.sin(ang);
          sc++;
        }
      }

      if (sc > 0) {
        bary[k] = Math.atan2(sumSin / sc, sumCos / sc);
        if (bary[k] < startAngle) {
          bary[k] += TAU;
        }
      } else {
        bary[k] = startAngle + (k / count) * TAU;
      }
    }

    const order = Array.from({ length: count }, (_, k) => k);
    order.sort((a, b) => {
      const d = bary[a] - bary[b];
      if (Math.abs(d) > 1e-12) {
        return d;
      }
      return compareString(nodeIds[ring[a]], nodeIds[ring[b]]);
    });

    const tmp = ring.slice();
    for (let k = 0; k < count; k++) {
      ring[k] = tmp[order[k]];
    }
  }

  private adjacentSwapPass(
    ring: number[],
    r: number,
    adjacencyByNodeId: Map<string, Set<string>>,
    nodeIds: string[],
    localIndex: Map<string, number>,
    layer: Int32Array,
    nodeAngle: Float64Array,
    startAngle: number
  ): void {
    const count = ring.length;
    const maxOuter = Math.min(count, 6);

    for (let outerPass = 0; outerPass < maxOuter; outerPass++) {
      let improved = false;
      for (let k = 0; k < count - 1; k++) {
        const a = ring[k];
        const b = ring[k + 1];
        const angA = startAngle + (k / count) * TAU;
        const angB = startAngle + ((k + 1) / count) * TAU;

        const crossBefore = this.localCrossings(
          a,
          b,
          angA,
          r,
          adjacencyByNodeId,
          nodeIds,
          localIndex,
          layer,
          nodeAngle
        );
        const crossAfter = this.localCrossings(
          b,
          a,
          angA,
          r,
          adjacencyByNodeId,
          nodeIds,
          localIndex,
          layer,
          nodeAngle
        );

        if (crossAfter < crossBefore) {
          ring[k] = b;
          ring[k + 1] = a;
          nodeAngle[b] = angA;
          nodeAngle[a] = angB;
          improved = true;
        }
      }
      if (!improved) {
        break;
      }
    }
  }

  private localCrossings(
    i: number,
    j: number,
    angI: number,
    r: number,
    adjacencyByNodeId: Map<string, Set<string>>,
    nodeIds: string[],
    localIndex: Map<string, number>,
    layer: Int32Array,
    nodeAngle: Float64Array
  ): number {
    const nbAngI: number[] = [];
    const nbAngJ: number[] = [];
    const nbsI = adjacencyByNodeId.get(nodeIds[i]);
    const nbsJ = adjacencyByNodeId.get(nodeIds[j]);
    if (nbsI) {
      for (const nb of nbsI) {
        const k = localIndex.get(nb);
        if (k != null && layer[k] !== r) {
          nbAngI.push(nodeAngle[k]);
        }
      }
    }
    if (nbsJ) {
      for (const nb of nbsJ) {
        const k = localIndex.get(nb);
        if (k != null && layer[k] !== r) {
          nbAngJ.push(nodeAngle[k]);
        }
      }
    }

    let crossings = 0;
    for (const u of nbAngI) {
      for (const v of nbAngJ) {
        const uWrapped = (u - angI + TAU * 2) % TAU;
        const vWrapped = (v - angI + TAU * 2) % TAU;
        if (uWrapped > vWrapped) {
          crossings++;
        }
      }
    }
    return crossings;
  }

  private alignRotation(
    targetX: Float64Array,
    targetY: Float64Array,
    nodeIds: string[],
    anchorX: number,
    anchorY: number,
    prevAnchor: { x: number; y: number },
    previousIndex: SnapshotIndex,
    n: number
  ): void {
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
    if (count < 3 || sumRe * sumRe + sumIm * sumIm < EPS) {
      return;
    }

    const angle = Math.atan2(sumIm, sumRe);
    if (Math.abs(angle) < 0.01) {
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
}

export const RADIAL_SUGIYAMA_STRATEGY_DEFINITION: NetworkLayoutStrategyDefinition = {
  strategy: "radial-sugiyama",
  label: "Radial Sugiyama",
  fields: [
    { key: "quality", label: "Quality", min: 0.5, max: 1.5, step: 0.05 },
    { key: "stability", label: "Stability", min: 0, max: 0.95, step: 0.05 },
    { key: "nodeSpacing", label: "Node Spacing", min: 4, max: 20, step: 1 },
    { key: "sweepPasses", label: "Sweep Passes", min: 1, max: 8, step: 1 }
  ],
  createInitialConfig: () => ({
    quality: DEFAULT_QUALITY,
    stability: DEFAULT_STABILITY,
    nodeSpacing: DEFAULT_NODE_SPACING,
    sweepPasses: DEFAULT_SWEEP_PASSES
  }),
  summarizeConfig: (c) =>
    `q=${Number(c.quality ?? DEFAULT_QUALITY).toFixed(2)} ` +
    `stab=${Number(c.stability ?? DEFAULT_STABILITY).toFixed(2)} ` +
    `sweeps=${Number(c.sweepPasses ?? DEFAULT_SWEEP_PASSES)}`,
  createAlgorithm: () => new RadialSugiyamaLayoutAlgorithm()
};
