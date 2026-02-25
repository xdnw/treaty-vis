import { resolveNumberConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutConfigUtils";
import type { NetworkLayoutStrategyDefinition } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyDefinition";
import {
  NetworkLayoutAlgorithmBase,
  type NodeLayoutParams,
  type NodeLayoutResult,
  EPS,
  TAU,
  GOLDEN_ANGLE,
  clamp
} from "@/domain/timelapse/networkLayout/NetworkLayoutAlgorithmBase";

const DEFAULT_QUALITY = 1.0;
const DEFAULT_STABILITY = 0.8;
const DEFAULT_NODE_SPACING = 8;
const DEFAULT_GRAVITY = 0.8;
const DEFAULT_SCALING_RATIO = 2.0;

type QTCell = {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  totalMass: number;
  comX: number;
  comY: number;
  nodeIdx: number;
  nodeX: number;
  nodeY: number;
  nodeMass: number;
  nw: QTCell | null;
  ne: QTCell | null;
  sw: QTCell | null;
  se: QTCell | null;
};

function makeCell(x1: number, y1: number, x2: number, y2: number): QTCell {
  return {
    x1,
    y1,
    x2,
    y2,
    totalMass: 0,
    comX: 0,
    comY: 0,
    nodeIdx: -2,
    nodeX: 0,
    nodeY: 0,
    nodeMass: 0,
    nw: null,
    ne: null,
    sw: null,
    se: null
  };
}

function qtInsert(cell: QTCell, idx: number, x: number, y: number, mass: number): void {
  if (cell.nodeIdx === -2) {
    cell.nodeIdx = idx;
    cell.nodeX = x;
    cell.nodeY = y;
    cell.nodeMass = mass;
    cell.comX = x;
    cell.comY = y;
    cell.totalMass = mass;
    return;
  }

  const newMass = cell.totalMass + mass;
  cell.comX = (cell.comX * cell.totalMass + x * mass) / newMass;
  cell.comY = (cell.comY * cell.totalMass + y * mass) / newMass;
  cell.totalMass = newMass;

  if (cell.nodeIdx >= 0) {
    const ei = cell.nodeIdx;
    const ex = cell.nodeX;
    const ey = cell.nodeY;
    const em = cell.nodeMass;
    cell.nodeIdx = -1;
    qtInsertChild(cell, ei, ex, ey, em);
  }

  qtInsertChild(cell, idx, x, y, mass);
}

function qtInsertChild(cell: QTCell, idx: number, x: number, y: number, mass: number): void {
  const midX = (cell.x1 + cell.x2) * 0.5;
  const midY = (cell.y1 + cell.y2) * 0.5;
  const west = x < midX;
  const north = y < midY;
  if (west && north) {
    if (!cell.nw) {
      cell.nw = makeCell(cell.x1, cell.y1, midX, midY);
    }
    qtInsert(cell.nw, idx, x, y, mass);
  } else if (!west && north) {
    if (!cell.ne) {
      cell.ne = makeCell(midX, cell.y1, cell.x2, midY);
    }
    qtInsert(cell.ne, idx, x, y, mass);
  } else if (west && !north) {
    if (!cell.sw) {
      cell.sw = makeCell(cell.x1, midY, midX, cell.y2);
    }
    qtInsert(cell.sw, idx, x, y, mass);
  } else {
    if (!cell.se) {
      cell.se = makeCell(midX, midY, cell.x2, cell.y2);
    }
    qtInsert(cell.se, idx, x, y, mass);
  }
}

function qtRepulsion(
  cell: QTCell,
  i: number,
  xi: number,
  yi: number,
  massi: number,
  theta: number,
  scalingRatio: number,
  fx: Float64Array,
  fy: Float64Array
): void {
  if (cell.nodeIdx === -2 || cell.totalMass === 0) {
    return;
  }
  if (cell.nodeIdx === i) {
    return;
  }

  const dx = xi - cell.comX;
  const dy = yi - cell.comY;
  const d2 = dx * dx + dy * dy;
  const d = Math.sqrt(d2) || EPS;

  const cellWidth = cell.x2 - cell.x1;
  const nodeInCell = xi >= cell.x1 && xi < cell.x2 && yi >= cell.y1 && yi < cell.y2;

  if (!nodeInCell && (cell.nodeIdx >= 0 || cellWidth / d < theta)) {
    const f = (scalingRatio * massi * cell.totalMass) / d;
    fx[i] += (dx / d) * f;
    fy[i] += (dy / d) * f;
    return;
  }

  if (cell.nw) {
    qtRepulsion(cell.nw, i, xi, yi, massi, theta, scalingRatio, fx, fy);
  }
  if (cell.ne) {
    qtRepulsion(cell.ne, i, xi, yi, massi, theta, scalingRatio, fx, fy);
  }
  if (cell.sw) {
    qtRepulsion(cell.sw, i, xi, yi, massi, theta, scalingRatio, fx, fy);
  }
  if (cell.se) {
    qtRepulsion(cell.se, i, xi, yi, massi, theta, scalingRatio, fx, fy);
  }
}

function qtBuild(x: Float64Array, y: Float64Array, masses: Float64Array, n: number): QTCell | null {
  if (n === 0) {
    return null;
  }
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  for (let i = 0; i < n; i++) {
    if (x[i] < minX) {
      minX = x[i];
    }
    if (y[i] < minY) {
      minY = y[i];
    }
    if (x[i] > maxX) {
      maxX = x[i];
    }
    if (y[i] > maxY) {
      maxY = y[i];
    }
  }
  const pad = 1 + (maxX - minX + maxY - minY) * 0.01;
  const root = makeCell(minX - pad, minY - pad, maxX + pad, maxY + pad);
  for (let i = 0; i < n; i++) {
    qtInsert(root, i, x[i], y[i], masses[i]);
  }
  return root;
}

export class BHForceAtlas2LayoutAlgorithm extends NetworkLayoutAlgorithmBase {
  public readonly strategy = "bh-fa2" as const;

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

    const gravity = resolveNumberConfig(strategyConfig, "gravity", DEFAULT_GRAVITY, 0.1, 3.0);
    const scalingRatio = resolveNumberConfig(strategyConfig, "scalingRatio", DEFAULT_SCALING_RATIO, 0.5, 5.0);
    const theta = 1.2;

    const localIndex = new Map<string, number>();
    for (let i = 0; i < n; i++) {
      localIndex.set(nodeIds[i], i);
    }

    const prevAnchor = previousIndex.communityAnchorById.get(communityId);
    const anchorDx = prevAnchor ? anchorX - prevAnchor.x : 0;
    const anchorDy = prevAnchor ? anchorY - prevAnchor.y : 0;
    const containR = communityRadius * 1.05;
    const angleOffset = ((this.hashId(communityId) % 10000) / 10000) * TAU;

    for (let i = 0; i < n; i++) {
      const prevPos = previousIndex.nodePositions[nodeIds[i]];
      if (prevPos) {
        x[i] = prevPos.x + anchorDx;
        y[i] = prevPos.y + anchorDy;
        const ax = x[i] - anchorX;
        const ay = y[i] - anchorY;
        const d = Math.sqrt(ax * ax + ay * ay);
        if (d > containR) {
          x[i] = anchorX + (ax * containR) / d;
          y[i] = anchorY + (ay * containR) / d;
        }
      } else {
        const t = (i + 0.5) / Math.max(1, n);
        const rad = Math.sqrt(t) * Math.max(nodeSpacing * 3, communityRadius - nodeSpacing * 2);
        const hash = nodeHashById.get(nodeIds[i]) ?? 0;
        const ang = i * GOLDEN_ANGLE + angleOffset + (((hash % 1024) / 1024) - 0.5) * 0.05;
        x[i] = anchorX + Math.cos(ang) * rad;
        y[i] = anchorY + Math.sin(ang) * rad;
      }
    }

    const degrees = new Float64Array(n);
    for (let i = 0; i < n; i++) {
      const nbs = adjacencyByNodeId.get(nodeIds[i]);
      if (!nbs) {
        continue;
      }
      for (const nb of nbs) {
        if (localIndex.has(nb)) {
          degrees[i]++;
        }
      }
    }
    const masses = new Float64Array(n);
    for (let i = 0; i < n; i++) {
      masses[i] = degrees[i] + 1;
    }

    if (!prevAnchor || n > 20) {
      let newNodeCount = 0;
      for (let i = 0; i < n; i++) {
        if (!previousIndex.nodePositions[nodeIds[i]]) {
          newNodeCount++;
        }
      }
      if (newNodeCount > n * 0.4) {
        this.coarseInit(
          x,
          y,
          nodeIds,
          anchorX,
          anchorY,
          communityRadius,
          nodeSpacing,
          degrees,
          adjacencyByNodeId,
          angleOffset
        );
      }
    }

    const edgesA: number[] = [];
    const edgesB: number[] = [];
    for (let i = 0; i < n; i++) {
      const nbs = adjacencyByNodeId.get(nodeIds[i]);
      if (!nbs) {
        continue;
      }
      for (const nb of nbs) {
        const j = localIndex.get(nb);
        if (j != null && j > i) {
          edgesA.push(i);
          edgesB.push(j);
        }
      }
    }

    const iters = clamp(
      Math.round(30 * quality * (n <= 80 ? 1.4 : n <= 300 ? 1.1 : n <= 1200 ? 0.85 : 0.55)),
      6,
      60
    );
    const fx = new Float64Array(n);
    const fy = new Float64Array(n);
    const pfx = new Float64Array(n);
    const pfy = new Float64Array(n);
    const swing = new Float64Array(n);
    const speed = new Float64Array(n).fill(nodeSpacing * 0.8);
    const maxDisplace = communityRadius * 0.3;

    for (let iter = 0; iter < iters; iter++) {
      fx.fill(0);
      fy.fill(0);

      const t = iter / Math.max(1, iters - 1);
      const alpha = 1 - t * 0.7;

      const root = qtBuild(x, y, masses, n);
      if (root) {
        for (let i = 0; i < n; i++) {
          qtRepulsion(root, i, x[i], y[i], masses[i], theta, scalingRatio, fx, fy);
        }
      }

      for (let e = 0; e < edgesA.length; e++) {
        const i = edgesA[e];
        const j = edgesB[e];
        let dx = x[j] - x[i];
        let dy = y[j] - y[i];
        let d2 = dx * dx + dy * dy;
        if (d2 < EPS) {
          const a = ((i * 997 + j * 991 + iter * 37) % 360) * (TAU / 360);
          dx = Math.cos(a);
          dy = Math.sin(a);
          d2 = 1;
        }
        const d = Math.sqrt(d2);
        const fa = Math.log(1 + d) / scalingRatio;
        const ux = dx / d;
        const uy = dy / d;
        const wi = fa / (degrees[i] + 1);
        const wj = fa / (degrees[j] + 1);
        fx[i] += ux * wi * masses[i];
        fy[i] += uy * wi * masses[i];
        fx[j] -= ux * wj * masses[j];
        fy[j] -= uy * wj * masses[j];
      }

      for (let i = 0; i < n; i++) {
        const dx = anchorX - x[i];
        const dy = anchorY - y[i];
        const d = Math.sqrt(dx * dx + dy * dy) || EPS;
        const fg = gravity * masses[i] * d;
        fx[i] += (dx / d) * fg;
        fy[i] += (dy / d) * fg;
      }

      let globalSwing = 0;
      let globalTraction = 0;
      for (let i = 0; i < n; i++) {
        const sdx = fx[i] - pfx[i];
        const sdy = fy[i] - pfy[i];
        swing[i] = Math.sqrt(sdx * sdx + sdy * sdy);
        const trx = fx[i] + pfx[i];
        const trY = fy[i] + pfy[i];
        globalSwing += masses[i] * swing[i];
        globalTraction += masses[i] * Math.sqrt(trx * trx + trY * trY) * 0.5;
      }
      const speedRatio = globalSwing > EPS ? globalTraction / globalSwing : 1;
      const gs = clamp(0.1 * speedRatio * alpha, 0.005, nodeSpacing * 1.2);

      for (let i = 0; i < n; i++) {
        speed[i] = Math.min(
          clamp(speed[i] * (1 + 0.2 * (swing[i] < 0.1 ? 1 : -1)), 0.005, maxDisplace),
          gs / (1 + gs * swing[i] * 0.15)
        );

        const fm = Math.sqrt(fx[i] * fx[i] + fy[i] * fy[i]) || EPS;
        const cap = Math.min(speed[i], maxDisplace / fm);
        x[i] += (fx[i] / fm) * fm * cap;
        y[i] += (fy[i] / fm) * fm * cap;

        pfx[i] = fx[i];
        pfy[i] = fy[i];
      }

      this.clampToContainment(x, y, anchorX, anchorY, containR, n);
    }

    if (stability > 0) {
      for (let i = 0; i < n; i++) {
        const prevPos = previousIndex.nodePositions[nodeIds[i]];
        if (!prevPos) {
          continue;
        }
        const px = prevPos.x + anchorDx;
        const py = prevPos.y + anchorDy;
        x[i] = px + (x[i] - px) * (1 - stability);
        y[i] = py + (y[i] - py) * (1 - stability);
      }
      this.clampToContainment(x, y, anchorX, anchorY, containR, n);
    }

    return { nodeIds, x, y };
  }

  private coarseInit(
    x: Float64Array,
    y: Float64Array,
    nodeIds: string[],
    anchorX: number,
    anchorY: number,
    communityRadius: number,
    nodeSpacing: number,
    degrees: Float64Array,
    adjacencyByNodeId: Map<string, Set<string>>,
    angleOffset: number
  ): void {
    const n = nodeIds.length;
    if (n <= 3) {
      return;
    }

    const sorted = Array.from({ length: n }, (_, i) => i).sort((a, b) => degrees[b] - degrees[a]);
    const hubCut = Math.max(1, Math.round(n * 0.12));
    const midCut = Math.max(hubCut + 1, Math.round(n * 0.45));

    const r0 = nodeSpacing * 1.5;
    const r1 = Math.max(nodeSpacing * 3, communityRadius * 0.38);
    const r2 = Math.max(nodeSpacing * 5, communityRadius * 0.78);

    for (let k = 0; k < n; k++) {
      const i = sorted[k];
      let radius: number;
      if (k < hubCut) {
        radius = r0;
      } else if (k < midCut) {
        radius = r1;
      } else {
        radius = r2;
      }

      // Keep read of adjacency for parity with old draft's locality intent.
      void adjacencyByNodeId.get(nodeIds[i]);

      const ang = angleOffset + (k / n) * TAU + (k < hubCut ? 0 : i * GOLDEN_ANGLE);
      x[i] = anchorX + Math.cos(ang) * radius;
      y[i] = anchorY + Math.sin(ang) * radius;
    }
  }
}

export const BH_FA2_STRATEGY_DEFINITION: NetworkLayoutStrategyDefinition = {
  strategy: "bh-fa2",
  label: "Barnes-Hut FA2",
  fields: [
    { key: "quality", label: "Quality", min: 0.5, max: 1.5, step: 0.05 },
    { key: "stability", label: "Stability", min: 0, max: 0.95, step: 0.05 },
    { key: "nodeSpacing", label: "Node Spacing", min: 4, max: 20, step: 1 },
    { key: "gravity", label: "Gravity", min: 0.1, max: 3.0, step: 0.1 },
    { key: "scalingRatio", label: "Scaling Ratio", min: 0.5, max: 5.0, step: 0.1 }
  ],
  createInitialConfig: () => ({
    quality: DEFAULT_QUALITY,
    stability: DEFAULT_STABILITY,
    nodeSpacing: DEFAULT_NODE_SPACING,
    gravity: DEFAULT_GRAVITY,
    scalingRatio: DEFAULT_SCALING_RATIO
  }),
  summarizeConfig: (c) =>
    `q=${Number(c.quality ?? DEFAULT_QUALITY).toFixed(2)} ` +
    `stab=${Number(c.stability ?? DEFAULT_STABILITY).toFixed(2)} ` +
    `g=${Number(c.gravity ?? DEFAULT_GRAVITY).toFixed(1)} ` +
    `k=${Number(c.scalingRatio ?? DEFAULT_SCALING_RATIO).toFixed(1)}`,
  createAlgorithm: () => new BHForceAtlas2LayoutAlgorithm()
};
