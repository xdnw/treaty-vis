export type Point = { x: number; y: number };
export type LayoutStrategy = "hash" | "topology-biased";

type LayoutTargetInput = {
  nodeIds: string[];
  adjacencyByNodeId: Map<string, Set<string>>;
  strategy: LayoutStrategy;
  topologyStrength: number;
  topologyMaxOffset: number;
};

const TAU = Math.PI * 2;

export function hashId(id: string): number {
  let hash = 2166136261;
  for (let index = 0; index < id.length; index += 1) {
    hash ^= id.charCodeAt(index);
    hash +=
      (hash << 1) +
      (hash << 4) +
      (hash << 7) +
      (hash << 8) +
      (hash << 24);
  }
  return hash >>> 0;
}

export function positionForNode(id: string): Point {
  const primary = hashId(id);
  const secondary = hashId(`${id}:orbit`);

  const angle = (primary / 0xffffffff) * TAU;
  const radius = 16 + Math.sqrt((secondary % 4096) + 1) * 2.4;

  return {
    x: Math.cos(angle) * radius,
    y: Math.sin(angle) * radius
  };
}

export function dampPosition(previous: Point, target: Point, damping: number): Point {
  const clampedDamping = Math.max(0, Math.min(1, damping));
  return {
    x: previous.x + (target.x - previous.x) * clampedDamping,
    y: previous.y + (target.y - previous.y) * clampedDamping
  };
}

export function distanceBetween(left: Point, right: Point): number {
  const dx = right.x - left.x;
  const dy = right.y - left.y;
  return Math.hypot(dx, dy);
}

export function clampDisplacement(origin: Point, next: Point, maxDistance: number): Point {
  const cap = Math.max(0, maxDistance);
  if (cap <= 0) {
    return { ...origin };
  }

  const dx = next.x - origin.x;
  const dy = next.y - origin.y;
  const distance = Math.hypot(dx, dy);
  if (distance <= cap || distance === 0) {
    return next;
  }

  const scale = cap / distance;
  return {
    x: origin.x + dx * scale,
    y: origin.y + dy * scale
  };
}

export function resolveLayoutTargets(params: LayoutTargetInput): Map<string, Point> {
  const { nodeIds, adjacencyByNodeId, strategy, topologyStrength, topologyMaxOffset } = params;
  const baseByNodeId = new Map<string, Point>();
  for (const nodeId of nodeIds) {
    baseByNodeId.set(nodeId, positionForNode(nodeId));
  }

  if (strategy === "hash") {
    return baseByNodeId;
  }

  const targets = new Map<string, Point>();
  const influence = Math.max(0, Math.min(1, topologyStrength));
  const maxOffset = Math.max(0, topologyMaxOffset);

  for (const nodeId of nodeIds) {
    const base = baseByNodeId.get(nodeId)!;
    const neighbors = adjacencyByNodeId.get(nodeId);

    if (!neighbors || neighbors.size === 0 || influence <= 0) {
      targets.set(nodeId, base);
      continue;
    }

    let sumX = 0;
    let sumY = 0;
    let count = 0;

    for (const neighborId of neighbors) {
      const neighborBase = baseByNodeId.get(neighborId);
      if (!neighborBase) {
        continue;
      }
      sumX += neighborBase.x;
      sumY += neighborBase.y;
      count += 1;
    }

    if (count === 0) {
      targets.set(nodeId, base);
      continue;
    }

    const centroid = { x: sumX / count, y: sumY / count };
    const biased = {
      x: base.x + (centroid.x - base.x) * influence,
      y: base.y + (centroid.y - base.y) * influence
    };
    targets.set(nodeId, clampDisplacement(base, biased, maxOffset));
  }

  return targets;
}
