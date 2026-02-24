export type Point = { x: number; y: number };

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
