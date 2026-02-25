import type { NetworkLayoutStrategyConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";

export function resolveNumberConfig(
  config: NetworkLayoutStrategyConfig | undefined,
  key: string,
  fallback: number,
  min: number,
  max: number
): number {
  const raw = Number(config?.[key]);
  if (!Number.isFinite(raw)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, raw));
}
