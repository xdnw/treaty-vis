import { NODE_MAX_RADIUS_DEFAULT } from "@/features/filters/filterStore";

const MIN_NODE_RADIUS = 5;
export const DEFAULT_MAX_NODE_RADIUS = NODE_MAX_RADIUS_DEFAULT;

export function clampRadius(value: number, maxNodeRadius: number): number {
  return Math.max(MIN_NODE_RADIUS, Math.min(maxNodeRadius, value));
}

export function colorWithOpacity(hexColor: string, opacity: number): string {
  const boundedOpacity = Math.max(0, Math.min(1, opacity));
  const raw = hexColor.trim();
  const sixDigitHex = raw.length === 4 ? `#${raw[1]}${raw[1]}${raw[2]}${raw[2]}${raw[3]}${raw[3]}` : raw;
  const match = /^#([0-9a-fA-F]{6})$/.exec(sixDigitHex);
  if (!match) {
    return hexColor;
  }
  const hex = match[1];
  const red = parseInt(hex.slice(0, 2), 16);
  const green = parseInt(hex.slice(2, 4), 16);
  const blue = parseInt(hex.slice(4, 6), 16);
  return `rgba(${red}, ${green}, ${blue}, ${boundedOpacity})`;
}

export function degreeRadius(degree: number, maxNodeRadius: number): number {
  return clampRadius(3 + Math.log2(degree + 1) * 1.2, maxNodeRadius);
}

export function applyScoreContrast(normalizedScore: number, contrast: number): number {
  const clampedScore = Math.max(0, Math.min(1, normalizedScore));
  if (!Number.isFinite(contrast) || contrast <= 0 || Math.abs(contrast - 1) <= Number.EPSILON) {
    return clampedScore;
  }
  return Math.pow(clampedScore, contrast);
}

export function scoreRadiusWithContrast(
  score: number,
  minScore: number,
  maxScore: number,
  contrast: number,
  maxNodeRadius: number = DEFAULT_MAX_NODE_RADIUS
): number {
  if (score <= 0 || minScore <= 0 || maxScore <= 0) {
    return MIN_NODE_RADIUS;
  }

  if (maxScore - minScore <= Number.EPSILON) {
    return maxNodeRadius;
  }

  const safeScore = Math.max(minScore, Math.min(maxScore, score));
  const range = maxScore - minScore;
  if (!Number.isFinite(range) || range <= 0) {
    return maxNodeRadius;
  }

  const normalized = (safeScore - minScore) / range;
  const contrasted = applyScoreContrast(normalized, contrast);
  return clampRadius(MIN_NODE_RADIUS + contrasted * (maxNodeRadius - MIN_NODE_RADIUS), maxNodeRadius);
}

export function resolveAllianceScoreWithFallback(
  allianceId: string,
  scoreByDay: Record<string, Record<string, number>>,
  scoreDays: string[],
  startDay: string | null
): number | null {
  if (!startDay) {
    return null;
  }

  const startIndex = scoreDays.indexOf(startDay);
  if (startIndex < 0) {
    return null;
  }

  for (let index = startIndex; index >= 0; index -= 1) {
    const day = scoreDays[index];
    const row = scoreByDay[day];
    if (!row) {
      continue;
    }
    const score = row[allianceId];
    if (typeof score === "number" && Number.isFinite(score) && score > 0) {
      return score;
    }
  }

  return null;
}
