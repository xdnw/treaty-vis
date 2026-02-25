import type { FlagPressureLevel, FlagRenderMode } from "@/features/network/flagRender";

export type NetworkFlagDiagnosticSample = {
  stage: "graph-build" | "refresh";
  ts: number;
  cameraRatio: number;
  nodeCount: number;
  framePressureScore: number;
  framePressure: boolean;
  framePressureLevel: FlagPressureLevel;
  mode: FlagRenderMode;
  spriteCandidateCount: number;
  refreshMs?: number;
  graphBuildMs?: number;
};

export function pushNetworkFlagDiagnostic(sample: NetworkFlagDiagnosticSample): void {
  if (!import.meta.env.DEV || typeof window === "undefined") {
    return;
  }

  const perfWindow = window as Window & {
    __timelapsePerf?: { enabled?: boolean };
    __networkFlagDiagnostics?: NetworkFlagDiagnosticSample[];
  };

  if (!perfWindow.__timelapsePerf?.enabled) {
    return;
  }

  const entries = perfWindow.__networkFlagDiagnostics ?? [];
  entries.push(sample);
  if (entries.length > 240) {
    entries.splice(0, entries.length - 240);
  }
  perfWindow.__networkFlagDiagnostics = entries;
}
