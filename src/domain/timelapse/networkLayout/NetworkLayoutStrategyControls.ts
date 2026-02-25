import type { NetworkLayoutStrategy, NetworkLayoutStrategyConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";

export type NetworkLayoutStrategyField = {
  key: string;
  label: string;
  min: number;
  max: number;
  step: number;
};

type StrategyControlDefinition = {
  strategy: NetworkLayoutStrategy;
  label: string;
  fields: NetworkLayoutStrategyField[];
  createInitialConfig: () => NetworkLayoutStrategyConfig;
  summarizeConfig: (config: NetworkLayoutStrategyConfig) => string;
};

export const DEFAULT_NETWORK_LAYOUT_STRATEGY: NetworkLayoutStrategy = "fa2line";

const STRATEGY_CONTROL_DEFINITIONS: StrategyControlDefinition[] = [
  {
    strategy: "hybrid-backbone",
    label: "Hybrid Backbone",
    fields: [
      { key: "refinementIterations", label: "Refine Iterations", min: 2, max: 20, step: 1 },
      { key: "communityPlacementScale", label: "Community Spread", min: 0.4, max: 2.4, step: 0.05 }
    ],
    createInitialConfig: () => ({
      refinementIterations: 6,
      communityPlacementScale: 1
    }),
    summarizeConfig: (config) => {
      const iterations = Number(config.refinementIterations ?? 6);
      const spread = Number(config.communityPlacementScale ?? 1);
      return `iter=${iterations} spread=${spread.toFixed(2)}`;
    }
  },
  {
    strategy: "fa2line",
    label: "FA2 Line",
    fields: [
      { key: "iterations", label: "Iterations", min: 12, max: 180, step: 1 },
      { key: "attractionStrength", label: "Attraction", min: 0.01, max: 0.4, step: 0.01 },
      { key: "repulsionStrength", label: "Repulsion", min: 2, max: 60, step: 1 },
      { key: "gravityStrength", label: "Gravity", min: 0.001, max: 0.2, step: 0.001 }
    ],
    createInitialConfig: () => ({
      iterations: 40,
      attractionStrength: 0.08,
      repulsionStrength: 14,
      gravityStrength: 0.02
    }),
    summarizeConfig: (config) => {
      const iterations = Number(config.iterations ?? 40);
      const gravity = Number(config.gravityStrength ?? 0.02);
      return `iter=${iterations} gravity=${gravity.toFixed(3)}`;
    }
  }
];

const definitionByStrategy = new Map<NetworkLayoutStrategy, StrategyControlDefinition>(
  STRATEGY_CONTROL_DEFINITIONS.map((definition) => [definition.strategy, definition])
);

export const NETWORK_LAYOUT_STRATEGY_OPTIONS = STRATEGY_CONTROL_DEFINITIONS.map((definition) => ({
  value: definition.strategy,
  label: definition.label
}));

export function createInitialStrategyConfig(strategy: NetworkLayoutStrategy): NetworkLayoutStrategyConfig {
  const definition = definitionByStrategy.get(strategy);
  if (!definition) {
    throw new Error(`[network-layout] Missing strategy controls for: ${strategy}`);
  }
  return definition.createInitialConfig();
}

export function summarizeStrategyConfig(
  strategy: NetworkLayoutStrategy,
  config: NetworkLayoutStrategyConfig | undefined
): string {
  const definition = definitionByStrategy.get(strategy);
  if (!definition) {
    throw new Error(`[network-layout] Missing strategy controls for: ${strategy}`);
  }
  return definition.summarizeConfig(config ?? definition.createInitialConfig());
}

export function getStrategyFields(strategy: NetworkLayoutStrategy): NetworkLayoutStrategyField[] {
  const definition = definitionByStrategy.get(strategy);
  if (!definition) {
    throw new Error(`[network-layout] Missing strategy controls for: ${strategy}`);
  }
  return definition.fields;
}

export function getStrategyLabel(strategy: NetworkLayoutStrategy): string {
  const definition = definitionByStrategy.get(strategy);
  if (!definition) {
    throw new Error(`[network-layout] Missing strategy controls for: ${strategy}`);
  }
  return definition.label;
}
