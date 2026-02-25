import { FA2_LINE_STRATEGY_DEFINITION } from "@/domain/timelapse/networkLayout/FA2LineLayoutAlgorithm";
import { BH_FA2_STRATEGY_DEFINITION } from "@/domain/timelapse/networkLayout/BHForceAtlas2LayoutAlgorithm";
import { HYBRID_BACKBONE_STRATEGY_DEFINITION } from "@/domain/timelapse/networkLayout/HybridBackboneLayoutAlgorithm";
import type {
  NetworkLayoutStrategyDefinition,
  NetworkLayoutStrategyField
} from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyDefinition";
import { RADIAL_SUGIYAMA_STRATEGY_DEFINITION } from "@/domain/timelapse/networkLayout/RadialSugiyamaLayoutAlgorithm";
import { STRESS_MAJORIZATION_STRATEGY_DEFINITION } from "@/domain/timelapse/networkLayout/StressMajorizationLayoutAlgorithm";
import { STRICT_TEMPORAL_STRATEGY_DEFINITION } from "@/domain/timelapse/networkLayout/StrictTemporal";
import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import type { NetworkLayoutStrategy, NetworkLayoutStrategyConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";
import { BALANCED_TEMPORAL_STRATEGY_DEFINITION } from "./BalancedTemporal";

export const NETWORK_LAYOUT_STRATEGY_DEFINITIONS: NetworkLayoutStrategyDefinition[] = [
  HYBRID_BACKBONE_STRATEGY_DEFINITION,
  BALANCED_TEMPORAL_STRATEGY_DEFINITION,
  STRICT_TEMPORAL_STRATEGY_DEFINITION,
  FA2_LINE_STRATEGY_DEFINITION,
  BH_FA2_STRATEGY_DEFINITION,
  STRESS_MAJORIZATION_STRATEGY_DEFINITION,
  RADIAL_SUGIYAMA_STRATEGY_DEFINITION
];

export const DEFAULT_NETWORK_LAYOUT_STRATEGY: NetworkLayoutStrategy = FA2_LINE_STRATEGY_DEFINITION.strategy;

const definitionByStrategy = new Map<NetworkLayoutStrategy, NetworkLayoutStrategyDefinition>(
  NETWORK_LAYOUT_STRATEGY_DEFINITIONS.map((definition) => [definition.strategy, definition])
);

export const NETWORK_LAYOUT_STRATEGY_OPTIONS = NETWORK_LAYOUT_STRATEGY_DEFINITIONS.map((definition) => ({
  value: definition.strategy,
  label: definition.label
}));

export function getNetworkLayoutStrategyDefinition(strategy: NetworkLayoutStrategy): NetworkLayoutStrategyDefinition {
  const definition = definitionByStrategy.get(strategy);
  if (!definition) {
    throw new Error(`[network-layout] Missing strategy definition for: ${strategy}`);
  }
  return definition;
}

export function createInitialStrategyConfig(strategy: NetworkLayoutStrategy): NetworkLayoutStrategyConfig {
  return getNetworkLayoutStrategyDefinition(strategy).createInitialConfig();
}

export function summarizeStrategyConfig(
  strategy: NetworkLayoutStrategy,
  config: NetworkLayoutStrategyConfig | undefined
): string {
  const definition = getNetworkLayoutStrategyDefinition(strategy);
  return definition.summarizeConfig(config ?? definition.createInitialConfig());
}

export function getStrategyFields(strategy: NetworkLayoutStrategy): NetworkLayoutStrategyField[] {
  return getNetworkLayoutStrategyDefinition(strategy).fields;
}

export function getStrategyLabel(strategy: NetworkLayoutStrategy): string {
  return getNetworkLayoutStrategyDefinition(strategy).label;
}

export function createNetworkLayoutAlgorithmByStrategy(): Map<NetworkLayoutStrategy, INetworkLayoutAlgorithm> {
  return new Map<NetworkLayoutStrategy, INetworkLayoutAlgorithm>(
    NETWORK_LAYOUT_STRATEGY_DEFINITIONS.map((definition) => {
      const algorithm = definition.createAlgorithm();
      return [definition.strategy, algorithm];
    })
  );
}
