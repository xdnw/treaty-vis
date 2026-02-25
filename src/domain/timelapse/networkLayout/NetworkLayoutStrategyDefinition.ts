import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import type { NetworkLayoutStrategy, NetworkLayoutStrategyConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";

export type NetworkLayoutStrategyField = {
  key: string;
  label: string;
  min: number;
  max: number;
  step: number;
};

export type NetworkLayoutStrategyDefinition = {
  strategy: NetworkLayoutStrategy;
  label: string;
  fields: NetworkLayoutStrategyField[];
  createInitialConfig: () => NetworkLayoutStrategyConfig;
  summarizeConfig: (config: NetworkLayoutStrategyConfig) => string;
  createAlgorithm: () => INetworkLayoutAlgorithm;
};
