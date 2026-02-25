import type { WorkerNetworkLayout } from "@/domain/timelapse/workerProtocol";

export type NetworkLayoutStrategy = string;

export type NetworkLayoutStrategyConfig = Record<string, unknown>;

export type NetworkLayoutInput = {
  nodeIds: string[];
  adjacencyByNodeId: Map<string, Set<string>>;
  temporalKey: string;
  previousState?: unknown;
  strategyConfig?: NetworkLayoutStrategyConfig;
};

export type NetworkLayoutOutput = {
  layout: WorkerNetworkLayout;
  metadata?: {
    state?: unknown;
  };
};
