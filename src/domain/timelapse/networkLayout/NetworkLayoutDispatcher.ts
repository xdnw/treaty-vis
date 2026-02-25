import { createNetworkLayoutAlgorithmByStrategy } from "@/domain/timelapse/networkLayout/NetworkLayoutStrategyRegistry";
import type { NetworkLayoutInput, NetworkLayoutOutput, NetworkLayoutStrategy } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";

const algorithmByStrategy = createNetworkLayoutAlgorithmByStrategy();

export function runNetworkLayoutStrategy(strategy: NetworkLayoutStrategy, input: NetworkLayoutInput): NetworkLayoutOutput {
  const algorithm = algorithmByStrategy.get(strategy);
  if (!algorithm) {
    throw new Error(`[network-layout] Unknown strategy: ${strategy}`);
  }
  return algorithm.run(input);
}
