import { FA2LineLayoutAlgorithm } from "@/domain/timelapse/networkLayout/FA2LineLayoutAlgorithm";
import { HybridBackboneLayoutAlgorithm } from "@/domain/timelapse/networkLayout/HybridBackboneLayoutAlgorithm";
import type { INetworkLayoutAlgorithm } from "@/domain/timelapse/networkLayout/INetworkLayoutAlgorithm";
import type { NetworkLayoutInput, NetworkLayoutOutput, NetworkLayoutStrategy } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";

const algorithms: INetworkLayoutAlgorithm[] = [
  new HybridBackboneLayoutAlgorithm(),
  new FA2LineLayoutAlgorithm()
];

const algorithmByStrategy = new Map<NetworkLayoutStrategy, INetworkLayoutAlgorithm>(
  algorithms.map((algorithm) => [algorithm.strategy, algorithm])
);

export function runNetworkLayoutStrategy(strategy: NetworkLayoutStrategy, input: NetworkLayoutInput): NetworkLayoutOutput {
  const algorithm = algorithmByStrategy.get(strategy);
  if (!algorithm) {
    throw new Error(`[network-layout] Unknown strategy: ${strategy}`);
  }
  return algorithm.run(input);
}
