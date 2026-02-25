import type { NetworkLayoutInput, NetworkLayoutOutput, NetworkLayoutStrategy } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";

export interface INetworkLayoutAlgorithm {
  readonly strategy: NetworkLayoutStrategy;
  run(input: NetworkLayoutInput): NetworkLayoutOutput;
}
