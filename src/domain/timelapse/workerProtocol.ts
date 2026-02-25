import type { NetworkLayoutStrategy, NetworkLayoutStrategyConfig } from "@/domain/timelapse/networkLayout/NetworkLayoutTypes";

export type WorkerEvidenceMode = "all" | "one-confirmed" | "both-confirmed";
export type WorkerSortField = "timestamp" | "action" | "type" | "from" | "to" | "source";
export type WorkerSortDirection = "asc" | "desc";

export type WorkerPulsePoint = {
  day: string;
  signed: number;
  terminal: number;
  inferred: number;
};

export type WorkerComponentTarget = {
  componentId: string;
  nodeIds: string[];
  anchorX: number;
  anchorY: number;
};

export type WorkerCommunityTarget = {
  communityId: string;
  componentId: string;
  nodeIds: string[];
  anchorX: number;
  anchorY: number;
};

export type WorkerNodeTarget = {
  nodeId: string;
  componentId: string;
  communityId: string;
  targetX: number;
  targetY: number;
  neighborX: number;
  neighborY: number;
  anchorX: number;
  anchorY: number;
};

export type WorkerNetworkLayout = {
  components: WorkerComponentTarget[];
  communities: WorkerCommunityTarget[];
  nodeTargets: WorkerNodeTarget[];
};

export type WorkerQueryState = {
  time: {
    start: string | null;
    end: string | null;
  };
  playback: {
    playhead: string | null;
  };
  focus: {
    allianceId: number | null;
    edgeKey: string | null;
    eventId: string | null;
  };
  filters: {
    alliances: number[];
    treatyTypes: string[];
    actions: string[];
    sources: string[];
    showFlags: boolean;
    includeInferred: boolean;
    includeNoise: boolean;
    evidenceMode: WorkerEvidenceMode;
    topXByScore: number | null;
    sizeByScore: boolean;
  };
  textQuery: string;
  sort: {
    field: WorkerSortField;
    direction: WorkerSortDirection;
  };
};

type LoaderWorkerPayload = {
  eventsRaw: unknown[];
  indicesRaw: {
    byAction: Record<string, number[]>;
    byType: Record<string, number[]>;
    bySource: Record<string, number[]>;
    byAlliance: Record<string, number[]>;
    eventIdToIndex: Record<string, number>;
    allActions: string[];
    allTypes: string[];
    allSources: string[];
    alliances: Array<{ id: number; name: string; count: number }>;
    minTimestamp: string | null;
    maxTimestamp: string | null;
  };
  summaryRaw: unknown;
  flagsRaw: unknown[];
  flagAssetsRaw: unknown | null;
  allianceFlagsRaw: unknown | null;
  scoreRanksRaw: unknown | null;
  manifestRaw: unknown | null;
};

export type LoaderWorkerRequest =
  | {
      kind: "load";
      includeFlags: boolean;
    }
  | {
      kind: "select";
      requestId: number;
      query: WorkerQueryState;
    }
  | {
      kind: "network";
      requestId: number;
      query: WorkerQueryState;
      playhead: string | null;
      maxEdges: number;
      strategy: NetworkLayoutStrategy;
      strategyConfig?: NetworkLayoutStrategyConfig;
    }
  | {
      kind: "pulse";
      requestId: number;
      query: WorkerQueryState;
      maxPoints: number;
      playhead: string | null;
    };

export type LoaderWorkerResponse =
  | {
      kind: "load";
      ok: true;
      payload: LoaderWorkerPayload;
    }
  | {
      kind: "load";
      ok: false;
      error: string;
    }
  | {
      kind: "select";
      ok: true;
      requestId: number;
      indexesBuffer: ArrayBuffer;
      length: number;
    }
  | {
      kind: "select";
      ok: false;
      requestId: number;
      error: string;
    }
  | {
      kind: "pulse";
      ok: true;
      requestId: number;
      days: string[];
      signedBuffer: ArrayBuffer;
      terminalBuffer: ArrayBuffer;
      inferredBuffer: ArrayBuffer;
      length: number;
    }
  | {
      kind: "pulse";
      ok: false;
      requestId: number;
      error: string;
    }
  | {
      kind: "network";
      ok: true;
      requestId: number;
      startedAt: number;
      finishedAt: number;
      edgeIndexesBuffer: ArrayBuffer;
      length: number;
      layout: WorkerNetworkLayout;
    }
  | {
      kind: "network";
      ok: false;
      requestId: number;
      error: string;
    };
