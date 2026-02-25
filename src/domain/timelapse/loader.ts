import {
  allianceFlagsPayloadSchema,
  allianceScoreRanksDailySchema,
  flagAssetsPayloadSchema,
  flagSchema,
  manifestSchema,
  summarySchema,
  timelapseEventSchema,
  type AllianceFlagSnapshot,
  type AllianceFlagTimelineByAlliance,
  type AllianceFlagsPayload,
  type AllianceScoreRanksByDay,
  type FlagAssetsPayload,
  type TimelapseEvent,
  type TimelapseFlag,
  type TimelapseManifest,
  type TimelapseSummary
} from "@/domain/timelapse/schema";
import { buildTimelapseIndices, type TimelapseIndices } from "@/domain/timelapse/selectors";
import type { QueryState } from "@/features/filters/filterStore";
import type {
  LoaderWorkerRequest,
  LoaderWorkerResponse,
  WorkerPulsePoint,
  WorkerQueryState
} from "@/domain/timelapse/workerProtocol";
import { decode } from "@msgpack/msgpack";

type WorkerIndicesPayload = Omit<TimelapseIndices, "allEventIndexes">;

export type TimelapseDataBundle = {
  manifest: TimelapseManifest | null;
  events: TimelapseEvent[];
  indices: TimelapseIndices;
  summary: TimelapseSummary;
  flags: TimelapseFlag[];
  allianceFlagsPayload: AllianceFlagsPayload | null;
  flagAssetsPayload: FlagAssetsPayload | null;
  allianceFlagTimelines: AllianceFlagTimelineByAlliance;
  allianceScoreRanksByDay: AllianceScoreRanksByDay | null;
};

type RawTimelapsePayload = {
  eventsRaw: unknown[];
  indicesRaw: WorkerIndicesPayload | null;
  summaryRaw: unknown;
  flagsRaw: unknown[];
  allianceFlagsRaw: unknown | null;
  flagAssetsRaw: unknown | null;
  scoreRanksRaw: unknown | null;
  manifestRaw: unknown | null;
};

export type TimelapseLoadOptions = {
  showFlags?: boolean;
};

type PendingSelectRequest = {
  resolve: (value: Uint32Array) => void;
  reject: (error: Error) => void;
};

type PendingPulseRequest = {
  resolve: (value: WorkerPulsePoint[]) => void;
  reject: (error: Error) => void;
};

type PendingNetworkRequest = {
  resolve: (value: Uint32Array) => void;
  reject: (error: Error) => void;
};

type TimelapseLoadMode = "flags-on" | "flags-off";

const bundlePromiseByMode = new Map<TimelapseLoadMode, Promise<TimelapseDataBundle>>();
let workerBaseLoadPromise: Promise<RawTimelapsePayload> | null = null;
let workerFlagsLoadPromise: Promise<RawTimelapsePayload> | null = null;
let workerLoadError: Error | null = null;
let selectionRequestId = 0;
let pulseRequestId = 0;
let networkRequestId = 0;
let workerInstance: Worker | null = null;
let optionalFlagManifestWarningShown = false;
const pendingSelectionRequests = new Map<number, PendingSelectRequest>();
const pendingPulseRequests = new Map<number, PendingPulseRequest>();
const pendingNetworkRequests = new Map<number, PendingNetworkRequest>();

const SAMPLE_VALIDATION_SIZE = 250;
const CACHE_DB_NAME = "discmcp_timelapse_cache";
const CACHE_STORE_NAME = "bundles";
const CACHE_VERSION = 1;

function modeForShowFlags(showFlags: boolean): TimelapseLoadMode {
  return showFlags ? "flags-on" : "flags-off";
}

function validateEventsStrict(eventsRaw: unknown[]): TimelapseEvent[] {
  return eventsRaw.map((item, index) => {
    const parsed = timelapseEventSchema.safeParse(item);
    if (!parsed.success) {
      throw new Error(`Invalid event at index ${index}: ${parsed.error.message}`);
    }
    return parsed.data;
  });
}

function validateEventsSampled(eventsRaw: unknown[]): TimelapseEvent[] {
  const sampleSize = Math.min(SAMPLE_VALIDATION_SIZE, eventsRaw.length);
  for (let index = 0; index < sampleSize; index += 1) {
    const parsed = timelapseEventSchema.safeParse(eventsRaw[index]);
    if (!parsed.success) {
      throw new Error(`Invalid event at index ${index}: ${parsed.error.message}`);
    }
  }
  return eventsRaw as TimelapseEvent[];
}

function sortEventsByTimestamp(events: TimelapseEvent[]): TimelapseEvent[] {
  return [...events].sort((left, right) => {
    const timestampOrder = left.timestamp.localeCompare(right.timestamp);
    if (timestampOrder !== 0) {
      return timestampOrder;
    }
    if (left.event_sequence !== right.event_sequence) {
      return left.event_sequence - right.event_sequence;
    }
    return left.event_id.localeCompare(right.event_id);
  });
}

function hydrateIndices(events: TimelapseEvent[], indicesRaw: WorkerIndicesPayload | null): TimelapseIndices {
  if (!indicesRaw) {
    return buildTimelapseIndices(events);
  }

  const allEventIndexes = new Array(events.length);
  for (let index = 0; index < events.length; index += 1) {
    allEventIndexes[index] = index;
  }

  return {
    ...indicesRaw,
    allEventIndexes
  };
}

function normalizeAllianceFlagTimelines(payload: AllianceFlagsPayload | null): AllianceFlagTimelineByAlliance {
  if (!payload) {
    return {};
  }

  const byAlliance = new Map<string, AllianceFlagTimelineByAlliance[string]>();
  for (const event of payload.events) {
    const allianceId = String(event.alliance_id);
    const list = byAlliance.get(allianceId) ?? [];
    list.push({
      timestamp: event.timestamp,
      day: event.timestamp.slice(0, 10),
      allianceName: event.alliance_name,
      action: event.action,
      flagKey: event.flag_key,
      sourceRef: event.source_ref
    });
    byAlliance.set(allianceId, list);
  }

  const normalized: AllianceFlagTimelineByAlliance = {};
  for (const [allianceId, entries] of byAlliance.entries()) {
    entries.sort((left, right) => {
      const timestampOrder = left.timestamp.localeCompare(right.timestamp);
      if (timestampOrder !== 0) {
        return timestampOrder;
      }
      return left.flagKey.localeCompare(right.flagKey);
    });
    normalized[allianceId] = entries;
  }
  return normalized;
}

export function resolveAllianceFlagSnapshot(
  timelines: AllianceFlagTimelineByAlliance,
  allianceId: number,
  playhead: string | null
): AllianceFlagSnapshot | null {
  const timeline = timelines[String(allianceId)];
  if (!timeline || timeline.length === 0) {
    return null;
  }

  if (!playhead) {
    const latest = timeline[timeline.length - 1];
    return {
      flagKey: latest.flagKey,
      action: latest.action,
      timestamp: latest.timestamp,
      day: latest.day,
      allianceName: latest.allianceName
    };
  }

  let lo = 0;
  let hi = timeline.length - 1;
  let best = -1;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (timeline[mid].timestamp <= playhead) {
      best = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }

  if (best < 0) {
    const earliestFuture = timeline[0];
    return {
      flagKey: earliestFuture.flagKey,
      action: earliestFuture.action,
      timestamp: earliestFuture.timestamp,
      day: earliestFuture.day,
      allianceName: earliestFuture.allianceName
    };
  }

  const match = timeline[best];
  return {
    flagKey: match.flagKey,
    action: match.action,
    timestamp: match.timestamp,
    day: match.day,
    allianceName: match.allianceName
  };
}

async function fetchMsgpack<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }
  const body = await response.arrayBuffer();
  return decode(new Uint8Array(body)) as T;
}

function createWorker(): Worker {
  if (workerInstance) {
    return workerInstance;
  }

  const worker = new Worker(new URL("./loader.worker.ts", import.meta.url), { type: "module" });
  worker.onmessage = (event: MessageEvent<LoaderWorkerResponse>) => {
    const response = event.data;
    if (response.kind === "select") {
      const pending = pendingSelectionRequests.get(response.requestId);
      if (!pending) {
        return;
      }
      pendingSelectionRequests.delete(response.requestId);

      if (!response.ok) {
        pending.reject(new Error(response.error));
        return;
      }

      pending.resolve(new Uint32Array(response.indexesBuffer, 0, response.length));
      return;
    }

    if (response.kind === "network") {
      const pending = pendingNetworkRequests.get(response.requestId);
      if (!pending) {
        return;
      }
      pendingNetworkRequests.delete(response.requestId);

      if (!response.ok) {
        pending.reject(new Error(response.error));
        return;
      }

      pending.resolve(new Uint32Array(response.edgeIndexesBuffer, 0, response.length));
      return;
    }

    if (response.kind !== "pulse") {
      return;
    }

    const pending = pendingPulseRequests.get(response.requestId);
    if (!pending) {
      return;
    }
    pendingPulseRequests.delete(response.requestId);

    if (!response.ok) {
      pending.reject(new Error(response.error));
      return;
    }

    const signed = new Uint32Array(response.signedBuffer, 0, response.length);
    const terminal = new Uint32Array(response.terminalBuffer, 0, response.length);
    const inferred = new Uint32Array(response.inferredBuffer, 0, response.length);
    const pulse: WorkerPulsePoint[] = [];
    for (let index = 0; index < response.length; index += 1) {
      pulse.push({
        day: response.days[index],
        signed: signed[index],
        terminal: terminal[index],
        inferred: inferred[index]
      });
    }

    pending.resolve(pulse);
  };

  worker.onerror = () => {
    for (const pending of pendingSelectionRequests.values()) {
      pending.reject(new Error("Loader worker failed"));
    }
    pendingSelectionRequests.clear();
    for (const pending of pendingPulseRequests.values()) {
      pending.reject(new Error("Loader worker failed"));
    }
    pendingPulseRequests.clear();
    for (const pending of pendingNetworkRequests.values()) {
      pending.reject(new Error("Loader worker failed"));
    }
    pendingNetworkRequests.clear();
    workerInstance?.terminate();
    workerInstance = null;
    workerBaseLoadPromise = null;
    workerFlagsLoadPromise = null;
    workerLoadError = new Error("Loader worker failed");
  };

  workerInstance = worker;
  return worker;
}

function toWorkerQueryState(query: QueryState): WorkerQueryState {
  return {
    time: {
      start: query.time.start,
      end: query.time.end
    },
    playback: {
      playhead: query.playback.playhead
    },
    focus: {
      allianceId: query.focus.allianceId,
      edgeKey: query.focus.edgeKey,
      eventId: query.focus.eventId
    },
    filters: {
      alliances: query.filters.alliances,
      treatyTypes: query.filters.treatyTypes,
      actions: query.filters.actions,
      sources: query.filters.sources,
      showFlags: query.filters.showFlags,
      includeInferred: query.filters.includeInferred,
      includeNoise: query.filters.includeNoise,
      evidenceMode: query.filters.evidenceMode,
      topXByScore: query.filters.topXByScore,
      sizeByScore: query.filters.sizeByScore
    },
    textQuery: query.textQuery,
    sort: {
      field: query.sort.field,
      direction: query.sort.direction
    }
  };
}

async function loadTimelapsePayloadInWorker(showFlags: boolean): Promise<RawTimelapsePayload> {
  if (typeof Worker === "undefined") {
    throw new Error("Timelapse loader worker is unavailable in this environment.");
  }

  if (workerLoadError) {
    throw workerLoadError;
  }

  if (!showFlags && workerBaseLoadPromise) {
    return workerBaseLoadPromise;
  }

  if (showFlags && workerFlagsLoadPromise) {
    return workerFlagsLoadPromise;
  }

  const nextPromise = new Promise<RawTimelapsePayload>((resolve, reject) => {
    const worker = createWorker();

    const onMessage = (event: MessageEvent<LoaderWorkerResponse>) => {
      const response = event.data;
      if (response.kind !== "load") {
        return;
      }
      worker.removeEventListener("message", onMessage);
      if (response.ok) {
        resolve(response.payload);
        return;
      }
      reject(new Error(response.error));
    };

    worker.addEventListener("message", onMessage);
    const request: LoaderWorkerRequest = { kind: "load", includeFlags: showFlags };
    worker.postMessage(request);
  }).catch((error) => {
      if (showFlags) {
        workerFlagsLoadPromise = null;
      } else {
        workerBaseLoadPromise = null;
      }
      workerLoadError = error instanceof Error ? error : new Error("Loader worker failed");
      throw workerLoadError;
    });

  if (showFlags) {
    workerFlagsLoadPromise = nextPromise;
  } else {
    workerBaseLoadPromise = nextPromise;
  }
  return nextPromise;
}

export async function selectTimelapseIndexes(query: QueryState): Promise<Uint32Array> {
  if (typeof Worker === "undefined") {
    throw new Error("Timelapse selection worker is unavailable in this environment.");
  }
  if (workerLoadError) {
    throw workerLoadError;
  }

  await loadTimelapsePayloadInWorker(query.filters.showFlags);
  if (!workerInstance || workerLoadError) {
    throw workerLoadError ?? new Error("Timelapse selection worker failed to initialize.");
  }
  const worker = workerInstance;

  return new Promise<Uint32Array>((resolve, reject) => {
    selectionRequestId += 1;
    const requestId = selectionRequestId;
    pendingSelectionRequests.set(requestId, { resolve, reject });

    const request: LoaderWorkerRequest = {
      kind: "select",
      requestId,
      query: toWorkerQueryState(query)
    };
    worker.postMessage(request);
  });
}

export async function selectTimelapsePulse(
  query: QueryState,
  maxPoints: number,
  playhead: string | null
): Promise<WorkerPulsePoint[]> {
  if (typeof Worker === "undefined") {
    throw new Error("Timelapse pulse worker is unavailable in this environment.");
  }
  if (workerLoadError) {
    throw workerLoadError;
  }

  await loadTimelapsePayloadInWorker(query.filters.showFlags);
  if (!workerInstance || workerLoadError) {
    throw workerLoadError ?? new Error("Timelapse pulse worker failed to initialize.");
  }
  const worker = workerInstance;

  return new Promise<WorkerPulsePoint[]>((resolve, reject) => {
    pulseRequestId += 1;
    const requestId = pulseRequestId;
    pendingPulseRequests.set(requestId, { resolve, reject });

    const request: LoaderWorkerRequest = {
      kind: "pulse",
      requestId,
      query: toWorkerQueryState(query),
      maxPoints,
      playhead
    };
    worker.postMessage(request);
  });
}

export async function selectTimelapseNetworkEventIndexes(
  query: QueryState,
  playhead: string | null,
  maxEdges: number
): Promise<Uint32Array> {
  if (typeof Worker === "undefined") {
    throw new Error("Timelapse network worker is unavailable in this environment.");
  }
  if (workerLoadError) {
    throw workerLoadError;
  }

  await loadTimelapsePayloadInWorker(query.filters.showFlags);
  if (!workerInstance || workerLoadError) {
    throw workerLoadError ?? new Error("Timelapse network worker failed to initialize.");
  }
  const worker = workerInstance;

  return new Promise<Uint32Array>((resolve, reject) => {
    networkRequestId += 1;
    const requestId = networkRequestId;
    pendingNetworkRequests.set(requestId, { resolve, reject });

    const request: LoaderWorkerRequest = {
      kind: "network",
      requestId,
      query: toWorkerQueryState(query),
      playhead,
      maxEdges
    };
    worker.postMessage(request);
  });
}

type CachedBundle = {
  datasetId: string;
  cacheIdentity: string;
  hasOptionalFlags: boolean;
  bundle: TimelapseDataBundle;
  cachedAt: string;
};

function stripOptionalBundle(bundle: TimelapseDataBundle): TimelapseDataBundle {
  if (bundle.allianceFlagsPayload === null && bundle.flagAssetsPayload === null) {
    return bundle;
  }

  return {
    ...bundle,
    allianceFlagsPayload: null,
    flagAssetsPayload: null,
    allianceFlagTimelines: {}
  };
}

function parseOptionalFlagArtifacts(
  allianceFlagsRaw: unknown | null,
  flagAssetsRaw: unknown | null,
  showFlags: boolean
): Pick<TimelapseDataBundle, "allianceFlagsPayload" | "flagAssetsPayload" | "allianceFlagTimelines"> {
  if (!showFlags) {
    return {
      allianceFlagsPayload: null,
      flagAssetsPayload: null,
      allianceFlagTimelines: {}
    };
  }

  if (allianceFlagsRaw === null) {
    console.warn("[timelapse] Optional flags.msgpack unavailable; alliance flag timelines disabled.");
  }
  if (flagAssetsRaw === null) {
    console.warn("[timelapse] Optional flag_assets.msgpack unavailable; network flag sprites disabled.");
  }

  const parsedAllianceFlags = allianceFlagsRaw ? allianceFlagsPayloadSchema.safeParse(allianceFlagsRaw) : null;
  const allianceFlagsPayload = parsedAllianceFlags?.success ? parsedAllianceFlags.data : null;
  if (allianceFlagsRaw && parsedAllianceFlags && !parsedAllianceFlags.success) {
    console.error("[timelapse] Invalid optional alliance flags dataset", parsedAllianceFlags.error.message);
  }

  const parsedFlagAssets = flagAssetsRaw ? flagAssetsPayloadSchema.safeParse(flagAssetsRaw) : null;
  const flagAssetsPayload = parsedFlagAssets?.success ? parsedFlagAssets.data : null;
  if (flagAssetsRaw && parsedFlagAssets && !parsedFlagAssets.success) {
    console.error("[timelapse] Invalid optional flag assets dataset", parsedFlagAssets.error.message);
  }

  if (allianceFlagsPayload && flagAssetsPayload === null) {
    console.warn("[timelapse] Flag timelines loaded without atlas assets; sprite rendering degraded.");
  }
  if (flagAssetsPayload && allianceFlagsPayload === null) {
    console.warn("[timelapse] Flag atlas loaded without timelines; no alliance flag sprites will resolve.");
  }

  return {
    allianceFlagsPayload,
    flagAssetsPayload,
    allianceFlagTimelines: normalizeAllianceFlagTimelines(allianceFlagsPayload)
  };
}

function validateOptionalFlagManifestEntries(manifest: TimelapseManifest | null, showFlags: boolean): void {
  if (!showFlags || !manifest) {
    return;
  }

  const requiredOptionalArtifacts = ["flags.msgpack", "flag_assets.msgpack"] as const;
  const missing = requiredOptionalArtifacts.filter((name) => !(name in manifest.files));
  if (missing.length === 0 || optionalFlagManifestWarningShown) {
    return;
  }

  optionalFlagManifestWarningShown = true;
  console.warn(
    `[timelapse] Manifest missing optional flag artifacts (${missing.join(", ")}); overlays will use fallback behavior.`
  );
}

async function enrichBundleWithOptionalFlags(baseBundle: TimelapseDataBundle): Promise<TimelapseDataBundle> {
  const payload = await loadTimelapsePayloadInWorker(true);
  const optional = parseOptionalFlagArtifacts(payload.allianceFlagsRaw, payload.flagAssetsRaw, true);
  return {
    ...baseBundle,
    ...optional
  };
}

function manifestCacheIdentity(manifest: TimelapseManifest): string {
  const fileIdentity = Object.entries(manifest.files)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([name, meta]) => `${name}:${meta.sha256}:${meta.sizeBytes}`)
    .join("|");
  return `${manifest.datasetId}|${manifest.generatedAt}|${fileIdentity}`;
}

async function fetchManifestForHydration(): Promise<TimelapseManifest | null> {
  const manifestRaw = await fetch("/data/manifest.json")
    .then(async (response) => {
      if (!response.ok) {
        return null;
      }
      return (await response.json()) as unknown;
    })
    .catch(() => null);
  if (!manifestRaw) {
    return null;
  }
  const parsed = manifestSchema.safeParse(manifestRaw);
  return parsed.success ? parsed.data : null;
}

function openCacheDb(): Promise<IDBDatabase | null> {
  if (typeof indexedDB === "undefined") {
    return Promise.resolve(null);
  }

  return new Promise((resolve) => {
    const request = indexedDB.open(CACHE_DB_NAME, CACHE_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(CACHE_STORE_NAME)) {
        db.createObjectStore(CACHE_STORE_NAME, { keyPath: "datasetId" });
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => resolve(null);
  });
}

async function readCachedBundle(
  datasetId: string,
  expectedIdentity?: string,
  requireOptionalFlags = false
): Promise<TimelapseDataBundle | null> {
  const db = await openCacheDb();
  if (!db) {
    return null;
  }

  return new Promise((resolve) => {
    const tx = db.transaction(CACHE_STORE_NAME, "readonly");
    const store = tx.objectStore(CACHE_STORE_NAME);
    const request = store.get(datasetId);
    request.onsuccess = () => {
      const value = request.result as CachedBundle | undefined;
      if (!value) {
        resolve(null);
        return;
      }
      if (expectedIdentity && value.cacheIdentity !== expectedIdentity) {
        resolve(null);
        return;
      }
      if (requireOptionalFlags && !value.hasOptionalFlags) {
        resolve(null);
        return;
      }
      resolve(value?.bundle ?? null);
    };
    request.onerror = () => resolve(null);
  });
}

async function writeCachedBundle(datasetId: string, cacheIdentity: string, bundle: TimelapseDataBundle): Promise<void> {
  const db = await openCacheDb();
  if (!db) {
    return;
  }

  await new Promise<void>((resolve) => {
    const tx = db.transaction(CACHE_STORE_NAME, "readwrite");
    const store = tx.objectStore(CACHE_STORE_NAME);
    const hasOptionalFlags = bundle.allianceFlagsPayload !== null && bundle.flagAssetsPayload !== null;
    const payload: CachedBundle = {
      datasetId,
      cacheIdentity,
      hasOptionalFlags,
      bundle,
      cachedAt: new Date().toISOString()
    };
    const request = store.put(payload);
    request.onsuccess = () => resolve();
    request.onerror = () => resolve();
  });
}

async function loadTimelapseBundleImpl(showFlags: boolean): Promise<TimelapseDataBundle> {
  const hydrationManifest = await fetchManifestForHydration();
  if (hydrationManifest?.datasetId) {
    const cached = await readCachedBundle(
      hydrationManifest.datasetId,
      manifestCacheIdentity(hydrationManifest),
      showFlags
    );
    if (cached) {
      return cached;
    }
  }

  const { eventsRaw, indicesRaw, summaryRaw, flagsRaw, allianceFlagsRaw, flagAssetsRaw, scoreRanksRaw, manifestRaw } =
    await loadTimelapsePayloadInWorker(showFlags);

  const manifestParsed = manifestRaw ? manifestSchema.safeParse(manifestRaw) : null;
  const manifest = manifestParsed?.success ? manifestParsed.data : hydrationManifest;
  validateOptionalFlagManifestEntries(manifest, showFlags);
  if (manifest?.datasetId) {
    const cached = await readCachedBundle(manifest.datasetId, manifestCacheIdentity(manifest), showFlags);
    if (cached) {
      return cached;
    }
  }

  const strictValidation = import.meta.env.DEV || import.meta.env.VITE_TIMELAPSE_STRICT_VALIDATION === "true";
  const validatedEvents = strictValidation ? validateEventsStrict(eventsRaw) : validateEventsSampled(eventsRaw);
  // Worker payload is already timestamp/event-id sorted with matching indices.
  const events = indicesRaw ? validatedEvents : sortEventsByTimestamp(validatedEvents);

  const summaryParsed = summarySchema.safeParse(summaryRaw);
  if (!summaryParsed.success) {
    throw new Error(`Invalid summary payload: ${summaryParsed.error.message}`);
  }

  const flags = flagsRaw.map((item, index) => {
    const parsed = flagSchema.safeParse(item);
    if (!parsed.success) {
      throw new Error(`Invalid flag at index ${index}`);
    }
    return parsed.data;
  });

  const indices = hydrateIndices(events, indicesRaw);

  const { allianceFlagsPayload, flagAssetsPayload, allianceFlagTimelines } = parseOptionalFlagArtifacts(
    allianceFlagsRaw,
    flagAssetsRaw,
    showFlags
  );

  const parsedRanks = scoreRanksRaw ? allianceScoreRanksDailySchema.safeParse(scoreRanksRaw) : null;
  const allianceScoreRanksByDay = parsedRanks?.success ? parsedRanks.data.ranks_by_day : null;
  if (scoreRanksRaw && parsedRanks && !parsedRanks.success) {
    console.error("[timelapse] Invalid optional alliance score-rank dataset", parsedRanks.error.message);
  }

  const bundle: TimelapseDataBundle = {
    manifest,
    events,
    indices,
    summary: summaryParsed.data,
    flags,
    allianceFlagsPayload,
    flagAssetsPayload,
    allianceFlagTimelines,
    allianceScoreRanksByDay
  };

  if (manifest?.datasetId) {
    void writeCachedBundle(manifest.datasetId, manifestCacheIdentity(manifest), bundle);
  }

  return bundle;
}

export function loadTimelapseBundle(options?: TimelapseLoadOptions): Promise<TimelapseDataBundle> {
  const showFlags = options?.showFlags ?? false;
  const mode = modeForShowFlags(showFlags);
  const existing = bundlePromiseByMode.get(mode);
  if (existing) {
    return existing;
  }

  if (!showFlags) {
    const existingOn = bundlePromiseByMode.get("flags-on");
    if (existingOn) {
      const derivedOff = existingOn.then((bundle) => stripOptionalBundle(bundle));
      bundlePromiseByMode.set("flags-off", derivedOff);
      return derivedOff;
    }
  }

  if (showFlags) {
    const existingOff = bundlePromiseByMode.get("flags-off");
    if (existingOff) {
      const upgradedOn = existingOff
        .then((bundle) => enrichBundleWithOptionalFlags(bundle))
        .catch((error) => {
          bundlePromiseByMode.delete("flags-on");
          throw error;
        });
      bundlePromiseByMode.set("flags-on", upgradedOn);
      return upgradedOn;
    }
  }

  const nextPromise = loadTimelapseBundleImpl(showFlags).catch((error) => {
    bundlePromiseByMode.delete(mode);
    throw error;
  });
  bundlePromiseByMode.set(mode, nextPromise);
  return nextPromise;
}


