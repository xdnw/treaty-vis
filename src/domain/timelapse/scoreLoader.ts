import type { AllianceScoresRuntime, TimelapseManifest } from "@/domain/timelapse/schema";
import { pushScoreLoadAttempt } from "@/lib/perf";

export type ScoreLoaderState =
  | "idle"
  | "cache-hit"
  | "fetching"
  | "decoding"
  | "ready"
  | "error-timeout"
  | "error-http"
  | "error-decode"
  | "error-abort";

export type ScoreLoaderTerminalState = "ready" | "error-timeout" | "error-http" | "error-decode" | "error-abort";

export type ScoreLoaderSnapshot = {
  requestId: string;
  datasetId: string;
  state: ScoreLoaderState;
  startedAtMs: number;
  atMs: number;
  elapsedMs: number;
  stageDurationMs: number;
  httpStatus: number | null;
  bytesFetched: number;
  totalBytes: number | null;
  decodeMs: number | null;
  dayCount: number;
  scoredNodeCount: number;
  reasonCode: string | null;
  message: string | null;
  runtime: AllianceScoresRuntime | null;
  fromCache: boolean;
};

type ScoreLoaderEventHandler = (snapshot: ScoreLoaderSnapshot) => void;

type LoadScoreRuntimeOptions = {
  manifest: TimelapseManifest;
  requestId: string;
  forceNetwork?: boolean;
  timeoutMs?: number;
  signal?: AbortSignal;
  onEvent?: ScoreLoaderEventHandler;
};

type CachedScoreRuntime = {
  datasetId: string;
  cacheIdentity: string;
  runtime: AllianceScoresRuntime;
  cachedAtMs: number;
};

type ReadCacheFn = (datasetId: string) => Promise<CachedScoreRuntime | null>;

type WriteCacheFn = (entry: CachedScoreRuntime) => Promise<void>;

type ScoreLoaderDeps = {
  now: () => number;
  fetchImpl: typeof fetch;
  readCache: ReadCacheFn;
  writeCache: WriteCacheFn;
  decodeInWorker: (payload: ArrayBuffer, requestId: string, signal?: AbortSignal) => Promise<AllianceScoresRuntime>;
};

const SCORE_CACHE_DB = "discmcp_score_runtime_cache";
const SCORE_CACHE_STORE = "scoreRuntime";
const SCORE_CACHE_VERSION = 1;
const SCORE_TIMEOUT_MS = 15_000;
const SCORE_URL = "/data/alliance_scores_v2.msgpack";
const TERMINAL_STATES = new Set<ScoreLoaderState>(["ready", "error-timeout", "error-http", "error-decode", "error-abort"]);
function isTerminalState(state: ScoreLoaderState): state is ScoreLoaderTerminalState {
  return TERMINAL_STATES.has(state);
}

const allowedTransitions: Record<ScoreLoaderState, ScoreLoaderState[]> = {
  idle: ["cache-hit", "fetching", "error-timeout", "error-http", "error-decode", "error-abort"],
  "cache-hit": ["fetching", "ready", "error-timeout", "error-http", "error-decode", "error-abort"],
  fetching: ["decoding", "error-timeout", "error-http", "error-abort"],
  decoding: ["ready", "error-decode", "error-abort", "error-timeout"],
  ready: [],
  "error-timeout": [],
  "error-http": [],
  "error-decode": [],
  "error-abort": []
};

const runtimeCache = new Map<string, CachedScoreRuntime>();

function scoreCacheIdentity(manifest: TimelapseManifest): string {
  const scoreMeta = manifest.files["alliance_scores_v2.msgpack"];
  return scoreMeta ? `${manifest.datasetId}|${scoreMeta.sha256}|${scoreMeta.sizeBytes}` : `${manifest.datasetId}|missing`;
}

function summarizeScoredNodes(runtime: AllianceScoresRuntime): number {
  const seen = new Set<string>();
  for (const row of Object.values(runtime.byDay)) {
    for (const allianceId of Object.keys(row)) {
      seen.add(allianceId);
    }
  }
  return seen.size;
}

function makeInitialSnapshot(requestId: string, datasetId: string, startedAtMs: number): ScoreLoaderSnapshot {
  return {
    requestId,
    datasetId,
    state: "idle",
    startedAtMs,
    atMs: startedAtMs,
    elapsedMs: 0,
    stageDurationMs: 0,
    httpStatus: null,
    bytesFetched: 0,
    totalBytes: null,
    decodeMs: null,
    dayCount: 0,
    scoredNodeCount: 0,
    reasonCode: null,
    message: null,
    runtime: null,
    fromCache: false
  };
}

function emitTransition(
  previous: ScoreLoaderSnapshot,
  nextState: ScoreLoaderState,
  deps: ScoreLoaderDeps,
  patch: Partial<ScoreLoaderSnapshot>,
  onEvent?: ScoreLoaderEventHandler
): ScoreLoaderSnapshot {
  const allowed = allowedTransitions[previous.state];
  if (!allowed.includes(nextState)) {
    throw new Error(`Invalid score loader transition ${previous.state} -> ${nextState}`);
  }

  const now = deps.now();
  const next: ScoreLoaderSnapshot = {
    ...previous,
    ...patch,
    state: nextState,
    atMs: now,
    elapsedMs: Math.max(0, now - previous.startedAtMs),
    stageDurationMs: Math.max(0, now - previous.atMs)
  };

  onEvent?.(next);

  if (isTerminalState(next.state)) {
    pushScoreLoadAttempt({
      requestId: next.requestId,
      datasetId: next.datasetId,
      state: next.state,
      elapsedMs: next.elapsedMs,
      httpStatus: next.httpStatus,
      bytesFetched: next.bytesFetched,
      totalBytes: next.totalBytes,
      decodeMs: next.decodeMs,
      dayCount: next.dayCount,
      scoredNodeCount: next.scoredNodeCount,
      reasonCode: next.reasonCode,
      message: next.message,
      fromCache: next.fromCache
    });
  }

  return next;
}

function createTimeoutController(timeoutMs: number): { signal: AbortSignal; clear: () => void } {
  const controller = new AbortController();
  const timeoutId = globalThis.setTimeout(() => {
    controller.abort("timeout");
  }, timeoutMs);

  const clear = () => {
    globalThis.clearTimeout(timeoutId);
  };

  controller.signal.addEventListener("abort", clear, { once: true });

  return {
    signal: controller.signal,
    clear
  };
}

function mergeSignals(signals: Array<AbortSignal | undefined>): AbortSignal | undefined {
  const present = signals.filter((value): value is AbortSignal => Boolean(value));
  if (present.length === 0) {
    return undefined;
  }

  const controller = new AbortController();
  const abort = (reason: unknown) => {
    if (!controller.signal.aborted) {
      controller.abort(reason);
    }
  };

  for (const signal of present) {
    if (signal.aborted) {
      abort(signal.reason);
      break;
    }
    signal.addEventListener(
      "abort",
      () => {
        abort(signal.reason);
      },
      { once: true }
    );
  }

  return controller.signal;
}

async function openScoreCacheDb(): Promise<IDBDatabase | null> {
  if (typeof indexedDB === "undefined") {
    return null;
  }

  return new Promise((resolve) => {
    const request = indexedDB.open(SCORE_CACHE_DB, SCORE_CACHE_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(SCORE_CACHE_STORE)) {
        db.createObjectStore(SCORE_CACHE_STORE, { keyPath: "datasetId" });
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => resolve(null);
  });
}

async function readCachedRuntime(datasetId: string): Promise<CachedScoreRuntime | null> {
  const memoryHit = runtimeCache.get(datasetId) ?? null;
  if (memoryHit) {
    return memoryHit;
  }

  const db = await openScoreCacheDb();
  if (!db) {
    return null;
  }

  return new Promise((resolve) => {
    const tx = db.transaction(SCORE_CACHE_STORE, "readonly");
    const store = tx.objectStore(SCORE_CACHE_STORE);
    const request = store.get(datasetId);
    request.onsuccess = () => {
      const value = request.result as CachedScoreRuntime | undefined;
      if (!value) {
        resolve(null);
        return;
      }
      runtimeCache.set(datasetId, value);
      resolve(value);
    };
    request.onerror = () => resolve(null);
  });
}

async function writeCachedRuntime(entry: CachedScoreRuntime): Promise<void> {
  runtimeCache.set(entry.datasetId, entry);

  const db = await openScoreCacheDb();
  if (!db) {
    return;
  }

  await new Promise<void>((resolve) => {
    const tx = db.transaction(SCORE_CACHE_STORE, "readwrite");
    const store = tx.objectStore(SCORE_CACHE_STORE);
    const request = store.put(entry);
    request.onsuccess = () => resolve();
    request.onerror = () => resolve();
  });
}

async function decodeScoreInWorker(payload: ArrayBuffer, requestId: string, signal?: AbortSignal): Promise<AllianceScoresRuntime> {
  if (typeof Worker === "undefined") {
    throw new Error("Worker unavailable for score decode");
  }

  const worker = new Worker(new URL("./scoreLoader.worker.ts", import.meta.url), { type: "module" });

  return new Promise<AllianceScoresRuntime>((resolve, reject) => {
    const cleanup = () => {
      worker.onmessage = null;
      worker.onerror = null;
      worker.terminate();
      signal?.removeEventListener("abort", onAbort);
    };

    const onAbort = () => {
      cleanup();
      reject(new DOMException("Aborted", "AbortError"));
    };

    if (signal?.aborted) {
      onAbort();
      return;
    }

    signal?.addEventListener("abort", onAbort, { once: true });

    worker.onerror = () => {
      cleanup();
      reject(new Error("Worker decode failed"));
    };

    worker.onmessage = (event: MessageEvent<{ kind: "decode"; requestId: string; ok: boolean; runtime?: AllianceScoresRuntime; error?: string }>) => {
      const message = event.data;
      if (message.kind !== "decode" || message.requestId !== requestId) {
        return;
      }
      cleanup();
      if (!message.ok || !message.runtime) {
        reject(new Error(message.error ?? "Worker decode returned empty payload"));
        return;
      }
      resolve(message.runtime);
    };

    worker.postMessage({ kind: "decode", requestId, payload }, [payload]);
  });
}

async function fetchWithProgress(
  fetchImpl: typeof fetch,
  url: string,
  signal: AbortSignal | undefined,
  onChunk: (bytesFetched: number, totalBytes: number | null) => void
): Promise<{ status: number; body: ArrayBuffer; bytesFetched: number; totalBytes: number | null }> {
  const response = await fetchImpl(url, { signal });
  const totalBytesHeader = response.headers.get("content-length");
  const totalBytes = totalBytesHeader ? Number(totalBytesHeader) : null;

  if (!response.ok) {
    throw new Error(`http:${response.status}`);
  }

  if (!response.body || typeof response.body.getReader !== "function") {
    const buffer = await response.arrayBuffer();
    onChunk(buffer.byteLength, totalBytes);
    return {
      status: response.status,
      body: buffer,
      bytesFetched: buffer.byteLength,
      totalBytes
    };
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let bytesFetched = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    if (!value) {
      continue;
    }
    chunks.push(value);
    bytesFetched += value.byteLength;
    onChunk(bytesFetched, totalBytes);
  }

  const merged = new Uint8Array(bytesFetched);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return {
    status: response.status,
    body: merged.buffer,
    bytesFetched,
    totalBytes
  };
}

function createScoreLoader(deps: ScoreLoaderDeps) {
  async function load(options: LoadScoreRuntimeOptions): Promise<ScoreLoaderSnapshot> {
    const timeoutMs = options.timeoutMs ?? SCORE_TIMEOUT_MS;
    const cacheIdentity = scoreCacheIdentity(options.manifest);

    let snapshot = makeInitialSnapshot(options.requestId, options.manifest.datasetId, deps.now());
    options.onEvent?.(snapshot);

    const cached = await deps.readCache(options.manifest.datasetId);
    const canUseCache = Boolean(cached?.runtime);

    if (cached?.runtime) {
      snapshot = emitTransition(
        snapshot,
        "cache-hit",
        deps,
        {
          runtime: cached.runtime,
          dayCount: cached.runtime.dayKeys.length,
          scoredNodeCount: summarizeScoredNodes(cached.runtime),
          fromCache: true,
          reasonCode: "cache-memory-or-idb",
          message: null
        },
        options.onEvent
      );

      if (!options.forceNetwork && cached.cacheIdentity === cacheIdentity) {
        snapshot = emitTransition(
          snapshot,
          "ready",
          deps,
          {
            reasonCode: "cache-fresh",
            message: null
          },
          options.onEvent
        );
        return snapshot;
      }
    }

    const timeout = createTimeoutController(timeoutMs);
    const mergedSignal = mergeSignals([timeout.signal, options.signal]);

    try {
      snapshot = emitTransition(snapshot, "fetching", deps, { reasonCode: null, message: null }, options.onEvent);

      let fetchedBody: ArrayBuffer;
      try {
        const fetched = await fetchWithProgress(
          deps.fetchImpl,
          SCORE_URL,
          mergedSignal,
          (bytesFetched, totalBytes) => {
            snapshot = {
              ...snapshot,
              bytesFetched,
              totalBytes,
              atMs: deps.now(),
              elapsedMs: Math.max(0, deps.now() - snapshot.startedAtMs)
            };
            options.onEvent?.(snapshot);
          }
        );
        snapshot = {
          ...snapshot,
          httpStatus: fetched.status,
          bytesFetched: fetched.bytesFetched,
          totalBytes: fetched.totalBytes
        };
        fetchedBody = fetched.body;
      } catch (error) {
        if (error instanceof Error && error.message.startsWith("http:")) {
          const status = Number(error.message.split(":")[1]);
          snapshot = emitTransition(
            snapshot,
            "error-http",
            deps,
            {
              httpStatus: Number.isFinite(status) ? status : null,
              reasonCode: "http-non-ok",
              message: Number.isFinite(status) ? `HTTP ${status}` : "HTTP error"
            },
            options.onEvent
          );
          return snapshot;
        }

        const aborted = mergedSignal?.aborted || (error instanceof DOMException && error.name === "AbortError");
        if (aborted) {
          const timeoutAbort = timeout.signal.aborted && timeout.signal.reason === "timeout";
          snapshot = emitTransition(
            snapshot,
            timeoutAbort ? "error-timeout" : "error-abort",
            deps,
            {
              reasonCode: timeoutAbort ? "fetch-timeout" : "fetch-abort",
              message: timeoutAbort ? `Timed out after ${timeoutMs}ms` : "Request aborted"
            },
            options.onEvent
          );
          return snapshot;
        }

        snapshot = emitTransition(
          snapshot,
          "error-http",
          deps,
          {
            reasonCode: "fetch-failed",
            message: error instanceof Error ? error.message : "Unknown fetch failure"
          },
          options.onEvent
        );
        return snapshot;
      }

      snapshot = emitTransition(snapshot, "decoding", deps, {}, options.onEvent);

      const decodeStartedAt = deps.now();
      let decoded: AllianceScoresRuntime;

      try {
        decoded = await deps.decodeInWorker(fetchedBody, options.requestId, mergedSignal);
      } catch (error) {
        const aborted = mergedSignal?.aborted || (error instanceof DOMException && error.name === "AbortError");
        if (aborted) {
          const timeoutAbort = timeout.signal.aborted && timeout.signal.reason === "timeout";
          snapshot = emitTransition(
            snapshot,
            timeoutAbort ? "error-timeout" : "error-abort",
            deps,
            {
              decodeMs: Math.max(0, deps.now() - decodeStartedAt),
              reasonCode: timeoutAbort ? "decode-timeout" : "decode-abort",
              message: timeoutAbort ? `Timed out after ${timeoutMs}ms` : "Decode aborted"
            },
            options.onEvent
          );
          return snapshot;
        }

        snapshot = emitTransition(
          snapshot,
          "error-decode",
          deps,
          {
            decodeMs: Math.max(0, deps.now() - decodeStartedAt),
            reasonCode: "decode-failed",
            message: error instanceof Error ? error.message : "Unknown decode failure"
          },
          options.onEvent
        );
        return snapshot;
      }

      const decodeMs = Math.max(0, deps.now() - decodeStartedAt);

      const cachedEntry: CachedScoreRuntime = {
        datasetId: options.manifest.datasetId,
        cacheIdentity,
        runtime: decoded,
        cachedAtMs: Date.now()
      };
      await deps.writeCache(cachedEntry);

      snapshot = emitTransition(
        snapshot,
        "ready",
        deps,
        {
          decodeMs,
          runtime: decoded,
          dayCount: decoded.dayKeys.length,
          scoredNodeCount: summarizeScoredNodes(decoded),
          reasonCode: canUseCache ? "refreshed-after-cache" : "network-success",
          message: null,
          fromCache: false
        },
        options.onEvent
      );

      return snapshot;
    } finally {
      timeout.clear();
    }
  }

  return {
    load
  };
}

const defaultLoader = createScoreLoader({
  now: () => performance.now(),
  fetchImpl: fetch,
  readCache: readCachedRuntime,
  writeCache: writeCachedRuntime,
  decodeInWorker: decodeScoreInWorker
});

export function loadScoreRuntime(options: LoadScoreRuntimeOptions): Promise<ScoreLoaderSnapshot> {
  return defaultLoader.load(options);
}

export function __createScoreLoaderForTests(partialDeps: Partial<ScoreLoaderDeps>) {
  const deps: ScoreLoaderDeps = {
    now: partialDeps.now ?? (() => performance.now()),
    fetchImpl: partialDeps.fetchImpl ?? fetch,
    readCache: partialDeps.readCache ?? (async () => null),
    writeCache: partialDeps.writeCache ?? (async () => {}),
    decodeInWorker: partialDeps.decodeInWorker ?? (async () => {
      throw new Error("decodeInWorker test dependency missing");
    })
  };

  return createScoreLoader(deps);
}

export function __resetScoreRuntimeCacheForTests(): void {
  runtimeCache.clear();
}
