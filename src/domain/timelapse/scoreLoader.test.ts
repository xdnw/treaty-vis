import { encode } from "@msgpack/msgpack";
import { describe, expect, it } from "vitest";
import { __createScoreLoaderForTests, type ScoreLoaderSnapshot } from "@/domain/timelapse/scoreLoader";
import { decodeAllianceScoresArrayBuffer } from "@/domain/timelapse/scoreLoader.worker";
import type { AllianceScoresRuntime, TimelapseManifest } from "@/domain/timelapse/schema";

function manifest(datasetId = "dataset-a", sha = "abc"): TimelapseManifest {
  return {
    datasetId,
    generatedAt: "2026-02-25T00:00:00.000Z",
    files: {
      "alliance_scores_v2.msgpack": {
        sizeBytes: 1024,
        sha256: sha
      }
    }
  };
}

function runtimePayload(): AllianceScoresRuntime {
  return {
    quantizationScale: 1000,
    dayKeys: ["2026-02-01", "2026-02-02"],
    byDay: {
      "2026-02-01": { "10": 12.3, "22": 9.4 },
      "2026-02-02": { "10": 13.1 }
    }
  };
}

describe("scoreLoader state machine", () => {
  it("emits staged transitions cache-hit -> fetching -> decoding -> ready", async () => {
    let now = 0;
    const events: ScoreLoaderSnapshot[] = [];
    const buffer = new ArrayBuffer(64);

    const loader = __createScoreLoaderForTests({
      now: () => now,
      readCache: async () => ({
        datasetId: "dataset-a",
        cacheIdentity: "dataset-a|stale|0",
        runtime: runtimePayload(),
        cachedAtMs: 0
      }),
      writeCache: async () => {},
      fetchImpl: (async () => {
        now += 120;
        return {
          ok: true,
          status: 200,
          headers: { get: () => "64" },
          body: null,
          arrayBuffer: async () => buffer
        } as unknown as Response;
      }) as typeof fetch,
      decodeInWorker: async () => {
        now += 80;
        return runtimePayload();
      }
    });

    const result = await loader.load({
      manifest: manifest(),
      requestId: "req-1",
      onEvent: (snapshot) => events.push(snapshot)
    });

    expect(events.map((event) => event.state)).toEqual(["idle", "cache-hit", "fetching", "fetching", "decoding", "ready"]);
    expect(result.state).toBe("ready");
    expect(result.reasonCode).toBe("refreshed-after-cache");
  });

  it("returns explicit timeout state when fetch exceeds timeout", async () => {
    let now = 0;

    const loader = __createScoreLoaderForTests({
      now: () => now,
      readCache: async () => null,
      writeCache: async () => {},
      fetchImpl: ((_: RequestInfo | URL, init?: RequestInit) =>
        new Promise((_, reject) => {
          if (init?.signal?.aborted) {
            reject(new DOMException("Aborted", "AbortError"));
            return;
          }
          init?.signal?.addEventListener("abort", () => {
            now += 11;
            reject(new DOMException("Aborted", "AbortError"));
          });
        })) as typeof fetch,
      decodeInWorker: async () => runtimePayload()
    });

    const result = await loader.load({
      manifest: manifest(),
      requestId: "req-timeout",
      timeoutMs: 10
    });

    expect(result.state).toBe("error-timeout");
    expect(result.reasonCode).toBe("fetch-timeout");
  });

  it("returns explicit abort state when caller aborts in-flight attempt", async () => {
    let now = 0;
    const controller = new AbortController();

    const loader = __createScoreLoaderForTests({
      now: () => now,
      readCache: async () => null,
      writeCache: async () => {},
      fetchImpl: ((_: RequestInfo | URL, init?: RequestInit) =>
        new Promise((_, reject) => {
          if (init?.signal?.aborted) {
            reject(new DOMException("Aborted", "AbortError"));
            return;
          }
          init?.signal?.addEventListener("abort", () => {
            now += 2;
            reject(new DOMException("Aborted", "AbortError"));
          });
        })) as typeof fetch,
      decodeInWorker: async () => runtimePayload()
    });

    const promise = loader.load({
      manifest: manifest(),
      requestId: "req-abort",
      signal: controller.signal
    });

    queueMicrotask(() => controller.abort("manual"));
    const result = await promise;

    expect(result.state).toBe("error-abort");
    expect(result.reasonCode).toBe("fetch-abort");
  });
});

describe("score decode contract", () => {
  it("decodes real-ish quantized msgpack payload into runtime structure", () => {
    const encoded = encode({
      schema_version: 2,
      quantization_scale: 1000,
      day_keys: ["2026-01-01", "2026-01-02"],
      days: [
        [
          [1001, 12345],
          [1002, 999]
        ],
        [[1001, 13001]]
      ]
    });

    const body = encoded.buffer.slice(encoded.byteOffset, encoded.byteOffset + encoded.byteLength);
    const decoded = decodeAllianceScoresArrayBuffer(body);

    expect(decoded.dayKeys).toEqual(["2026-01-01", "2026-01-02"]);
    expect(decoded.byDay["2026-01-01"]["1001"]).toBe(12.345);
    expect(decoded.byDay["2026-01-01"]["1002"]).toBe(0.999);
    expect(decoded.byDay["2026-01-02"]["1001"]).toBe(13.001);
  });
});

describe("score loader perf gates", () => {
  it("meets cache-hit and cold-load readiness budgets", async () => {
    let now = 0;

    const loader = __createScoreLoaderForTests({
      now: () => now,
      readCache: async () => ({
        datasetId: "dataset-a",
        cacheIdentity: "dataset-a|abc|1024",
        runtime: runtimePayload(),
        cachedAtMs: 0
      }),
      writeCache: async () => {},
      fetchImpl: (async () => {
        now += 140;
        return {
          ok: true,
          status: 200,
          headers: { get: () => "64" },
          body: null,
          arrayBuffer: async () => new ArrayBuffer(64)
        } as unknown as Response;
      }) as typeof fetch,
      decodeInWorker: async () => {
        now += 120;
        return runtimePayload();
      }
    });

    const cacheResult = await loader.load({ manifest: manifest(), requestId: "req-cache" });
    expect(cacheResult.state).toBe("ready");
    expect(cacheResult.elapsedMs).toBeLessThan(50);

    const coldResult = await loader.load({
      manifest: manifest("dataset-b", "def"),
      requestId: "req-cold",
      forceNetwork: true
    });
    expect(coldResult.state).toBe("ready");
    expect(coldResult.elapsedMs).toBeLessThan(400);
  });
});
