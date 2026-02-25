import { decode } from "@msgpack/msgpack";
import type { AllianceScoresRuntime } from "@/domain/timelapse/schema";

type WorkerDecodeRequest = {
  kind: "decode";
  requestId: string;
  payload: ArrayBuffer;
};

type WorkerDecodeSuccess = {
  kind: "decode";
  requestId: string;
  ok: true;
  runtime: AllianceScoresRuntime;
};

type WorkerDecodeFailure = {
  kind: "decode";
  requestId: string;
  ok: false;
  error: string;
};

type WorkerDecodeResponse = WorkerDecodeSuccess | WorkerDecodeFailure;

type WorkerScope = {
  addEventListener: (type: "message", listener: (event: MessageEvent<WorkerDecodeRequest>) => void) => void;
  postMessage: (message: WorkerDecodeResponse) => void;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function decodeAllianceScoresArrayBuffer(payload: ArrayBuffer): AllianceScoresRuntime {
  const decoded = decode(new Uint8Array(payload)) as unknown;
  if (!isRecord(decoded)) {
    throw new Error("Decoded score payload must be an object");
  }

  if (Number(decoded.schema_version) !== 2) {
    throw new Error("Score payload schema_version must be 2");
  }

  const quantizationScale = Number(decoded.quantization_scale);
  if (!Number.isFinite(quantizationScale) || quantizationScale <= 0) {
    throw new Error("Score payload quantization_scale must be > 0");
  }

  const dayKeysRaw = decoded.day_keys;
  const daysRaw = decoded.days;
  if (!Array.isArray(dayKeysRaw) || !Array.isArray(daysRaw)) {
    throw new Error("Score payload day_keys and days must be arrays");
  }
  if (dayKeysRaw.length !== daysRaw.length) {
    throw new Error("Score payload day_keys and days length mismatch");
  }

  const dayKeys: string[] = [];
  const byDay: Record<string, Record<string, number>> = {};

  for (let dayIndex = 0; dayIndex < dayKeysRaw.length; dayIndex += 1) {
    const day = String(dayKeysRaw[dayIndex]);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) {
      throw new Error(`Score payload contains invalid day key: ${day}`);
    }
    if (dayIndex > 0 && dayKeys[dayIndex - 1] >= day) {
      throw new Error("Score payload day_keys must be strictly ascending");
    }

    const rowRaw = daysRaw[dayIndex];
    if (!Array.isArray(rowRaw)) {
      throw new Error(`Score payload day row must be an array for ${day}`);
    }

    const row: Record<string, number> = {};
    for (const entry of rowRaw) {
      if (!Array.isArray(entry) || entry.length < 2) {
        continue;
      }
      const allianceId = Number(entry[0]);
      const quantized = Number(entry[1]);
      if (!Number.isFinite(allianceId) || allianceId <= 0) {
        continue;
      }
      if (!Number.isFinite(quantized) || quantized <= 0) {
        continue;
      }
      const score = quantized / quantizationScale;
      if (!Number.isFinite(score) || score <= 0) {
        continue;
      }
      row[String(Math.floor(allianceId))] = score;
    }

    if (Object.keys(row).length > 0) {
      dayKeys.push(day);
      byDay[day] = row;
    }
  }

  return {
    quantizationScale,
    dayKeys,
    byDay
  };
}

if (typeof self !== "undefined" && "addEventListener" in self) {
  const scope = self as unknown as WorkerScope;

  scope.addEventListener("message", (event: MessageEvent<WorkerDecodeRequest>) => {
    const request = event.data;
    if (!request || request.kind !== "decode") {
      return;
    }

    try {
      const runtime = decodeAllianceScoresArrayBuffer(request.payload);
      scope.postMessage({
        kind: "decode",
        requestId: request.requestId,
        ok: true,
        runtime
      });
    } catch (error) {
      scope.postMessage({
        kind: "decode",
        requestId: request.requestId,
        ok: false,
        error: error instanceof Error ? error.message : "Unknown score decode error"
      });
    }
  });
}

export type { WorkerDecodeRequest, WorkerDecodeResponse };
