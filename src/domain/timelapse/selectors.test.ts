import { describe, expect, it } from "vitest";
import type { TimelapseEvent } from "@/domain/timelapse/schema";
import { deriveNetworkEdges } from "@/domain/timelapse/selectors";

function buildEvent(index: number): TimelapseEvent {
  const pairA = 1000 + index * 2;
  const pairB = pairA + 1;
  const timestamp = new Date(Date.UTC(2020, 0, 1, 0, 0, index)).toISOString();
  return {
    event_id: `evt-${index}`,
    timestamp,
    action: "signed",
    treaty_type: index % 2 === 0 ? "MDP" : "ODP",
    from_alliance_id: pairA,
    from_alliance_name: `A-${pairA}`,
    to_alliance_id: pairB,
    to_alliance_name: `A-${pairB}`,
    pair_min_id: pairA,
    pair_max_id: pairB,
    source: "bot",
    source_ref: "",
    confidence: "high",
    inferred: false,
    inference_reason: null,
    time_remaining_turns: null,
    grounded_from: true,
    grounded_to: true,
    grounded_keep: true,
    noise_filtered: false,
    noise_reason: null
  };
}

describe("deriveNetworkEdges", () => {
  it("is deterministic for the same inputs", () => {
    const events = Array.from({ length: 1200 }, (_, index) => buildEvent(index));
    const indexes = events.map((_, index) => index);

    const left = deriveNetworkEdges(events, indexes, null, 500).map((edge) => edge.key);
    const right = deriveNetworkEdges(events, indexes, null, 500).map((edge) => edge.key);

    expect(left).toEqual(right);
  });

  it("changes capped membership gradually with small budget changes", () => {
    const events = Array.from({ length: 1500 }, (_, index) => buildEvent(index));
    const indexes = events.map((_, index) => index);

    const at500 = deriveNetworkEdges(events, indexes, null, 500).map((edge) => edge.key);
    const at501 = deriveNetworkEdges(events, indexes, null, 501).map((edge) => edge.key);

    const set500 = new Set(at500);
    const overlap = at501.filter((key) => set500.has(key)).length;
    expect(overlap).toBeGreaterThanOrEqual(500);
  });

  it("has low churn when one additional event becomes visible", () => {
    const events = Array.from({ length: 1300 }, (_, index) => buildEvent(index));
    const indexes = events.map((_, index) => index);

    const before = deriveNetworkEdges(events, indexes, events[1198].timestamp, 500).map((edge) => edge.key);
    const after = deriveNetworkEdges(events, indexes, events[1199].timestamp, 500).map((edge) => edge.key);

    const beforeSet = new Set(before);
    const afterSet = new Set(after);
    const removed = before.filter((key) => !afterSet.has(key)).length;
    const added = after.filter((key) => !beforeSet.has(key)).length;

    expect(removed).toBeLessThanOrEqual(1);
    expect(added).toBeLessThanOrEqual(1);
  });
});
