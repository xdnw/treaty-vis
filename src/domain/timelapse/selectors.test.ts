import { describe, expect, it } from "vitest";
import type { TimelapseEvent } from "@/domain/timelapse/schema";
import { buildPulseSeries, countByAction } from "@/domain/timelapse/selectors";

function buildEvent(index: number): TimelapseEvent {
  const pairA = 1000 + index * 2;
  const pairB = pairA + 1;
  const timestamp = new Date(Date.UTC(2020, 0, 1, 0, 0, index)).toISOString();
  return {
    event_id: `evt-${index}`,
    event_sequence: index,
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

describe("selectors helpers", () => {
  it("builds deterministic pulse buckets for the same inputs", () => {
    const events = Array.from({ length: 300 }, (_, index) => buildEvent(index));
    const indexes = events.map((_, index) => index);

    const left = buildPulseSeries(events, indexes, 24, null);
    const right = buildPulseSeries(events, indexes, 24, null);
    expect(left).toEqual(right);
  });

  it("counts actions consistently", () => {
    const events = [
      buildEvent(0),
      buildEvent(1),
      {
        ...buildEvent(2),
        action: "expired"
      }
    ];

    const counts = countByAction(events);
    expect(counts.signed).toBe(2);
    expect(counts.expired).toBe(1);
  });
});
