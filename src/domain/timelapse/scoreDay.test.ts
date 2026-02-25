import { describe, expect, it } from "vitest";
import { resolveScoreDay, resolveScoreRowForPlayhead } from "@/domain/timelapse/scoreDay";

describe("resolveScoreDay", () => {
  it("uses exact day when present", () => {
    const day = resolveScoreDay(["2020-01-01", "2020-01-05"], "2020-01-05T12:00:00.000Z");
    expect(day).toBe("2020-01-05");
  });

  it("falls back to nearest prior day when target day is missing", () => {
    const day = resolveScoreDay(["2020-01-01", "2020-01-05"], "2020-01-03T00:00:00.000Z");
    expect(day).toBe("2020-01-01");
  });

  it("falls back to earliest day when target is before dataset", () => {
    const day = resolveScoreDay(["2020-01-10", "2020-01-12"], "2020-01-01T00:00:00.000Z");
    expect(day).toBe("2020-01-10");
  });
});

describe("resolveScoreRowForPlayhead", () => {
  it("uses exact day row when available", () => {
    const scoreByDay = {
      "2020-01-01": { "1": 10 },
      "2020-01-05": { "1": 15 }
    };

    const result = resolveScoreRowForPlayhead(scoreByDay, ["2020-01-01", "2020-01-05"], "2020-01-05T00:00:00.000Z");

    expect(result.day).toBe("2020-01-05");
    expect(result.row?.["1"]).toBe(15);
    expect(result.usedFallback).toBe(false);
  });

  it("falls back only when requested day row is missing", () => {
    const scoreByDay = {
      "2020-01-01": { "1": 10 },
      "2020-01-05": { "1": 15 }
    };

    const result = resolveScoreRowForPlayhead(scoreByDay, ["2020-01-01", "2020-01-03", "2020-01-05"], "2020-01-03T00:00:00.000Z");

    expect(result.day).toBe("2020-01-01");
    expect(result.row?.["1"]).toBe(10);
    expect(result.usedFallback).toBe(true);
  });
});
