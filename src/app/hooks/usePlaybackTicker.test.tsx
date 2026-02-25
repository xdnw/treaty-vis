// @vitest-environment jsdom

import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { usePlaybackTicker } from "@/app/hooks/usePlaybackTicker";

declare global {
  // eslint-disable-next-line no-var
  var IS_REACT_ACT_ENVIRONMENT: boolean | undefined;
}

globalThis.IS_REACT_ACT_ENVIRONMENT = true;

type FrameCallback = (now: number) => void;

function installRafHarness() {
  let nextId = 1;
  const callbacks = new Map<number, FrameCallback>();

  vi.stubGlobal("requestAnimationFrame", (callback: FrameCallback) => {
    const id = nextId;
    nextId += 1;
    callbacks.set(id, callback);
    return id;
  });

  vi.stubGlobal("cancelAnimationFrame", (id: number) => {
    callbacks.delete(id);
  });

  return {
    step(now: number) {
      const first = callbacks.entries().next();
      if (first.done) {
        return;
      }
      const [id, callback] = first.value;
      callbacks.delete(id);
      callback(now);
    }
  };
}

type ProbeProps = {
  speed: number;
  playhead: string | null;
  timelineTicks: string[];
  setPlayhead: (playhead: string | null) => void;
  setPlaying: (isPlaying: boolean) => void;
};

function PlaybackProbe({ speed, playhead, timelineTicks, setPlayhead, setPlaying }: ProbeProps) {
  usePlaybackTicker({
    playback: {
      isPlaying: true,
      speed,
      playhead
    },
    timelineTicks,
    setPlayhead,
    setPlaying
  });

  return null;
}

describe("usePlaybackTicker", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  it("advances multiple ticks on a long frame", async () => {
    const raf = installRafHarness();
    vi.spyOn(performance, "now").mockReturnValue(0);

    const setPlayhead = vi.fn<(playhead: string | null) => void>();
    const setPlaying = vi.fn<(isPlaying: boolean) => void>();

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    const ticks = ["t0", "t1", "t2", "t3", "t4"];

    await act(async () => {
      root.render(
        <PlaybackProbe
          speed={16}
          playhead={"t0"}
          timelineTicks={ticks}
          setPlayhead={setPlayhead}
          setPlaying={setPlaying}
        />
      );
    });

    await act(async () => {
      raf.step(70);
    });

    expect(setPlayhead).toHaveBeenCalled();
    expect(setPlayhead).toHaveBeenLastCalledWith("t2");
    expect(setPlaying).not.toHaveBeenCalled();

    await act(async () => {
      root.unmount();
    });
  });

  it("caps catch-up work per frame to avoid runaway loops", async () => {
    const raf = installRafHarness();
    vi.spyOn(performance, "now").mockReturnValue(0);

    const setPlayhead = vi.fn<(playhead: string | null) => void>();
    const setPlaying = vi.fn<(isPlaying: boolean) => void>();

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    const ticks = Array.from({ length: 32 }, (_, index) => `t${index}`);

    await act(async () => {
      root.render(
        <PlaybackProbe
          speed={32}
          playhead={"t0"}
          timelineTicks={ticks}
          setPlayhead={setPlayhead}
          setPlaying={setPlaying}
        />
      );
    });

    await act(async () => {
      raf.step(1_000);
    });

    // Max eight steps per frame means first long frame reaches t8 from t0.
    expect(setPlayhead).toHaveBeenLastCalledWith("t8");

    await act(async () => {
      root.unmount();
    });
  });
});
