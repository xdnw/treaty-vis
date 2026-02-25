import { useEffect } from "react";

type PlaybackState = {
  isPlaying: boolean;
  speed: number;
  playhead: string | null;
};

export function usePlaybackTicker(params: {
  playback: PlaybackState;
  timelineTicks: string[];
  setPlayhead: (playhead: string | null) => void;
  setPlaying: (isPlaying: boolean) => void;
}): void {
  const { playback, timelineTicks, setPlayhead, setPlaying } = params;

  useEffect(() => {
    if (!playback.isPlaying || timelineTicks.length === 0) {
      return;
    }

    let frame = 0;
    let last = performance.now();
    let carry = 0;
    const frameBudgetMs = 380 / playback.speed;
    const currentTickIndex = timelineTicks.findIndex((item) => item === playback.playhead);
    const currentIndex = currentTickIndex >= 0 ? currentTickIndex : Math.max(timelineTicks.length - 1, 0);
    let nextIndex = currentIndex;

    const tick = (now: number) => {
      const delta = now - last;
      last = now;
      carry += delta;

      if (carry >= frameBudgetMs) {
        carry = 0;
        nextIndex += 1;
        if (nextIndex >= timelineTicks.length) {
          setPlaying(false);
          return;
        }
        setPlayhead(timelineTicks[nextIndex]);
      }
      frame = requestAnimationFrame(tick);
    };

    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [playback.isPlaying, playback.playhead, playback.speed, setPlayhead, setPlaying, timelineTicks]);
}
