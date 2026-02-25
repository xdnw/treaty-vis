import { useMemo } from "react";
import { type PlaybackSpeed, useFilterStore } from "@/features/filters/filterStore";

type Props = {
  timelineTicks: string[];
  className?: string;
};

const PLAYBACK_SPEEDS: PlaybackSpeed[] = [1, 2, 4, 8, 16, 32];

export function PlaybackControls({ timelineTicks, className }: Props) {
  const playhead = useFilterStore((state) => state.query.playback.playhead);
  const isPlaying = useFilterStore((state) => state.query.playback.isPlaying);
  const speed = useFilterStore((state) => state.query.playback.speed);
  const setPlayhead = useFilterStore((state) => state.setPlayhead);
  const setPlaying = useFilterStore((state) => state.setPlaying);
  const setPlaybackSpeed = useFilterStore((state) => state.setPlaybackSpeed);

  const playheadIndex = useMemo(() => {
    if (timelineTicks.length === 0) {
      return 0;
    }
    if (!playhead) {
      return timelineTicks.length - 1;
    }
    const index = timelineTicks.findIndex((tick) => tick === playhead);
    return index >= 0 ? index : timelineTicks.length - 1;
  }, [playhead, timelineTicks]);

  return (
    <div className={className}>
      <h3 className="text-xs font-semibold uppercase text-slate-600">Playback</h3>
      <div className="mt-2 flex items-center gap-2">
        <button
          className="rounded-md border border-slate-300 px-2 py-1 text-sm hover:bg-slate-50"
          onClick={() => setPlaying(!isPlaying)}
          type="button"
        >
          {isPlaying ? "Pause" : "Play"}
        </button>
        <select
          className="rounded-md border border-slate-300 px-2 py-1 text-sm"
          value={speed}
          onChange={(event) => setPlaybackSpeed(Number(event.target.value) as PlaybackSpeed)}
        >
          {PLAYBACK_SPEEDS.map((nextSpeed) => (
            <option key={nextSpeed} value={nextSpeed}>
              {nextSpeed}x
            </option>
          ))}
        </select>
      </div>
      <input
        className="mt-2 w-full"
        type="range"
        min={0}
        max={Math.max(0, timelineTicks.length - 1)}
        step={1}
        value={playheadIndex}
        onChange={(event) => {
          const nextIndex = Number(event.target.value);
          const nextPlayhead = timelineTicks[nextIndex] ?? null;
          setPlayhead(nextPlayhead);
        }}
      />
      <div className="text-xs text-muted">Playhead: {playhead?.slice(0, 16) ?? "latest"}</div>
    </div>
  );
}
