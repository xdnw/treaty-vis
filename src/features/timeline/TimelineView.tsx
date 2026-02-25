import { useEffect, useMemo, useRef } from "react";
import ReactECharts from "echarts-for-react";
import type { PulsePoint } from "@/domain/timelapse/selectors";
import { dayToEndIso, dayToStartIso } from "@/lib/dateTime";

type Props = {
  pulse: PulsePoint[];
  playhead: string | null;
  timeRange: {
    start: string | null;
    end: string | null;
  };
  onSetPlayhead: (playhead: string | null) => void;
  onSetRange: (start: string | null, end: string | null) => void;
};

export function TimelineView({ pulse, playhead, timeRange, onSetPlayhead, onSetRange }: Props) {
  const pendingZoomRef = useRef<{ startValue: number; endValue: number } | null>(null);
  const zoomFrameRef = useRef<number | null>(null);
  const lastRangeRef = useRef<{ start: string | null; end: string | null }>({
    start: timeRange.start,
    end: timeRange.end
  });

  useEffect(() => {
    lastRangeRef.current = {
      start: timeRange.start,
      end: timeRange.end
    };
  }, [timeRange.end, timeRange.start]);

  useEffect(() => {
    return () => {
      if (zoomFrameRef.current !== null) {
        cancelAnimationFrame(zoomFrameRef.current);
      }
    };
  }, []);

  const option = useMemo(() => {
    const days = pulse.map((point) => point.day);
    const signedSeries = pulse.map((point) => point.signed);
    const terminalSeries = pulse.map((point) => point.terminal);
    const inferredSeries = pulse.map((point) => point.inferred);
    const playheadDay = playhead ? playhead.slice(0, 10) : null;

    const startDay = timeRange.start ? timeRange.start.slice(0, 10) : null;
    const endDay = timeRange.end ? timeRange.end.slice(0, 10) : null;
    const startIndex = startDay ? Math.max(days.findIndex((day) => day >= startDay), 0) : 0;
    const endIndex = endDay
      ? (() => {
          const found = days.findIndex((day) => day >= endDay);
          return found === -1 ? Math.max(days.length - 1, 0) : found;
        })()
      : Math.max(days.length - 1, 0);

    return {
      animationDuration: 500,
      tooltip: { trigger: "axis" },
      legend: { top: 0 },
      grid: { left: 50, right: 20, top: 42, bottom: 40 },
      xAxis: {
        type: "category",
        data: days,
        axisLabel: { color: "#5a677d" }
      },
      yAxis: {
        type: "value",
        axisLabel: { color: "#5a677d" }
      },
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: [0],
          startValue: startIndex,
          endValue: endIndex
        },
        {
          type: "slider",
          xAxisIndex: [0],
          height: 20,
          bottom: 8,
          startValue: startIndex,
          endValue: endIndex
        }
      ],
      series: [
        {
          name: "Signed",
          type: "line",
          smooth: true,
          areaStyle: { opacity: 0.15 },
          lineStyle: { width: 2, color: "#2b8a3e" },
          data: signedSeries,
          markLine: playheadDay
            ? {
                silent: true,
                symbol: "none",
                lineStyle: {
                  color: "#364fc7",
                  width: 2,
                  type: "dashed"
                },
                data: [{ xAxis: playheadDay }]
              }
            : undefined
        },
        {
          name: "Terminal",
          type: "line",
          smooth: true,
          areaStyle: { opacity: 0.15 },
          lineStyle: { width: 2, color: "#c92a2a" },
          data: terminalSeries
        },
        {
          name: "Inferred",
          type: "bar",
          barMaxWidth: 12,
          itemStyle: { color: "#e67700", opacity: 0.7 },
          data: inferredSeries
        }
      ]
    };
  }, [playhead, pulse, timeRange.end, timeRange.start]);

  const onEvents = useMemo(
    () => ({
      click: (params: { name?: string }) => {
        if (!params.name) {
          return;
        }
        onSetPlayhead(dayToEndIso(params.name));
      },
      datazoom: (params: {
        batch?: Array<{ startValue?: number; endValue?: number }>;
        startValue?: number;
        endValue?: number;
      }) => {
        const zoom = params.batch?.[0] ?? params;
        pendingZoomRef.current = {
          startValue: zoom.startValue ?? 0,
          endValue: zoom.endValue ?? pulse.length - 1
        };

        if (zoomFrameRef.current !== null) {
          return;
        }

        zoomFrameRef.current = requestAnimationFrame(() => {
          zoomFrameRef.current = null;
          const pending = pendingZoomRef.current;
          pendingZoomRef.current = null;
          if (!pending) {
            return;
          }

          const safeStartIndex = Math.max(0, Math.min(pending.startValue, pulse.length - 1));
          const safeEndIndex = Math.max(0, Math.min(pending.endValue, pulse.length - 1));
          const startDay = pulse[safeStartIndex]?.day;
          const endDay = pulse[safeEndIndex]?.day;
          const nextStart = startDay ? dayToStartIso(startDay) : null;
          const nextEnd = endDay ? dayToEndIso(endDay) : null;

          if (lastRangeRef.current.start === nextStart && lastRangeRef.current.end === nextEnd) {
            return;
          }
          lastRangeRef.current = { start: nextStart, end: nextEnd };
          onSetRange(nextStart, nextEnd);
        });
      }
    }),
    [onSetPlayhead, onSetRange, pulse]
  );

  return (
    <section className="panel p-4">
      <header className="mb-2 flex items-center justify-between">
        <h2 className="text-lg">Temporal Pulse</h2>
        <span className="text-xs text-muted">Click to set playhead, drag to set time window</span>
      </header>
      <ReactECharts option={option} onEvents={onEvents} style={{ height: 340 }} notMerge lazyUpdate />
    </section>
  );
}
