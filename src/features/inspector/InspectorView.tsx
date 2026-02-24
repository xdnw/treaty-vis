import { useMemo, useRef, useState } from "react";
import { formatIsoDate } from "@/lib/format";
import type { TimelapseEvent } from "@/domain/timelapse/schema";

type Props = {
  events: TimelapseEvent[];
  onSelectPlayhead: (timestamp: string) => void;
  onFocusAlliance: (allianceId: number) => void;
};

const OVERSCAN = 10;
type InspectorSortField = "timestamp" | "action" | "type" | "from" | "to" | "source";
type InspectorSortDirection = "asc" | "desc";

const ACTION_LABELS: Record<string, string> = {
  signed: "Signed",
  cancelled: "Cancelled",
  expired: "Expired",
  ended: "Ended",
  extended: "Extended",
  inferred_cancelled: "Inferred Cancelled"
};

function compareEvents(left: TimelapseEvent, right: TimelapseEvent, field: InspectorSortField): number {
  if (field === "timestamp") {
    const ts = left.timestamp.localeCompare(right.timestamp);
    if (ts !== 0) {
      return ts;
    }
  } else if (field === "action") {
    const action = left.action.localeCompare(right.action);
    if (action !== 0) {
      return action;
    }
  } else if (field === "type") {
    const type = left.treaty_type.localeCompare(right.treaty_type);
    if (type !== 0) {
      return type;
    }
  } else if (field === "from") {
    const from = (left.from_alliance_name || String(left.from_alliance_id)).localeCompare(
      right.from_alliance_name || String(right.from_alliance_id)
    );
    if (from !== 0) {
      return from;
    }
  } else if (field === "to") {
    const to = (left.to_alliance_name || String(left.to_alliance_id)).localeCompare(
      right.to_alliance_name || String(right.to_alliance_id)
    );
    if (to !== 0) {
      return to;
    }
  } else {
    const source = (left.source || "unknown").localeCompare(right.source || "unknown");
    if (source !== 0) {
      return source;
    }
  }

  const id = left.event_id.localeCompare(right.event_id);
  if (id !== 0) {
    return id;
  }
  return left.timestamp.localeCompare(right.timestamp);
}

export function InspectorView({ events, onSelectPlayhead, onFocusAlliance }: Props) {
  const rowHeight = 38;
  const viewportHeight = 420;
  const bodyRef = useRef<HTMLDivElement | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [sortField, setSortField] = useState<InspectorSortField>("timestamp");
  const [sortDirection, setSortDirection] = useState<InspectorSortDirection>("desc");

  const sortedEvents = useMemo(() => {
    const direction = sortDirection === "asc" ? 1 : -1;
    return [...events].sort((left, right) => compareEvents(left, right, sortField) * direction);
  }, [events, sortDirection, sortField]);

  const totalHeight = sortedEvents.length * rowHeight;
  const visibleCount = Math.ceil(viewportHeight / rowHeight);

  const [startIndex, endIndex] = useMemo(() => {
    const rawStart = Math.floor(scrollTop / rowHeight);
    const start = Math.max(rawStart - OVERSCAN, 0);
    const end = Math.min(start + visibleCount + OVERSCAN * 2, sortedEvents.length);
    return [start, end];
  }, [rowHeight, scrollTop, sortedEvents.length, visibleCount]);

  const visibleRows = sortedEvents.slice(startIndex, endIndex);

  const toggleSort = (field: InspectorSortField) => {
    if (sortField === field) {
      setSortDirection((current) => (current === "asc" ? "desc" : "asc"));
      return;
    }
    setSortField(field);
    setSortDirection(field === "timestamp" ? "desc" : "asc");
  };

  const indicatorFor = (field: InspectorSortField): string => {
    if (sortField !== field) {
      return "";
    }
    return sortDirection === "asc" ? " ↑" : " ↓";
  };

  return (
    <section className="panel p-4">
      <header className="mb-3 flex items-center justify-between">
        <h2 className="text-lg">Event Inspector</h2>
        <span className="text-xs text-muted">{sortedEvents.length} scoped records</span>
      </header>

      <div
        ref={bodyRef}
        onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
        className="overflow-auto rounded-xl border border-slate-200"
        style={{ maxHeight: viewportHeight }}
      >
        <table className="w-full border-collapse text-sm" style={{ height: totalHeight }}>
          <thead className="sticky top-0 z-10 bg-slate-100 text-left text-xs uppercase text-slate-700">
            <tr>
              <th className="px-3 py-2">
                <button type="button" onClick={() => toggleSort("timestamp")}>Time{indicatorFor("timestamp")}</button>
              </th>
              <th className="px-3 py-2">
                <button type="button" onClick={() => toggleSort("action")}>Action{indicatorFor("action")}</button>
              </th>
              <th className="px-3 py-2">
                <button type="button" onClick={() => toggleSort("type")}>Type{indicatorFor("type")}</button>
              </th>
              <th className="px-3 py-2">
                <button type="button" onClick={() => toggleSort("from")}>From Alliance{indicatorFor("from")}</button>
              </th>
              <th className="px-3 py-2">
                <button type="button" onClick={() => toggleSort("to")}>To Alliance{indicatorFor("to")}</button>
              </th>
              <th className="px-3 py-2">
                <button type="button" onClick={() => toggleSort("source")}>Source{indicatorFor("source")}</button>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr style={{ height: startIndex * rowHeight }}>
              <td colSpan={6} />
            </tr>
            {visibleRows.map((event) => (
              <tr
                key={event.event_id}
                className="cursor-pointer border-t border-slate-100 hover:bg-slate-50"
                onClick={() => onSelectPlayhead(event.timestamp)}
                style={{ height: rowHeight }}
              >
                <td className="px-3 py-2 text-xs text-slate-600">{formatIsoDate(event.timestamp)}</td>
                <td className="px-3 py-2">
                  <span
                    className="badge"
                    style={{
                      backgroundColor:
                        event.action === "signed"
                          ? "rgba(43, 138, 62, 0.12)"
                          : "rgba(201, 42, 42, 0.12)",
                      color: event.action === "signed" ? "#2b8a3e" : "#c92a2a"
                    }}
                  >
                    {ACTION_LABELS[event.action] ?? event.action}
                  </span>
                </td>
                <td className="px-3 py-2">{event.treaty_type}</td>
                <td className="px-3 py-2">
                  <button
                    type="button"
                    className="text-left text-xs text-slate-700 underline-offset-2 hover:underline"
                    onClick={(tableEvent) => {
                      tableEvent.stopPropagation();
                      onFocusAlliance(event.from_alliance_id);
                    }}
                  >
                    {event.from_alliance_name || event.from_alliance_id}
                  </button>
                </td>
                <td className="px-3 py-2">
                  <button
                    type="button"
                    className="text-left text-xs text-slate-700 underline-offset-2 hover:underline"
                    onClick={(tableEvent) => {
                      tableEvent.stopPropagation();
                      onFocusAlliance(event.to_alliance_id);
                    }}
                  >
                    {event.to_alliance_name || event.to_alliance_id}
                  </button>
                </td>
                <td className="px-3 py-2">
                  <span className="text-xs text-slate-700">{event.source ?? "unknown"}</span>
                </td>
              </tr>
            ))}
            <tr style={{ height: Math.max(0, (sortedEvents.length - endIndex) * rowHeight) }}>
              <td colSpan={6} />
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  );
}
