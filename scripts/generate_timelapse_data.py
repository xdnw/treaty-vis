#!/usr/bin/env python3
"""Generate reconciled treaty change events for timelapse playback.

This script merges two sources:
- treaties_archive.json (snapshot graph states; top-50 alliances only)
- treaties.json (parsed bot treaty events)

Key behaviors:
- Reconciles snapshot deltas with explicit event stream.
- Keeps treaty type EXTENSION distinct from action "extended".
- Infers missing treaty type for "extended" events from last active state.
- Optionally infers cancellations from expiry turns (2h per turn).
- Optionally infers cancellations when alliance membership drops to zero.
- Supports top-50 grounding filters and optional noise filtering.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import msgpack

from alliance_archives import prepare_alliance_archives
from manifest_utils import refresh_manifest
from script_paths import WEB_PUBLIC_DATA_DIR, WEB_WORK_DATA_DIR


HOURS_PER_TURN = 2
CANONICAL_TYPES = {
	"EXTENSION",
	"MDOAP",
	"MDP",
	"NAP",
	"NPT",
	"ODOAP",
	"ODP",
	"OFFSHORE",
	"PIAT",
	"PROTECTORATE",
}
TERMINAL_ACTIONS = {"cancelled", "expired", "ended", "terminated", "termination", "inferred_cancelled"}
TERMINAL_ACTION_ALIASES = {
	"terminated": "ended",
	"termination": "ended",
}
FRAME_INDEX_SCHEMA_VERSION = 1


@dataclass
class TreatyState:
	treaty_type: str
	opened_at: datetime
	expires_at: datetime | None
	last_event_id: str


@dataclass
class SnapshotState:
	timestamp: datetime
	node_ids: set[int]
	node_names: dict[int, str]
	edges: set[tuple[int, int, str]]


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Reconcile treaty events for timelapse")
	parser.add_argument("--archive-input", default=str(WEB_WORK_DATA_DIR / "treaties_archive.json"))
	parser.add_argument("--bot-input", default=str(WEB_WORK_DATA_DIR / "treaties.json"))
	parser.add_argument("--nations-dir", default="H:/Github/locutus/data/nations")
	parser.add_argument("--alliances-dir", default=str(WEB_WORK_DATA_DIR / "alliance_downloads"))
	parser.add_argument("--alliances-index-url", default="https://politicsandwar.com/data/alliances/")
	parser.add_argument(
		"--skip-alliance-download",
		action="store_true",
		help="Use only local alliances files; do not fetch missing archives from politicsandwar.com",
	)
	parser.add_argument("--output", default=str(WEB_PUBLIC_DATA_DIR / "treaty_changes_reconciled.msgpack"))
	parser.add_argument(
		"--summary-output",
		default=str(WEB_PUBLIC_DATA_DIR / "treaty_changes_reconciled_summary.msgpack"),
	)
	parser.add_argument(
		"--flags-output",
		default=str(WEB_PUBLIC_DATA_DIR / "treaty_changes_reconciled_flags.msgpack"),
	)
	parser.add_argument(
		"--frame-index-output",
		default=None,
	)
	parser.add_argument(
		"--top50-mode",
		choices=("off", "semi", "strict"),
		default="off",
		help="off: keep all, semi: at least one alliance grounded, strict: both grounded",
	)
	parser.add_argument("--infer-expiry-cancels", action="store_true")
	parser.add_argument("--infer-deletion-cancels", action="store_true")
	parser.add_argument("--deletion-confirmation-days", type=int, default=1)
	parser.add_argument("--filter-noise", action="store_true")
	parser.add_argument("--noise-window-hours", type=int, default=24)
	parser.add_argument("--keep-noise", action="store_true")
	parser.add_argument(
		"--collapse-churn",
		dest="collapse_churn",
		action="store_true",
		default=False,
		help="Collapse high-frequency bot signed/cancelled churn clusters.",
	)
	parser.add_argument(
		"--no-collapse-churn",
		dest="collapse_churn",
		action="store_false",
		help="Disable churn collapsing pass.",
	)
	parser.add_argument(
		"--churn-window-minutes",
		type=int,
		default=10,
		help="Time window for bot churn cluster detection.",
	)
	parser.add_argument(
		"--churn-min-events",
		type=int,
		default=20,
		help="Minimum events in a pair/type window before churn collapsing applies.",
	)
	parser.add_argument("--dry-run", action="store_true")
	return parser.parse_args()


def load_json(path: Path) -> Any:
	with path.open("r", encoding="utf-8") as handle:
		return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def write_msgpack(path: Path, payload: Any) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	packed = msgpack.packb(payload, use_bin_type=True)
	path.write_bytes(packed)


def parse_dt(raw: str) -> datetime:
	text = str(raw).strip()
	if text.endswith("Z"):
		text = text[:-1] + "+00:00"
	dt = datetime.fromisoformat(text)
	if dt.tzinfo is None:
		dt = dt.replace(tzinfo=timezone.utc)
	return dt.astimezone(timezone.utc)


def dt_to_iso(dt: datetime) -> str:
	return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def norm_pair(a: int, b: int) -> tuple[int, int]:
	return (a, b) if a <= b else (b, a)


def norm_treaty_type(raw: str | None) -> str:
	text = str(raw or "").strip().upper()
	if not text:
		return ""
	text = re.sub(r"\s*\|\s*RANK\s*#:?\s*\d+\s*$", "", text)
	text = re.sub(r"\s+", " ", text).strip()
	return text


def clean_alliance_name(raw: Any) -> str:
	text = re.sub(r"\s+", " ", str(raw or "")).strip()
	return text


def normalize_action(raw: Any) -> str:
	action = str(raw or "").strip().lower()
	if not action:
		return ""
	return TERMINAL_ACTION_ALIASES.get(action, action)


def event_id(payload: dict[str, Any]) -> str:
	blob = "|".join(
		[
			str(payload.get("timestamp", "")),
			str(payload.get("event_sequence", "")),
			str(payload.get("action", "")),
			str(payload.get("treaty_type", "")),
			str(payload.get("pair_min_id", "")),
			str(payload.get("pair_max_id", "")),
			str(payload.get("source", "")),
			str(payload.get("source_ref", "")),
		]
	)
	return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:20]


def load_archive_snapshots(path: Path) -> list[SnapshotState]:
	data = load_json(path)
	snapshots: list[SnapshotState] = []
	for snap in data.get("snapshots", []):
		ts = parse_dt(snap["date"])
		node_ids = {int(node["id"]) for node in snap.get("nodes", [])}
		node_names: dict[int, str] = {}
		for node in snap.get("nodes", []):
			try:
				aid = int(node.get("id"))
			except (TypeError, ValueError):
				continue
			name = clean_alliance_name(node.get("label") or node.get("title") or node.get("name"))
			if name:
				node_names[aid] = name
		edges: set[tuple[int, int, str]] = set()
		for edge in snap.get("edges", []):
			left = int(edge.get("from"))
			right = int(edge.get("to"))
			type_norm = norm_treaty_type(str(edge.get("title") or ""))
			pair = norm_pair(left, right)
			edges.add((pair[0], pair[1], type_norm))
		snapshots.append(SnapshotState(timestamp=ts, node_ids=node_ids, node_names=node_names, edges=edges))
	snapshots.sort(key=lambda s: s.timestamp)
	return snapshots


def build_archive_delta_events(snapshots: list[SnapshotState]) -> list[dict[str, Any]]:
	events: list[dict[str, Any]] = []
	if len(snapshots) < 2:
		return events

	def lookup_name(aid: int, current: SnapshotState, previous: SnapshotState) -> str:
		name = clean_alliance_name(current.node_names.get(aid, ""))
		if name:
			return name
		return clean_alliance_name(previous.node_names.get(aid, ""))

	for idx in range(1, len(snapshots)):
		prev = snapshots[idx - 1]
		curr = snapshots[idx]

		added = curr.edges - prev.edges
		removed = prev.edges - curr.edges

		for a, b, treaty_type in sorted(added):
			events.append(
				{
					"timestamp": dt_to_iso(curr.timestamp),
					"action": "signed",
					"treaty_type": treaty_type,
					"from_alliance_id": a,
					"from_alliance_name": lookup_name(a, curr, prev),
					"to_alliance_id": b,
					"to_alliance_name": lookup_name(b, curr, prev),
					"pair_min_id": a,
					"pair_max_id": b,
					"source": "archive_delta",
					"source_ref": f"snapshot:{idx}:add",
					"confidence": "medium",
					"inferred": True,
					"inference_reason": "archive_snapshot_diff_added",
					"time_remaining_turns": None,
				}
			)

		for a, b, treaty_type in sorted(removed):
			events.append(
				{
					"timestamp": dt_to_iso(curr.timestamp),
					"action": "cancelled",
					"treaty_type": treaty_type,
					"from_alliance_id": a,
					"from_alliance_name": lookup_name(a, curr, prev),
					"to_alliance_id": b,
					"to_alliance_name": lookup_name(b, curr, prev),
					"pair_min_id": a,
					"pair_max_id": b,
					"source": "archive_delta",
					"source_ref": f"snapshot:{idx}:remove",
					"confidence": "medium",
					"inferred": True,
					"inference_reason": "archive_snapshot_diff_removed",
					"time_remaining_turns": None,
				}
			)

	return events


def _candidate_types_for_pair(active: dict[tuple[int, int], dict[str, TreatyState]], pair: tuple[int, int]) -> list[str]:
	state = active.get(pair, {})
	return sorted(state.keys())


def load_bot_events(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
	records = load_json(path)
	events: list[dict[str, Any]] = []
	flags: list[dict[str, Any]] = []
	for idx, rec in enumerate(records):
		action = normalize_action(rec.get("action"))
		if not action:
			flags.append({"severity": "warning", "flag": "missing_action", "record_index": idx})
			continue
		from_id = int(rec.get("from_alliance_id"))
		to_id = int(rec.get("to_alliance_id"))
		pair = norm_pair(from_id, to_id)
		treaty_type = norm_treaty_type(rec.get("treaty_type"))
		time_turns = rec.get("time_remaining_turns")
		time_turns_norm = int(time_turns) if isinstance(time_turns, int) else None

		base = {
			"timestamp": dt_to_iso(parse_dt(rec["timestamp"])),
			"from_alliance_id": from_id,
			"from_alliance_name": clean_alliance_name(rec.get("from_alliance_name")),
			"to_alliance_id": to_id,
			"to_alliance_name": clean_alliance_name(rec.get("to_alliance_name")),
			"pair_min_id": pair[0],
			"pair_max_id": pair[1],
			"source": "bot",
			"source_ref": f"bot:{idx}",
			"confidence": "high",
			"inferred": False,
			"inference_reason": None,
			"time_remaining_turns": time_turns_norm,
		}

		if action in {"upgraded", "downgraded"}:
			if "->" not in treaty_type:
				flags.append(
					{
						"severity": "warning",
						"flag": "upgrade_without_arrow_type",
						"record_index": idx,
						"value": treaty_type,
					}
				)
				continue
			left, right = treaty_type.split("->", 1)
			old_type = norm_treaty_type(left)
			new_type = norm_treaty_type(right)
			events.append({**base, "action": "cancelled", "treaty_type": old_type})
			events.append({**base, "action": "signed", "treaty_type": new_type})
			continue

		events.append({**base, "action": action, "treaty_type": treaty_type})

	return events, flags


def load_alliance_zero_markers(
	alliances_dir: Path,
	confirmation_days: int,
	*,
	index_url: str,
	download_missing: bool,
	start_day: datetime | None,
	end_day: datetime | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
	markers: list[dict[str, Any]] = []
	files, flags = prepare_alliance_archives(
		alliances_dir=alliances_dir,
		index_url=index_url,
		download_missing=download_missing,
		start_day=start_day,
		end_day=end_day,
	)

	alliance_daily_presence: dict[int, set[datetime]] = defaultdict(set)
	all_days: set[datetime] = set()

	for path in files:
		match = re.search(r"alliances-(\d{4}-\d{2}-\d{2})\.csv\.zip$", path.name)
		if not match:
			continue
		day = datetime.fromisoformat(match.group(1)).replace(tzinfo=timezone.utc)
		all_days.add(day)

		try:
			with zipfile.ZipFile(path, "r") as zf:
				csv_names = [name for name in zf.namelist() if name.lower().endswith(".csv")]
				if not csv_names:
					flags.append(
						{
							"severity": "warning",
							"flag": "alliances_zip_missing_csv",
							"file": path.name,
						}
					)
					continue

				with zf.open(csv_names[0], "r") as zipped_file:
					text_stream = io.TextIOWrapper(zipped_file, encoding="utf-8", newline="")
					reader = csv.DictReader(text_stream)
					for row in reader:
						raw_id = row.get("alliance_id") or row.get("id") or row.get("allianceid")
						try:
							alliance_id = int(str(raw_id or "0"))
						except ValueError:
							continue
						if alliance_id > 0:
							alliance_daily_presence[alliance_id].add(day)
		except Exception as exc:
			flags.append(
				{
					"severity": "warning",
					"flag": "alliances_zip_read_failed",
					"file": path.name,
					"value": str(exc),
				}
			)

	ordered_days = sorted(all_days)
	if len(ordered_days) < 2:
		return markers, flags

	confirm = max(1, int(confirmation_days))
	for aid, present_days in alliance_daily_presence.items():
		if len(ordered_days) < (confirm + 1):
			continue
		for idx in range(1, len(ordered_days) - confirm + 1):
			prev_day = ordered_days[idx - 1]
			if prev_day not in present_days:
				continue
			window = ordered_days[idx : idx + confirm]
			if all(day not in present_days for day in window):
				ts = ordered_days[idx]
				markers.append(
					{
						"timestamp": dt_to_iso(ts),
						"action": "alliance_zero_members",
						"alliance_id": aid,
						"source": "alliances",
						"source_ref": f"alliances:{aid}:{ordered_days[idx].date()}",
					}
				)
				break

	markers.sort(key=lambda item: item["timestamp"])
	return markers, flags


def build_grounding_lookup(snapshots: list[SnapshotState]) -> list[tuple[datetime, set[int]]]:
	return [(snap.timestamp, snap.node_ids) for snap in snapshots]


def grounded_count(lookup: list[tuple[datetime, set[int]]], when: datetime, aid: int) -> bool:
	if not lookup:
		return False
	if when < lookup[0][0]:
		return False
	idx = 0
	for i, (ts, _) in enumerate(lookup):
		if ts <= when:
			idx = i
		else:
			break
	return aid in lookup[idx][1]


def should_keep_by_top50(mode: str, grounded_from: bool, grounded_to: bool) -> bool:
	if mode == "off":
		return True
	if mode == "semi":
		return grounded_from or grounded_to
	if mode == "strict":
		return grounded_from and grounded_to
	return True


def infer_type_if_needed(
	event: dict[str, Any],
	active: dict[tuple[int, int], dict[str, TreatyState]],
	flags: list[dict[str, Any]],
) -> str:
	action = normalize_action(event.get("action"))
	type_norm = norm_treaty_type(event.get("treaty_type"))
	if type_norm and type_norm != "UNKNOWN":
		return type_norm

	pair = (int(event["pair_min_id"]), int(event["pair_max_id"]))
	candidates = _candidate_types_for_pair(active, pair)
	if len(candidates) == 1 and action in {"extended", "cancelled", "expired", "ended"}:
		inferred = candidates[0]
		flags.append(
			{
				"severity": "info",
				"flag": "inferred_missing_treaty_type",
				"event_ref": event.get("source_ref"),
				"action": action,
				"inferred_type": inferred,
			}
		)
		return inferred

	if action == "extended":
		flags.append(
			{
				"severity": "warning",
				"flag": "extended_without_inferable_type",
				"event_ref": event.get("source_ref"),
				"candidate_types": candidates,
			}
		)
	return type_norm


def maybe_expiry_ts(base_ts: datetime, turns: int | None) -> datetime | None:
	if turns is None:
		return None
	if turns < 0:
		return None
	return base_ts + timedelta(hours=turns * HOURS_PER_TURN)


def reconcile_events(
	*,
	bot_events: list[dict[str, Any]],
	archive_delta_events: list[dict[str, Any]],
	alliance_zero_markers: list[dict[str, Any]],
	grounding_lookup: list[tuple[datetime, set[int]]],
	top50_mode: str,
	infer_expiry_cancels: bool,
	infer_deletion_cancels: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
	flags: list[dict[str, Any]] = []
	all_stream: list[dict[str, Any]] = []
	all_stream.extend(bot_events)
	all_stream.extend(archive_delta_events)
	if infer_deletion_cancels:
		all_stream.extend(alliance_zero_markers)

	def stream_sort_key(item: dict[str, Any]) -> tuple[datetime, int, str]:
		source = str(item.get("source") or "")
		priority = 0 if source == "bot" else (1 if source == "archive_delta" else 2)
		return parse_dt(item["timestamp"]), priority, str(item.get("source_ref") or "")

	all_stream.sort(key=stream_sort_key)

	active: dict[tuple[int, int], dict[str, TreatyState]] = defaultdict(dict)
	alliance_names: dict[int, str] = {}
	out: list[dict[str, Any]] = []
	next_event_sequence = 0

	def append_record(record: dict[str, Any]) -> None:
		nonlocal next_event_sequence
		record["event_sequence"] = next_event_sequence
		next_event_sequence += 1
		record["event_id"] = event_id(record)
		out.append(record)

	def remember_name(aid: int, name_raw: Any) -> None:
		name = clean_alliance_name(name_raw)
		if name:
			alliance_names[aid] = name

	def resolve_name(aid: int, preferred_raw: Any = None) -> str:
		preferred = clean_alliance_name(preferred_raw)
		if preferred:
			alliance_names[aid] = preferred
			return preferred
		return clean_alliance_name(alliance_names.get(aid, ""))

	def flush_expired(now: datetime) -> None:
		if not infer_expiry_cancels:
			return
		for pair, by_type in list(active.items()):
			for treaty_type, state in list(by_type.items()):
				if state.expires_at and state.expires_at <= now:
					grounded_from = grounded_count(grounding_lookup, state.expires_at, pair[0])
					grounded_to = grounded_count(grounding_lookup, state.expires_at, pair[1])
					keep = should_keep_by_top50(top50_mode, grounded_from, grounded_to)
					from_name = resolve_name(pair[0])
					to_name = resolve_name(pair[1])
					append_record(
						{
							"timestamp": dt_to_iso(state.expires_at),
							"action": "inferred_cancelled",
							"treaty_type": treaty_type,
							"from_alliance_id": pair[0],
							"from_alliance_name": from_name,
							"to_alliance_id": pair[1],
							"to_alliance_name": to_name,
							"pair_min_id": pair[0],
							"pair_max_id": pair[1],
							"source": "expiry_inferred",
							"source_ref": state.last_event_id,
							"confidence": "low",
							"inferred": True,
							"inference_reason": "time_remaining_elapsed_without_terminal_event",
							"time_remaining_turns": None,
							"grounded_from": grounded_from,
							"grounded_to": grounded_to,
							"grounded_keep": keep,
							"noise_filtered": False,
							"noise_reason": None,
						}
					)
					del by_type[treaty_type]
			if not by_type:
				del active[pair]

	for raw in all_stream:
		current_ts = parse_dt(raw["timestamp"])
		flush_expired(current_ts)
		if "from_alliance_id" in raw:
			remember_name(int(raw["from_alliance_id"]), raw.get("from_alliance_name"))
		if "to_alliance_id" in raw:
			remember_name(int(raw["to_alliance_id"]), raw.get("to_alliance_name"))

		if raw.get("action") == "alliance_zero_members":
			if not infer_deletion_cancels:
				continue
			aid = int(raw["alliance_id"])
			for pair, by_type in list(active.items()):
				if aid not in pair:
					continue
				for treaty_type in list(by_type.keys()):
					grounded_from = grounded_count(grounding_lookup, current_ts, pair[0])
					grounded_to = grounded_count(grounding_lookup, current_ts, pair[1])
					keep = should_keep_by_top50(top50_mode, grounded_from, grounded_to)
					from_name = resolve_name(pair[0])
					to_name = resolve_name(pair[1])
					append_record(
						{
							"timestamp": raw["timestamp"],
							"action": "inferred_cancelled",
							"treaty_type": treaty_type,
							"from_alliance_id": pair[0],
							"from_alliance_name": from_name,
							"to_alliance_id": pair[1],
							"to_alliance_name": to_name,
							"pair_min_id": pair[0],
							"pair_max_id": pair[1],
							"source": "deletion_inferred",
							"source_ref": raw.get("source_ref"),
							"confidence": "medium",
							"inferred": True,
							"inference_reason": "alliance_membership_zero",
							"time_remaining_turns": None,
							"grounded_from": grounded_from,
							"grounded_to": grounded_to,
							"grounded_keep": keep,
							"noise_filtered": False,
							"noise_reason": None,
						}
					)
					del by_type[treaty_type]
				if not by_type:
					del active[pair]
			continue

		action = normalize_action(raw.get("action"))
		pair = (int(raw["pair_min_id"]), int(raw["pair_max_id"]))
		type_norm = infer_type_if_needed(raw, active, flags)
		if not type_norm:
			flags.append(
				{
					"severity": "warning",
					"flag": "empty_treaty_type_after_inference",
					"event_ref": raw.get("source_ref"),
					"action": action,
				}
			)
			continue

		if type_norm == "UNKNOWN":
			flags.append(
				{
					"severity": "warning",
					"flag": "unknown_treaty_type_event",
					"event_ref": raw.get("source_ref"),
				}
			)

		if action == "signed":
			expires_at = maybe_expiry_ts(current_ts, raw.get("time_remaining_turns"))
			new_state = TreatyState(
				treaty_type=type_norm,
				opened_at=current_ts,
				expires_at=expires_at,
				last_event_id=str(raw.get("source_ref") or ""),
			)
			active[pair][type_norm] = new_state
		elif action == "extended":
			expires_at = maybe_expiry_ts(current_ts, raw.get("time_remaining_turns"))
			if type_norm not in active[pair]:
				active[pair][type_norm] = TreatyState(
					treaty_type=type_norm,
					opened_at=current_ts,
					expires_at=expires_at,
					last_event_id=str(raw.get("source_ref") or ""),
				)
			else:
				state = active[pair][type_norm]
				state.expires_at = expires_at
				state.last_event_id = str(raw.get("source_ref") or "")
		elif action in TERMINAL_ACTIONS:
			if type_norm in active[pair]:
				del active[pair][type_norm]
			if not active[pair]:
				del active[pair]

		grounded_from = grounded_count(grounding_lookup, current_ts, int(raw["from_alliance_id"]))
		grounded_to = grounded_count(grounding_lookup, current_ts, int(raw["to_alliance_id"]))
		keep = should_keep_by_top50(top50_mode, grounded_from, grounded_to)
		from_id = int(raw["from_alliance_id"])
		to_id = int(raw["to_alliance_id"])
		from_name = resolve_name(from_id, raw.get("from_alliance_name"))
		to_name = resolve_name(to_id, raw.get("to_alliance_name"))

		append_record(
			{
				"timestamp": raw["timestamp"],
				"action": action,
				"treaty_type": type_norm,
				"from_alliance_id": from_id,
				"from_alliance_name": from_name,
				"to_alliance_id": to_id,
				"to_alliance_name": to_name,
				"pair_min_id": pair[0],
				"pair_max_id": pair[1],
				"source": raw.get("source"),
				"source_ref": raw.get("source_ref"),
				"confidence": raw.get("confidence"),
				"inferred": bool(raw.get("inferred")),
				"inference_reason": raw.get("inference_reason"),
				"time_remaining_turns": raw.get("time_remaining_turns"),
				"grounded_from": grounded_from,
				"grounded_to": grounded_to,
				"grounded_keep": keep,
				"noise_filtered": False,
				"noise_reason": None,
			}
		)

	flush_expired(datetime.max.replace(tzinfo=timezone.utc))
	out.sort(
		key=lambda item: (
			parse_dt(item["timestamp"]),
			int(item.get("event_sequence", -1)),
			str(item.get("event_id")),
		)
	)
	return out, flags


def apply_noise_filter(
	events: list[dict[str, Any]],
	window_hours: int,
	keep_noise: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
	flags: list[dict[str, Any]] = []
	window = timedelta(hours=max(1, int(window_hours)))
	by_key: dict[tuple[int, int, str], list[int]] = defaultdict(list)

	for idx, ev in enumerate(events):
		key = (int(ev["pair_min_id"]), int(ev["pair_max_id"]), str(ev["treaty_type"]))
		by_key[key].append(idx)

	for key, idxs in by_key.items():
		for i in range(1, len(idxs)):
			prev_idx = idxs[i - 1]
			curr_idx = idxs[i]
			prev = events[prev_idx]
			curr = events[curr_idx]
			prev_action = str(prev.get("action"))
			curr_action = str(curr.get("action"))
			if {prev_action, curr_action} != {"signed", "cancelled"}:
				continue
			delta = parse_dt(curr["timestamp"]) - parse_dt(prev["timestamp"])
			if delta < timedelta(0) or delta > window:
				continue
			if prev.get("source") != "bot" or curr.get("source") != "bot":
				continue
			prev["noise_filtered"] = True
			curr["noise_filtered"] = True
			prev["noise_reason"] = f"opposite_action_within_{window_hours}h"
			curr["noise_reason"] = f"opposite_action_within_{window_hours}h"
			flags.append(
				{
					"severity": "info",
					"flag": "noise_pair_filtered",
					"pair": [key[0], key[1]],
					"treaty_type": key[2],
					"prev_event_id": prev.get("event_id"),
					"curr_event_id": curr.get("event_id"),
				}
			)

	if keep_noise:
		return events, flags
	filtered = [ev for ev in events if not ev.get("noise_filtered")]
	return filtered, flags


def apply_churn_collapse(
	events: list[dict[str, Any]],
	window_minutes: int,
	min_events: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
	"""Collapse high-frequency bot signed/cancelled churn for same pair+treaty type.

	This targets glitchy bursts where the feed rapidly alternates upgrades/downgrades,
	which expand into symmetric signed/cancelled floods after normalization.
	"""
	flags: list[dict[str, Any]] = []
	window = timedelta(minutes=max(1, int(window_minutes)))
	minimum = max(2, int(min_events))

	eligible: dict[tuple[int, int, str], list[int]] = defaultdict(list)
	for idx, ev in enumerate(events):
		if str(ev.get("source") or "") != "bot":
			continue
		action = str(ev.get("action") or "")
		if action not in {"signed", "cancelled"}:
			continue
		key = (int(ev["pair_min_id"]), int(ev["pair_max_id"]), str(ev["treaty_type"]))
		eligible[key].append(idx)

	remove_indexes: set[int] = set()

	for key, idxs in eligible.items():
		if len(idxs) < minimum:
			continue

		start = 0
		while start < len(idxs):
			end = start
			start_ts = parse_dt(events[idxs[start]]["timestamp"])
			while end + 1 < len(idxs):
				next_ts = parse_dt(events[idxs[end + 1]]["timestamp"])
				if next_ts - start_ts <= window:
					end += 1
				else:
					break

			cluster = idxs[start : end + 1]
			start = end + 1

			if len(cluster) < minimum:
				continue

			signed_idxs = [i for i in cluster if str(events[i].get("action")) == "signed"]
			cancelled_idxs = [i for i in cluster if str(events[i].get("action")) == "cancelled"]
			if not signed_idxs or not cancelled_idxs:
				continue

			timestamp_counts: dict[str, int] = defaultdict(int)
			for idx_in_cluster in cluster:
				timestamp_counts[str(events[idx_in_cluster].get("timestamp") or "")] += 1
			max_same_timestamp = max(timestamp_counts.values()) if timestamp_counts else 0
			unique_timestamps = len(timestamp_counts)
			# Require dense repeated timestamps to avoid collapsing broad legitimate activity.
			if max_same_timestamp < 4:
				continue
			if unique_timestamps > max(4, len(cluster) // 3):
				continue

			net = len(signed_idxs) - len(cancelled_idxs)
			if abs(net) > 2:
				# Preserve directional runs; collapse only near-zero churn loops.
				continue

			keep: set[int] = set()
			if net > 0:
				keep.update(signed_idxs[-net:])
			elif net < 0:
				keep.update(cancelled_idxs[-abs(net):])

			cluster_removed = [i for i in cluster if i not in keep]
			if not cluster_removed:
				continue

			remove_indexes.update(cluster_removed)
			first_ts = events[cluster[0]]["timestamp"]
			last_ts = events[cluster[-1]]["timestamp"]
			flags.append(
				{
					"severity": "info",
					"flag": "churn_cluster_collapsed",
					"pair": [key[0], key[1]],
					"treaty_type": key[2],
					"window_start": first_ts,
					"window_end": last_ts,
					"cluster_events": len(cluster),
					"signed_events": len(signed_idxs),
					"cancelled_events": len(cancelled_idxs),
					"max_same_timestamp_events": max_same_timestamp,
					"unique_timestamps": unique_timestamps,
					"removed_events": len(cluster_removed),
					"kept_events": len(cluster) - len(cluster_removed),
					"net_action_balance": net,
				}
			)

	if not remove_indexes:
		return events, flags

	filtered = [ev for idx, ev in enumerate(events) if idx not in remove_indexes]
	return filtered, flags


def summarize(events: list[dict[str, Any]], flags: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
	counts_by_action: dict[str, int] = defaultdict(int)
	counts_by_type: dict[str, int] = defaultdict(int)
	counts_by_source: dict[str, int] = defaultdict(int)
	for ev in events:
		counts_by_action[str(ev.get("action"))] += 1
		counts_by_type[str(ev.get("treaty_type"))] += 1
		counts_by_source[str(ev.get("source"))] += 1

	return {
		"generated_at": dt_to_iso(datetime.now(timezone.utc)),
		"parameters": {
			"archive_input": args.archive_input,
			"bot_input": args.bot_input,
			"nations_dir": args.nations_dir,
			"alliances_dir": args.alliances_dir,
			"alliances_index_url": args.alliances_index_url,
			"skip_alliance_download": bool(args.skip_alliance_download),
			"top50_mode": args.top50_mode,
			"infer_expiry_cancels": bool(args.infer_expiry_cancels),
			"infer_deletion_cancels": bool(args.infer_deletion_cancels),
			"collapse_churn": bool(args.collapse_churn),
			"churn_window_minutes": int(args.churn_window_minutes),
			"churn_min_events": int(args.churn_min_events),
			"filter_noise": bool(args.filter_noise),
			"noise_window_hours": int(args.noise_window_hours),
			"keep_noise": bool(args.keep_noise),
		},
		"events_total": len(events),
		"flags_total": len(flags),
		"counts_by_action": dict(sorted(counts_by_action.items())),
		"counts_by_type": dict(sorted(counts_by_type.items())),
		"counts_by_source": dict(sorted(counts_by_source.items())),
	}


def build_frame_index(events: list[dict[str, Any]]) -> dict[str, Any]:
	day_keys: list[str] = []
	event_end_offset_by_day: list[int] = []
	active_edge_delta_by_day: list[dict[str, list[int]]] = []
	edge_dict: list[list[Any]] = []

	active_by_pair: dict[tuple[int, int, str], int] = {}
	current_day: str | None = None
	day_add: set[int] = set()
	day_remove: set[int] = set()

	def flush_day(day: str, end_offset_exclusive: int) -> None:
		day_keys.append(day)
		event_end_offset_by_day.append(end_offset_exclusive)
		active_edge_delta_by_day.append(
			{
				"add_edge_ids": sorted(day_add),
				"remove_edge_ids": sorted(day_remove),
			}
		)

	for event_index, event in enumerate(events):
		day = str(event.get("timestamp") or "")[:10]
		if not day:
			continue

		if current_day is None:
			current_day = day
		elif day != current_day:
			flush_day(current_day, event_index)
			current_day = day
			day_add = set()
			day_remove = set()

		pair_min_id = int(event["pair_min_id"])
		pair_max_id = int(event["pair_max_id"])
		treaty_type = str(event["treaty_type"])
		key = (pair_min_id, pair_max_id, treaty_type)
		action = normalize_action(event.get("action"))

		if action == "signed":
			edge_id = len(edge_dict)
			edge_dict.append([event_index, pair_min_id, pair_max_id, treaty_type])
			previous = active_by_pair.get(key)
			if previous is not None:
				if previous in day_add:
					day_add.discard(previous)
				else:
					day_remove.add(previous)
			active_by_pair[key] = edge_id
			day_add.add(edge_id)
		elif action in TERMINAL_ACTIONS:
			previous = active_by_pair.pop(key, None)
			if previous is not None:
				if previous in day_add:
					day_add.discard(previous)
				else:
					day_remove.add(previous)

	if current_day is not None:
		flush_day(current_day, len(events))

	return {
		"schema_version": FRAME_INDEX_SCHEMA_VERSION,
		"day_keys": day_keys,
		"event_end_offset_by_day": event_end_offset_by_day,
		"edge_dict": edge_dict,
		"active_edge_delta_by_day": active_edge_delta_by_day,
		"top_membership_by_day": {},
	}


def main() -> int:
	args = parse_args()

	archive_snapshots = load_archive_snapshots(Path(args.archive_input))
	archive_delta_events = build_archive_delta_events(archive_snapshots)
	grounding_lookup = build_grounding_lookup(archive_snapshots)
	start_day = (
		archive_snapshots[0].timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
		if archive_snapshots
		else None
	)
	end_day = (
		archive_snapshots[-1].timestamp.replace(hour=23, minute=59, second=59, microsecond=999999)
		if archive_snapshots
		else None
	)

	bot_events, bot_flags = load_bot_events(Path(args.bot_input))
	zero_markers, nation_flags = load_alliance_zero_markers(
		Path(args.alliances_dir),
		confirmation_days=int(args.deletion_confirmation_days),
		index_url=str(args.alliances_index_url),
		download_missing=not bool(args.skip_alliance_download),
		start_day=start_day,
		end_day=end_day,
	)

	reconciled, reconcile_flags = reconcile_events(
		bot_events=bot_events,
		archive_delta_events=archive_delta_events,
		alliance_zero_markers=zero_markers,
		grounding_lookup=grounding_lookup,
		top50_mode=args.top50_mode,
		infer_expiry_cancels=bool(args.infer_expiry_cancels),
		infer_deletion_cancels=bool(args.infer_deletion_cancels),
	)

	if args.top50_mode != "off":
		reconciled = [item for item in reconciled if bool(item.get("grounded_keep"))]

	extra_flags: list[dict[str, Any]] = []
	if bool(args.collapse_churn):
		reconciled, churn_flags = apply_churn_collapse(
			events=reconciled,
			window_minutes=int(args.churn_window_minutes),
			min_events=int(args.churn_min_events),
		)
		extra_flags.extend(churn_flags)

	if args.filter_noise:
		reconciled, noise_flags = apply_noise_filter(
			events=reconciled,
			window_hours=int(args.noise_window_hours),
			keep_noise=bool(args.keep_noise),
		)
		extra_flags.extend(noise_flags)

	all_flags = bot_flags + nation_flags + reconcile_flags + extra_flags
	summary = summarize(reconciled, all_flags, args)
	default_reconciled_output = Path(WEB_PUBLIC_DATA_DIR / "treaty_changes_reconciled.msgpack").resolve()
	output_path = Path(args.output).resolve()
	should_write_frame_index = bool(args.frame_index_output) or output_path == default_reconciled_output
	frame_index_output = (
		Path(args.frame_index_output).resolve()
		if args.frame_index_output
		else (WEB_PUBLIC_DATA_DIR / "treaty_frame_index_v1.msgpack").resolve()
	)
	frame_index = build_frame_index(reconciled) if should_write_frame_index else None

	if args.dry_run:
		print(f"Dry run complete. Events: {len(reconciled)}")
		print(f"Flags: {len(all_flags)}")
		print(json.dumps(summary, indent=2, ensure_ascii=True))
		return 0

	write_msgpack(Path(args.output), reconciled)
	write_msgpack(Path(args.summary_output), summary)
	write_msgpack(Path(args.flags_output), all_flags)
	if frame_index is not None:
		write_msgpack(frame_index_output, frame_index)
	manifest = refresh_manifest()

	print(f"Wrote reconciled events: {args.output}")
	print(f"Wrote summary: {args.summary_output}")
	print(f"Wrote flags: {args.flags_output}")
	if frame_index is not None:
		print(f"Wrote frame index: {frame_index_output}")
	print(f"Updated manifest: {WEB_PUBLIC_DATA_DIR / 'manifest.json'}")
	print(f"Dataset ID: {manifest.get('datasetId')}")
	print(f"Events total: {len(reconciled)}")
	print(f"Flags total: {len(all_flags)}")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
