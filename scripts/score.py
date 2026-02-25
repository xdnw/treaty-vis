from __future__ import annotations

import argparse
import csv
import io
import json
import msgpack
import sys
import zipfile
from pathlib import Path
from typing import Any

from alliance_archives import DEFAULT_ALLIANCES_INDEX_URL, parse_day_from_filename, prepare_alliance_archives
from progress import ProgressReporter
from script_paths import WEB_PUBLIC_DATA_DIR, WEB_WORK_DATA_DIR


ID_KEYS = ("alliance_id", "id", "allianceid")
SCORE_KEYS = ("score", "alliance_score", "alliancescore", "total_score", "totalscore")

EXPECTED_ID_KEYS = {"id", "allianceid"}
EXPECTED_SCORE_KEYS = {"score", "alliancescore", "totalscore"}

SCORE_SCHEMA_VERSION = 2
RANK_SCHEMA_VERSION = 2
FRAME_INDEX_SCHEMA_VERSION = 1
DEFAULT_QUANTIZATION_SCALE = 1000
DEFAULT_MAX_SCORE_BYTES = 10 * 1024 * 1024
DEFAULT_MAX_FRAME_INDEX_BYTES = 25 * 1024 * 1024
DEFAULT_TOP_MEMBERSHIP_VALUES = (10, 25, 50, 100)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Generate compact v2 alliance score and rank artifacts from daily alliance CSV archives."
	)
	parser.add_argument(
		"--alliances-dir",
		default=str(WEB_WORK_DATA_DIR / "alliance_downloads"),
		help="Directory containing alliances-YYYY-MM-DD.csv.zip files",
	)
	parser.add_argument(
		"--output-scores",
		default=str(WEB_PUBLIC_DATA_DIR / "alliance_scores_v2.msgpack"),
		help="Output MessagePack path for compact v2 score payload",
	)
	parser.add_argument(
		"--output-ranks",
		default=str(WEB_PUBLIC_DATA_DIR / "alliance_score_ranks_daily.msgpack"),
		help="Output MessagePack path for day -> alliance -> rank payload",
	)
	parser.add_argument(
		"--output",
		default=None,
		help="Deprecated alias for --output-scores; when provided it overrides --output-scores",
	)
	parser.add_argument(
		"--alliances-index-url",
		default=DEFAULT_ALLIANCES_INDEX_URL,
		help="Index URL for alliance archives (used when downloading missing files)",
	)
	parser.add_argument(
		"--download-missing-alliance-archives",
		action="store_true",
		help="Download missing alliances-YYYY-MM-DD.csv.zip files before processing",
	)
	parser.add_argument(
		"--quantization-scale",
		type=int,
		default=DEFAULT_QUANTIZATION_SCALE,
		help="Score quantization scale (stored_value = round(score * scale))",
	)
	parser.add_argument(
		"--max-score-bytes",
		type=int,
		default=DEFAULT_MAX_SCORE_BYTES,
		help="Fail if v2 score artifact exceeds this many bytes",
	)
	parser.add_argument(
		"--output-frame-index",
		default=str(WEB_PUBLIC_DATA_DIR / "treaty_frame_index_v1.msgpack"),
		help="Output MessagePack path for schema-versioned treaty frame index artifact",
	)
	parser.add_argument(
		"--events-input",
		default=str(WEB_PUBLIC_DATA_DIR / "treaty_changes_reconciled.msgpack"),
		help="Input MessagePack path for reconciled treaty events used when bootstrapping frame index",
	)
	parser.add_argument(
		"--top-membership-values",
		default=",".join(str(value) for value in DEFAULT_TOP_MEMBERSHIP_VALUES),
		help="Comma-separated top-X values to materialize membership rows for (e.g. 10,25,50,100)",
	)
	parser.add_argument(
		"--max-frame-index-bytes",
		type=int,
		default=DEFAULT_MAX_FRAME_INDEX_BYTES,
		help="Fail if frame-index artifact exceeds this many bytes",
	)
	parser.add_argument(
		"--pretty",
		action="store_true",
		help="Deprecated no-op retained for CLI compatibility",
	)
	return parser.parse_args()


def normalize_header(value: Any) -> str:
	if value is None:
		return ""
	text = str(value).replace("\ufeff", "").strip().lower()
	return "".join(ch for ch in text if ch.isalnum())


def build_row_lookup(row: dict[str, Any]) -> dict[str, Any]:
	lookup: dict[str, Any] = {}
	for key, value in row.items():
		normalized = normalize_header(key)
		if normalized and normalized not in lookup:
			lookup[normalized] = value
	return lookup


def first_present(row_lookup: dict[str, Any], keys: tuple[str, ...]) -> Any:
	for key in keys:
		normalized = normalize_header(key)
		if normalized in row_lookup:
			return row_lookup.get(normalized)
	return None


def parse_alliance_id(raw_id: Any) -> int | None:
	try:
		alliance_id = int(str(raw_id or "0").strip())
	except ValueError:
		return None
	return alliance_id if alliance_id > 0 else None


def parse_score(raw_score: Any) -> float | None:
	if raw_score is None:
		return None
	text = str(raw_score).strip()
	if not text:
		return None
	text = text.replace(",", "")
	try:
		value = float(text)
	except ValueError:
		return None
	if value < 0:
		return None
	return value


def round_score(value: float) -> float:
	return round(value, 6)


def quantize_score(value: float, scale: int) -> int:
	return max(0, int(round(value * scale)))


def read_csv_header_keys(archive: zipfile.ZipFile, csv_name: str) -> set[str]:
	with archive.open(csv_name, "r") as zip_stream:
		text_stream = io.TextIOWrapper(zip_stream, encoding="utf-8", newline="")
		reader = csv.reader(text_stream)
		header = next(reader, None)
		if header is None:
			return set()
		return {normalize_header(name) for name in header if normalize_header(name)}


def pick_best_csv_name(archive: zipfile.ZipFile, csv_names: list[str]) -> str:
	scored: list[tuple[int, int, str]] = []
	for csv_name in csv_names:
		headers = read_csv_header_keys(archive, csv_name)
		has_id = bool(headers & EXPECTED_ID_KEYS)
		has_score = bool(headers & EXPECTED_SCORE_KEYS)
		match_score = (2 if has_id else 0) + (2 if has_score else 0)
		scored.append((match_score, len(headers), csv_name))

	if not scored:
		return csv_names[0]

	scored.sort(key=lambda item: (item[0], item[1], item[2]))
	return scored[-1][2]


def load_day_scores(path: Path) -> tuple[dict[int, float], int, int]:
	try:
		with zipfile.ZipFile(path, "r") as archive:
			csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
			if not csv_names:
				raise ValueError("zip missing csv")

			chosen_csv = pick_best_csv_name(archive, csv_names)

			with archive.open(chosen_csv, "r") as zip_stream:
				text_stream = io.TextIOWrapper(zip_stream, encoding="utf-8", newline="")
				reader = csv.DictReader(text_stream)
				day_scores: dict[int, float] = {}
				rows_seen = 0
				rows_skipped = 0
				for row in reader:
					rows_seen += 1
					row_lookup = build_row_lookup(row)
					alliance_id = parse_alliance_id(first_present(row_lookup, ID_KEYS))
					score = parse_score(first_present(row_lookup, SCORE_KEYS))
					if alliance_id is None or score is None:
						rows_skipped += 1
						continue
					# Keep last-seen duplicate rows deterministically.
					day_scores[alliance_id] = round_score(score)
				return day_scores, rows_seen, rows_skipped
	except Exception as exc:
		raise ValueError(str(exc)) from exc


def build_score_and_rank_payloads(
	files: list[Path],
	quantization_scale: int,
) -> tuple[dict[str, Any], dict[str, Any], list[str], int, int, int]:
	score_rows_by_day: dict[str, list[list[int]]] = {}
	ranks_by_day: dict[str, dict[str, int]] = {}
	skipped: list[str] = []
	records_written = 0
	rows_seen_total = 0
	rows_skipped_total = 0
	total_files = len(files)
	progress = ProgressReporter(label="[score] file progress", total=total_files, unit="files", non_tty_every=50)

	for index, path in enumerate(files, start=1):
		day_dt = parse_day_from_filename(path)
		if day_dt is None:
			skipped.append(f"{path.name}:bad_filename")
			progress.step(records=records_written, skipped=len(skipped), done=index == total_files)
			continue
		day = day_dt.date().isoformat()

		try:
			day_scores, rows_seen, rows_skipped = load_day_scores(path)
		except ValueError as exc:
			skipped.append(f"{path.name}:read_error:{exc}")
			progress.step(records=records_written, skipped=len(skipped), done=index == total_files)
			continue

		rows_seen_total += rows_seen
		rows_skipped_total += rows_skipped

		ordered_ids = sorted(day_scores.keys())
		ranked = sorted(ordered_ids, key=lambda alliance_id: (-day_scores[alliance_id], alliance_id))
		day_ranks: dict[str, int] = {}
		for rank, alliance_id in enumerate(ranked, start=1):
			day_ranks[str(alliance_id)] = rank
		ranks_by_day[day] = day_ranks

		quantized_row: list[list[int]] = []
		for alliance_id in ordered_ids:
			quantized = quantize_score(day_scores[alliance_id], quantization_scale)
			if quantized <= 0:
				continue
			quantized_row.append([alliance_id, quantized])

		if quantized_row:
			score_rows_by_day[day] = quantized_row
			records_written += len(quantized_row)

		progress.step(records=records_written, skipped=len(skipped), done=index == total_files)
		print(
			f"[score] parsed {path.name}: rows={rows_seen}, skipped={rows_skipped}, kept={len(quantized_row)}",
			file=sys.stderr,
		)

	sorted_days = sorted(score_rows_by_day.keys())
	score_payload = {
		"schema_version": SCORE_SCHEMA_VERSION,
		"quantization_scale": quantization_scale,
		"day_keys": sorted_days,
		"days": [score_rows_by_day[day] for day in sorted_days],
	}
	ranks_payload = {
		"schema_version": RANK_SCHEMA_VERSION,
		"ranks_by_day": dict(sorted(ranks_by_day.items(), key=lambda item: item[0])),
	}
	return score_payload, ranks_payload, skipped, records_written, rows_seen_total, rows_skipped_total


def parse_top_membership_values(raw: str) -> list[int]:
	values: set[int] = set()
	for chunk in str(raw).split(","):
		text = chunk.strip()
		if not text:
			continue
		parsed = int(text)
		if parsed <= 0:
			continue
		values.add(parsed)
	return sorted(values)


def build_top_membership_by_day(ranks_by_day: dict[str, dict[str, int]], top_values: list[int]) -> dict[str, dict[str, list[int]]]:
	top_membership_by_day: dict[str, dict[str, list[int]]] = {}
	if not top_values:
		return top_membership_by_day

	for top_value in top_values:
		day_membership: dict[str, list[int]] = {}
		for day, rank_row in ranks_by_day.items():
			members = [
				int(alliance_id)
				for alliance_id, rank in rank_row.items()
				if int(rank) <= top_value and int(alliance_id) > 0
			]
			if members:
				members.sort()
				day_membership[day] = members
		top_membership_by_day[str(top_value)] = day_membership

	return top_membership_by_day


def normalize_action(raw: Any) -> str:
	action = str(raw or "").strip().lower()
	if action in {"terminated", "termination"}:
		return "ended"
	return action


def build_frame_index_from_events(events: list[dict[str, Any]]) -> dict[str, Any]:
	terminal_actions = {"cancelled", "expired", "ended", "inferred_cancelled"}
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

		pair_min = int(event["pair_min_id"])
		pair_max = int(event["pair_max_id"])
		treaty_type = str(event["treaty_type"])
		key = (pair_min, pair_max, treaty_type)
		action = normalize_action(event.get("action"))

		if action == "signed":
			edge_id = len(edge_dict)
			edge_dict.append([event_index, pair_min, pair_max, treaty_type])
			previous = active_by_pair.get(key)
			if previous is not None:
				if previous in day_add:
					day_add.discard(previous)
				else:
					day_remove.add(previous)
			active_by_pair[key] = edge_id
			day_add.add(edge_id)
		elif action in terminal_actions:
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


def load_frame_index(path: Path) -> dict[str, Any] | None:
	if not path.exists():
		return None
	try:
		payload = msgpack.unpackb(path.read_bytes(), raw=False)
	except Exception:
		return None
	if not isinstance(payload, dict):
		return None
	if int(payload.get("schema_version") or -1) != FRAME_INDEX_SCHEMA_VERSION:
		return None
	day_keys = payload.get("day_keys")
	end_offsets = payload.get("event_end_offset_by_day")
	deltas = payload.get("active_edge_delta_by_day")
	edges = payload.get("edge_dict")
	if not isinstance(day_keys, list) or not isinstance(end_offsets, list) or not isinstance(deltas, list) or not isinstance(edges, list):
		return None
	if len(day_keys) != len(end_offsets) or len(day_keys) != len(deltas):
		return None
	return payload


def main() -> int:
	args = parse_args()
	if args.quantization_scale <= 0:
		raise ValueError("--quantization-scale must be > 0")
	if args.max_score_bytes <= 0:
		raise ValueError("--max-score-bytes must be > 0")
	if args.max_frame_index_bytes <= 0:
		raise ValueError("--max-frame-index-bytes must be > 0")

	alliances_dir = Path(args.alliances_dir).resolve()
	output_scores = Path(args.output_scores).resolve()
	if args.output:
		output_scores = Path(args.output).resolve()
	output_ranks = Path(args.output_ranks).resolve()
	output_frame_index = Path(args.output_frame_index).resolve()
	events_input = Path(args.events_input).resolve()
	top_membership_values = parse_top_membership_values(args.top_membership_values)

	files, archive_flags = prepare_alliance_archives(
		alliances_dir=alliances_dir,
		index_url=str(args.alliances_index_url),
		download_missing=bool(args.download_missing_alliance_archives),
	)
	for flag in archive_flags:
		severity = str(flag.get("severity") or "info").lower()
		if severity == "warning":
			print(f"[score] warning: {json.dumps(flag, ensure_ascii=True)}", file=sys.stderr)
		else:
			print(f"[score] info: {json.dumps(flag, ensure_ascii=True)}", file=sys.stderr)

	scores_payload, ranks_payload, skipped_files, records_written, rows_seen_total, rows_skipped_total = (
		build_score_and_rank_payloads(files, args.quantization_scale)
	)

	scores_bytes = msgpack.packb(scores_payload, use_bin_type=True)
	if len(scores_bytes) > args.max_score_bytes:
		raise ValueError(
			f"Score artifact too large: {len(scores_bytes)} bytes > max {args.max_score_bytes} bytes. "
			"Tune quantization or prune source coverage."
		)
	ranks_bytes = msgpack.packb(ranks_payload, use_bin_type=True)
	top_membership_by_day = build_top_membership_by_day(ranks_payload["ranks_by_day"], top_membership_values)

	output_scores.parent.mkdir(parents=True, exist_ok=True)
	output_scores.write_bytes(scores_bytes)
	output_ranks.parent.mkdir(parents=True, exist_ok=True)
	output_ranks.write_bytes(ranks_bytes)

	frame_index_payload = load_frame_index(output_frame_index)
	if frame_index_payload is None and events_input.exists():
		events_payload = msgpack.unpackb(events_input.read_bytes(), raw=False)
		if isinstance(events_payload, list):
			frame_index_payload = build_frame_index_from_events(events_payload)

	if frame_index_payload is not None:
		frame_index_payload["top_membership_by_day"] = top_membership_by_day
		frame_index_bytes = msgpack.packb(frame_index_payload, use_bin_type=True)
		if len(frame_index_bytes) > args.max_frame_index_bytes:
			raise ValueError(
				f"Frame-index artifact too large: {len(frame_index_bytes)} bytes > max {args.max_frame_index_bytes} bytes."
			)
		output_frame_index.parent.mkdir(parents=True, exist_ok=True)
		output_frame_index.write_bytes(frame_index_bytes)
		print(
			f"[score] wrote frame index={output_frame_index} ({len(frame_index_bytes)} bytes) "
			f"with top-membership sets for {len(top_membership_values)} top-X values"
		)
 

	print(
		f"[score] wrote scores={output_scores} ({len(scores_bytes)} bytes) and ranks={output_ranks} ({len(ranks_bytes)} bytes) "
		f"({len(scores_payload['day_keys'])} days, {records_written} records, {len(skipped_files)} skipped files, "
		f"{rows_seen_total} rows parsed, {rows_skipped_total} rows skipped)"
	)
	if skipped_files:
		print(f"[score] skipped files: {len(skipped_files)}", file=sys.stderr)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
