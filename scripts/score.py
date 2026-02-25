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


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Generate day -> alliance -> score index from daily alliance CSV archives."
	)
	parser.add_argument(
		"--alliances-dir",
		default=str(WEB_WORK_DATA_DIR / "alliance_downloads"),
		help="Directory containing alliances-YYYY-MM-DD.csv.zip files",
	)
	parser.add_argument(
		"--output-scores",
		default=str(WEB_PUBLIC_DATA_DIR / "alliance_scores_daily.msgpack"),
		help="Output MessagePack path for full day -> alliance -> score payload",
	)
	parser.add_argument(
		"--output-ranks",
		default=str(WEB_PUBLIC_DATA_DIR / "alliance_score_ranks_daily.msgpack"),
		help="Output MessagePack path for compact day -> alliance -> strict rank payload",
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
					# If duplicates are present in a source file, keep last seen row deterministically.
					day_scores[alliance_id] = round_score(score)
				return day_scores, rows_seen, rows_skipped
	except Exception as exc:
		raise ValueError(str(exc)) from exc


def build_scores_payload(files: list[Path]) -> tuple[dict[str, dict[str, float]], list[str], int, int, int]:
	scores_by_day: dict[str, dict[str, float]] = {}
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

		ordered_day_scores: dict[str, float] = {}
		for alliance_id in sorted(day_scores.keys()):
			ordered_day_scores[str(alliance_id)] = day_scores[alliance_id]

		scores_by_day[day] = ordered_day_scores
		records_written += len(ordered_day_scores)
		progress.step(records=records_written, skipped=len(skipped), done=index == total_files)
		print(
			f"[score] parsed {path.name}: rows={rows_seen}, skipped={rows_skipped}, kept={len(ordered_day_scores)}",
			file=sys.stderr,
		)

	ordered_days = dict(sorted(scores_by_day.items(), key=lambda item: item[0]))
	return ordered_days, skipped, records_written, rows_seen_total, rows_skipped_total


def build_ranks_payload(scores_by_day: dict[str, dict[str, float]]) -> dict[str, dict[str, int]]:
	ranks_by_day: dict[str, dict[str, int]] = {}
	for day, day_scores in scores_by_day.items():
		ranked = sorted(day_scores.items(), key=lambda item: (-item[1], int(item[0])))
		day_ranks: dict[str, int] = {}
		for index, (alliance_id, _) in enumerate(ranked, start=1):
			day_ranks[alliance_id] = index
		ranks_by_day[day] = day_ranks
	return ranks_by_day


def main() -> int:
	args = parse_args()
	alliances_dir = Path(args.alliances_dir).resolve()
	output_scores = Path(args.output_scores).resolve()
	if args.output:
		output_scores = Path(args.output).resolve()
	output_ranks = Path(args.output_ranks).resolve()

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

	scores_by_day, skipped_files, records_written, rows_seen_total, rows_skipped_total = build_scores_payload(files)
	ranks_by_day = build_ranks_payload(scores_by_day)
	scores_payload = {"schema_version": 1, "scores_by_day": scores_by_day}
	ranks_payload = {"schema_version": 1, "ranks_by_day": ranks_by_day}

	output_scores.parent.mkdir(parents=True, exist_ok=True)
	output_scores.write_bytes(msgpack.packb(scores_payload, use_bin_type=True))
	output_ranks.parent.mkdir(parents=True, exist_ok=True)
	output_ranks.write_bytes(msgpack.packb(ranks_payload, use_bin_type=True))

	print(
		f"[score] wrote scores={output_scores} and ranks={output_ranks} "
		f"({len(scores_by_day)} days, {records_written} records, {len(skipped_files)} skipped files, "
		f"{rows_seen_total} rows parsed, {rows_skipped_total} rows skipped)"
	)
	if skipped_files:
		print(f"[score] skipped files: {len(skipped_files)}", file=sys.stderr)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
