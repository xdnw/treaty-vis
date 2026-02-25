from __future__ import annotations

import argparse
import concurrent.futures
import csv
import email.utils
import hashlib
import io
import json
import math
import os
import random
import socket
import sys
import threading
import time
import re
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import msgpack

try:
    from PIL import Image
except Exception as exc:  # pragma: no cover - environment dependent
    Image = None
    PIL_IMPORT_ERROR = exc
else:
    PIL_IMPORT_ERROR = None

from alliance_archives import DEFAULT_ALLIANCES_INDEX_URL, parse_day_from_filename, prepare_alliance_archives
from progress import ProgressReporter
from script_paths import WEB_PUBLIC_DATA_DIR, WEB_WORK_DATA_DIR


ID_KEYS = ("alliance_id", "id", "allianceid")
NAME_KEYS = ("alliance_name", "name", "alliance")
FLAG_KEYS = ("flag", "alliance_flag", "flag_url", "allianceflag")

EXPECTED_ID_KEYS = {"id", "allianceid"}
EXPECTED_NAME_KEYS = {"name", "alliancename", "alliance"}
EXPECTED_FLAG_KEYS = {"flag", "allianceflag", "flagurl"}

DEFAULT_ARCHIVE_CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
DEFAULT_ARCHIVE_REPLAY_PREFIX = "https://web.archive.org/web"
DEFAULT_LEGACY_FLAGS_PATH = Path("data") / "legacy_flags.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate alliance flag timeline from daily alliance CSV archives."
    )
    parser.add_argument(
        "--alliances-dir",
        default=str(WEB_WORK_DATA_DIR / "alliance_downloads"),
        help="Directory containing alliances-YYYY-MM-DD.csv.zip files",
    )
    parser.add_argument(
        "--output",
        default=str(WEB_PUBLIC_DATA_DIR / "flags.msgpack"),
        help="Output MessagePack path",
    )
    parser.add_argument(
        "--assets-output",
        default=str(WEB_PUBLIC_DATA_DIR / "flag_assets.msgpack"),
        help="Output MessagePack path for atlas metadata and per-flag assets",
    )
    parser.add_argument(
        "--atlas-webp-output",
        default=str(WEB_PUBLIC_DATA_DIR / "flag_atlas.webp"),
        help="Output path for WEBP atlas image",
    )
    parser.add_argument(
        "--atlas-png-output",
        default=str(WEB_PUBLIC_DATA_DIR / "flag_atlas.png"),
        help="Output path for PNG atlas image",
    )
    parser.add_argument("--tile-width", type=int, default=16, help="Flag atlas tile width in pixels")
    parser.add_argument("--tile-height", type=int, default=10, help="Flag atlas tile height in pixels")
    parser.add_argument(
        "--download-timeout-seconds",
        type=float,
        default=8.0,
        help="Per-image download timeout in seconds",
    )
    parser.add_argument(
        "--max-download-bytes",
        type=int,
        default=5242880,
        help="Maximum bytes allowed per image download (default: 5 MiB)",
    )
    parser.add_argument(
        "--max-flags",
        type=int,
        default=10000,
        help="Maximum number of unique normalized flags to keep",
    )
    parser.add_argument(
        "--state-file",
        default=str(WEB_WORK_DATA_DIR / "flag_download_state.json"),
        help="JSON state file for resumable per-URL flag downloads",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(WEB_WORK_DATA_DIR / "flag_cache"),
        help="Directory for cached raw flag downloads",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts per URL for transient download failures",
    )
    parser.add_argument(
        "--retry-base-delay",
        type=float,
        default=0.5,
        help="Base retry delay in seconds (exponential backoff + jitter)",
    )
    parser.add_argument(
        "--download-concurrency",
        type=int,
        default=6,
        help="Number of parallel worker threads for flag downloads",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry URLs currently marked as failed in state",
    )
    parser.add_argument(
        "--retry-failed-only",
        action="store_true",
        help="Process only failed URLs (plus stale downloaded records with missing cache)",
    )
    parser.add_argument(
        "--reset-failures",
        action="store_true",
        help="Reset failed URL records to pending before building the worklist",
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
        help="Deprecated for binary output; accepted for backward compatibility",
    )
    parser.add_argument(
        "--enable-archive-fallback",
        action="store_true",
        help="Enable Wayback archive fallback for failed top-X-eligible event URLs",
    )
    parser.add_argument(
        "--archive-ranks-path",
        default=str(WEB_PUBLIC_DATA_DIR / "alliance_score_ranks_daily.msgpack"),
        help="Path to alliance_score_ranks_daily.msgpack used for top-X event gating",
    )
    parser.add_argument(
        "--archive-top-x",
        type=int,
        default=50,
        help="Top-X threshold by event date for archive fallback eligibility",
    )
    parser.add_argument(
        "--archive-cdx-endpoint",
        default=DEFAULT_ARCHIVE_CDX_ENDPOINT,
        help="CDX endpoint used for archive fallback lookups",
    )
    parser.add_argument(
        "--archive-replay-prefix",
        default=DEFAULT_ARCHIVE_REPLAY_PREFIX,
        help="Replay prefix used for archive fallback downloads",
    )
    parser.add_argument(
        "--archive-window-days",
        type=int,
        default=45,
        help="Initial +/- day window around event date for nearest CDX capture selection",
    )
    parser.add_argument(
        "--archive-window-widen-days",
        type=int,
        default=365,
        help="Optional widened +/- day window when no capture exists in the initial window",
    )
    parser.add_argument(
        "--archive-max-cdx-rows",
        type=int,
        default=2000,
        help="Maximum CDX rows to request per URL/time-bucket query",
    )
    parser.add_argument(
        "--archive-concurrency",
        type=int,
        default=2,
        help="Maximum concurrent archive endpoint requests",
    )
    parser.add_argument(
        "--archive-max-retries",
        type=int,
        default=5,
        help="Maximum retries per archive request",
    )
    parser.add_argument(
        "--archive-retry-base-delay",
        type=float,
        default=1.0,
        help="Base retry delay for archive requests (with stronger 429 handling)",
    )
    parser.add_argument(
        "--legacy-flags-csv",
        default=str(DEFAULT_LEGACY_FLAGS_PATH),
        help="Tab-delimited legacy flags CSV source (Alliances, Flag)",
    )
    parser.add_argument(
        "--legacy-imgbb-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Keep only legacy URLs hosted on imgbb/i.ibb.co",
    )
    parser.add_argument(
        "--legacy-backfill-only",
        action="store_true",
        help="Only download legacy-injected URLs when rebuilding outputs",
    )
    return parser.parse_args()


def normalize_header(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\ufeff", "").strip().lower()
    return "".join(ch for ch in text if ch.isalnum())


def dt_to_iso(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


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


def parse_row(row: dict[str, Any]) -> tuple[int, str, str] | None:
    row_lookup = build_row_lookup(row)
    raw_id = first_present(row_lookup, ID_KEYS)
    try:
        alliance_id = int(str(raw_id or "0").strip())
    except ValueError:
        return None
    if alliance_id <= 0:
        return None

    name = normalize_text(first_present(row_lookup, NAME_KEYS))
    flag = normalize_text(first_present(row_lookup, FLAG_KEYS))
    return alliance_id, name, flag


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
        has_name = bool(headers & EXPECTED_NAME_KEYS)
        has_flag = bool(headers & EXPECTED_FLAG_KEYS)
        match_score = (3 if has_id else 0) + (3 if has_flag else 0) + (1 if has_name else 0)
        scored.append((match_score, len(headers), csv_name))

    if not scored:
        return csv_names[0]

    scored.sort(key=lambda item: (item[0], item[1], item[2]))
    return scored[-1][2]


def load_zip_rows(path: Path) -> tuple[list[tuple[int, str, str]], int, int]:
    try:
        with zipfile.ZipFile(path, "r") as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                print(f"[flags] warning: zip missing csv: {path.name}", file=sys.stderr)
                return [], 0, 0

            chosen_csv = pick_best_csv_name(archive, csv_names)

            with archive.open(chosen_csv, "r") as zip_stream:
                text_stream = io.TextIOWrapper(zip_stream, encoding="utf-8", newline="")
                reader = csv.DictReader(text_stream)
                parsed_rows: list[tuple[int, str, str]] = []
                rows_seen = 0
                rows_skipped = 0
                for row in reader:
                    rows_seen += 1
                    parsed = parse_row(row)
                    if parsed is not None:
                        parsed_rows.append(parsed)
                    else:
                        rows_skipped += 1
                return parsed_rows, rows_seen, rows_skipped
    except Exception as exc:
        print(f"[flags] warning: failed reading {path.name}: {exc}", file=sys.stderr)
        return [], 0, 0


def build_flag_events(files: list[Path]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    known_flags: dict[int, str] = {}
    known_names: dict[int, str] = {}
    total_files = len(files)
    progress = ProgressReporter(label="[flags] file progress", total=total_files, unit="files", non_tty_every=50)
    rows_seen_total = 0
    rows_skipped_total = 0

    for file_index, path in enumerate(files):
        done = file_index == (total_files - 1)
        day = parse_day_from_filename(path)
        if day is None:
            progress.step(events=len(events), done=done)
            continue
        timestamp = dt_to_iso(day)

        rows, rows_seen, rows_skipped = load_zip_rows(path)
        rows_seen_total += rows_seen
        rows_skipped_total += rows_skipped
        day_state: dict[int, tuple[str, str]] = {}
        for alliance_id, alliance_name, flag in rows:
            # If a CSV contains duplicates, keep the last seen row for deterministic output.
            day_state[alliance_id] = (alliance_name, flag)

        for alliance_id, (alliance_name, flag) in day_state.items():
            previously_seen = alliance_id in known_flags
            previous_flag = known_flags.get(alliance_id, "")
            previous_name = known_names.get(alliance_id, "")
            name = alliance_name or previous_name

            if file_index == 0 and not previously_seen:
                known_flags[alliance_id] = flag
                known_names[alliance_id] = name
                if flag:
                    events.append(
                        {
                            "timestamp": timestamp,
                            "action": "initial",
                            "alliance_id": alliance_id,
                            "alliance_name": name,
                            "raw_flag_url": flag,
                            "source_ref": f"{path.name}:{alliance_id}",
                        }
                    )
                continue

            if not previously_seen:
                known_flags[alliance_id] = flag
                known_names[alliance_id] = name
                if flag:
                    events.append(
                        {
                            "timestamp": timestamp,
                            "action": "created",
                            "alliance_id": alliance_id,
                            "alliance_name": name,
                            "raw_flag_url": flag,
                            "source_ref": f"{path.name}:{alliance_id}",
                        }
                    )
                continue

            if flag != previous_flag:
                known_flags[alliance_id] = flag
                known_names[alliance_id] = name
                events.append(
                    {
                        "timestamp": timestamp,
                        "action": "changed",
                        "alliance_id": alliance_id,
                        "alliance_name": name,
                        "raw_flag_url": flag,
                        "raw_previous_flag_url": previous_flag,
                        "source_ref": f"{path.name}:{alliance_id}",
                    }
                )
            elif name and name != previous_name:
                known_names[alliance_id] = name

        progress.step(events=len(events), done=done)
        print(
            f"[flags] parsed {path.name}: rows={rows_seen}, skipped={rows_skipped}, day_events={len(day_state)}",
            file=sys.stderr,
        )

    print(
        f"[flags] parse totals: rows={rows_seen_total}, skipped={rows_skipped_total}, events={len(events)}",
        file=sys.stderr,
    )

    return events


def canonicalize_alliance_name(value: Any) -> str:
    text = normalize_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_legacy_imgbb_url(url: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    host = str(parsed.netloc or "").lower()
    if not host:
        return False
    host = host.split(":", 1)[0]
    return host == "imgbb.com" or host.endswith(".imgbb.com") or host == "i.ibb.co" or host.endswith(".i.ibb.co")


def parse_legacy_flags_csv(
    path: Path,
    *,
    imgbb_only: bool,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    counters = {
        "legacy_rows_read": 0,
        "legacy_blank_flag": 0,
        "legacy_non_http": 0,
        "legacy_skipped_non_imgbb": 0,
        "legacy_imgbb_kept": 0,
    }
    out_rows: list[dict[str, Any]] = []

    if not path.exists():
        return out_rows, counters

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for index, row in enumerate(reader, start=2):
            counters["legacy_rows_read"] += 1
            row_lookup = build_row_lookup(row)
            alliance_name = normalize_text(first_present(row_lookup, ("alliances", "alliance", "name")))
            raw_flag_url = normalize_text(first_present(row_lookup, ("flag", "flagurl", "allianceflag")))

            if not raw_flag_url:
                counters["legacy_blank_flag"] += 1
                continue

            if not is_supported_remote_url(raw_flag_url):
                counters["legacy_non_http"] += 1
                continue

            if imgbb_only and not is_legacy_imgbb_url(raw_flag_url):
                counters["legacy_skipped_non_imgbb"] += 1
                continue

            out_rows.append(
                {
                    "row_num": index,
                    "alliance_name": alliance_name,
                    "canonical_name": canonicalize_alliance_name(alliance_name),
                    "raw_flag_url": raw_flag_url,
                }
            )
            counters["legacy_imgbb_kept"] += 1

    return out_rows, counters


def build_canonical_name_to_alliance_id(raw_events: list[dict[str, Any]]) -> tuple[dict[str, int], set[str]]:
    id_sets: dict[str, set[int]] = {}
    for event in raw_events:
        alliance_id = int(event.get("alliance_id") or 0)
        if alliance_id <= 0:
            continue
        canonical_name = canonicalize_alliance_name(event.get("alliance_name") or "")
        if not canonical_name:
            continue
        id_sets.setdefault(canonical_name, set()).add(alliance_id)

    name_to_id: dict[str, int] = {}
    ambiguous: set[str] = set()
    for canonical_name, candidates in id_sets.items():
        if len(candidates) == 1:
            name_to_id[canonical_name] = next(iter(candidates))
        elif len(candidates) > 1:
            ambiguous.add(canonical_name)
    return name_to_id, ambiguous


def build_alliance_name_by_id(raw_events: list[dict[str, Any]]) -> dict[int, str]:
    by_id: dict[int, str] = {}
    for event in raw_events:
        alliance_id = int(event.get("alliance_id") or 0)
        if alliance_id <= 0:
            continue
        name = normalize_text(event.get("alliance_name") or "")
        if name:
            by_id[alliance_id] = name
    return by_id


def build_alliance_flag_coverage(raw_events: list[dict[str, Any]]) -> dict[int, bool]:
    coverage: dict[int, bool] = {}
    for event in raw_events:
        alliance_id = int(event.get("alliance_id") or 0)
        if alliance_id <= 0:
            continue
        has_flag = bool(normalize_text(event.get("raw_flag_url") or ""))
        coverage[alliance_id] = bool(coverage.get(alliance_id) or has_flag)
    return coverage


def compute_global_min_archive_timestamp(files: list[Path]) -> str | None:
    min_day: datetime | None = None
    for path in files:
        day = parse_day_from_filename(path)
        if day is None:
            continue
        if min_day is None or day < min_day:
            min_day = day
    if min_day is None:
        return None
    return dt_to_iso(min_day)


def sort_and_dedupe_events(raw_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    action_order = {"initial": 0, "created": 1, "changed": 2}
    sorted_events = sorted(
        raw_events,
        key=lambda event: (
            str(event.get("timestamp") or ""),
            int(event.get("alliance_id") or 0),
            int(action_order.get(str(event.get("action") or ""), 99)),
            str(event.get("raw_flag_url") or ""),
            str(event.get("raw_previous_flag_url") or ""),
            str(event.get("source_ref") or ""),
        ),
    )

    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int, str, str]] = set()
    for event in sorted_events:
        dedupe_key = (
            str(event.get("timestamp") or ""),
            str(event.get("action") or ""),
            int(event.get("alliance_id") or 0),
            str(event.get("raw_flag_url") or ""),
            str(event.get("raw_previous_flag_url") or ""),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(event)
    return out


def inject_legacy_flag_backfill(
    raw_events: list[dict[str, Any]],
    *,
    files: list[Path],
    legacy_csv_path: Path,
    legacy_imgbb_only: bool,
) -> tuple[list[dict[str, Any]], dict[str, int], set[str]]:
    counters = {
        "legacy_rows_read": 0,
        "legacy_blank_flag": 0,
        "legacy_non_http": 0,
        "legacy_skipped_non_imgbb": 0,
        "legacy_imgbb_kept": 0,
        "legacy_unmatched": 0,
        "legacy_ambiguous": 0,
        "legacy_injected": 0,
    }

    legacy_rows, parse_counts = parse_legacy_flags_csv(legacy_csv_path, imgbb_only=legacy_imgbb_only)
    counters.update(parse_counts)
    if not legacy_rows:
        return raw_events, counters, set()

    timestamp = compute_global_min_archive_timestamp(files)
    if not timestamp:
        return raw_events, counters, set()

    name_to_id, ambiguous_names = build_canonical_name_to_alliance_id(raw_events)
    alliance_name_by_id = build_alliance_name_by_id(raw_events)
    coverage_by_id = build_alliance_flag_coverage(raw_events)

    # Keep first deterministic row per alliance id to avoid conflicting synthetic seeds.
    candidate_by_alliance_id: dict[int, dict[str, Any]] = {}
    for row in legacy_rows:
        canonical_name = str(row.get("canonical_name") or "")
        if not canonical_name:
            counters["legacy_unmatched"] += 1
            continue
        if canonical_name in ambiguous_names:
            counters["legacy_ambiguous"] += 1
            continue
        alliance_id = int(name_to_id.get(canonical_name) or 0)
        if alliance_id <= 0:
            counters["legacy_unmatched"] += 1
            continue
        if bool(coverage_by_id.get(alliance_id)):
            continue
        if alliance_id not in candidate_by_alliance_id:
            candidate_by_alliance_id[alliance_id] = row

    if not candidate_by_alliance_id:
        return raw_events, counters, set()

    injected_urls: set[str] = set()
    merged_events = list(raw_events)
    for alliance_id in sorted(candidate_by_alliance_id.keys()):
        row = candidate_by_alliance_id[alliance_id]
        raw_url = str(row.get("raw_flag_url") or "").strip()
        if not raw_url:
            continue
        alliance_name = alliance_name_by_id.get(alliance_id) or str(row.get("alliance_name") or "")
        row_num = int(row.get("row_num") or 0)
        merged_events.append(
            {
                "timestamp": timestamp,
                "action": "initial",
                "alliance_id": alliance_id,
                "alliance_name": alliance_name,
                "raw_flag_url": raw_url,
                "source_ref": f"legacy_flags.csv:{row_num}:{alliance_id}",
            }
        )
        counters["legacy_injected"] += 1
        injected_urls.add(raw_url)

    return sort_and_dedupe_events(merged_events), counters, injected_urls


def ensure_pillow() -> None:
    if Image is None:
        raise RuntimeError(
            "Pillow is required for flag normalization and atlas generation. Install with: pip install Pillow"
        ) from PIL_IMPORT_ERROR


def validate_runtime_args(args: argparse.Namespace) -> None:
    if args.tile_width <= 0 or args.tile_height <= 0:
        raise RuntimeError("tile dimensions must be positive")
    if args.download_timeout_seconds <= 0:
        raise RuntimeError("download timeout must be positive")
    if args.max_download_bytes <= 0:
        raise RuntimeError("max download bytes must be positive (default: 5242880)")
    if args.max_flags <= 0:
        raise RuntimeError("max flags must be positive")
    if args.max_retries < 0:
        raise RuntimeError("max retries must be >= 0")
    if args.retry_base_delay < 0:
        raise RuntimeError("retry base delay must be >= 0")
    if args.download_concurrency <= 0:
        raise RuntimeError("download concurrency must be >= 1")
    if args.archive_top_x <= 0:
        raise RuntimeError("archive top-x must be >= 1")
    if args.archive_window_days <= 0:
        raise RuntimeError("archive window days must be >= 1")
    if args.archive_window_widen_days <= 0:
        raise RuntimeError("archive window widen days must be >= 1")
    if args.archive_max_cdx_rows <= 0:
        raise RuntimeError("archive max CDX rows must be >= 1")
    if args.archive_concurrency <= 0:
        raise RuntimeError("archive concurrency must be >= 1")
    if args.archive_max_retries < 0:
        raise RuntimeError("archive max retries must be >= 0")
    if args.archive_retry_base_delay < 0:
        raise RuntimeError("archive retry base delay must be >= 0")


def parse_iso8601_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_wayback_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if len(text) < 14 or not text[:14].isdigit():
        return None
    try:
        return datetime.strptime(text[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def to_wayback_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def extract_day_key(timestamp_or_day: str) -> str:
    text = str(timestamp_or_day or "").strip()
    return text[:10] if len(text) >= 10 else text


def resolve_rank_day(rank_days: list[str], timestamp_or_day: str) -> str | None:
    if not rank_days:
        return None
    target_day = extract_day_key(timestamp_or_day)
    if not target_day:
        return rank_days[-1]

    lo = 0
    hi = len(rank_days) - 1
    best = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if rank_days[mid] <= target_day:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1

    if best >= 0:
        return rank_days[best]
    return rank_days[0]


def load_score_ranks_by_day(path: Path) -> dict[str, dict[str, int]]:
    if not path.exists():
        raise RuntimeError(f"rank file not found: {path}")

    try:
        payload = msgpack.unpackb(path.read_bytes(), raw=False)
    except Exception as exc:
        raise RuntimeError(f"failed reading rank file {path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"invalid rank payload (expected dict): {path}")

    if int(payload.get("schema_version", 0)) != 2:
        raise RuntimeError(f"unsupported rank payload schema_version in {path}; expected 2")

    rows_raw = payload.get("ranks_by_day")
    if not isinstance(rows_raw, dict):
        raise RuntimeError(f"invalid rank payload ranks_by_day in {path}")

    normalized: dict[str, dict[str, int]] = {}
    for day_key, row_raw in rows_raw.items():
        day = str(day_key)
        if not isinstance(row_raw, dict):
            continue
        row: dict[str, int] = {}
        for alliance_key, rank_raw in row_raw.items():
            try:
                rank = int(rank_raw)
            except (TypeError, ValueError):
                continue
            if rank <= 0:
                continue
            row[str(alliance_key)] = rank
        if row:
            normalized[day] = row

    if not normalized:
        raise RuntimeError(f"rank payload has no usable rank rows: {path}")
    return normalized


def build_archive_fallback_context(
    events: list[dict[str, Any]],
    *,
    enabled: bool,
    ranks_path: Path,
    top_x: int,
) -> dict[str, list[dict[str, Any]]]:
    if not enabled:
        return {}

    ranks_by_day = load_score_ranks_by_day(ranks_path)
    rank_days = sorted(ranks_by_day.keys())
    by_url: dict[str, list[dict[str, Any]]] = {}

    for event in events:
        alliance_id = int(event.get("alliance_id") or 0)
        if alliance_id <= 0:
            continue
        event_timestamp = str(event.get("timestamp") or "").strip()
        if not event_timestamp:
            continue
        resolved_day = resolve_rank_day(rank_days, event_timestamp)
        if not resolved_day:
            continue
        rank = int((ranks_by_day.get(resolved_day) or {}).get(str(alliance_id), 0))
        if rank <= 0 or rank > top_x:
            continue

        for key in ("raw_flag_url", "raw_previous_flag_url"):
            raw_url = str(event.get(key) or "").strip()
            if not raw_url:
                continue
            row = {
                "event_timestamp": event_timestamp,
                "event_day": extract_day_key(event_timestamp),
                "alliance_id": alliance_id,
                "rank_day": resolved_day,
                "rank": rank,
            }
            by_url.setdefault(raw_url, []).append(row)

    for url, rows in by_url.items():
        rows.sort(key=lambda item: (str(item.get("event_timestamp") or ""), int(item.get("alliance_id") or 0)))

    return by_url


def make_cdx_query_url(
    *,
    cdx_endpoint: str,
    original_url: str,
    from_ts: str,
    to_ts: str,
    max_rows: int,
) -> str:
    params = {
        "url": original_url,
        "from": from_ts,
        "to": to_ts,
        "output": "json",
        "fl": "timestamp,original,statuscode,mimetype,digest,length",
        "filter": ["statuscode:200"],
        "limit": str(max_rows),
    }
    query = urllib.parse.urlencode(params, doseq=True)
    return f"{cdx_endpoint}?{query}"


def parse_cdx_json_rows(payload_bytes: bytes) -> list[dict[str, str]]:
    try:
        parsed = json.loads(payload_bytes.decode("utf-8", errors="replace"))
    except Exception as exc:
        raise RuntimeError(f"failed to parse CDX json: {exc}") from exc

    if not isinstance(parsed, list) or not parsed:
        return []

    header = parsed[0]
    if not isinstance(header, list):
        return []
    columns = [str(item) for item in header]
    rows: list[dict[str, str]] = []

    for raw_row in parsed[1:]:
        if not isinstance(raw_row, list):
            continue
        row: dict[str, str] = {}
        for index, col in enumerate(columns):
            row[col] = str(raw_row[index]) if index < len(raw_row) else ""
        if row.get("statuscode") != "200":
            continue
        if not row.get("timestamp"):
            continue
        rows.append(row)

    return rows


def nearest_cdx_row(rows: list[dict[str, str]], event_at: datetime) -> dict[str, str] | None:
    best_row: dict[str, str] | None = None
    best_key: tuple[int, str, str] | None = None
    event_seconds = int(event_at.timestamp())

    for row in rows:
        snap_ts = str(row.get("timestamp") or "")
        snap_dt = parse_wayback_timestamp(snap_ts)
        if snap_dt is None:
            continue
        delta_seconds = abs(int(snap_dt.timestamp()) - event_seconds)
        tie_break_original = str(row.get("original") or "")
        key = (delta_seconds, snap_ts, tie_break_original)
        if best_key is None or key < best_key:
            best_key = key
            best_row = row

    return best_row


def build_wayback_replay_candidates(*, replay_prefix: str, timestamp: str, original_url: str) -> list[tuple[str, str]]:
    encoded_original = urllib.parse.quote(original_url, safe=":/?&=%#@+-._~")
    base = replay_prefix.rstrip("/")
    return [
        ("id_", f"{base}/{timestamp}id_/{encoded_original}"),
        ("if_", f"{base}/{timestamp}if_/{encoded_original}"),
        ("plain", f"{base}/{timestamp}/{encoded_original}"),
    ]


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(payload)
    last_error: Exception | None = None
    for attempt in range(8):
        try:
            os.replace(tmp_path, path)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.05 * (attempt + 1))
    if last_error is not None:
        raise last_error
    os.replace(tmp_path, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    data = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True)
    atomic_write_bytes(path, data.encode("utf-8"))


def atomic_replace_file(src: Path, dest: Path) -> None:
    last_error: Exception | None = None
    for attempt in range(8):
        try:
            os.replace(src, dest)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.05 * (attempt + 1))
    if last_error is not None:
        raise last_error
    os.replace(src, dest)


def load_json_or_default(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def default_download_state() -> dict[str, Any]:
    now = utc_now_iso()
    return {
        "schema_version": 2,
        "created_at": now,
        "updated_at": now,
        "urls": {},
    }


def load_download_state(path: Path) -> dict[str, Any]:
    loaded = load_json_or_default(path, default_download_state())
    if not isinstance(loaded, dict):
        return default_download_state()

    if not isinstance(loaded.get("urls"), dict):
        loaded["urls"] = {}
    try:
        schema_version = int(loaded.get("schema_version", 1))
    except Exception:
        schema_version = 1
    loaded["schema_version"] = max(schema_version, 2)
    if "created_at" not in loaded:
        loaded["created_at"] = utc_now_iso()
    if "updated_at" not in loaded:
        loaded["updated_at"] = utc_now_iso()
    return loaded


def persist_download_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = utc_now_iso()
    atomic_write_json(path, state)


def cache_extension_for_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
        suffix = Path(parsed.path).suffix.lower()
    except Exception:
        suffix = ""
    if suffix in {".png", ".webp", ".jpg", ".jpeg", ".gif", ".bmp", ".svg", ".avif"}:
        return suffix
    return ".bin"


def default_cache_filename(url: str) -> str:
    digest = hashlib.sha256(url.encode("utf-8", errors="replace")).hexdigest()
    return f"{digest}{cache_extension_for_url(url)}"


def resolve_cache_path(cache_dir: Path, entry: dict[str, Any], url: str) -> Path:
    cache_file = str(entry.get("cache_file") or "").strip()
    if not cache_file:
        cache_file = default_cache_filename(url)
        entry["cache_file"] = cache_file
    return cache_dir / cache_file


def upsert_url_state(
    *,
    state: dict[str, Any],
    url: str,
    status: str,
    cache_file: str | None = None,
    last_error: str | None = None,
    content_sha256: str | None = None,
    http_status: int | None = None,
    increment_attempts: bool = False,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url_states = state.setdefault("urls", {})
    current = url_states.get(url)
    if not isinstance(current, dict):
        current = {}

    attempts = int(current.get("attempts", 0)) + (1 if increment_attempts else 0)
    next_item: dict[str, Any] = {
        "status": status,
        "attempts": attempts,
        "updated_at": utc_now_iso(),
        "last_error": "",
        "cache_file": cache_file or str(current.get("cache_file") or ""),
        "content_sha256": content_sha256 or str(current.get("content_sha256") or ""),
    }
    if last_error:
        next_item["last_error"] = last_error
    if http_status is not None:
        next_item["http_status"] = int(http_status)
    elif "http_status" in current:
        next_item["http_status"] = current["http_status"]

    if extra_fields:
        for key, value in extra_fields.items():
            next_item[key] = value

    url_states[url] = next_item
    return next_item


def is_supported_remote_url(url: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def download_image_bytes(url: str, timeout_seconds: float, max_download_bytes: int) -> tuple[bytes, int | None]:
    req = urllib.request.Request(url, headers={"User-Agent": "treaty-vis-flags/1.0"})
    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
        data = response.read(max_download_bytes + 1)
        http_status = getattr(response, "status", None)
    if len(data) > max_download_bytes:
        raise RuntimeError(f"download too large (>{max_download_bytes} bytes)")
    if not data:
        raise RuntimeError("empty download")
    return data, http_status


WIKIMEDIA_THUMB_WIDTH_RE = re.compile(r"^(?P<width>\d+)px-")


def build_download_url_candidates(url: str) -> list[str]:
    candidates = [url]

    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return candidates

    host = str(parsed.netloc or "").lower()
    if host != "upload.wikimedia.org":
        return candidates

    parts = [segment for segment in parsed.path.split("/") if segment]
    # Expected thumb shape: /wikipedia/<lang>/thumb/<hash1>/<hash2>/<file>/<size>-<file>.png
    if len(parts) < 7 or parts[0] != "wikipedia" or parts[2] != "thumb":
        return candidates

    lang = parts[1]
    file_name = parts[5]
    size_token = parts[6]
    width_match = WIKIMEDIA_THUMB_WIDTH_RE.match(size_token)
    width = width_match.group("width") if width_match else ""
    encoded_file_name = urllib.parse.quote(file_name)

    special_hosts: list[str] = []
    if lang and lang != "commons":
        special_hosts.append(f"{lang}.wikipedia.org")
    special_hosts.append("commons.wikimedia.org")

    for special_host in special_hosts:
        special_url = f"https://{special_host}/wiki/Special:FilePath/{encoded_file_name}"
        if width:
            special_url = f"{special_url}?width={width}"
        if special_url not in candidates:
            candidates.append(special_url)

    return candidates


def is_transient_download_error(exc: Exception) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        code = int(exc.code)
        return code == 429 or 500 <= code <= 599

    if isinstance(exc, TimeoutError):
        return True

    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, urllib.error.HTTPError):
            return False
        if isinstance(reason, (TimeoutError, socket.timeout, ConnectionError, OSError)):
            return True
        if isinstance(reason, str):
            lowered = reason.lower()
            return (
                "timed out" in lowered
                or "temporary failure" in lowered
                or "connection reset" in lowered
                or "connection refused" in lowered
                or "name resolution" in lowered
            )
        return False

    return False


def extract_http_status_from_error(exc: Exception) -> int | None:
    if isinstance(exc, urllib.error.HTTPError):
        return int(exc.code)
    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, urllib.error.HTTPError):
            return int(reason.code)
    return None


def extract_retry_after_seconds_from_error(exc: Exception) -> float | None:
    if not isinstance(exc, urllib.error.HTTPError):
        return None

    raw_value = ""
    try:
        raw_value = str(exc.headers.get("Retry-After") or "").strip()
    except Exception:
        raw_value = ""

    if not raw_value:
        return None

    if raw_value.isdigit():
        seconds = float(raw_value)
        return seconds if seconds >= 0 else None

    try:
        retry_at = email.utils.parsedate_to_datetime(raw_value)
    except Exception:
        return None
    if retry_at is None:
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)

    delta_seconds = (retry_at - datetime.now(timezone.utc)).total_seconds()
    if delta_seconds <= 0:
        return 0.0
    return delta_seconds


def download_with_retries(
    *,
    url: str,
    timeout_seconds: float,
    max_download_bytes: int,
    max_retries: int,
    retry_base_delay: float,
) -> tuple[bytes, int | None]:
    attempts_total = max(1, max_retries + 1)
    last_error: Exception | None = None
    rate_limit_streak = 0
    for attempt in range(attempts_total):
        try:
            return download_image_bytes(url, timeout_seconds=timeout_seconds, max_download_bytes=max_download_bytes)
        except Exception as exc:
            last_error = exc
            status_code = extract_http_status_from_error(exc)
            if status_code == 429:
                rate_limit_streak += 1
            else:
                rate_limit_streak = 0
            if attempt >= (attempts_total - 1):
                break
            if not is_transient_download_error(exc):
                break

            if status_code == 429:
                retry_hint = extract_retry_after_seconds_from_error(exc)
                if retry_hint is not None:
                    base_delay = max(retry_hint, 0.0)
                    hint_source = "retry-after"
                else:
                    base_delay = 60.0
                    hint_source = "default-60s"

                scale_power = max(0, rate_limit_streak - 1)
                scaled_delay = base_delay * (2**scale_power)
                jitter_cap = max(1.0, min(scaled_delay * 0.25, 30.0))
                jitter = random.uniform(0.0, jitter_cap)
                delay = min(scaled_delay + jitter, 900.0)
                print(
                    "[flags] info: 429 backoff "
                    f"url={url} retry={attempt + 1}/{attempts_total - 1} "
                    f"wait={delay:.2f}s streak={rate_limit_streak} source={hint_source}",
                    file=sys.stderr,
                )
            else:
                base = retry_base_delay * (2**attempt)
                jitter = random.uniform(0.0, max(retry_base_delay, 0.001))
                delay = min(base + jitter, 30.0)

            if delay > 0:
                time.sleep(delay)

    assert last_error is not None
    raise last_error


def download_with_retries_archive(
    *,
    url: str,
    timeout_seconds: float,
    max_download_bytes: int,
    max_retries: int,
    retry_base_delay: float,
) -> tuple[bytes, int | None]:
    attempts_total = max(1, max_retries + 1)
    last_error: Exception | None = None
    rate_limit_streak = 0
    for attempt in range(attempts_total):
        try:
            return download_image_bytes(url, timeout_seconds=timeout_seconds, max_download_bytes=max_download_bytes)
        except Exception as exc:
            last_error = exc
            status_code = extract_http_status_from_error(exc)
            if status_code == 429:
                rate_limit_streak += 1
            else:
                rate_limit_streak = 0

            if attempt >= (attempts_total - 1):
                break
            if not is_transient_download_error(exc):
                break

            if status_code == 429:
                retry_hint = extract_retry_after_seconds_from_error(exc)
                base_delay = max(retry_hint, 0.0) if retry_hint is not None else 120.0
                scale_power = max(0, rate_limit_streak - 1)
                scaled_delay = base_delay * (2**scale_power)
                jitter_cap = max(2.0, min(scaled_delay * 0.30, 60.0))
                jitter = random.uniform(0.0, jitter_cap)
                delay = min(scaled_delay + jitter, 1200.0)
                print(
                    "[flags] info: archive 429 backoff "
                    f"url={url} retry={attempt + 1}/{attempts_total - 1} "
                    f"wait={delay:.2f}s streak={rate_limit_streak}",
                    file=sys.stderr,
                )
            else:
                base = max(retry_base_delay, 0.1) * (2**attempt)
                jitter = random.uniform(0.0, max(retry_base_delay * 2.0, 0.25))
                delay = min(base + jitter, 90.0)

            if delay > 0:
                time.sleep(delay)

    assert last_error is not None
    raise last_error


def normalize_image_bytes(image_bytes: bytes, tile_width: int, tile_height: int) -> Any:
    assert Image is not None
    try:
        with Image.open(io.BytesIO(image_bytes)) as src:
            # Always normalize GIFs from frame 0 so animated inputs produce deterministic output.
            if str(getattr(src, "format", "")).upper() == "GIF" and bool(getattr(src, "is_animated", False)):
                src.seek(0)
            rgba = src.convert("RGBA")
    except Exception as exc:
        raise RuntimeError(f"decode failed: {exc}") from exc

    resize_filter = getattr(Image, "Resampling", Image).LANCZOS
    normalized = rgba.resize((tile_width, tile_height), resize_filter)
    return normalized


def hash_normalized_image(image: Any) -> str:
    digest = hashlib.sha256(image.tobytes()).hexdigest()
    return digest


def collect_flag_assets(
    events: list[dict[str, Any]],
    *,
    tile_width: int,
    tile_height: int,
    timeout_seconds: float,
    max_download_bytes: int,
    max_flags: int,
    state_file: Path,
    cache_dir: Path,
    max_retries: int,
    retry_base_delay: float,
    download_concurrency: int,
    retry_failed: bool,
    retry_failed_only: bool,
    reset_failures: bool,
    archive_enabled: bool,
    archive_context_by_url: dict[str, list[dict[str, Any]]],
    archive_cdx_endpoint: str,
    archive_replay_prefix: str,
    archive_window_days: int,
    archive_window_widen_days: int,
    archive_max_cdx_rows: int,
    archive_concurrency: int,
    archive_max_retries: int,
    archive_retry_base_delay: float,
    download_url_allowlist: set[str] | None = None,
) -> tuple[dict[str, str], dict[str, Any], dict[str, str]]:
    url_set: set[str] = set()
    for event in events:
        url = str(event.get("raw_flag_url") or "").strip()
        if url:
            url_set.add(url)
        prev = str(event.get("raw_previous_flag_url") or "").strip()
        if prev:
            url_set.add(prev)

    all_urls = sorted(url_set)
    state = load_download_state(state_file)
    cache_dir.mkdir(parents=True, exist_ok=True)

    if reset_failures:
        for item in state.get("urls", {}).values():
            if isinstance(item, dict) and str(item.get("status") or "") == "failed":
                item["status"] = "pending"
                item["last_error"] = ""
                item["updated_at"] = utc_now_iso()
        persist_download_state(state_file, state)

    stale_repairs = 0
    worklist: list[tuple[str, dict[str, Any], bool]] = []
    for url in all_urls:
        item = state.setdefault("urls", {}).get(url)
        if not isinstance(item, dict):
            item = upsert_url_state(state=state, url=url, status="pending")

        cache_path = resolve_cache_path(cache_dir, item, url)
        status = str(item.get("status") or "pending")
        repaired_stale = False
        allow_download = download_url_allowlist is None or url in download_url_allowlist

        if allow_download and status == "downloaded" and not cache_path.exists():
            stale_repairs += 1
            repaired_stale = True
            item = upsert_url_state(
                state=state,
                url=url,
                status="pending",
                cache_file=cache_path.name,
                last_error="cache file missing; marked pending for repair",
            )
            status = "pending"

        include = False
        if allow_download:
            if status in {"pending", "new", "queued"}:
                include = (not retry_failed_only) or repaired_stale
            elif status == "failed":
                include = bool(retry_failed or retry_failed_only)
            elif status == "downloaded":
                include = False
            else:
                include = not retry_failed_only

        worklist.append((url, item, include))

    persist_download_state(state_file, state)

    queued = [entry for entry in worklist if entry[2]]
    progress = ProgressReporter(label="[flags] image progress", total=len(queued), unit="urls", non_tty_every=100)
    state_lock = threading.Lock()
    archive_lock = threading.Lock()
    archive_semaphore = threading.Semaphore(max(1, archive_concurrency))
    cdx_resolution_cache: dict[str, dict[str, Any] | None] = {}
    checkpoint_every = max(10, download_concurrency * 2)
    state_updates_since_checkpoint = 0

    def checkpoint_state(*, force: bool = False) -> None:
        nonlocal state_updates_since_checkpoint
        with state_lock:
            if force or state_updates_since_checkpoint >= checkpoint_every:
                persist_download_state(state_file, state)
                state_updates_since_checkpoint = 0

    def update_url_state(url: str, *, mark_for_checkpoint: bool = True, **kwargs: Any) -> dict[str, Any]:
        nonlocal state_updates_since_checkpoint
        with state_lock:
            updated = upsert_url_state(state=state, url=url, **kwargs)
            if mark_for_checkpoint:
                state_updates_since_checkpoint += 1
            return updated

    url_to_hash: dict[str, str] = {}
    hash_to_image: dict[str, Any] = {}
    hash_failures = 0
    skipped_downloaded = 0
    skipped_non_retry = 0
    archive_attempts = 0
    archive_resolutions = 0
    archive_downloaded = 0

    def fetch_cdx_nearest_capture(
        *,
        url: str,
        event_context: dict[str, Any],
        window_days: int,
    ) -> dict[str, Any] | None:
        event_timestamp = str(event_context.get("event_timestamp") or "")
        event_dt = parse_iso8601_utc(event_timestamp)
        if event_dt is None:
            return None
        bucket = event_dt.strftime("%Y-%m")
        cache_key = f"{url}|{bucket}|{window_days}"

        with archive_lock:
            if cache_key in cdx_resolution_cache:
                return cdx_resolution_cache[cache_key]

        start_ts = to_wayback_timestamp(event_dt - timedelta(days=window_days))
        end_ts = to_wayback_timestamp(event_dt + timedelta(days=window_days))
        query_url = make_cdx_query_url(
            cdx_endpoint=archive_cdx_endpoint,
            original_url=url,
            from_ts=start_ts,
            to_ts=end_ts,
            max_rows=archive_max_cdx_rows,
        )

        try:
            with archive_semaphore:
                payload, _ = download_with_retries_archive(
                    url=query_url,
                    timeout_seconds=timeout_seconds,
                    max_download_bytes=max(2 * 1024 * 1024, min(max_download_bytes, 8 * 1024 * 1024)),
                    max_retries=archive_max_retries,
                    retry_base_delay=archive_retry_base_delay,
                )
            rows = parse_cdx_json_rows(payload)
            nearest = nearest_cdx_row(rows, event_dt)
            if nearest is None:
                resolved = None
            else:
                resolved = {
                    "window_days": window_days,
                    "query_url": query_url,
                    "capture": nearest,
                }
        except Exception as exc:
            resolved = {
                "window_days": window_days,
                "query_url": query_url,
                "error": str(exc),
                "capture": None,
            }

        with archive_lock:
            cdx_resolution_cache[cache_key] = resolved
        return resolved

    def resolve_archive_candidates(url: str) -> dict[str, Any] | None:
        contexts = archive_context_by_url.get(url) or []
        if not contexts:
            return None

        for event_context in contexts:
            windows = [archive_window_days]
            if archive_window_widen_days > archive_window_days:
                windows.append(archive_window_widen_days)

            for window_days in windows:
                cdx_result = fetch_cdx_nearest_capture(
                    url=url,
                    event_context=event_context,
                    window_days=window_days,
                )
                if not cdx_result:
                    continue
                capture = cdx_result.get("capture")
                if not isinstance(capture, dict):
                    continue

                capture_timestamp = str(capture.get("timestamp") or "")
                capture_original = str(capture.get("original") or url)
                if not capture_timestamp:
                    continue

                replay_candidates = build_wayback_replay_candidates(
                    replay_prefix=archive_replay_prefix,
                    timestamp=capture_timestamp,
                    original_url=capture_original,
                )
                return {
                    "event_context": event_context,
                    "cdx": cdx_result,
                    "capture": capture,
                    "replay_candidates": replay_candidates,
                }

        return None

    def try_download_candidates(
        *,
        candidates: list[tuple[str, str]],
        use_archive_retry: bool,
    ) -> dict[str, Any]:
        if not candidates:
            raise RuntimeError("no candidates available")

        last_error: Exception | None = None
        for mode, candidate_url in candidates:
            try:
                if use_archive_retry:
                    with archive_semaphore:
                        raw, http_status = download_with_retries_archive(
                            url=candidate_url,
                            timeout_seconds=timeout_seconds,
                            max_download_bytes=max_download_bytes,
                            max_retries=archive_max_retries,
                            retry_base_delay=archive_retry_base_delay,
                        )
                else:
                    raw, http_status = download_with_retries(
                        url=candidate_url,
                        timeout_seconds=timeout_seconds,
                        max_download_bytes=max_download_bytes,
                        max_retries=max_retries,
                        retry_base_delay=retry_base_delay,
                    )

                normalized = normalize_image_bytes(raw, tile_width=tile_width, tile_height=tile_height)
                return {
                    "raw": raw,
                    "http_status": http_status,
                    "selected_url": candidate_url,
                    "selected_mode": mode,
                    "normalized": normalized,
                }
            except Exception as exc:
                last_error = exc

        if last_error is not None:
            raise last_error
        raise RuntimeError("download candidates exhausted")

    # Prime cached downloaded URLs so event mapping stays complete on resumed runs.
    for url, item, include in worklist:
        if include:
            continue
        cache_path = resolve_cache_path(cache_dir, item, url)
        status = str(item.get("status") or "")
        if status == "downloaded" and cache_path.exists():
            try:
                raw = cache_path.read_bytes()
                normalized = normalize_image_bytes(raw, tile_width=tile_width, tile_height=tile_height)
                image_hash = hash_normalized_image(normalized)
                url_to_hash[url] = image_hash
                if image_hash not in hash_to_image:
                    hash_to_image[image_hash] = normalized
                skipped_downloaded += 1
            except Exception as exc:
                hash_failures += 1
                update_url_state(
                    url,
                    status="failed",
                    cache_file=cache_path.name,
                    last_error=f"cached decode failed: {exc}",
                )
                checkpoint_state(force=True)
        else:
            skipped_non_retry += 1

    # Reserve attempt/state updates before scheduling to keep retries/resume deterministic.
    queued_jobs: list[tuple[str, Path, int | None]] = []
    for url, item, _ in queued:
        cache_path = resolve_cache_path(cache_dir, item, url)
        update_url_state(
            url,
            status="pending",
            cache_file=cache_path.name,
            increment_attempts=True,
        )
        queued_jobs.append((url, cache_path, item.get("http_status")))
    checkpoint_state(force=True)

    def process_single_url(url: str, cache_path: Path, existing_http_status: int | None) -> dict[str, Any]:
        archive_attempted_for_url = False
        if not is_supported_remote_url(url):
            return {
                "url": url,
                "status": "failed",
                "cache_file": cache_path.name,
                "error": "unsupported url scheme",
                "http_status": None,
                "extra_fields": {"archive_attempted": False, "archive_used": False},
            }

        try:
            if cache_path.exists():
                raw = cache_path.read_bytes()
                http_status = existing_http_status
                selected_url = url
                selected_mode = "cached"
                normalized = normalize_image_bytes(raw, tile_width=tile_width, tile_height=tile_height)
                extra_fields = {
                    "download_source": "cache",
                    "selected_url": selected_url,
                    "archive_attempted": False,
                }
            else:
                direct_candidates = [("direct", candidate) for candidate in build_download_url_candidates(url)]
                archive_error: Exception | None = None

                try:
                    direct_result = try_download_candidates(candidates=direct_candidates, use_archive_retry=False)
                    raw = bytes(direct_result["raw"])
                    http_status = direct_result.get("http_status")
                    selected_url = str(direct_result.get("selected_url") or url)
                    selected_mode = str(direct_result.get("selected_mode") or "direct")
                    normalized = direct_result["normalized"]
                    extra_fields = {
                        "download_source": "direct",
                        "selected_url": selected_url,
                        "archive_attempted": False,
                        "archive_used": False,
                    }
                except Exception as direct_error:
                    if archive_enabled and url in archive_context_by_url:
                        archive_attempted_for_url = True
                        archive_result = resolve_archive_candidates(url)
                        if archive_result and archive_result.get("replay_candidates"):
                            replay_candidates = [
                                (str(mode), str(candidate_url))
                                for mode, candidate_url in archive_result["replay_candidates"]
                            ]
                            try:
                                archive_download = try_download_candidates(
                                    candidates=replay_candidates,
                                    use_archive_retry=True,
                                )
                                raw = bytes(archive_download["raw"])
                                http_status = archive_download.get("http_status")
                                selected_url = str(archive_download.get("selected_url") or url)
                                selected_mode = str(archive_download.get("selected_mode") or "id_")
                                normalized = archive_download["normalized"]
                                capture = archive_result.get("capture") or {}
                                event_context = archive_result.get("event_context") or {}
                                cdx_meta = archive_result.get("cdx") or {}
                                extra_fields = {
                                    "download_source": "archive",
                                    "selected_url": selected_url,
                                    "archive_attempted": True,
                                    "archive_used": True,
                                    "archive_event_day": str(event_context.get("event_day") or ""),
                                    "archive_event_timestamp": str(event_context.get("event_timestamp") or ""),
                                    "archive_alliance_id": int(event_context.get("alliance_id") or 0),
                                    "archive_rank_day": str(event_context.get("rank_day") or ""),
                                    "archive_rank": int(event_context.get("rank") or 0),
                                    "archive_window_days": int(cdx_meta.get("window_days") or 0),
                                    "archive_cdx_query_url": str(cdx_meta.get("query_url") or ""),
                                    "archive_capture_timestamp": str(capture.get("timestamp") or ""),
                                    "archive_capture_original": str(capture.get("original") or ""),
                                    "archive_capture_statuscode": str(capture.get("statuscode") or ""),
                                    "archive_capture_mimetype": str(capture.get("mimetype") or ""),
                                    "archive_replay_mode": selected_mode,
                                }
                            except Exception as exc:
                                archive_error = exc
                        elif archive_result and archive_result.get("cdx") and (archive_result["cdx"].get("error")):
                            archive_error = RuntimeError(str(archive_result["cdx"].get("error") or "archive cdx failed"))
                        else:
                            archive_error = RuntimeError("archive cdx found no bounded capture")

                    if archive_error is not None:
                        raise RuntimeError(f"direct failed: {direct_error}; archive failed: {archive_error}") from archive_error
                    if archive_attempted_for_url:
                        raise RuntimeError(f"direct failed: {direct_error}; archive unavailable") from direct_error
                    raise direct_error

                if not raw:
                    raise RuntimeError("empty download")

                atomic_write_bytes(cache_path, raw)

                if archive_attempted_for_url and not bool(extra_fields.get("archive_attempted")):
                    extra_fields["archive_attempted"] = True

                if archive_attempted_for_url and not bool(extra_fields.get("archive_used")):
                    extra_fields["archive_used"] = False

            image_hash = hash_normalized_image(normalized)
            return {
                "url": url,
                "status": "downloaded",
                "cache_file": cache_path.name,
                "http_status": int(http_status) if http_status is not None else None,
                "content_sha256": hashlib.sha256(raw).hexdigest(),
                "image_hash": image_hash,
                "normalized": normalized,
                "extra_fields": extra_fields,
            }
        except Exception as exc:
            http_status = extract_http_status_from_error(exc)
            return {
                "url": url,
                "status": "failed",
                "cache_file": cache_path.name,
                "error": str(exc),
                "http_status": http_status,
                "extra_fields": {
                    "archive_attempted": archive_attempted_for_url,
                    "archive_used": False,
                },
            }

    completed = 0
    if queued_jobs:
        max_workers = min(download_concurrency, len(queued_jobs))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_single_url, url, cache_path, existing_http_status)
                for url, cache_path, existing_http_status in queued_jobs
            ]
            for future in concurrent.futures.as_completed(futures):
                completed += 1
                done = completed == len(queued_jobs)

                result = future.result()
                url = str(result.get("url") or "")
                cache_file = str(result.get("cache_file") or "")
                status = str(result.get("status") or "failed")

                if status != "downloaded":
                    hash_failures += 1
                    error_text = str(result.get("error") or "unknown download/decode failure")
                    if error_text == "unsupported url scheme":
                        print(f"[flags] warning: skipping unsupported flag url: {url}", file=sys.stderr)
                    else:
                        print(f"[flags] warning: failed flag download/decode {url}: {error_text}", file=sys.stderr)
                    update_url_state(
                        url,
                        status="failed",
                        cache_file=cache_file,
                        last_error=error_text,
                        http_status=result.get("http_status"),
                        extra_fields=result.get("extra_fields") if isinstance(result.get("extra_fields"), dict) else None,
                    )
                    checkpoint_state()
                    progress.step(kept=len(hash_to_image), failed=hash_failures, done=done)
                    continue

                image_hash = str(result["image_hash"])
                normalized = result["normalized"]
                url_to_hash[url] = image_hash
                if image_hash not in hash_to_image:
                    hash_to_image[image_hash] = normalized

                update_url_state(
                    url,
                    status="downloaded",
                    cache_file=cache_file,
                    content_sha256=str(result.get("content_sha256") or ""),
                    http_status=result.get("http_status"),
                    last_error="",
                    extra_fields=result.get("extra_fields") if isinstance(result.get("extra_fields"), dict) else None,
                )
                if bool((result.get("extra_fields") or {}).get("archive_attempted")):
                    archive_attempts += 1
                if bool((result.get("extra_fields") or {}).get("archive_used")):
                    archive_downloaded += 1
                checkpoint_state()
                progress.step(kept=len(hash_to_image), failed=hash_failures, done=done)

    with archive_lock:
        archive_resolutions = len(cdx_resolution_cache)

    checkpoint_state(force=True)

    sorted_hashes = sorted(hash_to_image.keys())
    if len(sorted_hashes) > max_flags:
        raise RuntimeError(
            f"unique normalized flags exceeded max ({len(sorted_hashes)} > {max_flags}); rerun with higher --max-flags"
        )

    hash_to_key: dict[str, str] = {}
    for index, image_hash in enumerate(sorted_hashes):
        hash_to_key[image_hash] = f"f{index:x}"

    print(
        "[flags] image totals: "
        f"urls={len(all_urls)}, "
        f"queued={len(queued)}, "
        f"stale_repairs={stale_repairs}, "
        f"skipped_downloaded={skipped_downloaded}, "
        f"skipped_non_retry={skipped_non_retry}, "
        f"unique={len(sorted_hashes)}, failures={hash_failures}, "
        f"archive_attempts={archive_attempts}, archive_used={archive_downloaded}, "
        f"archive_cdx_cached={archive_resolutions}",
        file=sys.stderr,
    )

    return url_to_hash, hash_to_image, hash_to_key


def build_runtime_events(
    raw_events: list[dict[str, Any]],
    *,
    url_to_hash: dict[str, str],
    hash_to_key: dict[str, str],
) -> list[dict[str, Any]]:
    runtime_events: list[dict[str, Any]] = []

    for event in raw_events:
        raw_url = str(event.get("raw_flag_url") or "").strip()
        current_hash = url_to_hash.get(raw_url)
        current_key = hash_to_key.get(current_hash or "") if current_hash else ""

        out: dict[str, Any] = {
            "timestamp": event["timestamp"],
            "action": event["action"],
            "alliance_id": event["alliance_id"],
            "alliance_name": event.get("alliance_name", ""),
            "flag_key": current_key,
            "source_ref": event["source_ref"],
        }

        prev_raw = str(event.get("raw_previous_flag_url") or "").strip()
        if prev_raw:
            prev_hash = url_to_hash.get(prev_raw)
            prev_key = hash_to_key.get(prev_hash or "") if prev_hash else None
            if prev_key:
                out["previous_flag_key"] = prev_key

        runtime_events.append(out)

    return runtime_events


def build_flag_atlas(
    hash_to_image: dict[str, Any],
    hash_to_key: dict[str, str],
    *,
    tile_width: int,
    tile_height: int,
) -> tuple[Any, dict[str, Any], dict[str, dict[str, Any]]]:
    assert Image is not None

    key_to_hash: dict[str, str] = {key: image_hash for image_hash, key in hash_to_key.items()}
    sorted_keys = sorted(key_to_hash.keys())
    count = len(sorted_keys)

    if count == 0:
        columns = 1
        rows = 1
    else:
        columns = int(math.ceil(math.sqrt(count)))
        rows = int(math.ceil(count / columns))

    atlas_width = columns * tile_width
    atlas_height = rows * tile_height
    atlas = Image.new("RGBA", (atlas_width, atlas_height), (0, 0, 0, 0))

    assets: dict[str, dict[str, Any]] = {}
    for index, key in enumerate(sorted_keys):
        col = index % columns
        row = index // columns
        x = col * tile_width
        y = row * tile_height
        image_hash = key_to_hash[key]
        tile = hash_to_image[image_hash]
        atlas.paste(tile, (x, y))
        assets[key] = {"x": x, "y": y, "w": tile_width, "h": tile_height, "hash": image_hash}

    atlas_meta = {
        "tile_width": tile_width,
        "tile_height": tile_height,
        "columns": columns,
        "rows": rows,
        "width": atlas_width,
        "height": atlas_height,
        "count": count,
    }
    return atlas, atlas_meta, assets


def write_outputs(
    *,
    flags_output_path: Path,
    assets_output_path: Path,
    atlas_webp_output_path: Path,
    atlas_png_output_path: Path,
    runtime_events: list[dict[str, Any]],
    atlas: Any,
    atlas_meta: dict[str, Any],
    assets: dict[str, dict[str, Any]],
) -> None:
    flags_output_path.parent.mkdir(parents=True, exist_ok=True)
    assets_output_path.parent.mkdir(parents=True, exist_ok=True)
    atlas_webp_output_path.parent.mkdir(parents=True, exist_ok=True)
    atlas_png_output_path.parent.mkdir(parents=True, exist_ok=True)

    flags_payload = {"events": runtime_events}
    atomic_write_bytes(flags_output_path, msgpack.packb(flags_payload, use_bin_type=True))

    atlas_meta_with_paths = {
        **atlas_meta,
        "webp": f"/data/{atlas_webp_output_path.name}",
        "png": f"/data/{atlas_png_output_path.name}",
    }
    assets_payload = {"atlas": atlas_meta_with_paths, "assets": assets}
    atomic_write_bytes(assets_output_path, msgpack.packb(assets_payload, use_bin_type=True))

    try:
        atlas_png_tmp_path = atlas_png_output_path.with_suffix(atlas_png_output_path.suffix + ".tmp")
        atlas.save(atlas_png_tmp_path, format="PNG")
        atomic_replace_file(atlas_png_tmp_path, atlas_png_output_path)
    except Exception as exc:
        raise RuntimeError(f"failed to write PNG atlas: {exc}") from exc

    try:
        atlas_webp_tmp_path = atlas_webp_output_path.with_suffix(atlas_webp_output_path.suffix + ".tmp")
        atlas.save(atlas_webp_tmp_path, format="WEBP", lossless=True, method=6)
        atomic_replace_file(atlas_webp_tmp_path, atlas_webp_output_path)
    except Exception as exc:
        raise RuntimeError(f"failed to write WEBP atlas: {exc}") from exc


def main() -> int:
    args = parse_args()
    ensure_pillow()
    validate_runtime_args(args)

    alliances_dir = Path(args.alliances_dir).resolve()
    output_path = Path(args.output).resolve()
    assets_output_path = Path(args.assets_output).resolve()
    atlas_webp_output_path = Path(args.atlas_webp_output).resolve()
    atlas_png_output_path = Path(args.atlas_png_output).resolve()
    state_file = Path(args.state_file).resolve()
    cache_dir = Path(args.cache_dir).resolve()
    effective_download_missing_archives = bool(args.download_missing_alliance_archives)
    if args.legacy_backfill_only:
        effective_download_missing_archives = False

    files, archive_flags = prepare_alliance_archives(
        alliances_dir=alliances_dir,
        index_url=str(args.alliances_index_url),
        download_missing=effective_download_missing_archives,
    )
    for flag in archive_flags:
        severity = str(flag.get("severity") or "info").lower()
        if severity == "warning":
            print(f"[flags] warning: {json.dumps(flag, ensure_ascii=True)}", file=sys.stderr)
        else:
            print(f"[flags] info: {json.dumps(flag, ensure_ascii=True)}", file=sys.stderr)

    files = [path for path in files if parse_day_from_filename(path) is not None]
    files.sort(key=lambda p: (parse_day_from_filename(p), p.name))

    raw_events = build_flag_events(files)
    legacy_csv_path = Path(args.legacy_flags_csv).resolve()
    raw_events, legacy_counters, legacy_injected_urls = inject_legacy_flag_backfill(
        raw_events,
        files=files,
        legacy_csv_path=legacy_csv_path,
        legacy_imgbb_only=bool(args.legacy_imgbb_only),
    )
    print(
        "[flags] legacy backfill: "
        f"source={legacy_csv_path} "
        f"rows_read={legacy_counters['legacy_rows_read']} "
        f"imgbb_kept={legacy_counters['legacy_imgbb_kept']} "
        f"injected={legacy_counters['legacy_injected']} "
        f"unmatched={legacy_counters['legacy_unmatched']} "
        f"ambiguous={legacy_counters['legacy_ambiguous']} "
        f"skipped_non_imgbb={legacy_counters['legacy_skipped_non_imgbb']} "
        f"non_http={legacy_counters['legacy_non_http']} "
        f"blank_flag={legacy_counters['legacy_blank_flag']}",
        file=sys.stderr,
    )

    raw_events = sort_and_dedupe_events(raw_events)

    effective_enable_archive_fallback = bool(args.enable_archive_fallback)
    asset_download_allowlist: set[str] | None = None
    if args.legacy_backfill_only:
        effective_download_missing_archives = False
        effective_enable_archive_fallback = False
        asset_download_allowlist = set(legacy_injected_urls)
        print(
            "[flags] legacy-backfill-only: "
            f"download_missing_alliance_archives={effective_download_missing_archives} "
            f"archive_fallback={effective_enable_archive_fallback} "
            f"allowed_download_urls={len(asset_download_allowlist)}",
            file=sys.stderr,
        )

    archive_context_by_url: dict[str, list[dict[str, Any]]] = {}
    if effective_enable_archive_fallback:
        ranks_path = Path(args.archive_ranks_path).resolve()
        try:
            archive_context_by_url = build_archive_fallback_context(
                raw_events,
                enabled=True,
                ranks_path=ranks_path,
                top_x=int(args.archive_top_x),
            )
            print(
                "[flags] info: archive fallback enabled "
                f"top_x={int(args.archive_top_x)} eligible_urls={len(archive_context_by_url)} ranks_path={ranks_path}",
                file=sys.stderr,
            )
        except Exception as exc:
            print(f"[flags] warning: disabling archive fallback: {exc}", file=sys.stderr)
            archive_context_by_url = {}

    url_to_hash, hash_to_image, hash_to_key = collect_flag_assets(
        raw_events,
        tile_width=int(args.tile_width),
        tile_height=int(args.tile_height),
        timeout_seconds=float(args.download_timeout_seconds),
        max_download_bytes=int(args.max_download_bytes),
        max_flags=int(args.max_flags),
        state_file=state_file,
        cache_dir=cache_dir,
        max_retries=int(args.max_retries),
        retry_base_delay=float(args.retry_base_delay),
        download_concurrency=int(args.download_concurrency),
        retry_failed=bool(args.retry_failed),
        retry_failed_only=bool(args.retry_failed_only),
        reset_failures=bool(args.reset_failures),
        archive_enabled=bool(args.enable_archive_fallback and archive_context_by_url),
        archive_context_by_url=archive_context_by_url,
        archive_cdx_endpoint=str(args.archive_cdx_endpoint),
        archive_replay_prefix=str(args.archive_replay_prefix),
        archive_window_days=int(args.archive_window_days),
        archive_window_widen_days=int(args.archive_window_widen_days),
        archive_max_cdx_rows=int(args.archive_max_cdx_rows),
        archive_concurrency=int(args.archive_concurrency),
        archive_max_retries=int(args.archive_max_retries),
        archive_retry_base_delay=float(args.archive_retry_base_delay),
        download_url_allowlist=asset_download_allowlist,
    )
    runtime_events = build_runtime_events(raw_events, url_to_hash=url_to_hash, hash_to_key=hash_to_key)
    atlas, atlas_meta, assets = build_flag_atlas(
        hash_to_image,
        hash_to_key,
        tile_width=int(args.tile_width),
        tile_height=int(args.tile_height),
    )

    write_outputs(
        flags_output_path=output_path,
        assets_output_path=assets_output_path,
        atlas_webp_output_path=atlas_webp_output_path,
        atlas_png_output_path=atlas_png_output_path,
        runtime_events=runtime_events,
        atlas=atlas,
        atlas_meta=atlas_meta,
        assets=assets,
    )

    if args.pretty:
        print("[flags] warning: --pretty is ignored for MessagePack output", file=sys.stderr)

    print(
        "[flags] wrote "
        f"{output_path} ({len(runtime_events)} events), "
        f"{assets_output_path} ({len(assets)} assets), "
        f"{atlas_webp_output_path}, {atlas_png_output_path}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
