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
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
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
        "schema_version": 1,
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
    if "schema_version" not in loaded:
        loaded["schema_version"] = 1
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


def normalize_image_bytes(image_bytes: bytes, tile_width: int, tile_height: int) -> Image.Image:
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


def hash_normalized_image(image: Image.Image) -> str:
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
) -> tuple[dict[str, str], dict[str, Image.Image], dict[str, str]]:
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

        if status == "downloaded" and not cache_path.exists():
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
    hash_to_image: dict[str, Image.Image] = {}
    hash_failures = 0
    skipped_downloaded = 0
    skipped_non_retry = 0

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
        if not is_supported_remote_url(url):
            return {
                "url": url,
                "status": "failed",
                "cache_file": cache_path.name,
                "error": "unsupported url scheme",
                "http_status": None,
            }

        try:
            if cache_path.exists():
                raw = cache_path.read_bytes()
                http_status = existing_http_status
            else:
                raw, http_status = download_with_retries(
                    url=url,
                    timeout_seconds=timeout_seconds,
                    max_download_bytes=max_download_bytes,
                    max_retries=max_retries,
                    retry_base_delay=retry_base_delay,
                )
                atomic_write_bytes(cache_path, raw)

            normalized = normalize_image_bytes(raw, tile_width=tile_width, tile_height=tile_height)
            image_hash = hash_normalized_image(normalized)
            return {
                "url": url,
                "status": "downloaded",
                "cache_file": cache_path.name,
                "http_status": int(http_status) if http_status is not None else None,
                "content_sha256": hashlib.sha256(raw).hexdigest(),
                "image_hash": image_hash,
                "normalized": normalized,
            }
        except Exception as exc:
            http_status = extract_http_status_from_error(exc)
            return {
                "url": url,
                "status": "failed",
                "cache_file": cache_path.name,
                "error": str(exc),
                "http_status": http_status,
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
                )
                checkpoint_state()
                progress.step(kept=len(hash_to_image), failed=hash_failures, done=done)

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
        f"unique={len(sorted_hashes)}, failures={hash_failures}",
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
    hash_to_image: dict[str, Image.Image],
    hash_to_key: dict[str, str],
    *,
    tile_width: int,
    tile_height: int,
) -> tuple[Image.Image, dict[str, Any], dict[str, dict[str, Any]]]:
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
    atlas: Image.Image,
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

    files, archive_flags = prepare_alliance_archives(
        alliances_dir=alliances_dir,
        index_url=str(args.alliances_index_url),
        download_missing=bool(args.download_missing_alliance_archives),
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