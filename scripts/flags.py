from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime
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
        default=524288,
        help="Maximum bytes allowed per image download",
    )
    parser.add_argument(
        "--max-flags",
        type=int,
        default=5000,
        help="Maximum number of unique normalized flags to keep",
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
        raise RuntimeError("max download bytes must be positive")
    if args.max_flags <= 0:
        raise RuntimeError("max flags must be positive")


def is_supported_remote_url(url: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def download_image_bytes(url: str, timeout_seconds: float, max_download_bytes: int) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "treaty-vis-flags/1.0"})
    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
        data = response.read(max_download_bytes + 1)
    if len(data) > max_download_bytes:
        raise RuntimeError(f"download too large (>{max_download_bytes} bytes)")
    if not data:
        raise RuntimeError("empty download")
    return data


def normalize_image_bytes(image_bytes: bytes, tile_width: int, tile_height: int) -> Image.Image:
    assert Image is not None
    try:
        with Image.open(io.BytesIO(image_bytes)) as src:
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
    progress = ProgressReporter(label="[flags] image progress", total=len(all_urls), unit="urls", non_tty_every=100)

    url_to_hash: dict[str, str] = {}
    hash_to_image: dict[str, Image.Image] = {}
    hash_failures = 0

    for index, url in enumerate(all_urls):
        done = index == (len(all_urls) - 1)
        if not is_supported_remote_url(url):
            hash_failures += 1
            print(f"[flags] warning: skipping unsupported flag url: {url}", file=sys.stderr)
            progress.step(kept=len(hash_to_image), failed=hash_failures, done=done)
            continue

        try:
            raw = download_image_bytes(url, timeout_seconds=timeout_seconds, max_download_bytes=max_download_bytes)
            normalized = normalize_image_bytes(raw, tile_width=tile_width, tile_height=tile_height)
            image_hash = hash_normalized_image(normalized)
        except Exception as exc:
            hash_failures += 1
            print(f"[flags] warning: failed flag download/decode {url}: {exc}", file=sys.stderr)
            progress.step(kept=len(hash_to_image), failed=hash_failures, done=done)
            continue

        url_to_hash[url] = image_hash
        if image_hash not in hash_to_image:
            hash_to_image[image_hash] = normalized

        progress.step(kept=len(hash_to_image), failed=hash_failures, done=done)

    sorted_hashes = sorted(hash_to_image.keys())
    if len(sorted_hashes) > max_flags:
        raise RuntimeError(
            f"unique normalized flags exceeded max ({len(sorted_hashes)} > {max_flags}); rerun with higher --max-flags"
        )

    hash_to_key: dict[str, str] = {}
    for index, image_hash in enumerate(sorted_hashes):
        hash_to_key[image_hash] = f"f{index:x}"

    print(
        f"[flags] image totals: urls={len(all_urls)}, unique={len(sorted_hashes)}, failures={hash_failures}",
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
    dropped = 0

    for event in raw_events:
        raw_url = str(event.get("raw_flag_url") or "").strip()
        current_hash = url_to_hash.get(raw_url)
        current_key = hash_to_key.get(current_hash or "") if current_hash else None
        if not current_key:
            dropped += 1
            continue

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

    if dropped:
        print(f"[flags] warning: dropped {dropped} events with unresolved flag assets", file=sys.stderr)

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
    flags_output_path.write_bytes(msgpack.packb(flags_payload, use_bin_type=True))

    atlas_meta_with_paths = {
        **atlas_meta,
        "webp": atlas_webp_output_path.name,
        "png": atlas_png_output_path.name,
    }
    assets_payload = {"atlas": atlas_meta_with_paths, "assets": assets}
    assets_output_path.write_bytes(msgpack.packb(assets_payload, use_bin_type=True))

    try:
        atlas.save(atlas_png_output_path, format="PNG")
    except Exception as exc:
        raise RuntimeError(f"failed to write PNG atlas: {exc}") from exc

    try:
        atlas.save(atlas_webp_output_path, format="WEBP", lossless=True, method=6)
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