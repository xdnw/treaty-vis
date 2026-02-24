#!/usr/bin/env python3
"""Download archived snapshots for a URL with stop/resume support.

Default source is archive.org CDX API, but custom CDX and replay endpoints
can be provided for other CDX-compatible archive services.
"""

from __future__ import annotations

import argparse
import gzip
import json
import mimetypes
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from progress import ProgressReporter
from script_paths import WEB_WORK_DATA_DIR
from waybackpy import WaybackMachineCDXServerAPI


DEFAULT_CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
DEFAULT_REPLAY_PREFIX = "https://web.archive.org/web"
DEFAULT_USER_AGENT = (
	"archive-snapshot-downloader/1.0 "
	"(+https://github.com; contact: local-script)"
)


@dataclass
class SnapshotMeta:
	timestamp: str
	original: str
	statuscode: str = ""
	mimetype: str = ""
	digest: str = ""
	length: str = ""


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Download archived snapshots for a URL with resume support.",
	)
	parser.add_argument(
		"url",
		nargs="?",
		default="https://politicsandwar.com/alliances/treatyweb/",
		help="Target URL to fetch archive snapshots for.",
	)
	parser.add_argument(
		"--out-dir",
		default=str(WEB_WORK_DATA_DIR / "archive_downloads"),
		help="Directory where snapshot files are written.",
	)
	parser.add_argument(
		"--state-file",
		default=str(WEB_WORK_DATA_DIR / "archive_state.json"),
		help="JSON state file used for stop/resume tracking.",
	)
	parser.add_argument(
		"--cdx-endpoint",
		default=DEFAULT_CDX_ENDPOINT,
		help="CDX API endpoint (archive.org default).",
	)
	parser.add_argument(
		"--replay-prefix",
		default=DEFAULT_REPLAY_PREFIX,
		help="Replay URL prefix for snapshot downloads.",
	)
	parser.add_argument(
		"--from",
		dest="from_ts",
		default=None,
		help="Start timestamp, e.g. 20180101000000.",
	)
	parser.add_argument(
		"--to",
		dest="to_ts",
		default=None,
		help="End timestamp, e.g. 20260101000000.",
	)
	parser.add_argument(
		"--timeout",
		type=int,
		default=45,
		help="HTTP timeout seconds.",
	)
	parser.add_argument(
		"--sleep",
		type=float,
		default=0.1,
		help="Delay between downloads (seconds).",
	)
	parser.add_argument(
		"--limit",
		type=int,
		default=0,
		help="Max snapshots to download this run (0 = no limit).",
	)
	parser.add_argument(
		"--max-retries",
		type=int,
		default=3,
		help="Retries per snapshot on transient errors.",
	)
	parser.add_argument(
		"--no-collapse-digest",
		action="store_true",
		help="Do not deduplicate by digest in CDX query.",
	)
	parser.add_argument(
		"--user-agent",
		default=DEFAULT_USER_AGENT,
		help="User-Agent header.",
	)
	parser.add_argument(
		"--dry-run",
		action="store_true",
		help="Only list what would be downloaded.",
	)
	parser.add_argument(
		"--retry-only-error-contains",
		default=None,
		help=(
			"Only process snapshots whose current state is failed and whose "
			"last_error contains this text (case-insensitive)."
		),
	)
	return parser.parse_args()


def utc_now_iso() -> str:
	return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_slug(url: str) -> str:
	parsed = urllib.parse.urlparse(url)
	host = parsed.netloc.replace(":", "_") or "unknown-host"
	path = parsed.path.strip("/") or "root"
	joined = f"{host}_{path}".replace("/", "_")
	joined = re.sub(r"[^A-Za-z0-9._-]+", "_", joined)
	return joined[:120]


def load_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
	if not path.exists():
		return default
	try:
		return json.loads(path.read_text(encoding="utf-8"))
	except Exception:
		return default


def save_json(path: Path, data: Dict[str, Any]) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	temp = path.with_suffix(path.suffix + ".tmp")
	temp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
	os.replace(temp, path)


def fetch_cdx_snapshots(args: argparse.Namespace) -> List[SnapshotMeta]:
	collapses = None if args.no_collapse_digest else ["digest"]
	cdx = WaybackMachineCDXServerAPI(
		url=args.url,
		user_agent=args.user_agent,
		start_timestamp=args.from_ts,
		end_timestamp=args.to_ts,
		collapses=collapses,
		max_tries=max(1, args.max_retries),
	)

	# waybackpy defaults to archive.org; allow custom CDX-compatible endpoints.
	if args.cdx_endpoint:
		cdx.endpoint = args.cdx_endpoint

	print(f"[info] querying CDX via waybackpy: {cdx.endpoint}")
	snapshots: List[SnapshotMeta] = []
	for snap in cdx.snapshots():
		snapshots.append(
			SnapshotMeta(
				timestamp=str(getattr(snap, "timestamp", "")),
				original=str(getattr(snap, "original", args.url)),
				statuscode=str(getattr(snap, "statuscode", "")),
				mimetype=str(getattr(snap, "mimetype", "")),
				digest=str(getattr(snap, "digest", "")),
				length=str(getattr(snap, "length", "")),
			)
		)

	if cdx.last_api_request_url:
		print(f"[info] last CDX URL: {cdx.last_api_request_url}")

	snapshots.sort(key=lambda s: s.timestamp)
	return snapshots


def content_extension(mimetype_value: str) -> str:
	ctype = (mimetype_value or "").split(";", 1)[0].strip().lower()
	if not ctype:
		return ".bin"
	ext = mimetypes.guess_extension(ctype)
	if ext:
		return ext
	if ctype.startswith("text/"):
		return ".txt"
	return ".bin"


def sanitize_piece(value: str, fallback: str) -> str:
	value = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-")
	return value or fallback


def build_snapshot_filename(meta: SnapshotMeta) -> str:
	digest_part = sanitize_piece(meta.digest[:14], "nodigest")
	ext = content_extension(meta.mimetype)
	return f"{meta.timestamp}_{digest_part}{ext}"


def build_replay_url(replay_prefix: str, meta: SnapshotMeta) -> str:
	encoded_original = urllib.parse.quote(meta.original, safe=":/?&=%#@+-._~")
	return f"{replay_prefix.rstrip('/')}/{meta.timestamp}id_/{encoded_original}"


def is_downloaded_and_present(meta: SnapshotMeta, state: Dict[str, Any], output_dir: Path) -> bool:
	item = state.get("snapshots", {}).get(meta.timestamp, {})
	if item.get("status") != "downloaded":
		return False
	filename = item.get("filename")
	if not filename:
		return False
	return (output_dir / filename).exists()


def write_state_snapshot(
	state_path: Path,
	state: Dict[str, Any],
	meta: SnapshotMeta,
	status: str,
	filename: str | None = None,
	error: str | None = None,
	size_bytes: int | None = None,
) -> None:
	snapshots = state.setdefault("snapshots", {})
	current = snapshots.get(meta.timestamp, {})
	attempts = int(current.get("attempts", 0)) + 1
	item: Dict[str, Any] = {
		"timestamp": meta.timestamp,
		"original": meta.original,
		"statuscode": meta.statuscode,
		"mimetype": meta.mimetype,
		"digest": meta.digest,
		"status": status,
		"attempts": attempts,
		"updated_at": utc_now_iso(),
	}
	if filename:
		item["filename"] = filename
	elif current.get("filename"):
		item["filename"] = current["filename"]
	if size_bytes is not None:
		item["size_bytes"] = size_bytes
	if error:
		item["last_error"] = error

	snapshots[meta.timestamp] = item
	state["last_updated"] = utc_now_iso()
	save_json(state_path, state)


def download_snapshot(
	meta: SnapshotMeta,
	replay_prefix: str,
	output_dir: Path,
	timeout: int,
	user_agent: str,
	max_retries: int,
) -> tuple[bool, str | None, int | None, str | None]:
	filename = build_snapshot_filename(meta)
	out_path = output_dir / filename

	if out_path.exists() and out_path.stat().st_size > 0:
		return True, filename, out_path.stat().st_size, None

	url = build_replay_url(replay_prefix, meta)
	last_error = None
	for attempt in range(1, max_retries + 1):
		try:
			req = urllib.request.Request(url, headers={"User-Agent": user_agent})
			with urllib.request.urlopen(req, timeout=timeout) as resp:
				data = resp.read()
			if len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B:
				try:
					data = gzip.decompress(data)
				except Exception as exc:
					last_error = f"Gzip decode failed: {exc}"
					time.sleep(min(2.0 * attempt, 6.0))
					continue
			tmp = out_path.with_suffix(out_path.suffix + ".part")
			with tmp.open("wb") as f:
				f.write(data)
			os.replace(tmp, out_path)
			return True, filename, len(data), None
		except urllib.error.HTTPError as exc:
			last_error = f"HTTP {exc.code}: {exc.reason}"
			if exc.code in (404, 410):
				break
		except urllib.error.URLError as exc:
			last_error = f"URL error: {exc.reason}"
		except TimeoutError:
			last_error = "Timeout"
		except Exception as exc:  # pragma: no cover - defensive
			last_error = f"Unexpected error: {exc}"

		# Mild backoff on retryable failures.
		time.sleep(min(2.0 * attempt, 6.0))

	return False, None, None, last_error or "Unknown error"


def repair_gzip_downloads_on_load(
	output_dir: Path,
	state_path: Path,
	state: Dict[str, Any],
) -> tuple[int, int, int]:
	"""Repair downloaded files that were saved as raw gzip payloads."""
	repaired = 0
	skipped = 0
	errors = 0
	state_changed = False

	for item in state.get("snapshots", {}).values():
		if item.get("status") != "downloaded":
			continue
		filename = item.get("filename")
		if not filename:
			continue
		path = output_dir / filename
		if not path.exists() or path.stat().st_size < 2:
			skipped += 1
			continue

		try:
			with path.open("rb") as f:
				prefix = f.read(2)
			if prefix != b"\x1f\x8b":
				skipped += 1
				continue

			raw = path.read_bytes()
			decoded = gzip.decompress(raw)
			tmp = path.with_suffix(path.suffix + ".repair")
			with tmp.open("wb") as f:
				f.write(decoded)
			os.replace(tmp, path)

			item["size_bytes"] = len(decoded)
			item["updated_at"] = utc_now_iso()
			repaired += 1
			state_changed = True
		except Exception:
			errors += 1

	if state_changed:
		state["last_updated"] = utc_now_iso()
		save_json(state_path, state)

	return repaired, skipped, errors


def ensure_state(args: argparse.Namespace, state_path: Path) -> Dict[str, Any]:
	default = {
		"target_url": args.url,
		"cdx_endpoint": args.cdx_endpoint,
		"replay_prefix": args.replay_prefix,
		"created_at": utc_now_iso(),
		"last_updated": utc_now_iso(),
		"snapshots": {},
	}
	state = load_json(state_path, default)
	state.setdefault("snapshots", {})
	state["target_url"] = args.url
	state["cdx_endpoint"] = args.cdx_endpoint
	state["replay_prefix"] = args.replay_prefix
	save_json(state_path, state)
	return state


def main() -> int:
	args = parse_args()

	output_root = Path(args.out_dir)
	site_folder = output_root / safe_slug(args.url)
	site_folder.mkdir(parents=True, exist_ok=True)

	state_path = Path(args.state_file)
	state_path.parent.mkdir(parents=True, exist_ok=True)
	state = ensure_state(args, state_path)
	repaired, checked_not_gzip, repair_errors = repair_gzip_downloads_on_load(
		output_dir=site_folder,
		state_path=state_path,
		state=state,
	)
	if repaired or repair_errors:
		print(
			"[info] load-repair "
			f"repaired={repaired} not_gzip={checked_not_gzip} errors={repair_errors}"
		)

	try:
		snapshots = fetch_cdx_snapshots(args)
	except Exception as exc:
		print(f"[error] failed to fetch snapshot list: {exc}")
		return 2

	if not snapshots:
		print("[info] no snapshots found.")
		return 0

	already = 0
	pending: List[SnapshotMeta] = []
	error_filter = (args.retry_only_error_contains or "").strip().lower()
	for meta in snapshots:
		if is_downloaded_and_present(meta, state, site_folder):
			already += 1
		else:
			if error_filter:
				item = state.get("snapshots", {}).get(meta.timestamp, {})
				if item.get("status") != "failed":
					continue
				last_error = str(item.get("last_error", "")).lower()
				if error_filter not in last_error:
					continue
			pending.append(meta)

	print(
		f"[info] snapshots total={len(snapshots)} already_downloaded={already} "
		f"to_process={len(pending)}"
	)

	if args.limit > 0:
		pending = pending[:args.limit]

	if args.dry_run:
		for meta in pending[:25]:
			print(f"[dry-run] {meta.timestamp} {meta.original}")
		if len(pending) > 25:
			print(f"[dry-run] ... and {len(pending) - 25} more")
		return 0

	downloaded = 0
	failed = 0
	skipped = 0
	progress = ProgressReporter(label="[archive] download progress", total=len(pending), unit="snapshots", non_tty_every=25)

	try:
		for idx, meta in enumerate(pending, start=1):
			ok, filename, size_bytes, error = download_snapshot(
				meta=meta,
				replay_prefix=args.replay_prefix,
				output_dir=site_folder,
				timeout=args.timeout,
				user_agent=args.user_agent,
				max_retries=max(1, args.max_retries),
			)

			if ok and filename:
				downloaded += 1
				write_state_snapshot(
					state_path=state_path,
					state=state,
					meta=meta,
					status="downloaded",
					filename=filename,
					size_bytes=size_bytes,
				)
				print(f"[ok] saved {filename} ({size_bytes} bytes)")
			else:
				if error and ("HTTP 404" in error or "HTTP 410" in error):
					skipped += 1
					write_state_snapshot(
						state_path=state_path,
						state=state,
						meta=meta,
						status="missing",
						error=error,
					)
					print(f"[skip] {meta.timestamp} not available: {error}")
				else:
					failed += 1
					write_state_snapshot(
						state_path=state_path,
						state=state,
						meta=meta,
						status="failed",
						error=error,
					)
					print(f"[fail] {meta.timestamp}: {error}")

			progress.step(
				downloaded=downloaded,
				failed=failed,
				skipped=skipped,
				done=idx == len(pending),
			)

			if args.sleep > 0:
				time.sleep(args.sleep)

	except KeyboardInterrupt:
		progress.close()
		print("\n[info] interrupted by user; state saved, safe to resume.")

	print(
		"[done] "
		f"downloaded={downloaded} failed={failed} skipped={skipped} "
		f"output={site_folder} state={state_path}"
	)
	return 0


if __name__ == "__main__":
	sys.exit(main())
