from __future__ import annotations

import re
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from progress import ProgressReporter


FILE_RE = re.compile(r"alliances-(\d{4}-\d{2}-\d{2})\.csv\.zip$", flags=re.IGNORECASE)
DEFAULT_ALLIANCES_INDEX_URL = "https://politicsandwar.com/data/alliances/"
DEFAULT_REQUEST_HEADERS = {
	# Some hosts block default urllib clients but allow standard browser requests.
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
	"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	"Accept-Language": "en-US,en;q=0.9",
	"Referer": "https://politicsandwar.com/",
}


def parse_day_from_filename(path: Path) -> datetime | None:
	match = FILE_RE.search(path.name)
	if not match:
		return None
	return datetime.fromisoformat(match.group(1)).replace(tzinfo=timezone.utc)


def _urlopen_with_headers(url: str, timeout: float):
	request = urllib.request.Request(url, headers=DEFAULT_REQUEST_HEADERS)
	return urllib.request.urlopen(request, timeout=timeout)


def _fetch_alliances_index_filenames(index_url: str) -> list[str]:
	with _urlopen_with_headers(index_url, timeout=30) as response:
		html = response.read().decode("utf-8", errors="replace")

	filenames = set(re.findall(r'href="(alliances-\d{4}-\d{2}-\d{2}\.csv\.zip)"', html, flags=re.IGNORECASE))
	return sorted(filenames)


def _download_missing_alliance_archives(
	*,
	alliances_dir: Path,
	index_url: str,
	start_day: datetime | None,
	end_day: datetime | None,
	flags: list[dict[str, Any]],
) -> None:
	try:
		filenames = _fetch_alliances_index_filenames(index_url)
	except Exception as exc:
		flags.append(
			{
				"severity": "warning",
				"flag": "alliances_index_fetch_failed",
				"value": str(exc),
				"index_url": index_url,
			}
		)
		return

	base = index_url if index_url.endswith("/") else f"{index_url}/"
	needed: list[tuple[str, str]] = []
	for filename in filenames:
		match = FILE_RE.match(filename)
		if not match:
			continue
		day = datetime.fromisoformat(match.group(1)).replace(tzinfo=timezone.utc)
		if start_day and day < start_day:
			continue
		if end_day and day > end_day:
			continue
		needed.append((filename, f"{base}{filename}"))

	downloaded = 0
	skipped = 0
	failed = 0
	processed = 0
	total = len(needed)
	progress = ProgressReporter(label="[alliances] download progress", total=total, unit="files", non_tty_every=100)
	for filename, url in needed:
		destination = alliances_dir / filename
		if destination.exists():
			skipped += 1
			processed += 1
			progress.step(
				downloaded=downloaded,
				skipped=skipped,
				failed=failed,
				done=processed == total,
			)
			continue
		try:
			with _urlopen_with_headers(url, timeout=60) as response, destination.open("wb") as out:
				out.write(response.read())
			downloaded += 1
		except Exception as exc:
			failed += 1
			flags.append(
				{
					"severity": "warning",
					"flag": "alliance_archive_download_failed",
					"file": filename,
					"value": str(exc),
				}
			)
		finally:
			processed += 1
			progress.step(
				downloaded=downloaded,
				skipped=skipped,
				failed=failed,
				done=processed == total,
			)

	if downloaded > 0:
		flags.append(
			{
				"severity": "info",
				"flag": "alliance_archives_downloaded",
				"count": downloaded,
				"target_dir": str(alliances_dir),
			}
		)


def prepare_alliance_archives(
	*,
	alliances_dir: Path,
	index_url: str = DEFAULT_ALLIANCES_INDEX_URL,
	download_missing: bool = False,
	start_day: datetime | None = None,
	end_day: datetime | None = None,
) -> tuple[list[Path], list[dict[str, Any]]]:
	"""Ensure/filter alliance archive files and optionally download missing files."""
	flags: list[dict[str, Any]] = []
	alliances_dir.mkdir(parents=True, exist_ok=True)

	if download_missing:
		_download_missing_alliance_archives(
			alliances_dir=alliances_dir,
			index_url=index_url,
			start_day=start_day,
			end_day=end_day,
			flags=flags,
		)

	files = sorted(alliances_dir.glob("alliances-*.csv.zip"))
	if start_day:
		files = [path for path in files if (day := parse_day_from_filename(path)) is not None and day >= start_day]
	if end_day:
		files = [path for path in files if (day := parse_day_from_filename(path)) is not None and day <= end_day]

	if not files:
		flags.append({"severity": "info", "flag": "alliances_csv_not_found", "value": str(alliances_dir)})

	return files, flags
