#!/usr/bin/env python3
"""Convert archived treaty web HTML snapshots into one JSON archive.

Input directory default:
	archive_downloads/politicsandwar.com_alliances_treatyweb

Output file default:
	treaties_archive.json

The output contains one entry per parseable snapshot with:
	- date (ISO timestamp from filename prefix)
	- timestamp (raw YYYYMMDDHHMMSS)
	- source_file
	- nodes
	- edges

Files that do not include treaty datasets (or fail to parse) are skipped and
printed to stdout with the reason.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from progress import ProgressReporter
from script_paths import WEB_WORK_DATA_DIR


DEFAULT_INPUT_DIR = WEB_WORK_DATA_DIR / "archive_downloads" / "politicsandwar.com_alliances_treatyweb"
DEFAULT_OUTPUT_FILE = WEB_WORK_DATA_DIR / "treaties_archive.json"

NODES_RE = re.compile(
	r"var\s+nodes\s*=\s*new\s+vis\.DataSet\(\s*(\[.*?\])\s*\)\s*;",
	re.IGNORECASE | re.DOTALL,
)
EDGES_RE = re.compile(
	r"var\s+edges\s*=\s*new\s+vis\.DataSet\(\s*(\[.*?\])\s*\)\s*;",
	re.IGNORECASE | re.DOTALL,
)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Convert treatyweb HTML snapshots into one treaties archive JSON.",
	)
	parser.add_argument(
		"--input-dir",
		default=str(DEFAULT_INPUT_DIR),
		help="Directory containing archived treatyweb HTML files.",
	)
	parser.add_argument(
		"--output",
		default=str(DEFAULT_OUTPUT_FILE),
		help="Output JSON file path.",
	)
	return parser.parse_args()


def extract_timestamp_from_filename(path: Path) -> str | None:
	match = re.match(r"^(\d{14})_", path.name)
	if not match:
		return None
	return match.group(1)


def timestamp_to_iso(timestamp: str) -> str:
	dt = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
	return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def js_object_literal_to_python(text: str) -> str:
	# Quote unquoted object keys: {id:1, name:'x'} -> {'id':1, 'name':'x'}
	normalized = re.sub(r"([{,]\s*)([A-Za-z_$][A-Za-z0-9_$]*)(\s*:)", r"\1'\2'\3", text)

	# Normalize JavaScript literals for Python's ast.literal_eval.
	normalized = re.sub(r"\btrue\b", "True", normalized, flags=re.IGNORECASE)
	normalized = re.sub(r"\bfalse\b", "False", normalized, flags=re.IGNORECASE)
	normalized = re.sub(r"\bnull\b", "None", normalized, flags=re.IGNORECASE)
	return normalized


def parse_dataset_block(raw_block: str) -> list[dict[str, Any]]:
	py_literal = js_object_literal_to_python(raw_block)
	parsed = ast.literal_eval(py_literal)
	if not isinstance(parsed, list):
		raise ValueError("Dataset block did not parse into a list")
	return parsed


def parse_snapshot_file(path: Path) -> tuple[dict[str, Any] | None, str | None]:
	timestamp = extract_timestamp_from_filename(path)
	if not timestamp:
		return None, "filename missing timestamp prefix"

	html = path.read_text(encoding="utf-8", errors="ignore")

	nodes_match = NODES_RE.search(html)
	edges_match = EDGES_RE.search(html)
	if not nodes_match or not edges_match:
		return None, "no treaty dataset found"

	try:
		nodes = parse_dataset_block(nodes_match.group(1))
		edges = parse_dataset_block(edges_match.group(1))
	except (SyntaxError, ValueError) as exc:
		return None, f"parse error: {exc}"

	entry = {
		"date": timestamp_to_iso(timestamp),
		"timestamp": timestamp,
		"source_file": path.name,
		"nodes": nodes,
		"edges": edges,
	}
	return entry, None


def main() -> int:
	args = parse_args()
	input_dir = Path(args.input_dir)
	output_path = Path(args.output)

	if not input_dir.exists() or not input_dir.is_dir():
		print(f"Input directory not found: {input_dir}")
		return 1

	html_files = sorted(input_dir.glob("*.html"))
	if not html_files:
		print(f"No .html files found in: {input_dir}")
		return 1

	results: list[dict[str, Any]] = []
	skipped: list[tuple[str, str]] = []
	progress = ProgressReporter(label="[archive_to_treaties] file progress", total=len(html_files), unit="files", non_tty_every=50)

	for idx, html_file in enumerate(html_files, start=1):
		entry, reason = parse_snapshot_file(html_file)
		if entry is None:
			skipped.append((html_file.name, reason or "unknown reason"))
			progress.step(parsed=len(results), skipped=len(skipped), done=idx == len(html_files))
			continue
		results.append(entry)
		progress.step(parsed=len(results), skipped=len(skipped), done=idx == len(html_files))

	payload = {
		"source_directory": str(input_dir),
		"total_files": len(html_files),
		"parsed_files": len(results),
		"skipped_files": len(skipped),
		"snapshots": results,
	}

	output_path.parent.mkdir(parents=True, exist_ok=True)
	output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

	print(f"Wrote {len(results)} snapshots to {output_path}")
	if skipped:
		print("Skipped files:")
		for name, reason in skipped:
			print(f"  - {name}: {reason}")

	return 0


if __name__ == "__main__":
	raise SystemExit(main())