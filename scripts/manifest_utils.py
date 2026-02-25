from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from script_paths import WEB_PUBLIC_DATA_DIR


WEB_MANIFEST_FILES = [
	"treaty_changes_reconciled.msgpack",
	"treaty_changes_reconciled_summary.msgpack",
	"treaty_changes_reconciled_flags.msgpack",
	"flags.msgpack",
	"flag_assets.msgpack",
	"flag_atlas.webp",
	"flag_atlas.png",
	"alliance_scores_daily.msgpack",
]


def _sha256_file(path: Path) -> str:
	return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_json(value: object) -> str:
	return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def refresh_manifest(data_dir: Path = WEB_PUBLIC_DATA_DIR, required_files: list[str] | None = None) -> dict:
	required = required_files or WEB_MANIFEST_FILES
	data_dir.mkdir(parents=True, exist_ok=True)

	files_section: dict[str, dict[str, int | str]] = {}
	for name in required:
		path = data_dir / name
		if not path.exists():
			raise FileNotFoundError(f"Cannot build manifest; missing required file: {path}")
		stat = path.stat()
		files_section[name] = {
			"sizeBytes": int(stat.st_size),
			"sha256": _sha256_file(path),
		}

	manifest = {
		"generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
		"files": files_section,
	}
	id_source = _canonical_json(files_section)
	manifest["datasetId"] = hashlib.sha256(id_source.encode("utf-8")).hexdigest()[:16]

	manifest_path = data_dir / "manifest.json"
	manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
	return manifest
