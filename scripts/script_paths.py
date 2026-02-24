from __future__ import annotations

from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent


def _resolve_web_dir() -> Path:
	"""Prefer current working directory when invoked from the web project root."""
	cwd = Path.cwd().resolve()
	if (cwd / "scripts").is_dir() and (cwd / "public").is_dir() and (cwd / "data").is_dir():
		return cwd
	return SCRIPTS_DIR.parent


WEB_DIR = _resolve_web_dir()
REPO_DIR = WEB_DIR.parent

WEB_PUBLIC_DATA_DIR = WEB_DIR / "public" / "data"
WEB_WORK_DATA_DIR = WEB_DIR / "data"
