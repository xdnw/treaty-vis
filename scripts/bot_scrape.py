#!/usr/bin/env python3
"""Scrape a specific bot's messages from a Discord channel.

This script uses Discord's REST API and paginates with `limit=100` (bulk fetch)
to collect messages efficiently.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from script_paths import WEB_WORK_DATA_DIR


DEFAULT_GUILD_ID = "446601982564892672"
DEFAULT_CHANNEL_ID = "880473974436401182"
DEFAULT_BOT_ID = "672237266940198960"
DISCORD_API_BASE = "https://discord.com/api/v10"


def atomic_write_json(path: Path, payload: Any) -> None:
	"""Write JSON atomically so checkpoints survive crashes."""
	path.parent.mkdir(parents=True, exist_ok=True)
	tmp_path = path.with_suffix(path.suffix + ".tmp")
	tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
	tmp_path.replace(path)


def load_json_or_default(path: Path, default: Any) -> Any:
	if not path.exists():
		return default
	with path.open("r", encoding="utf-8") as handle:
		return json.load(handle)


def discord_get_json(
	token: str,
	endpoint: str,
	params: dict[str, str] | None = None,
	base_delay: float = 0.0,
	max_retries: int = 8,
) -> Any:
	"""GET JSON with retry/backoff and Discord rate-limit handling."""
	url = f"{DISCORD_API_BASE}{endpoint}"
	if params:
		url = f"{url}?{parse.urlencode(params)}"

	headers = {
		"Authorization": f"Bot {token}",
		"User-Agent": "discord-bot-message-scraper/1.0",
	}

	attempt = 0
	while True:
		if base_delay > 0:
			time.sleep(base_delay)

		req = request.Request(url, headers=headers, method="GET")
		try:
			with request.urlopen(req, timeout=30) as resp:
				body = resp.read().decode("utf-8")
				return json.loads(body)
		except error.HTTPError as exc:
			raw = exc.read().decode("utf-8", errors="replace")
			data: dict[str, Any] = {}
			try:
				data = json.loads(raw) if raw else {}
			except json.JSONDecodeError:
				data = {}

			if exc.code == 429:
				retry_after = float(data.get("retry_after", 1.0))
				jitter = random.uniform(0, 0.25)
				wait_for = max(retry_after + jitter, 0.1)
				print(f"[rate-limit] Waiting {wait_for:.2f}s", flush=True)
				time.sleep(wait_for)
				continue

			if exc.code >= 500 and attempt < max_retries:
				backoff = min((2**attempt) + random.uniform(0, 0.5), 30.0)
				print(f"[server-error {exc.code}] retrying in {backoff:.2f}s", flush=True)
				time.sleep(backoff)
				attempt += 1
				continue

			raise RuntimeError(f"Discord API error {exc.code}: {raw}") from exc
		except error.URLError as exc:
			if attempt < max_retries:
				backoff = min((2**attempt) + random.uniform(0, 0.5), 30.0)
				print(f"[network-error] {exc}; retrying in {backoff:.2f}s", flush=True)
				time.sleep(backoff)
				attempt += 1
				continue
			raise RuntimeError(f"Network error: {exc}") from exc


def extract_embeds(message: dict[str, Any]) -> list[dict[str, Any]]:
	embeds: list[dict[str, Any]] = []
	for embed in message.get("embeds", []) or []:
		embeds.append(
			{
				"title": embed.get("title"),
				"description": embed.get("description"),
				"url": embed.get("url"),
				"type": embed.get("type"),
			}
		)
	return embeds


def normalize_message(message: dict[str, Any]) -> dict[str, Any]:
	author = message.get("author") or {}
	return {
		"id": message.get("id"),
		"channel_id": message.get("channel_id"),
		"timestamp": message.get("timestamp"),
		"edited_timestamp": message.get("edited_timestamp"),
		"content": message.get("content"),
		"author": {
			"id": author.get("id"),
			"username": author.get("username"),
			"discriminator": author.get("discriminator"),
			"bot": author.get("bot", False),
		},
		"embeds": extract_embeds(message),
	}


def verify_channel_in_guild(token: str, guild_id: str, channel_id: str, delay: float) -> None:
	channel_info = discord_get_json(
		token=token,
		endpoint=f"/channels/{channel_id}",
		params=None,
		base_delay=delay,
	)
	found_guild = str(channel_info.get("guild_id"))
	if found_guild != str(guild_id):
		raise RuntimeError(
			f"Channel {channel_id} belongs to guild {found_guild}, expected {guild_id}."
		)


def run_scrape(args: argparse.Namespace) -> int:
	state_path = Path(args.state_file)
	output_path = Path(args.output)

	state = load_json_or_default(
		state_path,
		{
			"before": None,
			"requests_made": 0,
			"saved_messages": 0,
			"done": False,
		},
	)
	collected: list[dict[str, Any]] = load_json_or_default(output_path, [])
	known_ids = {str(msg.get("id")) for msg in collected if msg.get("id")}

	print(f"Loaded {len(collected)} existing messages from {output_path}")
	if state.get("done"):
		print("State says scraping is already complete. Nothing to do.")
		return 0

	verify_channel_in_guild(args.token, args.guild_id, args.channel_id, args.delay)

	page_limit = min(max(int(args.page_limit), 1), 100)
	before = state.get("before")
	requests_made = int(state.get("requests_made", 0))

	while True:
		if args.max_requests is not None and requests_made >= args.max_requests:
			print("Reached max_requests; checkpoint saved.")
			break

		params = {"limit": str(page_limit)}
		if before:
			params["before"] = str(before)

		messages = discord_get_json(
			token=args.token,
			endpoint=f"/channels/{args.channel_id}/messages",
			params=params,
			base_delay=args.delay,
		)
		requests_made += 1

		if not messages:
			state.update(
				{
					"before": before,
					"requests_made": requests_made,
					"saved_messages": len(collected),
					"done": True,
				}
			)
			atomic_write_json(state_path, state)
			atomic_write_json(output_path, collected)
			print("No more messages returned. Scrape complete.")
			break

		# Discord returns newest->oldest. Cursor should move to oldest in this page.
		oldest_id = messages[-1].get("id")
		added_this_page = 0
		for msg in messages:
			if str((msg.get("author") or {}).get("id")) != str(args.bot_id):
				continue
			message_id = str(msg.get("id"))
			if not message_id or message_id in known_ids:
				continue

			collected.append(normalize_message(msg))
			known_ids.add(message_id)
			added_this_page += 1

		before = oldest_id
		state.update(
			{
				"before": before,
				"requests_made": requests_made,
				"saved_messages": len(collected),
				"done": False,
			}
		)
		atomic_write_json(output_path, collected)
		atomic_write_json(state_path, state)
		print(
			f"Fetched page {requests_made}: {len(messages)} messages, "
			f"added {added_this_page}, total saved {len(collected)}"
		)

	return 0


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Scrape one bot's messages from a Discord channel")
	parser.add_argument("token", help="Discord bot token")
	parser.add_argument("--guild-id", default=DEFAULT_GUILD_ID)
	parser.add_argument("--channel-id", default=DEFAULT_CHANNEL_ID)
	parser.add_argument("--bot-id", default=DEFAULT_BOT_ID)
	parser.add_argument(
		"--delay",
		type=float,
		default=0.5,
		help="Delay in seconds before each API request (default: 0.5)",
	)
	parser.add_argument(
		"--page-limit",
		type=int,
		default=100,
		help="Messages per request (1-100, default: 100)",
	)
	parser.add_argument(
		"--max-requests",
		type=int,
		default=None,
		help="Optional cap on requests for partial runs/testing",
	)
	parser.add_argument("--output", default=str(WEB_WORK_DATA_DIR / "bot_messages.json"), help="JSON output file")
	parser.add_argument(
		"--state-file",
		default=str(WEB_WORK_DATA_DIR / "scrape_state.json"),
		help="Checkpoint state file for resume support",
	)
	return parser


def main() -> int:
	parser = build_parser()
	args = parser.parse_args()

	try:
		return run_scrape(args)
	except KeyboardInterrupt:
		print("Interrupted by user.", file=sys.stderr)
		return 130
	except Exception as exc:  # noqa: BLE001 - surface fatal errors for CLI use
		print(f"Fatal error: {exc}", file=sys.stderr)
		return 1


if __name__ == "__main__":
	raise SystemExit(main())
