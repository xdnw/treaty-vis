#!/usr/bin/env python3
"""Reformat bot_messages.json into normalized treaty event records.

Single source of truth for parsing behavior:

SUPPORTED FORMATS
- Embed title/description:
	- "Signed|Cancelled|Expired|Extended|Ended <TYPE> | rank #N"
	- "Treaty signed|cancelled|ended <TYPE>"
  - "Upgraded|Downgraded <OLD_TYPE>-><NEW_TYPE> | rank #N"
  - Description with:
	- "Time remaining: Permanent (Permanent)" -> -1
	- "Time remaining: ... (123 turns)" -> 123
	- "From: [Alliance](.../alliance/id=1234)"
	- "To: [Alliance](.../alliance/id=5678)"
- Content-only batch blocks:
  - "**<header>**" segments with same body fields as above.

SKIPPED FORMATS
- Clearly unrelated bot messages (no treaty header and no treaty From/To evidence).

FLAGGED CONDITIONS
- Oversized content/embed payloads (likely buggy logs).
- Duplicate message payloads.
- Duplicate treaty events.
- Treaty-like but malformed entries.
- Missing time remaining on otherwise parseable entries.

Outputs:
- treaties.json: normalized treaty records (no message id field).
- treaties_skipped.json: parser flags excluding defaulted-time and skipped-non-treaty.
- treaties_skipped_auxiliary.json: only missing_time_remaining_defaulted and skipped_non_treaty.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from progress import ProgressReporter
from script_paths import WEB_WORK_DATA_DIR


SUPPORTED_FORMATS = [
	"embed_standard",
	"embed_action_only_header",
	"embed_legacy_treaty_prefix",
	"content_segmented_markdown",
]
SKIPPED_FORMATS = [
	"non_treaty_message",
	"treaty_like_unparseable_message",
]
FLAGGED_FORMATS = [
	"oversized_payload",
	"duplicate_message_payload",
	"duplicate_treaty_event",
	"churn_upgrade_downgrade_suppressed",
	"missing_treaty_type_defaulted",
	"missing_time_remaining_defaulted",
	"malformed_treaty_like",
	"truncated_treaty_fragment",
	"skipped_non_treaty",
]

SPLIT_FLAG_TYPES = {
	"duplicate_treaty_event",
	"missing_time_remaining_defaulted",
	"skipped_non_treaty",
}

SILENT_SKIP_CATEGORIES = {
	"non_treaty_created_alliance_embed",
	"non_treaty_empty_message",
}

DEFAULT_TIME_REMAINING_DAYS = 180
HOURS_PER_TURN = 2
DEFAULT_TIME_REMAINING_TURNS = (DEFAULT_TIME_REMAINING_DAYS * 24) // HOURS_PER_TURN

VALID_ACTIONS = {
	"signed",
	"cancelled",
	"expired",
	"extended",
	"ended",
	"terminated",
	"termination",
	"upgraded",
	"downgraded",
}

ACTION_ALIASES = {
	"terminated": "ended",
	"termination": "ended",
}

HEADER_RE = re.compile(
	r"^\s*(?:Treaty\s+)?(?P<action>Signed|Cancelled|Expired|Extended|Ended|Terminated|Termination|Upgraded|Downgraded)\s*(?:\|\s*)?"
	r"(?P<treaty>.+?)(?:\s*\|\s*rank\s*#\d+)?\s*$",
	re.IGNORECASE,
)
ACTION_ONLY_HEADER_RE = re.compile(
	r"^\s*(?:Treaty\s+)?(?P<action>Signed|Cancelled|Expired|Extended|Ended|Terminated|Termination|Upgraded|Downgraded)"
	r"(?:\s*\|\s*rank\s*#\d+)?\s*$",
	re.IGNORECASE,
)
SEGMENT_HEADER_RE = re.compile(r"\*\*(?P<header>[^*\n]+)\*\*")
TIME_REMAINING_RE = re.compile(r"Time\s+remaining\s*:\s*(?P<value>[^\n\r]+)", re.IGNORECASE)
TURN_COUNT_RE = re.compile(r"\((?P<turns>\d+)\s+turns?\)", re.IGNORECASE)
ALLIANCE_LINE_RE = re.compile(
	r"\b(?P<label>From|To)\s*:\s*\[(?P<name>.+?)\]"
	r"\(\s*<?https?\s*:?\s*/\s*/\s*politicsandwar\.com/alliance/id\s*=\s*(?P<id>\d+)"
	r"[^)]*?>?\s*\)",
	re.IGNORECASE | re.DOTALL,
)


@dataclass
class ParsedRecord:
	record: dict[str, Any]
	flags: list[dict[str, Any]]


def increment_counter(counter: dict[str, int], key: str) -> None:
	counter[key] = counter.get(key, 0) + 1


def message_excerpt(message: dict[str, Any], max_chars: int = 400) -> str:
	content = str(message.get("content") or "")
	if content.strip():
		text = re.sub(r"\s+", " ", content).strip()
		return text[:max_chars]

	embeds = message.get("embeds", []) or []
	if embeds:
		first = embeds[0]
		title = str(first.get("title") or "").strip()
		desc = re.sub(r"\s+", " ", str(first.get("description") or "")).strip()
		merged = f"{title} {desc}".strip()
		return merged[:max_chars]

	return ""


def load_json(path: Path) -> Any:
	with path.open("r", encoding="utf-8") as handle:
		return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def canonical_action(value: str) -> str:
	action = value.strip().lower()
	action = ACTION_ALIASES.get(action, action)
	if action not in VALID_ACTIONS:
		raise ValueError(f"Unsupported action: {value}")
	return action


def canonical_treaty_type(value: str) -> str:
	treaty = re.sub(r"\s+", " ", value.strip())
	treaty = treaty.replace(" -> ", "->").replace("- >", "->").replace("<-", "<-")
	return treaty.upper()


def parse_time_remaining_turns(text: str) -> int | None:
	match = TIME_REMAINING_RE.search(text)
	if not match:
		return None

	value = match.group("value")
	if "permanent" in value.lower():
		return -1

	turn_match = TURN_COUNT_RE.search(value)
	if turn_match:
		return int(turn_match.group("turns"))

	return None


def clean_name(name: str) -> str:
	return re.sub(r"\s+", " ", name).strip()


def parse_alliances(text: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
	found: dict[str, dict[str, Any]] = {}
	for match in ALLIANCE_LINE_RE.finditer(text):
		label = match.group("label").lower()
		found[label] = {
			"id": int(match.group("id")),
			"name": clean_name(match.group("name")),
		}

	if "from" not in found or "to" not in found:
		return None
	return found["from"], found["to"]


def parse_header(header: str) -> tuple[str, str] | None:
	match = HEADER_RE.match(header.strip())
	if match:
		action = canonical_action(match.group("action"))
		treaty_type = canonical_treaty_type(match.group("treaty"))
		return action, treaty_type

	action_only_match = ACTION_ONLY_HEADER_RE.match(header.strip())
	if not action_only_match:
		return None
	action = canonical_action(action_only_match.group("action"))
	# Action-only "Extended" means the action is known but the treaty type is omitted.
	if action == "extended":
		return action, ""
	return action, "UNKNOWN"


def treaty_like_text(text: str) -> bool:
	lowered = text.lower()
	return (
		"from:" in lowered
		and "to:" in lowered
		and any(
			word in lowered
			for word in (
				"signed",
				"cancelled",
				"expired",
				"extended",
				"ended",
				"terminated",
				"termination",
				"upgraded",
				"downgraded",
			)
		)
	)


def action_hint_present(text: str) -> bool:
	lowered = text.lower()
	return any(
		word in lowered
		for word in (
			"signed",
			"cancelled",
			"expired",
			"extended",
			"ended",
			"terminated",
			"termination",
			"upgraded",
			"downgraded",
		)
	)


def has_from_to_labels(text: str) -> bool:
	lowered = text.lower()
	return "from:" in lowered and "to:" in lowered


def has_truncated_alliance_line(body: str, *, label: str) -> bool:
	line_match = re.search(rf"\b{label}\s*:\s*(?P<value>[^\n\r]*)", body, re.IGNORECASE)
	if not line_match:
		return False
	value = (line_match.group("value") or "").strip()
	if not value:
		return True
	if "alliance/id=" in value.lower():
		return False
	if value.startswith("[") and "](http" not in value.lower():
		return True
	return False


def is_unknown_category(category: str) -> bool:
	lowered = category.lower()
	return "unknown" in lowered or lowered.startswith("other_")


def classify_treaty_unit_failure(header: str, body: str) -> str:
	if not parse_header(header):
		header_lower = header.lower()
		if action_hint_present(header):
			if "rank #" in header_lower:
				return "unrecognized_header_with_rank"
			if "|" in header:
				return "unrecognized_header_pipe_format"
			return "unrecognized_header_action_present"
		return "unrecognized_header_no_action"

	alliances = parse_alliances(body)
	if alliances:
		return "unknown"

	if has_truncated_alliance_line(body, label="From"):
		return "truncated_from_line"
	if has_truncated_alliance_line(body, label="To"):
		return "truncated_to_line"

	lowered = body.lower()
	has_from = "from:" in lowered
	has_to = "to:" in lowered
	has_alliance_url = "politicsandwar.com/alliance/id=" in lowered

	if has_from and has_to and has_alliance_url:
		return "alliance_link_format_error"
	if has_from and not has_to:
		return "missing_to_line"
	if has_to and not has_from:
		return "missing_from_line"
	if has_from and has_to:
		return "from_to_present_but_unparsed"
	if "time remaining:" in lowered:
		return "time_present_but_missing_from_to"
	return "missing_from_to_lines"


def classify_non_treaty_skip(message: dict[str, Any]) -> str:
	content = str(message.get("content") or "").strip()
	embeds = message.get("embeds", []) or []

	if not content and not embeds:
		return "empty_message"

	if embeds and not content:
		titles = " ".join(str(embed.get("title") or "") for embed in embeds).lower()
		descriptions = " ".join(str(embed.get("description") or "") for embed in embeds).lower()
		if "created: alliance" in titles:
			return "created_alliance_embed"
		if "politics & war" in titles or "politicsandwar.com" in descriptions:
			return "link_preview_embed"
		if not titles.strip() and not descriptions.strip():
			return "embed_only_blank"
		if action_hint_present(titles) or action_hint_present(descriptions):
			has_from_to_embed = has_from_to_labels(titles) or has_from_to_labels(descriptions)
			if has_from_to_embed:
				return "embed_action_and_from_to_unparsed"
			return "embed_action_without_from_to"
		if has_from_to_labels(titles) or has_from_to_labels(descriptions):
			return "embed_from_to_without_action"
		return "embed_only_non_treaty_plain"

	if content and not embeds:
		if has_from_to_labels(content) and not action_hint_present(content):
			return "from_to_without_action_words"
		if action_hint_present(content) and not has_from_to_labels(content):
			return "action_words_without_from_to"
		if content.startswith("remaining:"):
			return "truncated_content_fragment"
		if "**" in content and not SEGMENT_HEADER_RE.search(content):
			return "content_markdown_without_valid_headers"
		return "content_only_non_treaty_plain"

	if content and embeds:
		if action_hint_present(content) and not has_from_to_labels(content):
			return "mixed_message_action_without_from_to"
		if has_from_to_labels(content) and not action_hint_present(content):
			return "mixed_message_from_to_without_action"
		embed_text = " ".join(
			f"{str(embed.get('title') or '')} {str(embed.get('description') or '')}"
			for embed in embeds
		)
		if action_hint_present(embed_text) and has_from_to_labels(embed_text):
			return "mixed_message_embed_treaty_like_unparsed"
		return "mixed_message_non_treaty_plain"

	return "unknown_non_treaty_pattern"


def classify_unparseable_treaty_like_message(message: dict[str, Any]) -> str:
	content = str(message.get("content") or "")
	embeds = message.get("embeds", []) or []

	if content:
		has_headers = bool(SEGMENT_HEADER_RE.search(content))
		has_action = action_hint_present(content)
		has_from_to = has_from_to_labels(content)
		if has_action and has_from_to and not has_headers:
			return "content_treaty_like_without_segment_headers"
		if has_headers and not has_from_to:
			return "content_headers_without_from_to"
		if has_headers and has_from_to:
			return "content_headers_present_but_unparsed"

	if embeds:
		return "embed_treaty_like_but_unparsed"

	return "unknown_treaty_like_unparseable"


def register_category_example(
	store: dict[str, dict[str, Any]],
	*,
	category: str,
	message: dict[str, Any],
	context: dict[str, Any] | None = None,
) -> None:
	entry = store.get(category)
	if entry is None:
		store[category] = {
			"count": 1,
			"first_occurrence_timestamp": str(message.get("timestamp") or ""),
			"example_excerpt": message_excerpt(message),
			"example_message_json": message,
			"first_context": context,
		}
		return
	entry["count"] += 1


def build_flag(
	timestamp: str,
	severity: str,
	flag_type: str,
	details: str,
	raw_excerpt: str,
	source_message_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
	flag = {
		"timestamp": timestamp,
		"severity": severity,
		"flag_type": flag_type,
		"details": details,
		"raw_excerpt": raw_excerpt[:500],
	}
	if source_message_json is not None:
		flag["source_message_json"] = source_message_json
	return flag


def parse_treaty_unit(
	*,
	timestamp: str,
	header: str,
	body: str,
	parser_format: str,
	source_message_json: dict[str, Any] | None = None,
) -> ParsedRecord | None:
	parsed_header = parse_header(header)
	if not parsed_header:
		return None

	action, treaty_type = parsed_header
	alliances = parse_alliances(body)
	if not alliances:
		return None

	from_alliance, to_alliance = alliances
	time_remaining_turns = parse_time_remaining_turns(body)
	flags: list[dict[str, Any]] = []
	if treaty_type == "UNKNOWN":
		flags.append(
			build_flag(
				timestamp=timestamp,
				severity="warning",
				flag_type="missing_treaty_type_defaulted",
				details="Treaty parsed from action-only header; treaty type defaulted to UNKNOWN.",
				raw_excerpt=f"{header}\n{body}",
				source_message_json=source_message_json,
			)
		)
	if time_remaining_turns is None:
		time_remaining_turns = DEFAULT_TIME_REMAINING_TURNS
		flags.append(
			build_flag(
				timestamp=timestamp,
				severity="warning",
				flag_type="missing_time_remaining_defaulted",
				details=(
					"Treaty parsed without Time remaining; "
					f"defaulted to {DEFAULT_TIME_REMAINING_DAYS}d ({DEFAULT_TIME_REMAINING_TURNS} turns)."
				),
				raw_excerpt=f"{header}\n{body}",
				source_message_json=source_message_json,
			)
		)

	record = {
		"timestamp": timestamp,
		"action": action,
		"treaty_type": treaty_type,
		"from_alliance_id": from_alliance["id"],
		"from_alliance_name": from_alliance["name"],
		"to_alliance_id": to_alliance["id"],
		"to_alliance_name": to_alliance["name"],
		"time_remaining_turns": time_remaining_turns,
		"parser_format": parser_format,
	}
	return ParsedRecord(record=record, flags=flags)


def parse_embed_entries(
	message: dict[str, Any],
) -> tuple[
	list[dict[str, Any]],
	list[dict[str, Any]],
	bool,
	dict[str, int],
	dict[str, dict[str, Any]],
	list[dict[str, Any]],
]:
	timestamp = str(message.get("timestamp") or "")
	records: list[dict[str, Any]] = []
	flags: list[dict[str, Any]] = []
	malformed_counts: dict[str, int] = {}
	malformed_examples: dict[str, dict[str, Any]] = {}
	unknown_instances: list[dict[str, Any]] = []
	parsed_any = False

	for embed in message.get("embeds", []) or []:
		title = str(embed.get("title") or "").strip()
		description = str(embed.get("description") or "").strip()
		if not title:
			continue

		unit = parse_treaty_unit(
			timestamp=timestamp,
			header=title,
			body=description,
			parser_format="embed_standard",
			source_message_json=message,
		)
		if unit:
			parsed_any = True
			records.append(unit.record)
			flags.extend(unit.flags)
			continue

		combined = f"{title}\n{description}".strip()
		if treaty_like_text(combined):
			reason = f"embed_{classify_treaty_unit_failure(title, description)}"
			if reason in {"embed_truncated_from_line", "embed_truncated_to_line"}:
				flags.append(
					build_flag(
						timestamp=timestamp,
						severity="info",
						flag_type="truncated_treaty_fragment",
						details=f"Treaty-like embed appears truncated ({reason}).",
						raw_excerpt=combined,
						source_message_json=message,
					)
				)
				continue
			increment_counter(malformed_counts, reason)
			register_category_example(
				malformed_examples,
				category=reason,
				message=message,
				context={
					"source": "embed",
					"title": title,
					"description_excerpt": description[:500],
				},
			)
			if is_unknown_category(reason):
				unknown_instances.append(
					{
						"classification_group": "malformed",
						"category": reason,
						"message_json": message,
						"context": {
							"source": "embed",
							"title": title,
							"description_excerpt": description[:500],
						},
					}
				)
			flags.append(
				build_flag(
					timestamp=timestamp,
					severity="error",
					flag_type="malformed_treaty_like",
					details=f"Treaty-like embed could not be parsed ({reason}).",
					raw_excerpt=combined,
					source_message_json=message,
				)
			)
	return records, flags, parsed_any, malformed_counts, malformed_examples, unknown_instances


def parse_content_segments(
	message: dict[str, Any],
) -> tuple[
	list[dict[str, Any]],
	list[dict[str, Any]],
	bool,
	dict[str, int],
	dict[str, dict[str, Any]],
	list[dict[str, Any]],
]:
	timestamp = str(message.get("timestamp") or "")
	content = str(message.get("content") or "")
	records: list[dict[str, Any]] = []
	flags: list[dict[str, Any]] = []
	malformed_counts: dict[str, int] = {}
	malformed_examples: dict[str, dict[str, Any]] = {}
	unknown_instances: list[dict[str, Any]] = []
	parsed_any = False

	matches = list(SEGMENT_HEADER_RE.finditer(content))
	if not matches:
		return records, flags, parsed_any, malformed_counts, malformed_examples, unknown_instances

	for idx, match in enumerate(matches):
		header = match.group("header").strip()
		body_start = match.end()
		body_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(content)
		body = content[body_start:body_end].strip()

		unit = parse_treaty_unit(
			timestamp=timestamp,
			header=header,
			body=body,
			parser_format="content_segmented_markdown",
			source_message_json=message,
		)
		if unit:
			parsed_any = True
			records.append(unit.record)
			flags.extend(unit.flags)
			continue

		merged = f"{header}\n{body}"
		if treaty_like_text(merged):
			reason = f"content_{classify_treaty_unit_failure(header, body)}"
			if reason in {"content_truncated_from_line", "content_truncated_to_line"}:
				flags.append(
					build_flag(
						timestamp=timestamp,
						severity="info",
						flag_type="truncated_treaty_fragment",
						details=f"Treaty-like content segment appears truncated ({reason}).",
						raw_excerpt=merged,
						source_message_json=message,
					)
				)
				continue
			increment_counter(malformed_counts, reason)
			register_category_example(
				malformed_examples,
				category=reason,
				message=message,
				context={
					"source": "content_segment",
					"header": header,
					"body_excerpt": body[:500],
				},
			)
			if is_unknown_category(reason):
				unknown_instances.append(
					{
						"classification_group": "malformed",
						"category": reason,
						"message_json": message,
						"context": {
							"source": "content_segment",
							"header": header,
							"body_excerpt": body[:500],
						},
					}
				)
			flags.append(
				build_flag(
					timestamp=timestamp,
					severity="error",
					flag_type="malformed_treaty_like",
					details=f"Treaty-like content segment could not be parsed ({reason}).",
					raw_excerpt=merged,
					source_message_json=message,
				)
			)

	return records, flags, parsed_any, malformed_counts, malformed_examples, unknown_instances


def message_payload_signature(message: dict[str, Any]) -> str:
	embeds = message.get("embeds", []) or []
	embed_blob = "\n".join(
		f"{str(embed.get('title') or '').strip()}\n{str(embed.get('description') or '').strip()}"
		for embed in embeds
	)
	blob = f"{str(message.get('timestamp') or '')}\n{str(message.get('content') or '').strip()}\n{embed_blob}"
	return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def treaty_fingerprint(record: dict[str, Any]) -> str:
	# Bucket timestamp to minute to catch repeated dispatches with tiny timing differences.
	ts = str(record.get("timestamp") or "")
	ts_bucket = ts[:16]
	payload = "|".join(
		[
			str(record.get("action") or ""),
			str(record.get("treaty_type") or ""),
			str(record.get("from_alliance_id") or ""),
			str(record.get("to_alliance_id") or ""),
			str(record.get("time_remaining_turns")),
			ts_bucket,
		]
	)
	return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _pair_key_from_record(record: dict[str, Any]) -> tuple[int, int]:
	left = int(record.get("from_alliance_id") or 0)
	right = int(record.get("to_alliance_id") or 0)
	return (left, right) if left <= right else (right, left)


def suppress_upgrade_downgrade_churn(
	*,
	treaties: list[dict[str, Any]],
	flags: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
	"""Suppress clearly glitchy upgrade/downgrade storms while preserving normal events.

	This only triggers for extreme same-day, same-pair bursts with dense per-minute
	repetition and only for upgraded/downgraded records.
	"""
	if not treaties:
		return treaties, flags

	clusters: dict[tuple[str, int, int], list[int]] = {}
	for idx, record in enumerate(treaties):
		action = str(record.get("action") or "").lower()
		treaty_type = str(record.get("treaty_type") or "")
		if action not in {"upgraded", "downgraded"}:
			continue
		if "->" not in treaty_type:
			continue
		timestamp = str(record.get("timestamp") or "")
		if not timestamp:
			continue
		day = timestamp[:10]
		pair_min, pair_max = _pair_key_from_record(record)
		key = (day, pair_min, pair_max)
		clusters.setdefault(key, []).append(idx)

	to_remove: set[int] = set()
	new_flags = list(flags)

	for (day, pair_min, pair_max), indexes in clusters.items():
		if len(indexes) < 300:
			continue

		per_minute: dict[str, int] = {}
		for idx in indexes:
			ts_minute = str(treaties[idx].get("timestamp") or "")[:16]
			per_minute[ts_minute] = per_minute.get(ts_minute, 0) + 1

		unique_minutes = len(per_minute)
		max_per_minute = max(per_minute.values()) if per_minute else 0
		avg_per_minute = len(indexes) / max(1, unique_minutes)

		# Conservative thresholding to avoid suppressing normal high-activity periods.
		if unique_minutes < 10:
			continue
		if max_per_minute < 20:
			continue
		if avg_per_minute < 10:
			continue

		to_remove.update(indexes)
		cluster_timestamps = [str(treaties[idx].get("timestamp") or "") for idx in indexes]
		first_ts = min(cluster_timestamps) if cluster_timestamps else ""
		last_ts = max(cluster_timestamps) if cluster_timestamps else ""
		new_flags.append(
			build_flag(
				timestamp=first_ts,
				severity="warning",
				flag_type="churn_upgrade_downgrade_suppressed",
				details=(
					"Suppressed extreme upgrade/downgrade churn burst "
					f"for pair {pair_min}-{pair_max} on {day}: "
					f"records={len(indexes)}, unique_minutes={unique_minutes}, "
					f"max_per_minute={max_per_minute}, avg_per_minute={avg_per_minute:.2f}, "
					f"window={first_ts}..{last_ts}."
				),
				raw_excerpt=(
					f"day={day} pair={pair_min}-{pair_max} records={len(indexes)} "
					f"unique_minutes={unique_minutes} max_per_minute={max_per_minute} "
					f"avg_per_minute={avg_per_minute:.2f}"
				),
			)
		)

	if not to_remove:
		return treaties, new_flags

	filtered_treaties = [record for idx, record in enumerate(treaties) if idx not in to_remove]
	return filtered_treaties, new_flags


def parse_messages(
	messages: list[dict[str, Any]],
	strict: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int], dict[str, Any]]:
	treaties: list[dict[str, Any]] = []
	flags: list[dict[str, Any]] = []
	skipped_reason_counts: dict[str, int] = {}
	malformed_reason_counts: dict[str, int] = {}
	skipped_examples: dict[str, dict[str, Any]] = {}
	malformed_examples: dict[str, dict[str, Any]] = {}
	unknown_entries: list[dict[str, Any]] = []
	counters = {
		"messages_total": len(messages),
		"records_total": 0,
		"cancelled_total": 0,
		"skipped_total": 0,
		"flagged_total": 0,
	}

	seen_message_payloads: set[str] = set()
	seen_treaties: set[str] = set()
	progress = ProgressReporter(
		label="[bot_to_treaties] message progress",
		total=len(messages),
		unit="messages",
		non_tty_every=500,
	)

	for idx, message in enumerate(messages, start=1):
		timestamp = str(message.get("timestamp") or "")
		content = str(message.get("content") or "")

		if len(content) > 12000:
			flags.append(
				build_flag(
					timestamp=timestamp,
					severity="warning",
					flag_type="oversized_payload",
					details=f"Message content length {len(content)} is unusually large.",
					raw_excerpt=content,
					source_message_json=message,
				)
			)

		for embed in message.get("embeds", []) or []:
			description = str(embed.get("description") or "")
			if len(description) > 6000:
				flags.append(
					build_flag(
						timestamp=timestamp,
						severity="warning",
						flag_type="oversized_payload",
						details=f"Embed description length {len(description)} is unusually large.",
						raw_excerpt=description,
						source_message_json=message,
					)
				)

		payload_sig = message_payload_signature(message)
		if payload_sig in seen_message_payloads:
			flags.append(
				build_flag(
					timestamp=timestamp,
					severity="warning",
					flag_type="duplicate_message_payload",
					details="Message has same normalized payload as an earlier message.",
					raw_excerpt=content,
					source_message_json=message,
				)
			)
		else:
			seen_message_payloads.add(payload_sig)

		(
			embed_records,
			embed_flags,
			parsed_embed,
			embed_malformed,
			embed_malformed_examples,
			embed_unknown_instances,
		) = parse_embed_entries(message)
		(
			content_records,
			content_flags,
			parsed_content,
			content_malformed,
			content_malformed_examples,
			content_unknown_instances,
		) = parse_content_segments(message)
		unknown_entries.extend(embed_unknown_instances)
		unknown_entries.extend(content_unknown_instances)

		for reason, value in embed_malformed.items():
			malformed_reason_counts[reason] = malformed_reason_counts.get(reason, 0) + value
		for reason, value in content_malformed.items():
			malformed_reason_counts[reason] = malformed_reason_counts.get(reason, 0) + value
		for reason, value in embed_malformed_examples.items():
			if reason not in malformed_examples:
				malformed_examples[reason] = value
		for reason, value in content_malformed_examples.items():
			if reason not in malformed_examples:
				malformed_examples[reason] = value

		local_records = embed_records + content_records
		local_flags = embed_flags + content_flags

		treaty_like = treaty_like_text(content)
		if not parsed_embed and not parsed_content:
			if treaty_like:
				reason = classify_unparseable_treaty_like_message(message)
				increment_counter(skipped_reason_counts, reason)
				register_category_example(
					skipped_examples,
					category=reason,
					message=message,
					context={"kind": "treaty_like_unparseable"},
				)
				if is_unknown_category(reason):
					unknown_entries.append(
						{
							"classification_group": "skipped",
							"category": reason,
							"message_json": message,
							"context": {"kind": "treaty_like_unparseable"},
						}
					)
				counters["skipped_total"] += 1
				local_flags.append(
					build_flag(
						timestamp=timestamp,
						severity="error",
						flag_type="malformed_treaty_like",
						details="Treaty-like message could not be parsed with supported formats.",
						raw_excerpt=content,
						source_message_json=message,
					)
				)
				if strict:
					raise RuntimeError(
						f"Encountered treaty-like unparseable message at timestamp {timestamp}"
					)
			else:
				non_treaty_reason = classify_non_treaty_skip(message)
				full_reason = f"non_treaty_{non_treaty_reason}"
				if full_reason not in SILENT_SKIP_CATEGORIES:
					increment_counter(skipped_reason_counts, full_reason)
					register_category_example(
						skipped_examples,
						category=full_reason,
						message=message,
						context={"kind": "non_treaty"},
					)
					if is_unknown_category(non_treaty_reason):
						unknown_entries.append(
							{
								"classification_group": "skipped",
								"category": full_reason,
								"message_json": message,
								"context": {"kind": "non_treaty"},
							}
						)
					local_flags.append(
						build_flag(
							timestamp=timestamp,
							severity="info",
							flag_type="skipped_non_treaty",
							details=f"Skipped non-treaty message ({full_reason}).",
							raw_excerpt=content,
							source_message_json=message,
						)
					)
					counters["skipped_total"] += 1

		for record in local_records:
			fp = treaty_fingerprint(record)
			if fp in seen_treaties:
				local_flags.append(
					build_flag(
						timestamp=record["timestamp"],
						severity="warning",
						flag_type="duplicate_treaty_event",
						details="Treaty event fingerprint matched an earlier parsed record.",
						raw_excerpt=json.dumps(record, ensure_ascii=True),
						source_message_json=message,
					)
				)
			else:
				seen_treaties.add(fp)
			treaties.append(record)

		flags.extend(local_flags)
		progress.step(records=len(treaties), flags=len(flags), skipped=counters["skipped_total"], done=idx == len(messages))

	counters["records_total"] = len(treaties)
	counters["cancelled_total"] = sum(1 for item in treaties if item.get("action") == "cancelled")
	counters["flagged_total"] = len(flags)
	for reason, value in skipped_reason_counts.items():
		counters[f"skipped_reason_{reason}"] = value
	for reason, value in malformed_reason_counts.items():
		counters[f"malformed_reason_{reason}"] = value

	for reason, count in malformed_reason_counts.items():
		if reason in malformed_examples:
			malformed_examples[reason]["count"] = count
		else:
			malformed_examples[reason] = {
				"count": count,
				"first_occurrence_timestamp": "",
				"example_excerpt": "",
				"example_message_json": None,
				"first_context": None,
			}

	treaties, flags = suppress_upgrade_downgrade_churn(treaties=treaties, flags=flags)

	# Recompute counters after churn suppression.
	counters["records_total"] = len(treaties)
	counters["cancelled_total"] = sum(1 for item in treaties if item.get("action") == "cancelled")
	counters["flagged_total"] = len(flags)

	summary = {
		"messages_processed": len(messages),
		"records_written": len(treaties),
		"cancelled_records": counters["cancelled_total"],
		"skipped_total": counters["skipped_total"],
		"flags_total": counters["flagged_total"],
		"skipped_categories": skipped_examples,
		"malformed_categories": malformed_examples,
		"unknowns_full_list": unknown_entries,
	}
	return treaties, flags, counters, summary


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Convert bot_messages.json to normalized treaty records")
	parser.add_argument(
		"--input",
		default=str(WEB_WORK_DATA_DIR / "bot_messages.json"),
		help="Path to source bot messages JSON",
	)
	parser.add_argument(
		"--output",
		default=str(WEB_WORK_DATA_DIR / "treaties.json"),
		help="Path to output treaty JSON",
	)
	parser.add_argument(
		"--flags-output",
		default=str(WEB_WORK_DATA_DIR / "treaties_skipped.json"),
		help="Path to primary parser flags JSON",
	)
	parser.add_argument(
		"--flags-aux-output",
		default=str(WEB_WORK_DATA_DIR / "treaties_skipped_auxiliary.json"),
		help=(
			"Path to auxiliary parser flags JSON "
			"(duplicate_treaty_event, missing_time_remaining_defaulted, skipped_non_treaty)"
		),
	)
	parser.add_argument(
		"--strict",
		action="store_true",
		help="Raise error for treaty-like messages that cannot be parsed",
	)
	parser.add_argument(
		"--summary-output",
		default=str(WEB_WORK_DATA_DIR / "treaties_parse_summary.json"),
		help="Path to parsing summary JSON with examples per category",
	)
	return parser


def main() -> int:
	args = build_parser().parse_args()
	input_path = Path(args.input)
	output_path = Path(args.output)
	flags_path = Path(args.flags_output)
	flags_aux_path = Path(args.flags_aux_output)
	summary_path = Path(args.summary_output)

	messages = load_json(input_path)
	if not isinstance(messages, list):
		raise RuntimeError("Input JSON must be a list of message objects")

	treaties, flags, counters, summary = parse_messages(messages, strict=bool(args.strict))
	primary_flags = [flag for flag in flags if str(flag.get("flag_type") or "") not in SPLIT_FLAG_TYPES]
	aux_flags = [flag for flag in flags if str(flag.get("flag_type") or "") in SPLIT_FLAG_TYPES]
	write_json(output_path, treaties)
	write_json(flags_path, primary_flags)
	write_json(flags_aux_path, aux_flags)
	summary["flags_primary_total"] = len(primary_flags)
	summary["flags_aux_total"] = len(aux_flags)
	summary["flags_aux_types"] = sorted(SPLIT_FLAG_TYPES)
	write_json(summary_path, summary)

	print("Formats supported:", ", ".join(SUPPORTED_FORMATS))
	print("Formats skipped:", ", ".join(SKIPPED_FORMATS))
	print("Formats flagged:", ", ".join(FLAGGED_FORMATS))
	print(f"Messages processed: {counters['messages_total']}")
	print(f"Treaty records written: {counters['records_total']}")
	print(f"Cancelled records: {counters['cancelled_total']}")
	print(f"Skipped events total: {counters['skipped_total']}")
	print("Skipped events by reason:")
	for key in sorted(counters):
		if key.startswith("skipped_reason_"):
			reason = key.replace("skipped_reason_", "")
			print(f"  - {reason}: {counters[key]}")
	print("Malformed treaty units by reason:")
	malformed_printed = False
	for key in sorted(counters):
		if key.startswith("malformed_reason_"):
			malformed_printed = True
			reason = key.replace("malformed_reason_", "")
			print(f"  - {reason}: {counters[key]}")
	if not malformed_printed:
		print("  - none")
	print(f"Flags written (primary): {len(primary_flags)}")
	print(f"Flags written (auxiliary): {len(aux_flags)}")
	print(f"Flags total (all): {counters['flagged_total']}")
	print(f"Output written to: {output_path}")
	print(f"Flags written to: {flags_path}")
	print(f"Auxiliary flags written to: {flags_aux_path}")
	print(f"Summary written to: {summary_path}")
	print("Unknown classifications (full list is in summary output):")
	unknowns = summary.get("unknowns_full_list", [])
	if unknowns:
		print(f"  - total unknown entries: {len(unknowns)}")
		for idx, item in enumerate(unknowns[:10], start=1):
			print(f"  - {idx}. {item.get('classification_group')}::{item.get('category')}")
		if len(unknowns) > 10:
			print(f"  - ... {len(unknowns) - 10} more (see summary file)")
	else:
		print("  - none")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())