from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
	sys.path.insert(0, str(SCRIPT_DIR))

from generate_timelapse_data import grounded_count, reconcile_events


class GenerateTimelapseDataTests(unittest.TestCase):
	def test_reconcile_preserves_same_timestamp_processing_order(self) -> None:
		bot_events = [
			{
				"timestamp": "2024-01-01T00:00:00Z",
				"action": "signed",
				"treaty_type": "MDP",
				"from_alliance_id": 1,
				"from_alliance_name": "A",
				"to_alliance_id": 2,
				"to_alliance_name": "B",
				"pair_min_id": 1,
				"pair_max_id": 2,
				"source": "bot",
				"source_ref": "bot:0",
				"confidence": "high",
				"inferred": False,
				"inference_reason": None,
				"time_remaining_turns": -1,
			},
			{
				"timestamp": "2024-01-01T00:00:00Z",
				"action": "cancelled",
				"treaty_type": "MDP",
				"from_alliance_id": 1,
				"from_alliance_name": "A",
				"to_alliance_id": 2,
				"to_alliance_name": "B",
				"pair_min_id": 1,
				"pair_max_id": 2,
				"source": "bot",
				"source_ref": "bot:1",
				"confidence": "high",
				"inferred": False,
				"inference_reason": None,
				"time_remaining_turns": None,
			},
		]

		reconciled, flags = reconcile_events(
			bot_events=bot_events,
			archive_delta_events=[],
			alliance_zero_markers=[],
			grounding_lookup=[],
			top50_mode="off",
			infer_expiry_cancels=False,
			infer_deletion_cancels=False,
		)

		self.assertEqual(flags, [])
		self.assertEqual([event["source_ref"] for event in reconciled], ["bot:0", "bot:1"])
		self.assertEqual([event["event_sequence"] for event in reconciled], [0, 1])

	def test_grounded_count_returns_false_before_first_snapshot(self) -> None:
		lookup = [
			(
				datetime(2024, 1, 2, tzinfo=timezone.utc),
				{10, 20},
			)
		]

		before_first = datetime(2024, 1, 1, tzinfo=timezone.utc)
		at_first = datetime(2024, 1, 2, tzinfo=timezone.utc)

		self.assertFalse(grounded_count(lookup, before_first, 10))
		self.assertTrue(grounded_count(lookup, at_first, 10))


if __name__ == "__main__":
	unittest.main()
