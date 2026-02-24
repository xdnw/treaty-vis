from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import TextIO


@dataclass
class ProgressReporter:
    """TTY-aware progress output with sparse logging in non-interactive runs."""

    label: str
    total: int
    unit: str = "items"
    stream: TextIO = field(default_factory=lambda: sys.stderr)
    non_tty_every: int = 100
    line_width: int = 120

    _processed: int = 0
    _last_printed: int = 0
    _completed: bool = False

    def step(self, increment: int = 1, *, done: bool = False, **metrics: int) -> None:
        if self.total <= 0:
            return

        self._processed = min(max(0, self._processed + increment), self.total)
        is_done = bool(done or self._processed >= self.total)
        line = self._render_line(metrics)

        if self.stream.isatty():
            print(
                line.ljust(self.line_width),
                end="\n" if is_done else "\r",
                file=self.stream,
                flush=True,
            )
            self._last_printed = self._processed
            self._completed = is_done
            return

        should_log = is_done or self._processed in (1, self.total)
        if not should_log and self.non_tty_every > 0:
            should_log = (self._processed % self.non_tty_every == 0) and self._processed != self._last_printed

        if should_log:
            print(line, file=self.stream)
            self._last_printed = self._processed
            self._completed = is_done

    def close(self) -> None:
        """Ensure the current TTY line is finalized before other output prints."""
        if self.total <= 0:
            return
        if self.stream.isatty() and self._last_printed > 0 and not self._completed:
            print(file=self.stream, flush=True)

    def _render_line(self, metrics: dict[str, int]) -> str:
        percent = (self._processed / self.total) * 100.0
        base = f"{self.label}: {self._processed}/{self.total} ({percent:5.1f}%)"
        if not metrics:
            return base
        parts = [f"{key}={value}" for key, value in metrics.items()]
        return f"{base} {' '.join(parts)}"
