"""
Output writers for expanded product rows.

Supports CSV, JSON array, and JSONL (one JSON object per line).
"""

from __future__ import annotations

import csv
import json
import sys
from collections.abc import Iterable, Sequence
from typing import IO, Any


def write_csv(
    rows: Iterable[dict[str, Any]],
    output: IO[str] | None = None,
    fieldnames: Sequence[str] | None = None,
) -> int:
    """
    Write rows as CSV. Returns the number of rows written.

    If *fieldnames* is None, they are derived from the union of all keys
    across the first pass. This requires buffering all rows; for very large
    exports, pass fieldnames explicitly.
    """
    out = output or sys.stdout
    buffered: list[dict[str, Any]] | None = None

    if fieldnames is None:
        buffered = list(rows)
        seen: dict[str, None] = {}
        for row in buffered:
            for k in row:
                seen.setdefault(k, None)
        fieldnames = list(seen)

    writer = csv.DictWriter(
        out,
        fieldnames=fieldnames,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()

    count = 0
    for row in buffered if buffered is not None else rows:
        writer.writerow(row)
        count += 1
    return count


def write_json(
    rows: Iterable[dict[str, Any]],
    output: IO[str] | None = None,
) -> int:
    """Write rows as a JSON array."""
    out = output or sys.stdout
    items = list(rows)
    json.dump(items, out, ensure_ascii=False, indent=2, default=str)
    out.write("\n")
    return len(items)


def write_jsonl(
    rows: Iterable[dict[str, Any]],
    output: IO[str] | None = None,
) -> int:
    """Write rows as JSONL (one JSON object per line). Fully streaming."""
    out = output or sys.stdout
    count = 0
    for row in rows:
        json.dump(row, out, ensure_ascii=False, default=str)
        out.write("\n")
        count += 1
    return count
