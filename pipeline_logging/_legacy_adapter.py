"""Convert legacy `_pipeline_logging` row dicts → v2 parquet rows.

Used by both:
- the parent-side ``RunLoggerBase.event()`` / ``QueryRecord.__exit__`` mirror,
- the parent listener thread (when forwarding worker queue messages).

Adapters are stateless and never raise — caller wraps in try/except.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Optional

# Legacy event keys that already have first-class v2 columns or are
# subsumed by ts/thread/level. Anything outside this set lands inside the
# JSON `fields` blob.
_EVENT_KNOWN = frozenset({
    "run_id", "ts", "ts_local", "tz", "level", "thread", "event",
    "owner", "table", "batch", "query_id", "sql", "sql_hash",
    "date", "hour", "minute", "weekday", "day_name",
    "week", "month", "year",
    "_pid",
})


def _parse_iso(v) -> Optional[datetime]:
    if v is None or isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None


def event_row_to_v2(legacy: dict, run_id: str) -> dict:
    """Adapt a legacy event row dict → v2 events row."""
    extras = {k: v for k, v in legacy.items() if k not in _EVENT_KNOWN}
    return {
        "run_id":   legacy.get("run_id") or run_id,
        "ts":       _parse_iso(legacy.get("ts")) or datetime.now(timezone.utc),
        "level":    legacy.get("level") or "INFO",
        "event":    legacy.get("event") or "",
        "target":   legacy.get("table"),
        "batch_id": str(legacy["batch"]) if legacy.get("batch") is not None else None,
        "query_id": legacy.get("query_id"),
        "message":  None,
        "fields":   json.dumps(extras, default=str) if extras else None,
        "thread":   legacy.get("thread") or threading.current_thread().name,
        "pid":      int(legacy.get("_pid") or os.getpid()),
    }


def query_row_to_v2(legacy: dict, system: str, run_id: str) -> dict:
    """Adapt a legacy QueryRecord row dict → v2 queries row."""
    started = _parse_iso(legacy.get("start_ts"))
    ended = _parse_iso(legacy.get("end_ts"))
    seconds = legacy.get("seconds")
    duration_ms = None
    if seconds is not None:
        try:
            duration_ms = int(round(float(seconds) * 1000))
        except (TypeError, ValueError):
            duration_ms = None

    rows = legacy.get("rows")
    if rows in (None, ""):
        rows = None
    else:
        try:
            rows = int(rows)
        except (TypeError, ValueError):
            rows = None

    rps = legacy.get("rows_per_sec")
    if rps in (None, ""):
        rps = None
    else:
        try:
            rps = float(rps)
        except (TypeError, ValueError):
            rps = None

    sql_text = legacy.get("sql")
    if sql_text:
        sql_text = str(sql_text)[:8000]

    return {
        "query_id":     legacy.get("query_id"),
        "run_id":       legacy.get("run_id") or run_id,
        "system":       system,
        "target":       legacy.get("table"),
        "operation":    legacy.get("operation"),
        "batch_id":     str(legacy["batch"]) if legacy.get("batch") is not None else None,
        "sql_hash":     legacy.get("sql_hash"),
        "sql_text":     sql_text,
        "params":       legacy.get("params"),
        "started_at":   started,
        "ended_at":     ended,
        "duration_ms":  duration_ms,
        "rows":         rows,
        "rows_per_sec": rps,
        "status":       legacy.get("status") or "ok",
        "error":        legacy.get("error"),
        "owner":        legacy.get("owner"),
        "thread":       legacy.get("thread"),
        "pid":          int(legacy.get("_pid") or os.getpid()),
    }
