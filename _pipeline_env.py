"""Cross-cutting tiny helpers (env-var parsing, ES env normalization,
timestamp parsing, diff-mode aliases). Used by main.py, app.py, settings, and
all three connectors so the same constants/logic live in exactly one place."""
from __future__ import annotations

import os
from datetime import datetime

TS_FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d")

DIFF_MODE_ALIASES = {
    "change": "changes", "changes": "changes",
    "missing": "missing", "mission": "missing",
    "both": "both",
}

_TRUTHY = ("1", "true", "yes", "on", "t", "y")


def env_truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "")
    if raw == "":
        return default
    return raw.strip().lower() in _TRUTHY


def normalize_es_env(value) -> str:
    """'prod' / 'prode' / 'production' (any case) -> 'prod'; everything else -> 'stage'."""
    s = str(value or "stage").strip().lower()
    return "prod" if s in ("prod", "prode", "production") else "stage"


def parse_ts(v) -> datetime:
    """Parse one of the supported pipeline timestamp string formats. Raises ValueError on miss."""
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"bad timestamp: {v!r}")
