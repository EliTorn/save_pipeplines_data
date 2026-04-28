"""Pipeline-wide runtime config derived from env + events.yaml."""
from __future__ import annotations

import os
from pathlib import Path

from _pipeline_env import DIFF_MODE_ALIASES, env_truthy
from settings.loader import PIPELINE_SETTINGS

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "out"
OUT_DIR.mkdir(exist_ok=True)

VERBOSE = env_truthy("PIPELINE_VERBOSE")
SAVE_FULL_CSV = env_truthy("PIPELINE_SAVE_FULL_CSV")

_DEFAULT_PIPELINE_WORKERS = min(4, os.cpu_count() or 1)
PIPELINE_WORKERS = max(1, int(os.getenv("PIPELINE_WORKERS", str(_DEFAULT_PIPELINE_WORKERS))))

_diff_env = os.getenv("PIPELINE_DIFF_MODE", "").strip().lower()
DIFF_MODE = DIFF_MODE_ALIASES.get(_diff_env, PIPELINE_SETTINGS.get("PIPELINE_DIFF_MODE", "both")) \
    if _diff_env else PIPELINE_SETTINGS.get("PIPELINE_DIFF_MODE", "both")
SAVE_CHANGES = DIFF_MODE in ("changes", "both")
SAVE_MISSING = DIFF_MODE in ("missing", "both")
