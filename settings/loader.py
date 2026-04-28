from __future__ import annotations

import csv
import sys
from pathlib import Path

import yaml

_SETTINGS_DIR = Path(__file__).parent
_PARENT = _SETTINGS_DIR.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from _pipeline_env import DIFF_MODE_ALIASES  # noqa: E402

_TOP_LEVEL_KEYS = {"PIPELINE_DIFF_MODE"}


def _load_part(event: str, part: dict) -> dict:
    sql_path = _SETTINGS_DIR / part["sql_file"]
    if not sql_path.is_file():
        raise FileNotFoundError(f"{event}: sql_file not found at {sql_path}")
    part["scama"] = sql_path.read_text(encoding="utf-8")
    map_path = _SETTINGS_DIR / part["VALUE_COLM"]
    if not map_path.is_file():
        raise FileNotFoundError(f"{event}: VALUE_COLM not found at {map_path}")
    with open(map_path, encoding="utf-8", newline="") as f:
        part["mapping"] = list(csv.DictReader(f))
    return part


def load_pipeline_settings() -> dict:
    with open(_SETTINGS_DIR / "events.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    raw = str(cfg.get("PIPELINE_DIFF_MODE", "both")).strip().lower()
    return {"PIPELINE_DIFF_MODE": DIFF_MODE_ALIASES.get(raw, "both")}


def load_events() -> dict:
    with open(_SETTINGS_DIR / "events.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    for k in _TOP_LEVEL_KEYS:
        cfg.pop(k, None)

    for event, entry in cfg.items():
        if entry.get("parts"):
            entry["parts"] = [_load_part(event, p) for p in entry["parts"]]
        else:
            _load_part(event, entry)

        entry["IS_RUNNING"] = bool(entry.get("IS_RUNNING", False))
        raw = entry.get("FILED_THAT_RUN") or ""
        entry["FILED_THAT_RUN"] = [p.strip() for p in str(raw).split(",") if p.strip()]

    return cfg


PIPELINE_SETTINGS = load_pipeline_settings()
