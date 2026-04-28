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

_PATH_KEYS_PART = ("sql_file", "VALUE_COLM", "LOOKUP_SQL")


def _rewrite_paths(part: dict, base: Path) -> dict:
    """Rewrite per-index relative paths to settings-relative (forward slashes)."""
    for k in _PATH_KEYS_PART:
        rel = part.get(k)
        if not rel:
            continue
        abs_path = (base / rel).resolve()
        try:
            settings_rel = abs_path.relative_to(_SETTINGS_DIR.resolve())
            part[k] = settings_rel.as_posix()
        except ValueError:
            part[k] = str(abs_path)
    return part


def _resolve_index_config(event: str, entry: dict) -> dict:
    """Merge `indexes/<X>/config.yaml` referenced by `index_config` into entry.
    Event-level keys (START_TIME/END_TIME/IS_RUNNING/FILED_THAT_RUN/...) take precedence."""
    ref = entry.pop("index_config", None)
    if not ref:
        return entry
    cfg_path = _SETTINGS_DIR / ref
    if not cfg_path.is_file():
        raise FileNotFoundError(f"{event}: index_config not found at {cfg_path}")
    base = cfg_path.parent
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    parts = cfg.pop("parts", None)
    if parts:
        cfg["parts"] = [_rewrite_paths(dict(p), base) for p in parts]
    else:
        _rewrite_paths(cfg, base)
    merged = {**cfg, **entry}
    return merged


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

    out: dict = {}
    for event, entry in cfg.items():
        entry = _resolve_index_config(event, dict(entry))
        if entry.get("parts"):
            entry["parts"] = [_load_part(event, p) for p in entry["parts"]]
        else:
            _load_part(event, entry)

        entry["IS_RUNNING"] = bool(entry.get("IS_RUNNING", False))
        raw = entry.get("FILED_THAT_RUN") or ""
        entry["FILED_THAT_RUN"] = [p.strip() for p in str(raw).split(",") if p.strip()]
        out[event] = entry

    return out


PIPELINE_SETTINGS = load_pipeline_settings()
