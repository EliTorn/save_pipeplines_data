"""Index name -> IndexAdapter instance.

Each index ships a `settings/indexes/<X>/helpers.py` with a top-level
`Adapter` class subclassing `core.adapter.IndexAdapter`. Resolved on demand;
adapters are cached.
"""
from __future__ import annotations

import importlib
from pathlib import Path

from core.adapter import IndexAdapter

_INDEXES_DIR = Path(__file__).resolve().parent.parent / "settings" / "indexes"
_CACHE: dict[str, IndexAdapter] = {}


def _import_adapter(index: str) -> IndexAdapter:
    helpers_path = _INDEXES_DIR / index / "helpers.py"
    if not helpers_path.is_file():
        raise FileNotFoundError(f"adapter helpers.py not found at {helpers_path}")
    module = importlib.import_module(f"settings.indexes.{index}.helpers")
    cls = getattr(module, "Adapter", None)
    if cls is None or not isinstance(cls, type) or not issubclass(cls, IndexAdapter):
        raise TypeError(f"settings.indexes.{index}.helpers must define class Adapter(IndexAdapter)")
    return cls()


def get_adapter(index: str) -> IndexAdapter:
    """Return cached adapter instance for `index`. Loads on first use."""
    if index not in _CACHE:
        _CACHE[index] = _import_adapter(index)
    return _CACHE[index]


def known_indexes() -> list[str]:
    """List index names that have a helpers.py adapter on disk."""
    if not _INDEXES_DIR.is_dir():
        return []
    return sorted(p.name for p in _INDEXES_DIR.iterdir()
                  if p.is_dir() and (p / "helpers.py").is_file())
