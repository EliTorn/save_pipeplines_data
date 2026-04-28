"""IndexAdapter ABC.

One subclass per index lives at `settings/indexes/<X>/helpers.py`. Generic
pipeline code (core/runner, core/batch, apply_changes) talks only to this
interface — never imports index-specific lambdas directly.
"""
from __future__ import annotations

from typing import Any, Callable


class IndexAdapter:
    """Per-index hooks. Subclass and set INDEX_NAME; override the methods you need."""

    INDEX_NAME: str = ""

    # --- Transform side (phase 3) -----------------------------------------

    def lambdas(self) -> dict[str, Callable]:
        """{lambda_name -> callable} for transform_to_es_shape mapping CSV refs.

        Default: empty (index has no extra lambdas beyond common_lambdas)."""
        return {}

    def transform(self, df, mapping):
        """Shape an Oracle DataFrame into ES-keyed columns using this adapter's lambdas."""
        from core.compare import transform_to_es_shape
        return transform_to_es_shape(df, mapping, adapter=self)

    # --- Apply side (phase 4 — defaults are safe no-ops) ------------------

    def before_apply(self, doc: dict) -> dict:
        """Hook for per-doc mutation before sending to ES (e.g. stamp updateDate)."""
        return doc

    def coerce_for_es(self, field: str, value: Any) -> Any:
        """Type-coerce a CSV string into the ES-typed value. Default: pass through."""
        return value

    def validate(self, doc: dict) -> list[str]:
        """Return list of validation errors. Empty = ok."""
        return []
