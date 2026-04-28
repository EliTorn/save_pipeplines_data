"""IndexAdapter ABC.

One subclass per index lives at `settings/indexes/<X>/helpers.py`. Generic
pipeline code (core/runner, core/batch, apply_changes) talks only to this
interface — never imports index-specific lambdas or special-cases an index.
"""
from __future__ import annotations

from typing import Any, Callable


class IndexAdapter:
    """Per-index hooks. Subclass and set INDEX_NAME; override what you need."""

    INDEX_NAME: str = ""

    def __init__(self):
        self._field_types: dict[str, str] = {}

    # --- Transform side --------------------------------------------------

    def lambdas(self) -> dict[str, Callable]:
        """{lambda_name -> callable} for transform_to_es_shape mapping CSV refs."""
        return {}

    def transform(self, df, mapping):
        """Shape an Oracle DataFrame into ES-keyed columns using this adapter's lambdas."""
        from core.compare import transform_to_es_shape
        return transform_to_es_shape(df, mapping, adapter=self)

    # --- Apply side ------------------------------------------------------

    def field_kind_overrides(self) -> dict[str, str]:
        """Per-field kind overrides applied on top of the lambda-derived kinds.
        Default: empty. Used by `core.coerce.field_types(rows, overrides=...)`."""
        return {}

    def bind_field_types(self, types: dict[str, str]) -> None:
        """Cache the resolved {field -> kind} table on the adapter so
        `coerce_for_es` can look up by name."""
        self._field_types = dict(types)

    def coerce_for_es(self, field: str, value: Any) -> Any:
        """Type-coerce a CSV string into the ES-typed value for one field."""
        from core.coerce import coerce
        return coerce(value, self._field_types.get(field, "str"))

    def before_apply(self, doc: dict) -> dict:
        """Hook for per-doc mutation before sending to ES (e.g. stamp updateDate).
        Default: pass through. Adapters that mutate must return the new dict."""
        return doc

    def validate(self, doc: dict) -> list[str]:
        """Return list of validation errors. Empty = ok. (Reserved; ES schema
        validation currently lives in apply_changes.es_schema.)"""
        return []
