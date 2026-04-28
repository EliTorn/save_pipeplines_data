"""Compatibility shim. Compare logic now lives in core.compare.

Kept so external imports `from settings.compare import ...` keep working.
New code should import from `core.compare` directly.
"""
from core.compare import (  # noqa: F401
    LAMBDAS,
    apply_composite_mappings,
    compare_records,
    compare_shaped,
    transform_to_es_shape,
)
