"""Generic CSV-string -> ES-typed value coercion.

`_LAMBDA_TYPE` maps mapping-CSV lambda names to the ES coercion kind a doc field
needs. Per-event/per-index overrides live on each `IndexAdapter`.
"""
from __future__ import annotations

import ast
import re
from datetime import datetime
from typing import Any

# Lambda name -> coerce kind. Generic across all indexes.
_LAMBDA_TYPE = {
    "convert_int_to_string": "str_int",
    "convert_to_int_strict":  "int",
    "convert_int_or_zero":    "int",
    "convert_int_or_neg_one": "int",
    "constant_zero_int":      "int",
    "constant_zero_long":     "int",
    "convert_num_to_bool":    "bool",
    "convert_truthy_bool":    "bool",
    "constant_false_bool":    "bool",
    "constant_zero_float":    "float",
    "convert_str_to_list":    "list",
    "compose_jackpot_menu_items_ids": "list",
    "parse_menu_items_ids":   "list",
    "compose_chip_count_left_freespins": "int",
    "convert_time_to_string": "str",
    "convert_time_to_date_as_string": "str",
    "convert_yymmdd_to_iso":  "str",
    "compose_expiration_from_days":   "str",
}


def field_types(mapping_rows: list[dict],
                overrides: dict[str, str] | None = None) -> dict[str, str]:
    """{filed_es -> coerce_kind} where kind is one of int/float/bool/list/str/str_int.

    When the same ES field is declared in multiple templates with different
    lambdas, prefer the strongest kind: never let a "str" fallback overwrite
    an already-seen typed kind."""
    out: dict[str, str] = {}
    for r in mapping_rows:
        fe = (r.get("filed_es") or "").strip()
        if not fe:
            continue
        fn = (r.get("funciton_lambda") or "").strip()
        kind = _LAMBDA_TYPE.get(fn, "str")
        if kind != "str" or fe not in out:
            out[fe] = kind
    if overrides:
        for fe, kind in overrides.items():
            if fe in out:
                out[fe] = kind
    return out


# ES date mapping uses "yyyy-MM-dd'T'HH:mm:ss.SSS"; pipeline lambdas emit
# "YYYY-MM-DD HH:MM:SS.fff" (space). Swap space → 'T' just before send.
_ISO_DT_SPACE_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$")


def coerce(value: Any, kind: str):
    """String CSV value -> ES-typed value. Empty/None handled."""
    if value is None:
        return None
    s = value.strip() if isinstance(value, str) else value
    if s == "" or (isinstance(s, str) and s.lower() in ("none", "nan", "<na>", "null")):
        return None
    if kind in ("int", "str_int"):
        try:
            return int(float(s))
        except (TypeError, ValueError):
            return None
    if kind == "int_str":
        try:
            return str(int(float(s)))
        except (TypeError, ValueError):
            return None
    if kind == "float":
        try:
            return float(s)
        except (TypeError, ValueError):
            return None
    if kind == "bool":
        if isinstance(s, str):
            return s.strip().lower() in ("1", "true", "yes", "t", "y")
        return bool(s)
    if kind == "list":
        if isinstance(s, list):
            return s
        if isinstance(s, str) and s.startswith("[") and s.endswith("]"):
            try:
                v = ast.literal_eval(s)
                return list(v) if isinstance(v, (list, tuple)) else [v]
            except (ValueError, SyntaxError):
                return [p.strip() for p in s.strip("[]").split(",") if p.strip()]
        if isinstance(s, str) and "," in s:
            return [p.strip() for p in s.split(",") if p.strip()]
        return [s]
    out = str(s)
    if _ISO_DT_SPACE_RE.match(out):
        return out.replace(" ", "T", 1)
    return out


def now_es_iso() -> str:
    """ES `updateDate` format: yyyy-MM-dd'T'HH:mm:ss.SSS (matches PlayerBonusVO @Field DateFormat)."""
    n = datetime.now()
    return n.strftime("%Y-%m-%dT%H:%M:%S.") + f"{n.microsecond // 1000:03d}"
