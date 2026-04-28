"""Generic value converters shared across all indexes.

Index-specific lambdas live in `settings/indexes/<X>/helpers.py`.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional


def _is_nan_or_none(value: Any) -> bool:
    if value is None:
        return True
    try:
        import pandas as _pd
        if _pd.isna(value):
            return True
    except (TypeError, ValueError, ImportError):
        pass
    return False


def _to_int_or(value: Any, default):
    if _is_nan_or_none(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def convert_time_to_string(value: Optional[datetime | date]) -> Optional[str]:
    """Oracle DATE/TIMESTAMP -> 'YYYY-MM-DD HH:MM:SS.fff'. None/NaT stays None."""
    if _is_nan_or_none(value):
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.") + f"{value.microsecond // 1000:03d}"
    return value.strftime("%Y-%m-%d")


def convert_num_to_bool(value: Any) -> Optional[bool]:
    """Oracle NUMBER(1) -> bool. 0 -> False, 1 -> True, None -> None."""
    if value is None:
        return None
    return bool(int(value))


def convert_int_to_string(value: Any) -> Optional[str]:
    """Numeric id -> string (avoid JS/ES precision loss on big ints). None/NaN -> None."""
    if _is_nan_or_none(value):
        return None
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


def convert_str_to_list(value: Any) -> Optional[list]:
    """CSV string like '343,234,54453' -> ['343','234','54453']. None -> None, '' -> []."""
    if value is None:
        return None
    s = str(value)
    if s == "":
        return []
    return s.split(",")


def convert_yymmdd_to_iso(value: Any) -> Optional[str]:
    """'241231' -> '2024-12-31'. None / bad / out-of-range -> None."""
    if value is None:
        return None
    s = str(value).strip()
    if len(s) != 6 or not s.isdigit():
        return None
    yy, mm, dd = int(s[0:2]), int(s[2:4]), int(s[4:6])
    if not (1 <= mm <= 12) or not (1 <= dd <= 31):
        return None
    yyyy = 2000 + yy if yy < 70 else 1900 + yy
    try:
        return date(yyyy, mm, dd).strftime("%Y-%m-%d")
    except ValueError:
        return None


def convert_int_or_zero(value: Any) -> int:
    """None/bad -> 0, else int. (parentId, skinGroupId default per Java int default.)"""
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def convert_int_or_neg_one(value: Any) -> int:
    """None/bad -> -1, else int. (externalParentId default per EventManagerService.java:842.)"""
    if value is None:
        return -1
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def convert_truthy_bool(value: Any) -> bool:
    """Any truthy -> True, None/0/empty -> False. (internalAccount.)"""
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "y", "yes", "t")
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return bool(value)


def constant_zero_int(value: Any) -> int:
    """Always returns 0 (int)."""
    return 0


def constant_zero_float(value: Any) -> float:
    """Always returns 0.0 (float)."""
    return 0.0


def constant_zero_long(value: Any) -> int:
    return 0


def constant_false_bool(value: Any) -> bool:
    return False


def convert_to_int_strict(value: Any) -> Optional[int]:
    """Force int. None/NaT/NaN -> None. Float 10000.0 -> int 10000."""
    if _is_nan_or_none(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


COMMON_LAMBDAS = {
    "convert_time_to_string": convert_time_to_string,
    "convert_num_to_bool": convert_num_to_bool,
    "convert_int_to_string": convert_int_to_string,
    "convert_str_to_list": convert_str_to_list,
    "convert_yymmdd_to_iso": convert_yymmdd_to_iso,
    "convert_int_or_zero": convert_int_or_zero,
    "convert_int_or_neg_one": convert_int_or_neg_one,
    "convert_truthy_bool": convert_truthy_bool,
    "constant_zero_int": constant_zero_int,
    "constant_zero_float": constant_zero_float,
    "constant_zero_long": constant_zero_long,
    "constant_false_bool": constant_false_bool,
    "convert_to_int_strict": convert_to_int_strict,
}
