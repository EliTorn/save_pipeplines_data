"""CARDUSERS index adapter + lambdas (Apple Pay + Regular templates)."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Optional

from core.adapter import IndexAdapter


def convert_last_four(value: Any) -> Optional[str]:
    """'XXXX1234' -> '1234'. Trim then take last 4 chars. None stays None."""
    if value is None:
        return None
    s = str(value).strip()
    return s[-4:] if s else None


def convert_wallet_type_to_payment_system(value: Any) -> Optional[str]:
    """Apple/Google Pay wallet type code -> enum name. 1 -> APPLE_PAY, 2 -> GOOGLE_PAY."""
    if value is None:
        return None
    try:
        v = int(value)
    except (TypeError, ValueError):
        return None
    return {1: "APPLE_PAY", 2: "GOOGLE_PAY"}.get(v)


def compose_apple_pay_id(row: dict) -> Optional[str]:
    """Multi-col: {WALLET_TYPE, DPAN_ID} -> 'APPLE_PAY12345' / 'GOOGLE_PAY54321'."""
    if row is None:
        return None
    wt = row.get("WALLET_TYPE")
    dp = row.get("DPAN_ID")
    if dp is None:
        return None
    name = convert_wallet_type_to_payment_system(wt)
    if not name:
        return None
    return f"{name}{int(dp)}"


def convert_regular_id(value: Any) -> Optional[str]:
    """REGULAR cardId -> 'REGULAR12345'."""
    if value is None:
        return None
    try:
        return f"REGULAR{int(value)}"
    except (TypeError, ValueError):
        return None


def convert_first_six(value: Any) -> Optional[str]:
    """First 6 chars (used for `bin` from HIDDEN_CARD_NUMBER). None stays None."""
    if value is None:
        return None
    s = str(value).strip()
    return s[:6] if len(s) >= 6 else (s or None)


def convert_blacklist_to_iswhitelisted(value: Any) -> bool:
    """REGULAR.BLACKLIST tri-state: -1 -> True, else False."""
    if value is None:
        return False
    try:
        return int(value) == -1
    except (TypeError, ValueError):
        return False


def convert_blacklist_to_isblacklisted(value: Any) -> bool:
    """REGULAR.BLACKLIST tri-state: 0/-1/NULL -> False, else True."""
    if value is None:
        return False
    try:
        return int(value) not in (0, -1)
    except (TypeError, ValueError):
        return False


def convert_verified_to_isverified(value: Any) -> bool:
    """REGULAR.CREDITCARDS_VERIFIED.VERIFIED: 0/-1/NULL -> False, else True."""
    if value is None:
        return False
    try:
        return int(value) not in (0, -1)
    except (TypeError, ValueError):
        return False


def compose_regular_expiry_date(row: dict) -> Optional[str]:
    """Multi-col: {EXPYEAR, EXPMONTH} -> 'YYYY-MM-DD' (last day of month).
    EXPMONTH=0 treated as January (per live behavior)."""
    if row is None:
        return None
    yy_raw = row.get("EXPYEAR")
    mm_raw = row.get("EXPMONTH")
    if yy_raw is None or mm_raw is None:
        return None
    try:
        yy = int(yy_raw)
        mm = int(mm_raw)
    except (TypeError, ValueError):
        return None
    if mm == 0:
        mm = 1
    if not (1 <= mm <= 12):
        return None
    yyyy = 2000 + yy if yy < 70 else 1900 + yy
    try:
        nxt = date(yyyy + 1, 1, 1) if mm == 12 else date(yyyy, mm + 1, 1)
        return (nxt - timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return None


def constant_regular_payment_system(value: Any) -> str:
    """Always returns the literal 'REGULAR' (REGULAR side has no enum column)."""
    return "REGULAR"


_CARDUSERS_LAMBDAS = {
    "convert_last_four": convert_last_four,
    "convert_wallet_type_to_payment_system": convert_wallet_type_to_payment_system,
    "compose_apple_pay_id": compose_apple_pay_id,
    "convert_regular_id": convert_regular_id,
    "convert_first_six": convert_first_six,
    "convert_blacklist_to_iswhitelisted": convert_blacklist_to_iswhitelisted,
    "convert_blacklist_to_isblacklisted": convert_blacklist_to_isblacklisted,
    "convert_verified_to_isverified": convert_verified_to_isverified,
    "compose_regular_expiry_date": compose_regular_expiry_date,
    "constant_regular_payment_system": constant_regular_payment_system,
}


class Adapter(IndexAdapter):
    INDEX_NAME = "cardusers"

    def lambdas(self):
        return dict(_CARDUSERS_LAMBDAS)
