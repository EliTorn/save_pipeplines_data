from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import yaml

_SETTINGS_DIR = Path(__file__).resolve().parent
_ENUMS_PATH = _SETTINGS_DIR / "playerbonus_enums.yaml"


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
    """Multi-col lambda: {WALLET_TYPE, DPAN_ID} -> 'APPLE_PAY12345' / 'GOOGLE_PAY54321'."""
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
    """Multi-col lambda: {EXPYEAR, EXPMONTH} -> 'YYYY-MM-DD' (last day of month).
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
    """Always returns the literal 'REGULAR' (the REGULAR side has no enum column)."""
    return "REGULAR"


# ---------------------------------------------------------------------------
# PlayerBonus lambdas
# ---------------------------------------------------------------------------


def compose_playerbonus_redeem_id(value: Any) -> Optional[str]:
    """USERBONUSID -> '<id>_0'  (RedeemEligble = 0)."""
    if value is None:
        return None
    try:
        return f"{int(value)}_0"
    except (TypeError, ValueError):
        return None


def compose_playerbonus_wheelspin_id(value: Any) -> Optional[str]:
    """BONUSWHEELSPINID -> '<id>_2'  (WheelSpin = 2)."""
    if value is None:
        return None
    try:
        return f"{int(value)}_2"
    except (TypeError, ValueError):
        return None


def compose_playerbonus_jackpot_id(value: Any) -> Optional[str]:
    """USERBONUSID -> '<id>_3'  (JackpotWheel = 3)."""
    if value is None:
        return None
    try:
        return f"{int(value)}_3"
    except (TypeError, ValueError):
        return None


def compose_playerbonus_freespins_id(row: dict) -> Optional[str]:
    """Multi-col: {FREEBONUSID, SPECIALBONUSTYPEID} -> '<id>_1' (FreeSpins) or '<id>_4' (FreeChips)."""
    if row is None:
        return None
    fid = row.get("FREEBONUSID")
    if fid is None:
        return None
    try:
        sid = int(row.get("SPECIALBONUSTYPEID") or 1)
    except (TypeError, ValueError):
        sid = 1
    type_code = 4 if sid == 4 else 1
    try:
        return f"{int(fid)}_{type_code}"
    except (TypeError, ValueError):
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
    """Always returns 0 (int). For ES fields Java fills as type-default 0 on docs of other bonus types."""
    return 0


def constant_zero_float(value: Any) -> float:
    """Always returns 0.0 (float). For ES fields Java fills as type-default 0.0 on docs of other bonus types."""
    return 0.0


def convert_to_int_strict(value: Any) -> Optional[int]:
    """Force int. None/NaT/NaN -> None. Float 10000.0 -> int 10000. Mismatch-safe for ES long/integer fields."""
    if _is_nan_or_none(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def compose_expiration_from_days(row: dict) -> Optional[str]:
    """{CREATEDDATE, EXPIERY_DAYS} -> CREATEDDATE + EXPIERY_DAYS as 'YYYY-MM-DD HH:MM:SS.fff'.

    Mirrors Java exactly:
      - RedeemEligible: UserBonus.UserBonusBuilder.expieryDays (UserBonus.java:105-113)
        feeds ISU.EXPIERY_DAYS into expirationDate2; producer sends that value.
      - FreeSpins/WheelSpin/JackpotWheel: PlayerBonusService.calculateExpirationDate
        (PlayerBonusService.java:48-57) does Calendar.add(DAY_OF_MONTH, BONUSES.EXPIERY_DAYS).

    Java rs.getInt() returns 0 for SQL NULL, so a null EXPIERY_DAYS is treated as 0 days
    (UserBonusBuilder still computes since `expieryDays >= 0` holds)."""
    if row is None:
        return None
    cd = row.get("CREATEDDATE")
    days = row.get("EXPIERY_DAYS")
    if _is_nan_or_none(cd) or not isinstance(cd, (datetime, date)):
        return None
    days_int = 0 if _is_nan_or_none(days) else None
    if days_int is None:
        try:
            days_int = int(days)
        except (TypeError, ValueError):
            return None
    if days_int < 0:
        return None
    try:
        ed = cd + timedelta(days=days_int)
    except (TypeError, ValueError):
        return None
    if isinstance(ed, datetime):
        return ed.strftime("%Y-%m-%d %H:%M:%S.") + f"{ed.microsecond // 1000:03d}"
    return ed.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# PlayerBonus enrichment — enum tables loaded from settings/playerbonus_enums.yaml
# ---------------------------------------------------------------------------


def _load_enums() -> dict[str, dict]:
    """Load `{name: {"values": {int: str}, "default": Any}}` once at import."""
    if not _ENUMS_PATH.is_file():
        raise FileNotFoundError(f"playerbonus_enums.yaml not found at {_ENUMS_PATH}")
    raw = yaml.safe_load(_ENUMS_PATH.read_text(encoding="utf-8")) or {}
    out: dict[str, dict] = {}
    for name, spec in raw.items():
        if not isinstance(spec, dict):
            continue
        values = spec.get("values") or {}
        out[name] = {
            "values": {int(k): v for k, v in values.items()},
            "default": spec.get("default"),
        }
    return out


_ENUMS = _load_enums()

ALL_APPLICATIONS_VALUE = ",-1,"


def _to_int_or(value, default):
    if _is_nan_or_none(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _make_enum_lookup(enum_name: str, *, none_for_missing: bool = False):
    """Build a lambda(value) -> str using the named enum table from YAML.
    If `none_for_missing` and the input is None/NaN, return None instead of default."""
    spec = _ENUMS.get(enum_name)
    if spec is None:
        raise KeyError(f"enum '{enum_name}' not in playerbonus_enums.yaml")
    table = spec["values"]
    default = spec["default"]

    if none_for_missing:
        def _lookup(value: Any):
            if _is_nan_or_none(value):
                return None
            return table.get(_to_int_or(value, -10 ** 12), default)
        return _lookup

    def _lookup(value: Any):
        return table.get(_to_int_or(value, -10 ** 12), default)
    return _lookup


def constant_bonus_type_redeem(value: Any) -> str: return "RedeemEligble"
def constant_bonus_type_wheelspin(value: Any) -> str: return "WheelSpin"
def constant_bonus_type_jackpot(value: Any) -> str: return "JackpotWheel"


def compose_bonus_type_freespins(value: Any) -> str:
    """SPECIALBONUSTYPEID -> 'FreeChips' if id==4 else 'FreeSpins' (also accepts dict for parity)."""
    if isinstance(value, dict):
        value = value.get("SPECIALBONUSTYPEID")
    return "FreeChips" if _to_int_or(value, 1) == 4 else "FreeSpins"


lookup_bonus_status_redeem = _make_enum_lookup("bonus_status")
lookup_bonus_status_freespins = _make_enum_lookup("bonus_spins_status")
lookup_bonus_status_wheelspin = _make_enum_lookup("bonus_wheel_status")
lookup_bonus_status_jackpot = _make_enum_lookup("jackpot_wheel_status")
lookup_skin_group = _make_enum_lookup("skin_group")
lookup_skin_origin = _make_enum_lookup("skin_origin", none_for_missing=True)


def compose_worth_freespins_wheelspin(row: dict) -> float:
    """{DENOMINATION, LINES, COINS, MULTIPLIER} -> product as float."""
    if row is None: return 0.0
    try:
        d = float(row.get("DENOMINATION") or 0)
        l = int(row.get("LINES") or 0)
        c = int(row.get("COINS") or 0)
        m = float(row.get("MULTIPLIER") or 0)
        return d * l * c * m
    except (TypeError, ValueError):
        return 0.0


def compose_chip_count_left_freespins(row: dict) -> Optional[int]:
    """{SPECIALBONUSTYPEID, FREESPINS_LEFT} -> FREESPINS_LEFT if FreeChips (4); 0 for FS; None otherwise."""
    if row is None: return None
    sid = _to_int_or(row.get("SPECIALBONUSTYPEID"), 1)
    fsl = row.get("FREESPINS_LEFT")
    if _is_nan_or_none(fsl):
        return 0
    try:
        return int(fsl) if sid == 4 else 0
    except (TypeError, ValueError):
        return 0


def parse_menu_items_ids(value: Any) -> Optional[list]:
    """MENUITEMIDS/MENUITEMSIDS CSV string -> list[str]. ',-1,' -> []. None -> None."""
    if _is_nan_or_none(value):
        return None
    s = str(value)
    if s == ALL_APPLICATIONS_VALUE:
        return []
    out = []
    for p in s.split(","):
        p = p.strip()
        if p.isdigit():
            out.append(p)
    return out


def compose_jackpot_menu_items_ids(value: Any) -> Optional[list]:
    """JW MENU_ITEM_ID -> [str(id)] or None."""
    if _is_nan_or_none(value):
        return None
    try:
        return [str(int(value))]
    except (TypeError, ValueError):
        s = str(value).strip()
        return [s] if s else None


def constant_class_playerbonus(value: Any) -> str:
    return "com.gth.messagingstreamer.common.vo.PlayerBonusVO"


def constant_false_bool(value: Any) -> bool: return False
def constant_zero_long(value: Any) -> int: return 0
