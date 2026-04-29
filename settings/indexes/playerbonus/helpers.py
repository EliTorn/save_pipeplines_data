"""PLAYERBONUS index adapter + lambdas.

Covers four bonus templates (RedeemEligible, FreeSpins/FreeChips, WheelSpin,
JackpotWheel) and loads enum tables from sibling enums.yaml.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import yaml

from core.adapter import IndexAdapter
from settings.common_lambdas import _is_nan_or_none, _to_int_or

_HELPERS_DIR = Path(__file__).resolve().parent
_ENUMS_PATH = _HELPERS_DIR / "enums.yaml"

ALL_APPLICATIONS_VALUE = ",-1,"


# ---------------------------------------------------------------------------
# Enum tables
# ---------------------------------------------------------------------------

def _load_enums() -> dict[str, dict]:
    """Load `{name: {"values": {int: str}, "default": Any}}` once at import."""
    if not _ENUMS_PATH.is_file():
        raise FileNotFoundError(f"enums.yaml not found at {_ENUMS_PATH}")
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


def _make_enum_lookup(enum_name: str, *, none_for_missing: bool = False):
    """Build a lambda(value) -> str using the named enum table from YAML.
    If `none_for_missing` and the input is None/NaN, return None instead of default."""
    spec = _ENUMS.get(enum_name)
    if spec is None:
        raise KeyError(f"enum '{enum_name}' not in indexes/playerbonus/enums.yaml")
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


# ---------------------------------------------------------------------------
# ID composers — encode bonus type into ES `id`
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
    """{FREEBONUSID, SPECIALBONUSTYPEID} -> '<id>_1' (FreeSpins) or '<id>_4' (FreeChips)."""
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


# ---------------------------------------------------------------------------
# Date / numeric composers
# ---------------------------------------------------------------------------

def compose_expiration_from_days(row: dict) -> Optional[str]:
    """{CREATEDDATE, EXPIERY_DAYS} -> CREATEDDATE + EXPIERY_DAYS as 'YYYY-MM-DD HH:MM:SS.fff'.

    Mirrors Java exactly:
      - RedeemEligible: UserBonus.UserBonusBuilder.expieryDays
        feeds ISU.EXPIERY_DAYS into expirationDate2; producer sends that value.
      - FreeSpins/WheelSpin/JackpotWheel: PlayerBonusService.calculateExpirationDate
        does Calendar.add(DAY_OF_MONTH, BONUSES.EXPIERY_DAYS).
    Java rs.getInt() returns 0 for SQL NULL, so a null EXPIERY_DAYS == 0 days."""
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


def compose_worth_freespins_wheelspin(row: dict) -> float:
    """{DENOMINATION, LINES, COINS, MULTIPLIER} -> product as float."""
    if row is None:
        return 0.0
    try:
        d = float(row.get("DENOMINATION") or 0)
        l = int(row.get("LINES") or 0)
        c = int(row.get("COINS") or 0)
        m = float(row.get("MULTIPLIER") or 0)
        return d * l * c * m
    except (TypeError, ValueError):
        return 0.0


def compose_twister_prize_won(row: dict) -> bool:
    """{STATUSID, PRIZETYPE, AMOUNTWON, PRIZE_FREESPINS} -> bool.

    Mirrors Java EventManager.sendJackpotWheelEndEvent prizeWon logic:
      Only set true when the JackpotWheel reached END (statusId == 4 = Closed).
      Per JackpotWheelSegmentType (id):
        1 FREE_SPINS    -> prize template freeSpins > 0
        2 CASH_BONUS    -> userJackpotWheel.amountWon > 0
        4 MEGA_JACKPOT  -> amountWon > 0
        5 MIDI_JACKPOT  -> amountWon > 0
        6 MINI_JACKPOT  -> amountWon > 0
        9 BINGO_TICKET  -> amountWon > 0  (proxy; Java uses bingoTicket.freeTickets)
        10 SCRATCH_CARD -> amountWon > 0  (proxy; Java uses scratchCard.freeTickets)
        3 NO_PRIZE / 7 LEVEL_UP / 8 TICKET_TO_MEGA_LOTTERY / null -> false
    """
    if row is None:
        return False
    if _to_int_or(row.get("STATUSID"), -1) != 4:
        return False
    pt = _to_int_or(row.get("PRIZETYPE"), -1)
    if pt == 1:
        try:
            return int(row.get("PRIZE_FREESPINS") or 0) > 0
        except (TypeError, ValueError):
            return False
    if pt in (2, 4, 5, 6, 9, 10):
        try:
            return float(row.get("AMOUNTWON") or 0) > 0
        except (TypeError, ValueError):
            return False
    return False


def compose_freespins_left(row: dict) -> Optional[int]:
    """{SPECIALBONUSTYPEID, FREESPINS_LEFT} -> int for FreeSpins (sid != 4); None for FreeChips (sid == 4).

    Mirrors Java EventManagerService.buildFreeSpinsBonusFields:
      .freeSpinsLeft(freeChips ? null : freeSpinsUserBonus.getFreeSpinsLeft())
    """
    if row is None:
        return None
    sid = _to_int_or(row.get("SPECIALBONUSTYPEID"), 1)
    if sid == 4:
        return None
    fsl = row.get("FREESPINS_LEFT")
    if _is_nan_or_none(fsl):
        return None
    try:
        return int(fsl)
    except (TypeError, ValueError):
        return None


def compose_chip_count_left_freespins(row: dict) -> Optional[int]:
    """{SPECIALBONUSTYPEID, FREESPINS_LEFT} -> FREESPINS_LEFT if FreeChips (4); 0 for FS; None otherwise."""
    if row is None:
        return None
    sid = _to_int_or(row.get("SPECIALBONUSTYPEID"), 1)
    fsl = row.get("FREESPINS_LEFT")
    if _is_nan_or_none(fsl):
        return 0
    try:
        return int(fsl) if sid == 4 else 0
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

def constant_bonus_type_redeem(value: Any) -> str:
    return "RedeemEligble"


def constant_bonus_type_wheelspin(value: Any) -> str:
    return "WheelSpin"


def constant_bonus_type_jackpot(value: Any) -> str:
    return "JackpotWheel"


def compose_bonus_type_freespins(value: Any) -> str:
    """SPECIALBONUSTYPEID -> 'FreeChips' if id==4 else 'FreeSpins' (also accepts dict)."""
    if isinstance(value, dict):
        value = value.get("SPECIALBONUSTYPEID")
    return "FreeChips" if _to_int_or(value, 1) == 4 else "FreeSpins"


def constant_class_playerbonus(value: Any) -> str:
    return "com.gth.messagingstreamer.common.vo.PlayerBonusVO"


# ---------------------------------------------------------------------------
# Menu items parsing
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Enum-backed lookups
# ---------------------------------------------------------------------------

lookup_bonus_status_redeem = _make_enum_lookup("bonus_status")
lookup_bonus_status_freespins = _make_enum_lookup("bonus_spins_status")
lookup_bonus_status_wheelspin = _make_enum_lookup("bonus_wheel_status")
lookup_bonus_status_jackpot = _make_enum_lookup("jackpot_wheel_status")
lookup_skin_group = _make_enum_lookup("skin_group")
lookup_skin_origin = _make_enum_lookup("skin_origin", none_for_missing=True)


_PLAYERBONUS_LAMBDAS = {
    "compose_playerbonus_redeem_id": compose_playerbonus_redeem_id,
    "compose_playerbonus_wheelspin_id": compose_playerbonus_wheelspin_id,
    "compose_playerbonus_jackpot_id": compose_playerbonus_jackpot_id,
    "compose_playerbonus_freespins_id": compose_playerbonus_freespins_id,
    "compose_expiration_from_days": compose_expiration_from_days,
    "compose_worth_freespins_wheelspin": compose_worth_freespins_wheelspin,
    "compose_chip_count_left_freespins": compose_chip_count_left_freespins,
    "compose_freespins_left": compose_freespins_left,
    "compose_twister_prize_won": compose_twister_prize_won,
    "constant_bonus_type_redeem": constant_bonus_type_redeem,
    "constant_bonus_type_wheelspin": constant_bonus_type_wheelspin,
    "constant_bonus_type_jackpot": constant_bonus_type_jackpot,
    "compose_bonus_type_freespins": compose_bonus_type_freespins,
    "constant_class_playerbonus": constant_class_playerbonus,
    "parse_menu_items_ids": parse_menu_items_ids,
    "compose_jackpot_menu_items_ids": compose_jackpot_menu_items_ids,
    "lookup_bonus_status_redeem": lookup_bonus_status_redeem,
    "lookup_bonus_status_freespins": lookup_bonus_status_freespins,
    "lookup_bonus_status_wheelspin": lookup_bonus_status_wheelspin,
    "lookup_bonus_status_jackpot": lookup_bonus_status_jackpot,
    "lookup_skin_group": lookup_skin_group,
    "lookup_skin_origin": lookup_skin_origin,
}


_FIELD_KIND_OVERRIDES = {
    "parentId":                 "int_str",  # ES=keyword, lambdas emit int
    "maxWin":                   "int_str",  # ES=keyword, lambdas emit int
    "triggeringTransactionId":  "int",      # ES=integer, mapping lambda empty -> str fallback
}


class Adapter(IndexAdapter):
    INDEX_NAME = "playerbonus"

    def lambdas(self):
        return dict(_PLAYERBONUS_LAMBDAS)

    def field_kind_overrides(self):
        return dict(_FIELD_KIND_OVERRIDES)

    def before_apply(self, doc: dict) -> dict:
        """Stamp updateDate=now() on every PLAYERBONUS doc before sending to ES.

        Mirrors Java `playerBonusVO.setUpdateDate(Timestamp.valueOf(DBManager.nowLocalDateTime()))`."""
        from core.coerce import now_es_iso
        out = dict(doc)
        out["updateDate"] = now_es_iso()
        return out
