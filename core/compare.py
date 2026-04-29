"""Mapping -> ES-shape transform + Oracle/ES diff.

Lambda registry `LAMBDAS` is the union of common_lambdas + every per-index
adapter's lambdas, assembled explicitly here (no auto-registration).
Pass `adapter=` to scope a transform to one index's lambdas.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from core.adapter_loader import known_indexes
from settings.common_lambdas import COMMON_LAMBDAS

if TYPE_CHECKING:
    from core.adapter import IndexAdapter


def _build_union_lambdas() -> dict:
    """Union of common + every known adapter's lambdas (explicit, no side-effect)."""
    out: dict = dict(COMMON_LAMBDAS)
    from core.adapter_loader import get_adapter
    for idx in known_indexes():
        try:
            out.update(get_adapter(idx).lambdas())
        except Exception as e:
            print(f"[compare] WARN: failed to load adapter for {idx}: {e}")
    return out


LAMBDAS = _build_union_lambdas()

_BAD_PK_VALUES = {"", "none", "nan", "<na>", "null"}


def _resolve_lambdas(adapter: "IndexAdapter | None") -> dict:
    if adapter is None:
        return LAMBDAS
    return {**COMMON_LAMBDAS, **adapter.lambdas()}


def transform_to_es_shape(df_raw: "pd.DataFrame", mapping: list[dict],
                          adapter: "IndexAdapter | None" = None) -> "pd.DataFrame":
    """Build a new DataFrame keyed by `filed_es` names with lambda-transformed values.
    Mapping rows with empty `filed_orcal` produce empty columns (e.g. isWhitelisted/bin).
    Pass `adapter=` to scope lookups to one index; default uses the union registry."""
    lambdas = _resolve_lambdas(adapter)
    df = apply_composite_mappings(df_raw, mapping, adapter=adapter)
    out = pd.DataFrame()
    n = len(df)
    for m in mapping:
        fo = (m.get("filed_orcal") or "").strip()
        fe = (m.get("filed_es") or "").strip()
        fn_name = (m.get("funciton_lambda") or "").strip()
        if not fe:
            continue
        if not fo:
            fn = lambdas.get(fn_name) if fn_name else None
            out[fe] = [fn(None) if fn else None] * n
            continue
        if "+" in fo:
            out[fe] = df[fe] if fe in df.columns else [None] * n
            continue
        if fo not in df.columns:
            out[fe] = [None] * n
            continue
        col = df[fo]
        fn = lambdas.get(fn_name) if fn_name else None
        if fn:
            out[fe] = col.apply(lambda v, _fn=fn: _fn(v))
        else:
            out[fe] = col
    return out


def apply_composite_mappings(df: pd.DataFrame, mapping: list[dict],
                             adapter: "IndexAdapter | None" = None) -> pd.DataFrame:
    """For mapping entries with multi-col filed_orcal (e.g. 'WALLET_TYPE+DPAN_ID'),
    call the lambda with a row dict and store result in df[filed_es]."""
    lambdas = _resolve_lambdas(adapter)
    df = df.copy()
    for m in mapping:
        fo = (m.get("filed_orcal") or "").strip()
        fe = (m.get("filed_es") or "").strip()
        fn_name = (m.get("funciton_lambda") or "").strip()
        if "+" not in fo or not fe or not fn_name:
            continue
        fn = lambdas.get(fn_name)
        if fn is None:
            continue
        cols = [c.strip() for c in fo.split("+")]
        df[fe] = df.apply(lambda r, _cols=cols, _fn=fn: _fn({c: r.get(c) for c in _cols}), axis=1)
    return df


def _is_missing(v):
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def _norm(v):
    s = str(v)
    if len(s) >= 11 and s[10] in ("T", " ") and s[4] == "-" and s[7] == "-":
        s = s[:10] + " " + s[11:]
        if len(s) == 19:
            s += ".000"
    return s


def _is_zero_like(v):
    if _is_missing(v) or isinstance(v, bool):
        return False
    try:
        return float(v) == 0.0
    except (TypeError, ValueError):
        return False


def _equal(a, b):
    if _is_missing(a) and _is_missing(b):
        return True
    if _is_missing(a) and _is_zero_like(b):
        return True
    if _is_missing(b) and _is_zero_like(a):
        return True
    if _is_missing(a) or _is_missing(b):
        return False
    if isinstance(a, bool) or isinstance(b, bool):
        return bool(a) == bool(b)
    try:
        return float(a) == float(b)
    except (TypeError, ValueError):
        pass
    return _norm(a) == _norm(b)


def _pick_row(idx, key):
    row = idx.loc[key]
    if isinstance(row, pd.DataFrame):
        row = row.iloc[0]
    return row


def _norm_pk_value(v):
    if _is_missing(v):
        return None
    s = str(v).strip()
    return None if s.lower() in _BAD_PK_VALUES else s


def _normalize_pk_columns(*frames: pd.DataFrame, pk: str) -> list[pd.DataFrame]:
    """Strip and drop bad-PK rows from each frame; returns the cleaned copies."""
    cleaned: list[pd.DataFrame] = []
    for f in frames:
        f = f.copy()
        f[pk] = f[pk].map(_norm_pk_value)
        cleaned.append(f[f[pk].notna()])
    return cleaned


def compare_shaped(shaped: pd.DataFrame, df_es: pd.DataFrame,
                   pk: str, fields: list[str] | None = None) -> pd.DataFrame:
    """Compare an already-ES-shaped Oracle DataFrame against ES docs."""
    if shaped.empty or df_es.empty or pk not in shaped.columns or pk not in df_es.columns:
        return pd.DataFrame(columns=[pk, "field", "oracle_value", "es_value", "status"])
    if fields is None:
        fields = [c for c in shaped.columns if c != pk]
    shaped, df_es = _normalize_pk_columns(shaped, df_es, pk=pk)
    idx_ora = shaped.set_index(pk)
    idx_es = df_es.set_index(pk)
    keys_ora = {k for k in idx_ora.index if k is not None}
    keys_es = {k for k in idx_es.index if k is not None}
    diffs: list[dict] = []
    for key in keys_ora & keys_es:
        r_ora = _pick_row(idx_ora, key)
        r_es = _pick_row(idx_es, key)
        for fe in fields:
            if fe == pk:
                continue
            v_ora = r_ora.get(fe)
            v_es = r_es.get(fe)
            if not _equal(v_ora, v_es):
                diffs.append({pk: key, "field": fe, "oracle_value": v_ora,
                              "es_value": v_es, "status": "diff"})
    for key in keys_ora - keys_es:
        diffs.append({pk: key, "field": "*", "oracle_value": "<row>",
                      "es_value": None, "status": "missing_in_es"})
    for key in keys_es - keys_ora:
        diffs.append({pk: key, "field": "*", "oracle_value": None,
                      "es_value": "<row>", "status": "missing_in_oracle"})
    return pd.DataFrame(diffs)


def compare_records(df_ora: pd.DataFrame, df_es: pd.DataFrame,
                    mapping: list[dict], pk: str,
                    adapter: "IndexAdapter | None" = None) -> pd.DataFrame:
    if df_ora.empty or df_es.empty or pk not in df_es.columns:
        return pd.DataFrame(columns=[pk, "field", "oracle_value", "es_value", "status"])
    shaped = transform_to_es_shape(df_ora, mapping, adapter=adapter)
    if pk not in shaped.columns:
        return pd.DataFrame(columns=[pk, "field", "oracle_value", "es_value", "status"])
    fields = [(m.get("filed_es") or "").strip() for m in mapping
              if (m.get("filed_es") or "").strip() and (m.get("filed_es") or "").strip() != pk]
    return compare_shaped(shaped, df_es, pk, fields=fields)
