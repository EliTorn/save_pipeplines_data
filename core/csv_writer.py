"""CSV scrubbing + per-batch save helpers (Oracle/ES/diffs)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.config import SAVE_CHANGES, SAVE_FULL_CSV, SAVE_MISSING, VERBOSE

_NL_TRANS = str.maketrans({"\r": " ", "\n": " ", "\t": " "})


def _scrub(value):
    """Recursively replace newline/CR/tab in any string anywhere in the value."""
    if isinstance(value, str):
        return value.translate(_NL_TRANS)
    if isinstance(value, list):
        return [_scrub(x) for x in value]
    if isinstance(value, tuple):
        return tuple(_scrub(x) for x in value)
    if isinstance(value, dict):
        return {k: _scrub(v) for k, v in value.items()}
    return value


def strip_nl(df: pd.DataFrame) -> pd.DataFrame:
    """Bulletproof: scrub newlines from any string in any cell so each CSV row is one line.
    Handles object/string/category dtypes plus nested lists/tuples/dicts."""
    if df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        kind = out[c].dtype
        if kind == "object" or pd.api.types.is_string_dtype(out[c]) or str(kind) == "category":
            try:
                out[c] = out[c].map(_scrub)
            except Exception:
                out[c] = out[c].astype(str).map(_scrub)
    return out


def save_oracle_csv(df: pd.DataFrame, path: Path,
                    event: str, batch: int, logger, **extra) -> None:
    if not SAVE_FULL_CSV:
        return
    strip_nl(df).to_csv(path, index=False, encoding="utf-8-sig")
    logger.event("csv_saved", table=event, batch=batch, source="oracle",
                 path=str(path), rows=len(df), **extra)


def save_es_csv(df: pd.DataFrame, path: Path,
                event: str, batch: int, logger) -> None:
    if not SAVE_FULL_CSV:
        return
    strip_nl(df).to_csv(path, index=False, encoding="utf-8-sig")
    logger.event("csv_saved", table=event, batch=batch, source="es",
                 path=str(path), rows=len(df))


def save_diffs(diffs: pd.DataFrame, event_dir: Path, stamp: str,
               event: str, batch: int, logger,
               shaped_ora: "pd.DataFrame | None" = None, pk: str = "id") -> None:
    if diffs.empty:
        if VERBOSE:
            print(f"[{event}] batch {batch} diffs:  0 rows (skipped write)")
        return
    changes_dir = event_dir / "changes"
    changes_dir.mkdir(exist_ok=True)

    if SAVE_CHANGES:
        diff_only = diffs[diffs["status"] == "diff"]
        if not diff_only.empty:
            p = changes_dir / f"changes_{stamp}.csv"
            strip_nl(diff_only).to_csv(p, index=False, encoding="utf-8-sig")
            logger.event("diffs_saved", table=event, batch=batch, path=str(p), rows=len(diff_only))
            if VERBOSE:
                print(f"[{event}] batch {batch} diffs:        {len(diff_only)} rows -> {p}")

    if not SAVE_MISSING:
        return

    BAD_ID = {"", "none", "nan", "<na>", "null"}
    miss_es = diffs[diffs["status"] == "missing_in_es"]
    if not miss_es.empty and shaped_ora is not None and pk in shaped_ora.columns:
        ids = {i for i in miss_es[pk].astype(str).str.strip().unique()
               if i and i.lower() not in BAD_ID}
        if ids:
            sa = shaped_ora.copy()
            sa[pk] = sa[pk].astype(str).str.strip()
            sa = sa[~sa[pk].str.lower().isin(BAD_ID)]
            full = sa[sa[pk].isin(ids)].copy()
            if not full.empty:
                full["error"] = "missing_in_es"
                p = changes_dir / f"missing_in_es_{stamp}.csv"
                strip_nl(full).to_csv(p, index=False, encoding="utf-8-sig")
                logger.event("missing_in_es_saved", table=event, batch=batch, path=str(p), rows=len(full))
                if VERBOSE:
                    print(f"[{event}] batch {batch} missing_in_es: {len(full)} rows -> {p}")
