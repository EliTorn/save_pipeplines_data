"""CSV scrubbing + per-batch save helpers (Oracle/ES/diffs).

Phase C: local files (CSV + Parquet) are the source of truth. PG no longer
receives heavy diff/missing rows. DuckDB queries Parquet directly.

save_diffs returns {'changes': N, 'missing': M} so the runner can aggregate
per-event row counts for pipeline_run_summary."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.config import SAVE_CHANGES, SAVE_FULL_CSV, SAVE_MISSING, VERBOSE
from core.parquet_writer import save_parquet, parquet_path_for

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


def _save_one(df: pd.DataFrame, csv_path: Path,
              event: str, batch: int, logger, kind: str) -> None:
    """Write CSV + Parquet sibling. Both are best-effort; one can fail
    independently."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    scrubbed = strip_nl(df)
    try:
        scrubbed.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logger.event(f"{kind}_saved", table=event, batch=batch,
                     path=str(csv_path), rows=len(df))
    except Exception as e:
        logger.event(f"{kind}_csv_failed", level="WARN", table=event,
                     batch=batch, path=str(csv_path), error=str(e))
    try:
        pq = parquet_path_for(csv_path)
        written = save_parquet(scrubbed, pq)
        if written is not None:
            logger.event(f"{kind}_parquet_saved", table=event, batch=batch,
                         path=str(written), rows=len(df))
    except Exception as e:
        logger.event(f"{kind}_parquet_failed", level="WARN", table=event,
                     batch=batch, error=str(e))


def save_diffs(diffs: pd.DataFrame, event_dir: Path, stamp: str,
               event: str, batch: int, logger,
               shaped_ora: "pd.DataFrame | None" = None,
               pk: str = "id") -> dict:
    """Persist diff/missing rows to CSV + Parquet. PG receives no per-row
    writes — DuckDB reads Parquet for downstream consumers.

    Returns {'changes': N_diff_rows, 'missing': N_missing_rows} so the runner
    can aggregate per-event totals for pipeline_run_summary."""
    counts = {"changes": 0, "missing": 0}
    if diffs.empty:
        if VERBOSE:
            print(f"[{event}] batch {batch} diffs:  0 rows (skipped write)")
        return counts

    changes_dir = event_dir / "changes"
    changes_dir.mkdir(exist_ok=True)

    diff_only = diffs[diffs["status"] == "diff"]
    if not diff_only.empty and SAVE_CHANGES:
        p = changes_dir / f"changes_{stamp}.csv"
        _save_one(diff_only, p, event, batch, logger, "diffs")
        counts["changes"] = len(diff_only)
        if VERBOSE:
            print(f"[{event}] batch {batch} diffs: {len(diff_only)} rows -> {p}")

    if not SAVE_MISSING:
        return counts

    BAD_ID = {"", "none", "nan", "<na>", "null"}
    miss_es = diffs[diffs["status"] == "missing_in_es"]
    if miss_es.empty or shaped_ora is None or pk not in shaped_ora.columns:
        return counts
    ids = {i for i in miss_es[pk].astype(str).str.strip().unique()
           if i and i.lower() not in BAD_ID}
    if not ids:
        return counts
    sa = shaped_ora.copy()
    sa[pk] = sa[pk].astype(str).str.strip()
    sa = sa[~sa[pk].str.lower().isin(BAD_ID)]
    full = sa[sa[pk].isin(ids)].copy()
    if full.empty:
        return counts
    full["error"] = "missing_in_es"
    p = changes_dir / f"missing_in_es_{stamp}.csv"
    _save_one(full, p, event, batch, logger, "missing_in_es")
    counts["missing"] = len(full)
    if VERBOSE:
        print(f"[{event}] batch {batch} missing_in_es: {len(full)} rows -> {p}")
    return counts
