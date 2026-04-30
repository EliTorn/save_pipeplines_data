"""Phase B: pending diffs/missing read from local Parquet via DuckDB.

Mirrors the API of `apply_changes.pg_source` so apply_changes.py can swap
between sources via `--source {pg,duckdb,csv,auto}`.

Pending semantics:
  - DuckDB sees ALL rows in v_changes / v_missing (no applied_ts column on disk).
  - To respect prior apply runs, this module consults PG `pipeline_apply_batches`
    for already-applied source_files and excludes them.
  - If PG is unreachable, returns ALL rows. Apply step is naturally idempotent
    (ES upsert / _create with skip-on-409), so no double-apply hazard, only
    extra work.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from core import duckdb_catalog
from connect_into_postgres._pg_cache import CachedConnection

_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = _ROOT / "out"

_cache = CachedConnection("duckdb-source")


def _get_pg():
    """Optional PG conn for applied-batches lookup. Cached. Best-effort."""
    return _cache.get()


def reset_state() -> None:
    _cache.reset()


def is_available() -> bool:
    """DuckDB usable AND v_changes/v_missing have at least one file."""
    if not duckdb_catalog.is_available():
        print("[duckdb-source] catalog not available — DuckDB source disabled")
        return False
    try:
        df = duckdb_catalog.query(
            "SELECT (SELECT COUNT(*) FROM v_changes) AS c, "
            "       (SELECT COUNT(*) FROM v_missing) AS m"
        )
        if df is None or len(df) == 0:
            print("[duckdb-source] catalog query returned None")
            return False
        c = int(df.iloc[0, 0]); m = int(df.iloc[0, 1])
        ok = (c + m) > 0
        if not ok:
            print(f"[duckdb-source] both views empty (v_changes={c} v_missing={m})  "
                  f"→ catalog likely points at empty placeholder views; "
                  f"call duckdb_catalog.init_catalog() to refresh")
        else:
            print(f"[duckdb-source] available: v_changes={c} rows, "
                  f"v_missing={m} rows (across all events/envs)")
        return ok
    except Exception as e:
        print(f"[duckdb-source] is_available query failed: "
              f"{type(e).__name__}: {e}")
        return False


def _applied_source_files(event: str, env: str, mode: str) -> set[str]:
    """source_file values already pushed to ES per pipeline_apply_batches."""
    conn = _get_pg()
    if conn is None:
        return set()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_file FROM pipeline_apply_batches "
                "WHERE event = %s AND env = %s AND mode = %s",
                (event, env, mode),
            )
            return {r[0] for r in cur.fetchall() if r[0]}
    except Exception as e:
        print(f"[duckdb-source] applied lookup failed: "
              f"{type(e).__name__}: {e}")
        try: conn.rollback()
        except Exception: pass
        return set()


def _filename_to_rel(fn: str) -> str:
    """`<...>/out/EVENT/env/changes/file.parquet` -> `EVENT/env/changes/file.parquet`.
    Normalizes both parquet (DuckDB) and csv (PG-stored) paths."""
    s = fn.replace("\\", "/")
    parts = s.split("/out/", 1)
    return parts[1] if len(parts) == 2 else s


def _parquet_to_csv_rel(rel: str) -> str:
    """Map a Parquet rel_path back to the corresponding CSV rel_path so we
    can match against pipeline_apply_batches.source_file (which records CSV)."""
    if rel.endswith(".parquet"):
        return rel[: -len(".parquet")] + ".csv"
    return rel


def events_with_pending(env: str) -> list[str]:
    """Distinct events with at least one diff/missing parquet for this env."""
    if not duckdb_catalog.is_available():
        return []
    sql = """
    WITH src AS (
        SELECT regexp_extract(replace(filename, chr(92), '/'),
                              '/out/([^/]+)/([^/]+)/changes/', 1) AS event,
               regexp_extract(replace(filename, chr(92), '/'),
                              '/out/([^/]+)/([^/]+)/changes/', 2) AS env
        FROM v_changes
        UNION ALL
        SELECT regexp_extract(replace(filename, chr(92), '/'),
                              '/out/([^/]+)/([^/]+)/changes/', 1) AS event,
               regexp_extract(replace(filename, chr(92), '/'),
                              '/out/([^/]+)/([^/]+)/changes/', 2) AS env
        FROM v_missing
    )
    SELECT DISTINCT event FROM src
    WHERE event IS NOT NULL AND event <> '' AND env = ?
    ORDER BY event
    """
    try:
        df = duckdb_catalog.query(sql, (env,))
        if df is None: return []
        return [str(e) for e in df["event"].tolist() if e]
    except Exception as e:
        print(f"[duckdb-source] events_with_pending failed: "
              f"{type(e).__name__}: {e}")
        return []


def pending_counts(env: str) -> dict[str, dict]:
    """{event: {'changes': N, 'missing': M}} for the given env (parquet count,
    NOT subtracting applied — for inventory display only)."""
    if not duckdb_catalog.is_available():
        return {}
    out: dict[str, dict] = {}
    base = """
    SELECT regexp_extract(replace(filename, chr(92), '/'),
                          '/out/([^/]+)/([^/]+)/changes/', 1) AS event,
           COUNT(*) AS n
    FROM {view}
    WHERE regexp_extract(replace(filename, chr(92), '/'),
                          '/out/([^/]+)/([^/]+)/changes/', 2) = ?
    GROUP BY event
    """
    try:
        df = duckdb_catalog.query(base.format(view="v_changes"), (env,))
        if df is not None:
            for _, r in df.iterrows():
                ev = str(r["event"] or "")
                if ev:
                    out.setdefault(ev, {"changes": 0, "missing": 0})["changes"] = int(r["n"])
        df = duckdb_catalog.query(base.format(view="v_missing"), (env,))
        if df is not None:
            for _, r in df.iterrows():
                ev = str(r["event"] or "")
                if ev:
                    out.setdefault(ev, {"changes": 0, "missing": 0})["missing"] = int(r["n"])
    except Exception as e:
        print(f"[duckdb-source] pending_counts failed: "
              f"{type(e).__name__}: {e}")
    return out


def load_pending_changes(event: str, env: str, pk: str = "id") -> list[dict]:
    """Same shape as pg_source.load_pending_changes — list[dict] keyed by
    pk_col, 'id', 'field', 'oracle_value', 'es_value', 'status', 'source_file'.
    """
    if not duckdb_catalog.is_available():
        return []
    applied_csvs = _applied_source_files(event, env, "changes")
    sql = """
    SELECT
        CAST(COALESCE(TRY_CAST("{pk}" AS VARCHAR), CAST(id AS VARCHAR)) AS VARCHAR) AS doc_id,
        field, oracle_value, es_value, status,
        replace(filename, chr(92), '/') AS filename
    FROM v_changes
    WHERE regexp_extract(replace(filename, chr(92), '/'),
                         '/out/([^/]+)/([^/]+)/changes/', 1) = ?
      AND regexp_extract(replace(filename, chr(92), '/'),
                         '/out/([^/]+)/([^/]+)/changes/', 2) = ?
    """.format(pk=pk if pk and pk.replace("_", "").isalnum() else "id")
    try:
        df = duckdb_catalog.query(sql, (event, env))
    except Exception as e:
        print(f"[duckdb-source] load_pending_changes failed: "
              f"{type(e).__name__}: {e}")
        return []
    if df is None or df.empty:
        return []

    rows: list[dict] = []
    skipped_files: set[str] = set()
    bad_doc_ids = 0
    for _, r in df.iterrows():
        fn = str(r["filename"] or "")
        rel_pq = _filename_to_rel(fn)
        rel_csv = _parquet_to_csv_rel(rel_pq)
        if rel_csv in applied_csvs:
            skipped_files.add(rel_csv)
            continue
        doc_id = str(r["doc_id"] or "").strip()
        if not doc_id or doc_id.lower() in ("none", "nan", "<na>", "null"):
            bad_doc_ids += 1
            continue
        rows.append({
            pk: doc_id, "id": doc_id,
            "field": str(r["field"] or ""),
            "oracle_value": "" if r["oracle_value"] is None else str(r["oracle_value"]),
            "es_value": "" if r["es_value"] is None else str(r["es_value"]),
            "status": str(r["status"] or ""),
            "source_file": rel_pq,
        })
    print(f"[duckdb-source] load_pending_changes({event}/{env}): "
          f"{len(df)} raw rows in v_changes -> "
          f"{len(rows)} pending after filters "
          f"(applied_skip={len(skipped_files)} files, "
          f"bad_doc_ids={bad_doc_ids})")
    if skipped_files:
        for s in list(skipped_files)[:3]:
            print(f"[duckdb-source]   already-applied: {s}")
        if len(skipped_files) > 3:
            print(f"[duckdb-source]   ... +{len(skipped_files)-3} more")
    return rows


def load_pending_missing(event: str, env: str, pk: str = "id") -> list[dict]:
    """Returns full-row dicts. Skips files already applied (per PG). Always
    includes a 'source_file' key for parity with pg_source."""
    if not duckdb_catalog.is_available():
        return []
    applied_csvs = _applied_source_files(event, env, "missing")
    # Pull * + filename. v_missing row shape varies by event, so we let
    # DuckDB give us all columns and we forward as a dict.
    sql = """
    SELECT *,
           replace(filename, chr(92), '/') AS _filename
    FROM v_missing
    WHERE regexp_extract(replace(filename, chr(92), '/'),
                         '/out/([^/]+)/([^/]+)/changes/', 1) = ?
      AND regexp_extract(replace(filename, chr(92), '/'),
                         '/out/([^/]+)/([^/]+)/changes/', 2) = ?
    """
    try:
        df = duckdb_catalog.query(sql, (event, env))
    except Exception as e:
        print(f"[duckdb-source] load_pending_missing failed: "
              f"{type(e).__name__}: {e}")
        return []
    if df is None or df.empty:
        return []

    rows: list[dict] = []
    skipped_files: set[str] = set()
    bad_doc_ids = 0
    for r in df.to_dict(orient="records"):
        fn = str(r.pop("_filename", "") or r.get("filename", "") or "")
        rel_pq = _filename_to_rel(fn)
        rel_csv = _parquet_to_csv_rel(rel_pq)
        if rel_csv in applied_csvs:
            skipped_files.add(rel_csv); continue
        # Drop bookkeeping cols we don't want to forward as ES fields.
        r.pop("filename", None)
        # Stringify everything (parity with pg_source/csv path).
        clean = {}
        for k, v in r.items():
            if v is None:
                continue
            if isinstance(v, float):
                # pyarrow nan → skip
                import math
                if math.isnan(v): continue
            s = str(v)
            if s.strip() == "" or s.lower() in ("nan", "none", "<na>", "null"):
                continue
            clean[k] = s
        doc_id = clean.get(pk) or clean.get("id")
        if not doc_id:
            bad_doc_ids += 1
            continue
        clean.setdefault(pk, doc_id)
        clean.setdefault("id", doc_id)
        clean["source_file"] = rel_pq
        rows.append(clean)
    print(f"[duckdb-source] load_pending_missing({event}/{env}): "
          f"{len(df)} raw rows in v_missing -> "
          f"{len(rows)} pending after filters "
          f"(applied_skip={len(skipped_files)} files, "
          f"bad_doc_ids={bad_doc_ids})")
    if skipped_files:
        for s in list(skipped_files)[:3]:
            print(f"[duckdb-source]   already-applied: {s}")
        if len(skipped_files) > 3:
            print(f"[duckdb-source]   ... +{len(skipped_files)-3} more")
    return rows


def source_files_for_changes(event: str, env: str) -> set[str]:
    """Returned in CSV-rel form so pg_tracking.mark_applied can match
    pipeline_apply_batches.source_file (which records CSV paths)."""
    rows = load_pending_changes(event, env)
    return {_parquet_to_csv_rel(r["source_file"]) for r in rows if r.get("source_file")}


def source_files_for_missing(event: str, env: str) -> set[str]:
    rows = load_pending_missing(event, env)
    return {_parquet_to_csv_rel(r["source_file"]) for r in rows if r.get("source_file")}


def close() -> None:
    _cache.close()
