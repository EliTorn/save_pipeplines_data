"""LEGACY fallback: pull unapplied diffs/missing rows from the legacy PG
heavy tables so `apply_changes --source pg` keeps working against historical
data.

Phase C+ status:
  - The active flow writes diffs/missing only to local Parquet, never to PG.
  - `apply_changes` defaults to `--source duckdb`. This module is selected
    only by `--source pg` or as the first step of `--source auto`.
  - Used as: `pipeline_changes WHERE applied_ts IS NULL` /
             `pipeline_missing WHERE applied_ts IS NULL`.

Reconstructs CSV-shaped dicts so the existing apply_changes logic can iterate
unchanged. Each dict has the same keys as `changes_*.csv` / `missing_in_es_*.csv`
rows.
"""
from __future__ import annotations

import json

from connect_into_postgres._pg_cache import CachedConnection

_cache = CachedConnection("pg-source")


def _get_conn():
    return _cache.get()


def reset_state() -> None:
    _cache.reset()


def is_available() -> bool:
    return _get_conn() is not None


def events_with_pending(env: str) -> list[str]:
    """Events that have at least one pending (unapplied) diff or missing row."""
    conn = _get_conn()
    if conn is None:
        return []
    out: set[str] = set()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT event FROM pipeline_changes "
                "WHERE env = %s AND applied_ts IS NULL "
                "  AND COALESCE(status, '') <> 'applied'",
                (env,),
            )
            out.update(r[0] for r in cur.fetchall() if r[0])
            cur.execute(
                "SELECT DISTINCT event FROM pipeline_missing "
                "WHERE env = %s AND applied_ts IS NULL",
                (env,),
            )
            out.update(r[0] for r in cur.fetchall() if r[0])
    except Exception as e:
        print(f"[pg-source] events_with_pending failed: {type(e).__name__}: {e}")
        try: conn.rollback()
        except Exception: pass
    return sorted(out)


def pending_counts(env: str) -> dict[str, dict]:
    """{event: {'changes': N, 'missing': M}} for the given env."""
    conn = _get_conn()
    if conn is None:
        return {}
    out: dict[str, dict] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT event, COUNT(*) FROM pipeline_changes "
                "WHERE env = %s AND applied_ts IS NULL "
                "  AND COALESCE(status, '') <> 'applied' "
                "GROUP BY event",
                (env,),
            )
            for ev, n in cur.fetchall():
                out.setdefault(ev, {"changes": 0, "missing": 0})["changes"] = int(n)
            cur.execute(
                "SELECT event, COUNT(*) FROM pipeline_missing "
                "WHERE env = %s AND applied_ts IS NULL "
                "GROUP BY event",
                (env,),
            )
            for ev, n in cur.fetchall():
                out.setdefault(ev, {"changes": 0, "missing": 0})["missing"] = int(n)
    except Exception as e:
        print(f"[pg-source] pending_counts failed: {type(e).__name__}: {e}")
        try: conn.rollback()
        except Exception: pass
    return out


def load_pending_changes(event: str, env: str, pk: str = "id") -> list[dict]:
    """Return CSV-shaped dicts (one per (doc, field) diff) for unapplied rows.
    Keys: pk_col, 'id', 'field', 'oracle_value', 'es_value', 'status', 'source_file'
    """
    conn = _get_conn()
    if conn is None:
        return []
    rows: list[dict] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_file, doc_id, field, oracle_value, es_value, status "
                "FROM pipeline_changes "
                "WHERE event = %s AND env = %s AND applied_ts IS NULL "
                "  AND COALESCE(status, '') <> 'applied'",
                (event, env),
            )
            for source_file, doc_id, field, ora, es, status in cur.fetchall():
                row = {
                    pk: doc_id, "id": doc_id,
                    "field": field or "",
                    "oracle_value": "" if ora is None else ora,
                    "es_value": "" if es is None else es,
                    "status": status or "",
                    "source_file": source_file,
                }
                rows.append(row)
    except Exception as e:
        print(f"[pg-source] load_pending_changes failed: {type(e).__name__}: {e}")
        try: conn.rollback()
        except Exception: pass
    return rows


def load_pending_missing(event: str, env: str, pk: str = "id") -> list[dict]:
    """Return CSV-shaped dicts (one per missing doc) for unapplied missing rows.
    Each dict carries the original payload fields plus a 'source_file' key.
    """
    conn = _get_conn()
    if conn is None:
        return []
    rows: list[dict] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_file, doc_id, payload FROM pipeline_missing "
                "WHERE event = %s AND env = %s AND applied_ts IS NULL",
                (event, env),
            )
            for source_file, doc_id, payload in cur.fetchall():
                # payload is JSONB → psycopg returns dict for psycopg3, str for psycopg2.
                if isinstance(payload, str):
                    try:
                        payload_d = json.loads(payload)
                    except Exception:
                        payload_d = {}
                elif isinstance(payload, dict):
                    payload_d = payload
                else:
                    payload_d = {}
                row = {**payload_d}
                # Ensure pk + 'id' exist
                if pk not in row and doc_id is not None:
                    row[pk] = doc_id
                if "id" not in row and doc_id is not None:
                    row["id"] = doc_id
                row["source_file"] = source_file
                rows.append(row)
    except Exception as e:
        print(f"[pg-source] load_pending_missing failed: {type(e).__name__}: {e}")
        try: conn.rollback()
        except Exception: pass
    return rows


def source_files_for_changes(event: str, env: str) -> set[str]:
    """Return source_file set involved in unapplied changes — used to mark each
    file applied via pg_tracking after a successful run."""
    conn = _get_conn()
    if conn is None:
        return set()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT source_file FROM pipeline_changes "
                "WHERE event = %s AND env = %s AND applied_ts IS NULL "
                "  AND COALESCE(status, '') <> 'applied'",
                (event, env),
            )
            return {r[0] for r in cur.fetchall() if r[0]}
    except Exception:
        try: conn.rollback()
        except Exception: pass
        return set()


def source_files_for_missing(event: str, env: str) -> set[str]:
    conn = _get_conn()
    if conn is None:
        return set()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT source_file FROM pipeline_missing "
                "WHERE event = %s AND env = %s AND applied_ts IS NULL",
                (event, env),
            )
            return {r[0] for r in cur.fetchall() if r[0]}
    except Exception:
        try: conn.rollback()
        except Exception: pass
        return set()


def close() -> None:
    _cache.close()
