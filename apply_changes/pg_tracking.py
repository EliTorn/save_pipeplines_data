"""Track which apply_changes batches (per CSV file) have already been pushed to ES,
and propagate the result back into Postgres so pipeline_changes / pipeline_missing
reflect the post-apply state.

Granularity is per source CSV file (one CSV = one window = one "batch"):
    out/<EVENT>/<env>/changes/changes_<stamp>.csv
    out/<EVENT>/<env>/changes/missing_in_es_<stamp>.csv

Workflow:
    1. apply_changes asks fetch_applied_files() which CSVs are already done -> skips.
    2. After successfully applying a CSV, calls mark_applied() which:
        - INSERTs a row into pipeline_apply_batches (PK collision = no-op)
        - UPDATEs matching rows in pipeline_changes / pipeline_missing
          (status='applied', es_value=oracle_value, applied_ts=now())

All functions are no-ops on PG unreachable / module load failure — apply_changes
must keep working even if Postgres is down.
"""
from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent

# Module-level cached connection (None on failure).
_conn = None
_pg_failed = False


def _get_conn():
    global _conn, _pg_failed
    if _conn is not None or _pg_failed:
        return _conn
    try:
        from connect_into_postgres import connect_to_postgres as pg
        _conn = pg.create_connection()
    except (Exception, SystemExit) as e:
        print(f"[pg-tracking] disabled: {type(e).__name__}: {e}")
        _pg_failed = True
        _conn = None
    return _conn


def _csv_to_source_file(csv_path: Path, event: str, env: str) -> str:
    """Match the source_file format that sync_out writes:
        '<EVENT>/<env>/changes/<filename>.csv'  (relative to out/)
    """
    try:
        rel = csv_path.relative_to(_ROOT / "out").as_posix()
    except ValueError:
        # Fallback: build it from the parts we know.
        rel = f"{event}/{env}/changes/{csv_path.name}"
    return rel


def fetch_applied_files(event: str, env: str, mode: str) -> set[str]:
    """Return the set of source_file values already recorded in
    pipeline_apply_batches for this (event, env, mode)."""
    conn = _get_conn()
    if conn is None:
        return set()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_file FROM pipeline_apply_batches "
                "WHERE event = %s AND env = %s AND mode = %s",
                (event, env, mode),
            )
            return {r[0] for r in cur.fetchall()}
    except Exception as e:
        print(f"[pg-tracking] fetch_applied_files failed: {type(e).__name__}: {e}")
        try: conn.rollback()
        except Exception: pass
        return set()


def filter_unapplied(files: list[Path], event: str, env: str, mode: str,
                     force: bool = False) -> tuple[list[Path], list[Path]]:
    """Split files into (to_run, skipped) based on pipeline_apply_batches.
    `force=True` returns everything as to_run."""
    if force:
        return list(files), []
    applied = fetch_applied_files(event, env, mode)
    to_run, skipped = [], []
    for f in files:
        rel = _csv_to_source_file(f, event, env)
        (skipped if rel in applied else to_run).append(f)
    return to_run, skipped


def mark_applied(csv_path: Path, *, event: str, env: str, mode: str,
                 run_id: str | None = None,
                 docs_planned: int = 0, es_updated: int = 0, es_created: int = 0,
                 es_conflicts: int = 0, es_failures: int = 0,
                 notes: str | None = None) -> bool:
    """Record this CSV as applied + flip its rows in pipeline_changes/missing.
    Returns True if PG state was updated, False on any failure (or PG unreachable).
    """
    conn = _get_conn()
    if conn is None:
        return False
    rel = _csv_to_source_file(csv_path, event, env)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_apply_batches "
                "(event, env, mode, source_file, run_id, docs_planned, "
                " es_updated, es_created, es_conflicts, es_failures, notes) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (event, env, mode, source_file) DO UPDATE SET "
                "  applied_ts = now(), run_id = EXCLUDED.run_id, "
                "  docs_planned = EXCLUDED.docs_planned, "
                "  es_updated = EXCLUDED.es_updated, es_created = EXCLUDED.es_created, "
                "  es_conflicts = EXCLUDED.es_conflicts, es_failures = EXCLUDED.es_failures, "
                "  notes = EXCLUDED.notes",
                (event, env, mode, rel, run_id,
                 docs_planned, es_updated, es_created, es_conflicts, es_failures, notes),
            )
            if mode == "changes":
                cur.execute(
                    "UPDATE pipeline_changes "
                    "SET status = 'applied', es_value = oracle_value, applied_ts = now() "
                    "WHERE source_file = %s AND (status IS NULL OR status <> 'applied')",
                    (rel,),
                )
            elif mode == "missing":
                cur.execute(
                    "UPDATE pipeline_missing "
                    "SET applied_ts = now() "
                    "WHERE source_file = %s AND applied_ts IS NULL",
                    (rel,),
                )
        conn.commit()
        return True
    except Exception as e:
        print(f"[pg-tracking] mark_applied failed for {rel}: {type(e).__name__}: {e}")
        try: conn.rollback()
        except Exception: pass
        return False


def close() -> None:
    global _conn
    if _conn is not None:
        try: _conn.close()
        except Exception: pass
        _conn = None
