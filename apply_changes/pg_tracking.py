"""Track which apply_changes batches (per source file) were already pushed
to ES. This is the small operational state that lets `--source duckdb` skip
already-applied Parquet files between runs.

Granularity is one row per source file:
    out/<EVENT>/<env>/changes/changes_<stamp>.csv      (legacy)
    out/<EVENT>/<env>/changes/changes_<stamp>.parquet  (Phase C+ source)

Workflow:
    1. apply_changes asks fetch_applied_files() which source files are
       already done -> skips them.
    2. After successfully applying a file, calls mark_applied() which:
        - INSERTs / UPDATEs `pipeline_apply_batches` (active small-state table)
        - UPDATEs matching rows in legacy pipeline_changes / pipeline_missing
          if any exist (status='applied', applied_ts=now()).

All functions are best-effort on PG unreachable — apply_changes must keep
working even if Postgres is down.
"""
from __future__ import annotations

from pathlib import Path

from connect_into_postgres._pg_cache import CachedConnection

_ROOT = Path(__file__).resolve().parent.parent

_cache = CachedConnection("pg-tracking")


def _get_conn():
    return _cache.get()


def reset_state() -> None:
    _cache.reset()


_DDL = [
    """CREATE TABLE IF NOT EXISTS pipeline_apply_batches (
        event       TEXT NOT NULL,
        env         TEXT NOT NULL,
        mode        TEXT NOT NULL,
        source_file TEXT NOT NULL,
        applied_ts  TIMESTAMP NOT NULL DEFAULT now(),
        run_id      TEXT,
        docs_planned INT,
        es_updated   INT,
        es_created   INT,
        es_conflicts INT,
        es_failures  INT,
        notes        TEXT,
        PRIMARY KEY (event, env, mode, source_file)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_apply_batches_event_env_mode "
    "ON pipeline_apply_batches (event, env, mode)",
]


def init_schema() -> bool:
    """Ensure pipeline_apply_batches exists. Idempotent. Best-effort."""
    try:
        from connect_into_postgres import connect_to_postgres as pg
        conn = pg.create_connection(application_name="oraes:pg-tracking-init")
    except (Exception, SystemExit) as e:
        print(f"[pg-tracking] PG unreachable, skipping schema init: "
              f"{type(e).__name__}: {e}", flush=True)
        return False
    try:
        with conn.cursor() as cur:
            for stmt in _DDL:
                cur.execute(stmt)
        conn.commit()
        print("[pg-tracking] schema ensured (pipeline_apply_batches)", flush=True)
        return True
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        print(f"[pg-tracking] schema init failed: {type(e).__name__}: {e}",
              flush=True)
        return False
    finally:
        try: conn.close()
        except Exception: pass


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
    """Record this CSV as applied. Returns True if pipeline_apply_batches
    was updated, False on PG unreachable or insert failure.

    The active write here is the pipeline_apply_batches INSERT (small state
    table). A best-effort secondary UPDATE flips rows in legacy
    pipeline_changes / pipeline_missing if those tables still exist — it
    runs in its OWN transaction so a missing legacy table doesn't roll back
    the active INSERT. Once the legacy tables are dropped (loop 6), this
    secondary step turns into a silent no-op.
    """
    conn = _get_conn()
    if conn is None:
        return False
    rel = _csv_to_source_file(csv_path, event, env)

    # 1) Active small-state INSERT — must succeed.
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
        conn.commit()
    except Exception as e:
        print(f"[pg-tracking] mark_applied (active) failed for {rel}: "
              f"{type(e).__name__}: {e}")
        try: conn.rollback()
        except Exception: pass
        return False

    # 2) Legacy UPDATE — separate tx, swallow any error (table may be dropped).
    try:
        with conn.cursor() as cur:
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
    except Exception:
        # Legacy tables likely dropped — fine. Silent.
        try: conn.rollback()
        except Exception: pass

    return True


def close() -> None:
    _cache.close()
