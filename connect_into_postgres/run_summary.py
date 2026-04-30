"""Phase C: pipeline_run_summary — the only PG table receiving NEW writes.

One row per (run_id, env, target_name, operation) describing what happened:
how many rows, when, status, optional source_file.

Heavy data lives in local Parquet/CSV; DuckDB queries it directly. PG holds
only this thin run history.

All operations best-effort: PG unreachable -> warn + return None, never raise.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from connect_into_postgres._pg_cache import CachedConnection

DDL = [
    """CREATE TABLE IF NOT EXISTS pipeline_run_summary (
        id           BIGSERIAL PRIMARY KEY,
        run_id       TEXT NOT NULL,
        env          TEXT NOT NULL,
        target_name  TEXT NOT NULL,
        operation    TEXT NOT NULL,
        rows_count   BIGINT NOT NULL DEFAULT 0,
        source_file  TEXT,
        started_at   TIMESTAMPTZ NOT NULL,
        ended_at     TIMESTAMPTZ,
        status       TEXT NOT NULL,
        error        TEXT,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS ix_run_summary_run_id "
    "ON pipeline_run_summary (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_run_summary_target_op "
    "ON pipeline_run_summary (target_name, operation)",
    "CREATE INDEX IF NOT EXISTS ix_run_summary_started "
    "ON pipeline_run_summary (started_at DESC)",
]

INSERT_SQL = """
INSERT INTO pipeline_run_summary
    (run_id, env, target_name, operation, rows_count,
     source_file, started_at, ended_at, status, error)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING id
"""

OPERATIONS = ("compare", "apply_changes", "apply_missing")

# One cached PG connection per process. Failure is sticky to avoid per-event
# retry storms when PG is down at pipeline start.
_cache = CachedConnection("run-summary")


def reset_state() -> None:
    """Force a fresh PG connect on the next record_run call."""
    _cache.reset()


def init_schema() -> bool:
    """Create pipeline_run_summary table + indexes. Idempotent.
    Uses a one-shot connection (NOT the cached one) so init failure doesn't
    permanently mark the cached conn as failed."""
    try:
        from connect_into_postgres import connect_to_postgres as pg
        conn = pg.create_connection()
    except (Exception, SystemExit) as e:
        print(f"[run-summary] PG unreachable, skipping schema init: "
              f"{type(e).__name__}: {e}", flush=True)
        return False
    try:
        with conn.cursor() as cur:
            for stmt in DDL:
                cur.execute(stmt)
        conn.commit()
        print("[run-summary] schema ensured (pipeline_run_summary)", flush=True)
        return True
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        print(f"[run-summary] schema init failed: {type(e).__name__}: {e}",
              flush=True)
        return False
    finally:
        try: conn.close()
        except Exception: pass


def record_run(*, run_id: str, env: str, target_name: str, operation: str,
               rows_count: int = 0, source_file: Optional[str] = None,
               started_at: Optional[datetime] = None,
               ended_at: Optional[datetime] = None,
               status: str = "ok", error: Optional[str] = None,
               conn=None) -> Optional[int]:
    """Insert one row. Returns new id or None on failure.

    Best-effort: if PG is unreachable the call silently returns None. The
    failure is cached at module level so subsequent calls don't waste time
    re-attempting connect.
    """
    if started_at is None:
        started_at = datetime.now(timezone.utc)
    if conn is None:
        with _cache.lock:
            conn = _cache.get()
        if conn is None:
            return None
    own_conn = False  # cached conn — never close here
    try:
        with _cache.lock, conn.cursor() as cur:
            cur.execute(INSERT_SQL, (
                run_id, env, target_name, operation, int(rows_count or 0),
                source_file, started_at, ended_at, status,
                error[:4000] if error else None,
            ))
            row = cur.fetchone()
            new_id = int(row[0]) if row else None
        conn.commit()
        return new_id
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        print(f"[run-summary] insert failed for {target_name}/{operation}: "
              f"{type(e).__name__}: {e}", flush=True)
        return None
    finally:
        if own_conn:
            try: conn.close()
            except Exception: pass
