"""Postgres business-summary writer.

Two tables only:
    pipeline_run         — one row per run (insert at start, update at end)
    pipeline_run_target  — one row per (run_id × target × operation)

All inserts/updates best-effort via CachedConnection. PG unreachable -> warn
once + silent no-op.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from connect_into_postgres._pg_cache import CachedConnection

DDL = [
    """CREATE TABLE IF NOT EXISTS pipeline_run (
        run_id              TEXT PRIMARY KEY,
        env                 TEXT NOT NULL,
        host                TEXT NOT NULL,
        trigger             TEXT NOT NULL,
        schema_version      TEXT NOT NULL,
        started_at          TIMESTAMPTZ NOT NULL,
        ended_at            TIMESTAMPTZ,
        duration_ms         BIGINT,
        status              TEXT NOT NULL,
        events_count        INT,
        total_rows_changed  BIGINT,
        final_error         TEXT,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS ix_pipeline_run_started "
    "ON pipeline_run (started_at DESC)",
    "CREATE INDEX IF NOT EXISTS ix_pipeline_run_status "
    "ON pipeline_run (status, started_at DESC)",

    """CREATE TABLE IF NOT EXISTS pipeline_run_target (
        id              BIGSERIAL PRIMARY KEY,
        run_id          TEXT NOT NULL,
        source_system   TEXT NOT NULL,
        target_system   TEXT NOT NULL,
        target_name     TEXT NOT NULL,
        operation       TEXT NOT NULL,
        started_at      TIMESTAMPTZ NOT NULL,
        ended_at        TIMESTAMPTZ,
        duration_ms     BIGINT,
        rows_source     BIGINT,
        rows_target     BIGINT,
        rows_inserted   BIGINT,
        rows_updated    BIGINT,
        rows_deleted    BIGINT,
        rows_missing    BIGINT,
        rows_unchanged  BIGINT,
        status          TEXT NOT NULL,
        error           TEXT,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS ix_run_target_run "
    "ON pipeline_run_target (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_run_target_target "
    "ON pipeline_run_target (target_name, started_at DESC)",
]

INSERT_RUN = """
INSERT INTO pipeline_run
    (run_id, env, host, trigger, schema_version, started_at, status)
VALUES (%s, %s, %s, %s, %s, %s, 'running')
ON CONFLICT (run_id) DO NOTHING
"""

UPDATE_RUN = """
UPDATE pipeline_run
   SET ended_at = %s,
       duration_ms = %s,
       status = %s,
       events_count = %s,
       total_rows_changed = %s,
       final_error = %s,
       updated_at = now()
 WHERE run_id = %s
"""

INSERT_TARGET = """
INSERT INTO pipeline_run_target
    (run_id, source_system, target_system, target_name, operation,
     started_at, status)
VALUES (%s, %s, %s, %s, %s, %s, 'running')
RETURNING id
"""

UPDATE_TARGET = """
UPDATE pipeline_run_target
   SET ended_at = %s,
       duration_ms = %s,
       rows_source = %s,
       rows_target = %s,
       rows_inserted = %s,
       rows_updated = %s,
       rows_deleted = %s,
       rows_missing = %s,
       rows_unchanged = %s,
       status = %s,
       error = %s
 WHERE id = %s
"""

_cache = CachedConnection("pipeline-summary")


def reset_state() -> None:
    _cache.reset()


def init_schema() -> bool:
    """Create pipeline_run + pipeline_run_target. Idempotent. One-shot conn."""
    try:
        from connect_into_postgres import connect_to_postgres as pg
        conn = pg.create_connection()
    except (Exception, SystemExit) as e:
        print(f"[pipeline-summary] PG unreachable, skipping schema init: "
              f"{type(e).__name__}: {e}", flush=True)
        return False
    try:
        with conn.cursor() as cur:
            for stmt in DDL:
                cur.execute(stmt)
        conn.commit()
        print("[pipeline-summary] schema ensured "
              "(pipeline_run, pipeline_run_target)", flush=True)
        return True
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        print(f"[pipeline-summary] schema init failed: "
              f"{type(e).__name__}: {e}", flush=True)
        return False
    finally:
        try: conn.close()
        except Exception: pass


def _exec(sql: str, params: tuple, *, fetch: bool = False):
    conn = _get_conn()
    if conn is None:
        return None
    try:
        with _cache.lock, conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone() if fetch else None
        conn.commit()
        return row
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        print(f"[pipeline-summary] insert/update failed (silenced): "
              f"{type(e).__name__}: {e}", flush=True)
        return None


def _get_conn():
    return _cache.get()


def insert_run(*, run_id: str, env: str, host: str, trigger: str,
               schema_version: str,
               started_at: Optional[datetime] = None) -> None:
    started_at = started_at or datetime.now(timezone.utc)
    _exec(INSERT_RUN, (run_id, env, host, trigger, schema_version, started_at))


def update_run(*, run_id: str,
               ended_at: Optional[datetime] = None,
               duration_ms: Optional[int] = None,
               status: str = "ok",
               events_count: Optional[int] = None,
               total_rows_changed: Optional[int] = None,
               final_error: Optional[str] = None) -> None:
    ended_at = ended_at or datetime.now(timezone.utc)
    _exec(UPDATE_RUN, (
        ended_at, duration_ms, status, events_count, total_rows_changed,
        final_error[:4000] if final_error else None,
        run_id,
    ))


def insert_target(*, run_id: str, source_system: str, target_system: str,
                  target_name: str, operation: str,
                  started_at: Optional[datetime] = None) -> Optional[int]:
    started_at = started_at or datetime.now(timezone.utc)
    row = _exec(INSERT_TARGET, (
        run_id, source_system, target_system, target_name, operation, started_at,
    ), fetch=True)
    return int(row[0]) if row else None


def update_target(*, target_id: int,
                  ended_at: Optional[datetime] = None,
                  duration_ms: Optional[int] = None,
                  rows_source: Optional[int] = None,
                  rows_target: Optional[int] = None,
                  rows_inserted: Optional[int] = None,
                  rows_updated: Optional[int] = None,
                  rows_deleted: Optional[int] = None,
                  rows_missing: Optional[int] = None,
                  rows_unchanged: Optional[int] = None,
                  status: str = "ok",
                  error: Optional[str] = None) -> None:
    if target_id is None:
        return
    ended_at = ended_at or datetime.now(timezone.utc)
    _exec(UPDATE_TARGET, (
        ended_at, duration_ms,
        rows_source, rows_target,
        rows_inserted, rows_updated, rows_deleted,
        rows_missing, rows_unchanged,
        status, error[:4000] if error else None,
        target_id,
    ))
