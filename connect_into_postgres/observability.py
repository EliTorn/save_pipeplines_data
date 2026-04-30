"""Phase D: PostgreSQL observability tables for Oracle/ES/PG/DuckDB.

Three small metadata-only tables — NO heavy data, just timing + counts:

    connection_log  one row per connection attempt
    query_log       one row per Oracle SQL / ES request
    batch_log       one row per pipeline batch operation

All inserts are best-effort. PG unreachable -> warn once, then silently no-op.
A single long-lived parent-process PG connection is reused across thousands of
inserts, protected by a thread lock so the listener thread + main thread don't
collide. Workers do NOT get their own PG connection: they push log events via
the existing multiprocessing queue, and the parent's listener thread mirrors
them to PG via this module.

`from_connection_row()` and `from_query_row()` adapt the row shape produced by
_pipeline_logging.RunLoggerBase / QueryRecord into the insert call — so the
existing logger pipeline only needs one extra line at each emit point.

When ``PIPELINE_LOGGING_V2=1`` and ``LOG_LEGACY_PG`` is NOT set, all writes
in this module are no-ops — the new ``pipeline_logging`` stack owns the PG
side. Schema init is also skipped so we don't keep extending dead tables.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional

from connect_into_postgres._pg_cache import CachedConnection


def _legacy_pg_writes_off() -> bool:
    """True when v2 is enabled and the legacy escape hatch is not set."""
    v2 = os.getenv("PIPELINE_LOGGING_V2", "").strip().lower() in ("1", "true", "yes", "on", "t", "y")
    legacy = os.getenv("LOG_LEGACY_PG", "").strip().lower() in ("1", "true", "yes", "on", "t", "y")
    return v2 and not legacy

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL = [
    """CREATE TABLE IF NOT EXISTS connection_log (
        id           BIGSERIAL PRIMARY KEY,
        run_id       TEXT,
        env          TEXT,
        system_name  TEXT NOT NULL,
        target_name  TEXT,
        host         TEXT,
        started_at   TIMESTAMPTZ NOT NULL,
        ended_at     TIMESTAMPTZ,
        duration_ms  BIGINT,
        status       TEXT NOT NULL,
        error        TEXT,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS ix_connection_log_run "
    "ON connection_log (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_connection_log_system "
    "ON connection_log (system_name, started_at DESC)",

    """CREATE TABLE IF NOT EXISTS query_log (
        id            BIGSERIAL PRIMARY KEY,
        run_id        TEXT,
        env           TEXT,
        batch_id      TEXT,
        system_name   TEXT NOT NULL,
        target_name   TEXT,
        operation     TEXT,
        query_name    TEXT,
        query_hash    TEXT,
        query_text    TEXT,
        started_at    TIMESTAMPTZ NOT NULL,
        ended_at      TIMESTAMPTZ,
        duration_ms   BIGINT,
        rows_returned BIGINT,
        rows_affected BIGINT,
        status        TEXT NOT NULL,
        error         TEXT,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS ix_query_log_run "
    "ON query_log (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_query_log_system_target "
    "ON query_log (system_name, target_name)",
    "CREATE INDEX IF NOT EXISTS ix_query_log_hash "
    "ON query_log (query_hash)",
    "CREATE INDEX IF NOT EXISTS ix_query_log_started "
    "ON query_log (started_at DESC)",

    """CREATE TABLE IF NOT EXISTS batch_log (
        id              BIGSERIAL PRIMARY KEY,
        run_id          TEXT,
        env             TEXT,
        batch_id        TEXT,
        target_name     TEXT NOT NULL,
        operation       TEXT NOT NULL,
        source_system   TEXT,
        rows_requested  BIGINT,
        rows_returned   BIGINT,
        rows_changed    BIGINT,
        rows_missing    BIGINT,
        started_at      TIMESTAMPTZ NOT NULL,
        ended_at        TIMESTAMPTZ,
        duration_ms     BIGINT,
        status          TEXT NOT NULL,
        error           TEXT,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS ix_batch_log_run "
    "ON batch_log (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_batch_log_target_op "
    "ON batch_log (target_name, operation)",
    "CREATE INDEX IF NOT EXISTS ix_batch_log_started "
    "ON batch_log (started_at DESC)",
]

INSERT_CONNECTION = """
INSERT INTO connection_log
    (run_id, env, system_name, target_name, host,
     started_at, ended_at, duration_ms, status, error)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_QUERY = """
INSERT INTO query_log
    (run_id, env, batch_id, system_name, target_name, operation,
     query_name, query_hash, query_text,
     started_at, ended_at, duration_ms,
     rows_returned, rows_affected, status, error)
VALUES (%s, %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s)
"""

INSERT_BATCH = """
INSERT INTO batch_log
    (run_id, env, batch_id, target_name, operation, source_system,
     rows_requested, rows_returned, rows_changed, rows_missing,
     started_at, ended_at, duration_ms, status, error)
VALUES (%s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s)
"""

OPERATIONS = ("compare", "fetch_oracle", "fetch_es", "diff",
              "apply_changes", "apply_missing")


# ---------------------------------------------------------------------------
# Cached parent-process PG connection (sticky failure)
# ---------------------------------------------------------------------------

_cache = CachedConnection("observability")
_disabled = False  # workers set this to skip ALL inserts (queue routes events
                   # to parent which writes for them).


def disable() -> None:
    """Mark this process as observability-disabled (used by worker procs).
    Workers route events through the multiprocessing log queue; only the
    parent's listener thread should hit PG."""
    global _disabled
    _disabled = True


def _get_conn():
    """Lazily open + cache a parent-process PG conn. None if unreachable
    or if disable() was called in this process."""
    if _disabled:
        return None
    return _cache.get()


def reset_state() -> None:
    """Force a fresh PG connect on the next call."""
    _cache.reset()


def close() -> None:
    _cache.close()


# ---------------------------------------------------------------------------
# Schema init
# ---------------------------------------------------------------------------

def init_schema() -> bool:
    """Ensure connection_log / query_log / batch_log exist. Idempotent."""
    if _disabled or _legacy_pg_writes_off():
        return False
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with _cache.lock, conn.cursor() as cur:
            for stmt in DDL:
                cur.execute(stmt)
        conn.commit()
        print("[observability] schema ensured "
              "(connection_log, query_log, batch_log)", flush=True)
        return True
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        print(f"[observability] schema init failed: {type(e).__name__}: {e}",
              flush=True)
        return False


# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------

def _exec(sql: str, params: tuple) -> None:
    """Run one INSERT under the parent PG conn lock. Best-effort."""
    if _disabled or _legacy_pg_writes_off():
        return
    conn = _get_conn()
    if conn is None:
        return
    try:
        with _cache.lock, conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception as e:
        # Roll back so the connection stays usable for the next insert.
        try: conn.rollback()
        except Exception: pass
        # Quiet — observability must NEVER break the pipeline.
        print(f"[observability] insert failed (silenced): "
              f"{type(e).__name__}: {e}", flush=True)


def log_connection(*, run_id: Optional[str], env: Optional[str],
                   system_name: str, target_name: Optional[str],
                   host: Optional[str], started_at: datetime,
                   ended_at: Optional[datetime] = None,
                   duration_ms: Optional[int] = None,
                   status: str = "ok",
                   error: Optional[str] = None) -> None:
    _exec(INSERT_CONNECTION, (
        run_id, env, system_name, target_name, host,
        started_at, ended_at, duration_ms, status,
        error[:4000] if error else None,
    ))


def log_query(*, run_id: Optional[str], env: Optional[str],
              batch_id: Optional[str], system_name: str,
              target_name: Optional[str] = None,
              operation: Optional[str] = None,
              query_name: Optional[str] = None,
              query_hash: Optional[str] = None,
              query_text: Optional[str] = None,
              started_at: datetime,
              ended_at: Optional[datetime] = None,
              duration_ms: Optional[int] = None,
              rows_returned: Optional[int] = None,
              rows_affected: Optional[int] = None,
              status: str = "ok",
              error: Optional[str] = None) -> None:
    _exec(INSERT_QUERY, (
        run_id, env, batch_id, system_name, target_name, operation,
        query_name, query_hash,
        (query_text[:8000] if query_text else None),
        started_at, ended_at, duration_ms,
        rows_returned, rows_affected, status,
        error[:4000] if error else None,
    ))


def log_batch(*, run_id: Optional[str], env: Optional[str],
              batch_id: Optional[str], target_name: str, operation: str,
              source_system: Optional[str] = None,
              rows_requested: Optional[int] = None,
              rows_returned: Optional[int] = None,
              rows_changed: Optional[int] = None,
              rows_missing: Optional[int] = None,
              started_at: datetime,
              ended_at: Optional[datetime] = None,
              duration_ms: Optional[int] = None,
              status: str = "ok",
              error: Optional[str] = None) -> None:
    _exec(INSERT_BATCH, (
        run_id, env, batch_id, target_name, operation, source_system,
        rows_requested, rows_returned, rows_changed, rows_missing,
        started_at, ended_at, duration_ms, status,
        error[:4000] if error else None,
    ))


# ---------------------------------------------------------------------------
# Adapters from existing logger row shape -> insert call
# ---------------------------------------------------------------------------

def _parse_iso(s: Any) -> Optional[datetime]:
    if s is None:
        return None
    if isinstance(s, datetime):
        return s
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _ms_from_seconds(sec: Any) -> Optional[int]:
    if sec is None:
        return None
    try:
        return int(round(float(sec) * 1000))
    except (TypeError, ValueError):
        return None


def from_connection_row(row: dict, system_name: str,
                        env: Optional[str] = None) -> None:
    """Mirror a row written by RunLoggerBase.connection() into connection_log.
    The CSV row has 'ts' (start) — we treat ts as both started_at and ended_at
    (connection emit happens after the connection succeeded; duration_ms = 0)."""
    ts = _parse_iso(row.get("ts")) or datetime.now(timezone.utc)
    target = (row.get("oracle_service") or row.get("pg_db")
              or row.get("es_url") or row.get("target_name"))
    host = (row.get("oracle_host") or row.get("pg_host")
            or row.get("hostname") or row.get("host"))
    log_connection(
        run_id=row.get("run_id"), env=env, system_name=system_name,
        target_name=str(target) if target else None,
        host=str(host) if host else None,
        started_at=ts, ended_at=ts, duration_ms=0,
        status="ok", error=None,
    )


def from_query_row(row: dict, system_name: str,
                   env: Optional[str] = None) -> None:
    """Mirror a row written by QueryRecord into query_log.

    Phase D loop 3: env + operation are now carried by QueryRecord and land
    in the row dict directly. The `env=` arg is kept as a fallback for
    callers that don't yet thread env into the logger.
    """
    started = _parse_iso(row.get("start_ts")) or datetime.now(timezone.utc)
    ended = _parse_iso(row.get("end_ts"))
    duration = _ms_from_seconds(row.get("seconds"))
    status = row.get("status") or "ok"
    log_query(
        run_id=row.get("run_id"),
        env=row.get("env") or env,
        batch_id=str(row.get("batch")) if row.get("batch") is not None else None,
        system_name=system_name,
        target_name=row.get("table"),
        operation=row.get("operation"),
        query_name=row.get("owner"),
        query_hash=row.get("sql_hash"),
        query_text=row.get("sql"),
        started_at=started, ended_at=ended, duration_ms=duration,
        rows_returned=int(row["rows"]) if row.get("rows") not in (None, "") else None,
        rows_affected=None,
        status=str(status),
        error=row.get("error"),
    )
