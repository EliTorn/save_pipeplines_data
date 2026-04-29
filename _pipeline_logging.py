"""Shared run-logging primitives.

Per-connector `logging_setup.py` modules subclass `RunLoggerBase` and bind their
own CSV paths + CONN_FIELDS via class attributes. Everything else (event/query
records, queue logger for multiprocessing, listener thread) is identical across
connectors and lives here.
"""
from __future__ import annotations

import csv
import hashlib
import json
import threading
import time as _time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from _pipeline_env import env_truthy

_VERBOSE = env_truthy("PIPELINE_VERBOSE")

TIME_FIELDS = [
    "ts", "ts_local", "tz",
    "date", "hour", "minute", "weekday", "day_name",
    "week", "month", "year",
]

EVENT_FIELDS = [
    "run_id", "query_id", *TIME_FIELDS, "level", "thread", "event",
    "owner", "table", "batch", "offset", "limit",
    "rows", "seconds", "rows_per_sec",
    "sql", "sql_hash", "error", "path",
    "count", "total_batches", "total_rows", "batch_size", "workers",
    "completed", "total",
    "approx_rows", "exact_rows", "total_to_fetch", "user_limit",
    "total_seconds",
]

QUERY_FIELDS = [
    "query_id", "run_id", "env", "operation", "sql_hash",
    "start_ts", "start_ts_local", "tz",
    "start_date", "start_hour", "start_minute", "start_weekday",
    "start_day_name", "start_week", "start_month", "start_year",
    "end_ts", "end_ts_local",
    "end_date", "end_hour", "end_minute", "end_weekday",
    "end_day_name", "end_week", "end_month", "end_year",
    "seconds", "rows", "rows_per_sec",
    "status", "error",
    "owner", "table", "batch", "thread",
    "params", "sql",
]

_lock = threading.Lock()


def _pg_mirror_connection(row: dict, system_name: str) -> None:
    """Best-effort PG mirror. Never raises."""
    try:
        from connect_into_postgres import observability
        observability.from_connection_row(row, system_name)
    except Exception:
        pass


def _pg_mirror_query(row: dict, system_name: str) -> None:
    try:
        from connect_into_postgres import observability
        observability.from_query_row(row, system_name)
    except Exception:
        pass


def _append_row(path: Path, fields: list[str], row: dict) -> None:
    with _lock:
        new_file = not path.exists()
        with open(path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            if new_file:
                w.writeheader()
            w.writerow(row)


def _time_breakdown(now_utc: datetime, now_local: datetime, prefix: str = "") -> dict:
    p = prefix + "_" if prefix else ""
    return {
        f"{p}ts": now_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        f"{p}ts_local": now_local.isoformat(timespec="milliseconds"),
        f"{p}date": now_local.strftime("%Y-%m-%d"),
        f"{p}hour": now_local.hour,
        f"{p}minute": now_local.minute,
        f"{p}weekday": now_local.weekday(),
        f"{p}day_name": now_local.strftime("%A"),
        f"{p}week": now_local.isocalendar().week,
        f"{p}month": now_local.month,
        f"{p}year": now_local.year,
    }


def _time_fields() -> dict:
    now_utc = datetime.now(timezone.utc)
    now_local = datetime.now().astimezone()
    tz = now_local.tzname() or _time.strftime("%Z")
    return {**_time_breakdown(now_utc, now_local), "tz": tz}


def _sql_hash(sql: str) -> str:
    norm = " ".join(sql.split()).upper()
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()[:12]


class QueryRecord:
    def __init__(self, logger: "RunLoggerBase", sql: str, owner=None, table=None,
                 batch=None, params=None, env: str | None = None,
                 operation: str | None = None):
        self.logger = logger
        self.sql = sql
        self.owner = owner
        self.table = table
        self.batch = batch
        self.params = params
        self.env = env
        self.operation = operation
        self.rows: int | None = None
        self.query_id = uuid.uuid4().hex[:12]
        self.sql_hash = _sql_hash(sql)
        self._t0: float | None = None
        self._start_utc: datetime | None = None
        self._start_local: datetime | None = None
        self._tz: str | None = None

    def __enter__(self) -> "QueryRecord":
        self._t0 = _time.perf_counter()
        self._start_utc = datetime.now(timezone.utc)
        self._start_local = datetime.now().astimezone()
        self._tz = self._start_local.tzname() or _time.strftime("%Z")
        return self

    def set_rows(self, rows: int) -> None:
        self.rows = rows

    def _build_row(self, exc) -> dict:
        dt = _time.perf_counter() - self._t0
        end_utc = datetime.now(timezone.utc)
        end_local = datetime.now().astimezone()
        return {
            "query_id": self.query_id,
            "run_id": self.logger.run_id,
            "env": self.env,
            "operation": self.operation,
            "sql_hash": self.sql_hash,
            "tz": self._tz,
            **_time_breakdown(self._start_utc, self._start_local, "start"),
            **_time_breakdown(end_utc, end_local, "end"),
            "seconds": round(dt, 3),
            "rows": self.rows,
            "rows_per_sec": round(self.rows / dt, 1) if (self.rows and dt > 0) else None,
            "status": "error" if exc else "ok",
            "error": str(exc) if exc else None,
            "owner": self.owner,
            "table": self.table,
            "batch": self.batch,
            "thread": threading.current_thread().name,
            "params": json.dumps(self.params, default=str) if self.params else None,
            "sql": self.sql,
        }

    def __exit__(self, exc_type, exc, tb) -> bool:
        row = self._build_row(exc)
        _append_row(self.logger.QUERIES_CSV, QUERY_FIELDS, row)
        _pg_mirror_query(row, getattr(self.logger, "SYSTEM_NAME", "unknown"))
        return False


class QueueQueryRecord(QueryRecord):
    """QueryRecord variant that ships finished rows to a multiprocessing queue."""

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.logger.queue.put(("query_row", self._build_row(exc)))
        return False


class RunLoggerBase:
    """Subclass and override: CONN_CSV, EVENTS_CSV, QUERIES_CSV, CONN_FIELDS, PREFIX."""

    CONN_CSV: Path
    EVENTS_CSV: Path
    QUERIES_CSV: Path
    CONN_FIELDS: list[str]
    PREFIX: str = ""
    CONN_PRINT_VERBOSE_ONLY: bool = False  # postgres overrides to True
    SYSTEM_NAME: str = "unknown"  # subclass override: oracle / elasticsearch / postgres

    def __init__(self, run_id: str):
        self.run_id = run_id

    def connection(self, **fields) -> None:
        row = {"run_id": self.run_id, **_time_fields(), **fields}
        _append_row(self.CONN_CSV, self.CONN_FIELDS, row)
        _pg_mirror_connection(row, self.SYSTEM_NAME)
        if not self.CONN_PRINT_VERBOSE_ONLY or _VERBOSE:
            print(f"[{self.PREFIX}{self.run_id}] connection -> {self.CONN_CSV.name}")

    def event(self, event: str, level: str = "INFO", query_id: str | None = None,
              **fields) -> None:
        sql = fields.get("sql")
        if sql and "sql_hash" not in fields:
            fields["sql_hash"] = _sql_hash(sql)
        row = {
            "run_id": self.run_id,
            "query_id": query_id,
            **_time_fields(),
            "level": level,
            "thread": threading.current_thread().name,
            "event": event,
            **fields,
        }
        _append_row(self.EVENTS_CSV, EVENT_FIELDS, row)
        if _VERBOSE or level == "ERROR":
            short = " ".join(f"{k}={v}" for k, v in fields.items()
                             if k in ("owner", "table", "batch", "rows", "seconds", "error"))
            print(f"[{self.PREFIX}{self.run_id}] {event} {short}".rstrip())

    def query(self, sql: str, owner=None, table=None, batch=None, params=None,
              env: str | None = None, operation: str | None = None) -> QueryRecord:
        return QueryRecord(self, sql, owner=owner, table=table, batch=batch,
                           params=params, env=env, operation=operation)


class QueueLogger:
    """Drop-in for RunLoggerBase usable in worker processes; pushes events to a
    multiprocessing Queue. Parent must run start_log_listener()."""

    def __init__(self, queue, run_id: str):
        self.queue = queue
        self.run_id = run_id

    def event(self, event: str, level: str = "INFO", query_id: str | None = None,
              **fields) -> None:
        self.queue.put(("event", {
            "event": event, "level": level, "query_id": query_id, **fields,
        }))

    def query(self, sql: str, owner=None, table=None, batch=None, params=None,
              env: str | None = None, operation: str | None = None) -> QueueQueryRecord:
        return QueueQueryRecord(self, sql, owner=owner, table=table, batch=batch,
                                params=params, env=env, operation=operation)


def start_log_listener(queue, parent_logger: RunLoggerBase) -> threading.Thread:
    """Spawn a daemon thread in the parent that drains queue and writes via
    `parent_logger`. Send `None` to stop. Returns the thread."""
    system = getattr(parent_logger, "SYSTEM_NAME", "unknown")

    def _run():
        while True:
            msg = queue.get()
            if msg is None:
                return
            kind, payload = msg
            if kind == "event":
                parent_logger.event(**payload)
            elif kind == "query_row":
                _append_row(parent_logger.QUERIES_CSV, QUERY_FIELDS, payload)
                _pg_mirror_query(payload, system)
    t = threading.Thread(target=_run, name="log-listener", daemon=True)
    t.start()
    return t
