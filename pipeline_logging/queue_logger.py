"""Worker-side logger.

Mirrors the parent-side ``RunLogger`` API but pushes every row to the
multiprocessing queue instead of writing directly. The parent listener
thread (see ``listener.py``) drains the queue and writes parquet shards
+ PG rows.

Workers never touch parquet or PG directly.
"""
from __future__ import annotations

import json
import os
import threading
import time as _time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ms_between(t0: float, t1: float) -> int:
    return int(round((t1 - t0) * 1000))


class _WorkerQueryCtx:
    def __init__(self, qlogger: "QueueLogger", system: str, sql: str,
                 *, target: Optional[str] = None,
                 operation: Optional[str] = None,
                 batch_id: Optional[str] = None,
                 params: Any = None, owner: Optional[str] = None):
        self.qlogger = qlogger
        self.system = system
        self.sql = sql
        self.target = target
        self.operation = operation
        self.batch_id = batch_id
        self.params = params
        self.owner = owner
        self.query_id = "q_" + uuid.uuid4().hex[:10]
        self.rows: Optional[int] = None
        self._t0: Optional[float] = None
        self._started: Optional[datetime] = None

    def set_rows(self, n: int) -> None:
        self.rows = int(n)

    def __enter__(self):
        self._t0 = _time.perf_counter()
        self._started = _now_utc()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        ended = _now_utc()
        duration_ms = _ms_between(self._t0, _time.perf_counter()) if self._t0 else None
        rps = None
        if self.rows and duration_ms and duration_ms > 0:
            rps = round(self.rows * 1000.0 / duration_ms, 2)
        from _pipeline_logging import _sql_hash
        row = {
            "query_id":     self.query_id,
            "run_id":       self.qlogger.run_id,
            "system":       self.system,
            "target":       self.target,
            "operation":    self.operation,
            "batch_id":     self.batch_id,
            "sql_hash":     _sql_hash(self.sql) if self.sql else None,
            "sql_text":     (self.sql or "")[:8000] if self.sql else None,
            "params":       json.dumps(self.params, default=str) if self.params else None,
            "started_at":   self._started,
            "ended_at":     ended,
            "duration_ms":  duration_ms,
            "rows":         self.rows,
            "rows_per_sec": rps,
            "status":       "error" if exc else "ok",
            "error":        f"{type(exc).__name__}: {exc}" if exc else None,
            "owner":        self.owner,
            "thread":       threading.current_thread().name,
            "pid":          os.getpid(),
        }
        self.qlogger._push("v2_row", {"table": "queries", "row": row})
        if exc is not None:
            err_row = {
                "run_id":      self.qlogger.run_id,
                "ts":          ended,
                "where":       f"query.{self.system}",
                "target":      self.target,
                "batch_id":    self.batch_id,
                "query_id":    self.query_id,
                "error_type":  type(exc).__name__,
                "error_msg":   str(exc)[:4000],
                "traceback":   None,
                "retried":     False,
                "retry_count": 0,
                "recovered":   False,
            }
            self.qlogger._push("v2_row", {"table": "errors", "row": err_row})
        return False


class QueueLogger:
    """Drop-in for RunLogger usable inside worker procs. Pushes everything
    to the multiprocessing queue; parent listener writes."""

    def __init__(self, queue, run_id: str):
        self.queue = queue
        self.run_id = run_id

    def _push(self, kind: str, payload: dict) -> None:
        try:
            self.queue.put((kind, payload))
        except Exception:
            # Worker shouldn't crash because the queue is closed mid-shutdown.
            pass

    def event(self, name: str, *, level: str = "INFO",
              target: Optional[str] = None, batch_id: Optional[str] = None,
              query_id: Optional[str] = None,
              message: Optional[str] = None,
              **fields) -> None:
        row = {
            "run_id":   self.run_id,
            "ts":       _now_utc(),
            "level":    level,
            "event":    name,
            "target":   target,
            "batch_id": batch_id,
            "query_id": query_id,
            "message":  message,
            "fields":   json.dumps(fields, default=str) if fields else None,
            "thread":   threading.current_thread().name,
            "pid":      os.getpid(),
        }
        self._push("v2_row", {"table": "events", "row": row})

    def query(self, sql: str, *, system: str,
              target: Optional[str] = None, operation: Optional[str] = None,
              batch_id: Optional[str] = None, params: Any = None,
              owner: Optional[str] = None) -> _WorkerQueryCtx:
        return _WorkerQueryCtx(self, system, sql, target=target,
                               operation=operation, batch_id=batch_id,
                               params=params, owner=owner)

    def batch(self, *, batch_id: str, target: str, operation: str,
              started_at: datetime, ended_at: datetime,
              window_from: Optional[datetime] = None,
              window_to: Optional[datetime] = None,
              id_from: Optional[int] = None, id_to: Optional[int] = None,
              rows_oracle: Optional[int] = None,
              rows_es: Optional[int] = None,
              rows_changed: Optional[int] = None,
              rows_missing: Optional[int] = None,
              oracle_query_id: Optional[str] = None,
              es_query_id: Optional[str] = None,
              status: str = "ok",
              error: Optional[str] = None) -> None:
        duration_ms = int((ended_at - started_at).total_seconds() * 1000)
        row = {
            "batch_id":        batch_id,
            "run_id":          self.run_id,
            "target":          target,
            "operation":       operation,
            "window_from":     window_from,
            "window_to":       window_to,
            "id_from":         id_from,
            "id_to":           id_to,
            "started_at":      started_at,
            "ended_at":        ended_at,
            "duration_ms":     duration_ms,
            "rows_oracle":     rows_oracle,
            "rows_es":         rows_es,
            "rows_changed":    rows_changed,
            "rows_missing":    rows_missing,
            "oracle_query_id": oracle_query_id,
            "es_query_id":     es_query_id,
            "status":          status,
            "error":           error,
            "worker_pid":      os.getpid(),
        }
        self._push("v2_row", {"table": "batches", "row": row})

    def connection(self, *, system: str, target: Optional[str] = None,
                   host: Optional[str] = None, port: Optional[int] = None,
                   user: Optional[str] = None,
                   started_at: Optional[datetime] = None,
                   ended_at: Optional[datetime] = None,
                   duration_ms: Optional[int] = None,
                   status: str = "ok",
                   error: Optional[str] = None) -> None:
        now = _now_utc()
        row = {
            "run_id":      self.run_id,
            "system":      system,
            "target":      target,
            "host":        host,
            "port":        int(port) if port is not None else None,
            "user":        user,
            "started_at":  started_at or now,
            "ended_at":    ended_at or now,
            "duration_ms": int(duration_ms or 0),
            "status":      status,
            "error":       error,
            "pid":         os.getpid(),
            "thread":      threading.current_thread().name,
        }
        self._push("v2_row", {"table": "connections", "row": row})
