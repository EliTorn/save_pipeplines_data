"""Parent-process v2 RunLogger.

Owns the run lifecycle:
- meta.json atomic write (start + end)
- pipeline_run row insert/update in PG
- pipeline_run_target ctx mgr (.target() — INSERT at enter, UPDATE at exit)
- direct row append into the local RunSink (since this lives in the parent
  process, no queue hop needed for parent emits)

Workers use ``QueueLogger`` instead — same API surface, pushes to mp queue.
"""
from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time as _time
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pipeline_logging import pg_summary
from pipeline_logging.parquet_sink import RunSink
from pipeline_logging.schemas import SCHEMA_VERSION


_HOT_PATH_TS_MS_FACTOR = 1000  # for duration_ms computation


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ms_between(t0: float, t1: float) -> int:
    return int(round((t1 - t0) * 1000))


def _atomic_write_json(path: Path, data: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    try:
        # best-effort fsync — not critical
        with open(tmp, "rb") as f:
            os.fsync(f.fileno())
    except Exception:
        pass
    os.replace(tmp, path)


def _git_commit() -> Optional[str]:
    try:
        import subprocess
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


def _short_uuid() -> str:
    return uuid.uuid4().hex[:8]


def make_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + _short_uuid()


class _TargetCtx:
    """Context manager returned by ``RunLogger.target(...)``.

    On enter: INSERT pipeline_run_target (status='running'), grab id.
    On exit: UPDATE the row with totals (status, duration, row counts,
    error). Caller mutates totals via ``set_*`` methods.
    """

    def __init__(self, logger: "RunLogger", source_system: str,
                 target_system: str, target_name: str, operation: str):
        self.logger = logger
        self.source_system = source_system
        self.target_system = target_system
        self.target_name = target_name
        self.operation = operation
        self._target_id: Optional[int] = None
        self._t0: Optional[float] = None
        self._started: Optional[datetime] = None
        self._totals: dict[str, Optional[int]] = {
            "rows_source": None, "rows_target": None,
            "rows_inserted": None, "rows_updated": None,
            "rows_deleted": None, "rows_missing": None,
            "rows_unchanged": None,
        }

    # public mutators
    def set_rows(self, *, source: Optional[int] = None,
                 target: Optional[int] = None,
                 inserted: Optional[int] = None,
                 updated: Optional[int] = None,
                 deleted: Optional[int] = None,
                 missing: Optional[int] = None,
                 unchanged: Optional[int] = None) -> None:
        for k, v in (("rows_source", source), ("rows_target", target),
                     ("rows_inserted", inserted), ("rows_updated", updated),
                     ("rows_deleted", deleted), ("rows_missing", missing),
                     ("rows_unchanged", unchanged)):
            if v is not None:
                self._totals[k] = int(v)

    def __enter__(self) -> "_TargetCtx":
        self._t0 = _time.perf_counter()
        self._started = _now_utc()
        self._target_id = pg_summary.insert_target(
            run_id=self.logger.run_id,
            source_system=self.source_system,
            target_system=self.target_system,
            target_name=self.target_name,
            operation=self.operation,
            started_at=self._started,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        ended = _now_utc()
        duration_ms = _ms_between(self._t0, _time.perf_counter()) if self._t0 else None
        if exc is not None:
            status, err = "failed", f"{type(exc).__name__}: {exc}"
            self.logger._record_error(
                where=f"target.{self.target_name}.{self.operation}",
                target=self.target_name, error=exc,
            )
        else:
            status, err = "ok", None
        if self._target_id is not None:
            pg_summary.update_target(
                target_id=self._target_id,
                ended_at=ended,
                duration_ms=duration_ms,
                status=status,
                error=err,
                **self._totals,
            )
        # accumulate run-level rows_changed
        changed = sum(int(self._totals.get(k) or 0) for k in
                      ("rows_inserted", "rows_updated", "rows_deleted"))
        with self.logger._lock:
            self.logger._total_rows_changed += changed
            if status == "failed":
                self.logger._any_target_failed = True
        return False


class _QueryCtx:
    """Wraps one SQL/HTTP call. Emits a row into queries.parquet on exit."""

    def __init__(self, logger: "RunLogger", system: str, sql: str,
                 *, target: Optional[str] = None,
                 operation: Optional[str] = None,
                 batch_id: Optional[str] = None,
                 params: Any = None,
                 owner: Optional[str] = None):
        self.logger = logger
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

    def __enter__(self) -> "_QueryCtx":
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
            "run_id":       self.logger.run_id,
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
        self.logger._emit_row("queries", row)
        if exc is not None:
            self.logger._record_error(
                where=f"query.{self.system}", target=self.target,
                batch_id=self.batch_id, query_id=self.query_id, error=exc,
            )
        return False


class RunLogger:
    """Parent-process v2 logger. Direct writes — no queue hop needed."""

    def __init__(self, *, run_id: str, env: str, host: str, trigger: str,
                 logs_root: Path, args: Optional[dict] = None):
        self.run_id = run_id
        self.env = env
        self.host = host
        self.trigger = trigger
        self.logs_root = logs_root
        self.run_dir = logs_root / "runs" / run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.sink = RunSink(self.run_dir)
        self._args = args or {}
        self._t0 = _time.perf_counter()
        self._started = _now_utc()
        self._lock = threading.Lock()
        self._total_rows_changed = 0
        self._any_target_failed = False
        self._events_count = 0

        # write meta.json (initial)
        self._write_meta(initial=True)
        # PG run row (start)
        pg_summary.insert_run(
            run_id=run_id, env=env, host=host, trigger=trigger,
            schema_version=SCHEMA_VERSION, started_at=self._started,
        )

    # -- meta.json -------------------------------------------------------
    def _meta_dict(self, ended: Optional[datetime] = None) -> dict:
        return {
            "run_id":         self.run_id,
            "schema_version": SCHEMA_VERSION,
            "env":            self.env,
            "host":           self.host,
            "trigger":        self.trigger,
            "started_at":     self._started.isoformat(),
            "ended_at":       ended.isoformat() if ended else None,
            "status":         getattr(self, "_final_status", "running"),
            "final_error":    getattr(self, "_final_error", None),
            "args":           self._args,
            "git_commit":     _git_commit(),
            "python":         sys.version.split()[0],
            "pid":            os.getpid(),
        }

    def _write_meta(self, *, initial: bool = False,
                    ended: Optional[datetime] = None) -> None:
        try:
            _atomic_write_json(self.run_dir / "meta.json",
                               self._meta_dict(ended=ended))
        except Exception as e:
            print(f"[run_logger] meta.json write failed: "
                  f"{type(e).__name__}: {e}", flush=True)

    # -- emit primitives -------------------------------------------------
    def _emit_row(self, table: str, row: dict) -> None:
        self.sink.append(table, row)

    def _record_error(self, *, where: str, error: BaseException,
                      target: Optional[str] = None,
                      batch_id: Optional[str] = None,
                      query_id: Optional[str] = None,
                      retried: bool = False, retry_count: int = 0,
                      recovered: bool = False) -> None:
        try:
            tb = "".join(traceback.format_exception(
                type(error), error, error.__traceback__))[:8000]
        except Exception:
            tb = None
        row = {
            "run_id":      self.run_id,
            "ts":          _now_utc(),
            "where":       where,
            "target":      target,
            "batch_id":    batch_id,
            "query_id":    query_id,
            "error_type":  type(error).__name__,
            "error_msg":   str(error)[:4000],
            "traceback":   tb,
            "retried":     bool(retried),
            "retry_count": int(retry_count),
            "recovered":   bool(recovered),
        }
        self._emit_row("errors", row)

    # -- public API ------------------------------------------------------
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
        self._emit_row("connections", row)

    def event(self, name: str, *, level: str = "INFO",
              target: Optional[str] = None, batch_id: Optional[str] = None,
              query_id: Optional[str] = None,
              message: Optional[str] = None,
              **fields) -> None:
        with self._lock:
            self._events_count += 1
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
        self._emit_row("events", row)

    def query(self, sql: str, *, system: str,
              target: Optional[str] = None, operation: Optional[str] = None,
              batch_id: Optional[str] = None, params: Any = None,
              owner: Optional[str] = None) -> _QueryCtx:
        return _QueryCtx(self, system, sql, target=target, operation=operation,
                         batch_id=batch_id, params=params, owner=owner)

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
              error: Optional[str] = None,
              worker_pid: Optional[int] = None) -> None:
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
            "worker_pid":      worker_pid if worker_pid is not None else os.getpid(),
        }
        self._emit_row("batches", row)

    def target(self, target_name: str, operation: str, *,
               source_system: str = "oracle",
               target_system: str = "elasticsearch") -> _TargetCtx:
        return _TargetCtx(self, source_system, target_system,
                          target_name, operation)

    # -- lifecycle -------------------------------------------------------
    def close(self, *, status: Optional[str] = None,
              final_error: Optional[str] = None) -> None:
        ended = _now_utc()
        duration_ms = _ms_between(self._t0, _time.perf_counter())
        if status is None:
            if final_error:
                status = "failed"
            elif self._any_target_failed:
                status = "partial"
            else:
                status = "ok"
        self._final_status = status
        self._final_error = final_error
        pg_summary.update_run(
            run_id=self.run_id, ended_at=ended, duration_ms=duration_ms,
            status=status, events_count=self._events_count,
            total_rows_changed=self._total_rows_changed,
            final_error=final_error,
        )
        self._write_meta(ended=ended)
        self.sink.close()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_run_logger(*, env: str, trigger: str = "manual",
                   args: Optional[dict] = None,
                   logs_root: Optional[Path] = None,
                   run_id: Optional[str] = None,
                   host: Optional[str] = None) -> RunLogger:
    if logs_root is None:
        logs_root = Path(os.getenv("PIPELINE_LOGS_ROOT", "logs"))
    if run_id is None:
        run_id = make_run_id()
    if host is None:
        try:
            host = socket.gethostname()
        except Exception:
            host = "unknown"
    return RunLogger(run_id=run_id, env=env, host=host, trigger=trigger,
                     logs_root=logs_root, args=args)
