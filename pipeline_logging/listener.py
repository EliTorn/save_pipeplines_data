"""Parent-process listener thread.

Drains the multiprocessing log queue and routes each message to the
parquet shard writer + PG summary writer. Runs in the parent process
only; workers push, parent writes.

Message format: ``(kind, payload)`` where kind is one of:

    "v2_row"           payload = {"table": <table_name>, "row": dict}
    "v2_target_start"  payload = {"target_key": str, "fields": dict}
    "v2_target_end"    payload = {"target_key": str, "fields": dict}

    # legacy (existing _pipeline_logging.start_log_listener kinds, kept
    # for back-compat — main listener forwards them to the legacy logger)
    "event"            payload = {... event row fields ...}
    "query_row"        payload = {... query row fields ...}

Send ``None`` to stop.
"""
from __future__ import annotations

import threading
from typing import Optional

from pipeline_logging import pg_summary
from pipeline_logging.parquet_sink import RunSink


class V2Listener:
    """Owns the RunSink + the target_id map keyed by `target_key`. The
    target_key is whatever string the producer chose (e.g.
    ``"<run_id>:<target>:<operation>"``); listener uses it to correlate
    target_start (which returns a PG id) with target_end (which UPDATEs
    that id)."""

    def __init__(self, sink: RunSink):
        self.sink = sink
        self._target_ids: dict[str, int] = {}
        self._lock = threading.Lock()

    def handle(self, kind: str, payload: dict) -> None:
        if kind == "v2_row":
            table = payload.get("table")
            row = payload.get("row") or {}
            if table:
                self.sink.append(table, row)
            return
        if kind == "v2_target_start":
            self._on_target_start(payload)
            return
        if kind == "v2_target_end":
            self._on_target_end(payload)
            return

    def _on_target_start(self, payload: dict) -> None:
        key = payload.get("target_key")
        fields = payload.get("fields") or {}
        target_id = pg_summary.insert_target(**fields)
        if key and target_id is not None:
            with self._lock:
                self._target_ids[key] = target_id

    def _on_target_end(self, payload: dict) -> None:
        key = payload.get("target_key")
        fields = payload.get("fields") or {}
        with self._lock:
            target_id = self._target_ids.pop(key, None)
        if target_id is None:
            return
        pg_summary.update_target(target_id=target_id, **fields)

    def close(self) -> None:
        self.sink.close()


def start_listener(queue, sink: RunSink,
                   legacy_logger=None) -> tuple[threading.Thread, V2Listener]:
    """Spawn the parent listener thread. Returns (thread, listener_obj).
    Thread stops when queue receives ``None``."""
    v2 = V2Listener(sink)

    def _run() -> None:
        from _pipeline_logging import _append_row, QUERY_FIELDS  # legacy
        try:
            while True:
                msg = queue.get()
                if msg is None:
                    return
                try:
                    kind, payload = msg
                except Exception:
                    continue
                # v2 routes
                if kind in ("v2_row", "v2_target_start", "v2_target_end"):
                    try:
                        v2.handle(kind, payload)
                    except Exception as e:
                        print(f"[listener:v2] handler failed: "
                              f"{type(e).__name__}: {e}", flush=True)
                    continue
                # legacy fallthrough — preserve existing CSV+PG mirror behavior
                if legacy_logger is None:
                    continue
                try:
                    if kind == "event":
                        legacy_logger.event(**payload)
                    elif kind == "query_row":
                        _append_row(legacy_logger.QUERIES_CSV, QUERY_FIELDS, payload)
                        try:
                            from connect_into_postgres import observability
                            observability.from_query_row(
                                payload,
                                getattr(legacy_logger, "SYSTEM_NAME", "unknown"),
                            )
                        except Exception:
                            pass
                except Exception as e:
                    print(f"[listener:legacy] handler failed: "
                          f"{type(e).__name__}: {e}", flush=True)
        finally:
            v2.close()

    t = threading.Thread(target=_run, name="v2-log-listener", daemon=True)
    t.start()
    return t, v2
