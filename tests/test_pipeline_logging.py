"""Smoke tests for pipeline_logging — Phase 1.

Covers:
- schemas import + every table has expected columns
- parquet_sink writes shards by row threshold and on close
- run_logger emits rows that round-trip through pyarrow
- queue_logger pushes correct (kind, payload) tuples
- listener routes v2 messages to RunSink
- meta.json atomic write at start + end

PG is not exercised here — pg_summary writes are no-ops when PG is
unreachable, which is the test environment.

Run with: python -m pytest tests/test_pipeline_logging.py -v
"""
from __future__ import annotations

import json
import multiprocessing as mp
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

# Make repo root importable when invoked directly.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pyarrow.parquet as pq
import pytest


def test_schemas_present():
    from pipeline_logging.schemas import SCHEMAS, TABLES, SCHEMA_VERSION
    assert SCHEMA_VERSION
    assert set(TABLES) == set(SCHEMAS.keys())
    for name, schema in SCHEMAS.items():
        assert "run_id" in [f.name for f in schema], f"{name} missing run_id"


def test_parquet_sink_flush_on_threshold(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("LOG_SHARD_ROWS", "3")
    monkeypatch.setenv("LOG_SHARD_SECONDS", "300")
    # Re-import sink so env takes effect
    import importlib
    import pipeline_logging.parquet_sink as ps
    importlib.reload(ps)

    sink = ps.RunSink(tmp_path / "run_x")
    now = datetime.now(timezone.utc)
    base_row = {
        "run_id": "run_x", "ts": now, "level": "INFO", "event": "ev",
        "target": None, "batch_id": None, "query_id": None,
        "message": None, "fields": None, "thread": "t1", "pid": os.getpid(),
    }
    for i in range(5):
        sink.append("events", {**base_row, "event": f"ev_{i}"})
    sink.close()

    shards = sorted((tmp_path / "run_x" / "events").glob("part_*.parquet"))
    assert len(shards) >= 1
    total = sum(pq.read_table(p).num_rows for p in shards)
    assert total == 5


def test_run_logger_meta_json(tmp_path: Path, monkeypatch):
    # PG unreachable in tests — pg_summary will silently no-op.
    monkeypatch.setenv("PG_HOST", "127.0.0.1")
    monkeypatch.setenv("PG_PORT", "1")  # guaranteed connect failure
    from pipeline_logging.run_logger import RunLogger

    logger = RunLogger(
        run_id="20260101_000000_test",
        env="dev", host="testhost", trigger="test",
        logs_root=tmp_path,
        args={"foo": "bar"},
    )

    meta_path = logger.run_dir / "meta.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta["run_id"] == "20260101_000000_test"
    assert meta["schema_version"]
    assert meta["ended_at"] is None  # initial write

    logger.event("started", target="foo", message="hello")
    logger.connection(system="oracle", target="testdb", host="x", port=1521)
    logger.close()

    meta = json.loads(meta_path.read_text())
    assert meta["ended_at"] is not None  # rewrite at close

    # parquet rows should exist for events + connections.
    ev_files = list((logger.run_dir / "events").glob("*.parquet"))
    cn_files = list((logger.run_dir / "connections").glob("*.parquet"))
    assert ev_files and cn_files


def test_queue_logger_push_format():
    from pipeline_logging.queue_logger import QueueLogger
    q = mp.get_context("spawn").Queue()
    qlog = QueueLogger(q, run_id="r1")
    qlog.event("hello", target="t1", level="INFO", custom_field=42)
    kind, payload = q.get(timeout=2)
    assert kind == "v2_row"
    assert payload["table"] == "events"
    assert payload["row"]["event"] == "hello"
    assert payload["row"]["target"] == "t1"
    fields = json.loads(payload["row"]["fields"])
    assert fields["custom_field"] == 42


def test_listener_routes_to_sink(tmp_path: Path):
    from pipeline_logging.parquet_sink import RunSink
    from pipeline_logging.listener import start_listener

    sink = RunSink(tmp_path / "run_y")
    q = mp.get_context("spawn").Queue()
    t, _v2 = start_listener(q, sink, legacy_logger=None)

    # push two event rows + sentinel
    now = datetime.now(timezone.utc)
    for i in range(2):
        q.put(("v2_row", {"table": "events", "row": {
            "run_id": "run_y", "ts": now, "level": "INFO",
            "event": f"e{i}", "target": None, "batch_id": None,
            "query_id": None, "message": None, "fields": None,
            "thread": "t", "pid": os.getpid(),
        }}))
    q.put(None)
    t.join(timeout=10)

    files = sorted((tmp_path / "run_y" / "events").glob("part_*.parquet"))
    assert files
    rows = sum(pq.read_table(p).num_rows for p in files)
    assert rows == 2


def test_run_creates_all_table_dirs(tmp_path: Path, monkeypatch):
    """Every parquet table dir must exist after RunSink init, even if empty."""
    monkeypatch.setenv("PG_HOST", "127.0.0.1")
    monkeypatch.setenv("PG_PORT", "1")
    from pipeline_logging.run_logger import RunLogger
    from pipeline_logging.schemas import TABLES

    logger = RunLogger(
        run_id="20260101_000000_dirs",
        env="dev", host="testhost", trigger="test",
        logs_root=tmp_path,
    )
    logger.close()

    for table in TABLES:
        assert (logger.run_dir / table).is_dir(), f"{table}/ not created"


def test_batch_emit(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PG_HOST", "127.0.0.1")
    monkeypatch.setenv("PG_PORT", "1")
    from pipeline_logging.run_logger import RunLogger

    logger = RunLogger(
        run_id="20260101_000000_btest",
        env="dev", host="testhost", trigger="test",
        logs_root=tmp_path,
    )
    now = datetime.now(timezone.utc)
    logger.batch(
        batch_id="idx_a#2026-01-01",
        target="idx_a", operation="compare",
        started_at=now, ended_at=now,
        rows_oracle=100, rows_es=99, rows_changed=1, rows_missing=0,
    )
    logger.close()

    files = sorted((logger.run_dir / "batches").glob("part_*.parquet"))
    assert files
    table = pq.read_table(files[0])
    assert table.num_rows == 1
    assert table.column("rows_oracle").to_pylist()[0] == 100


def test_legacy_event_mirror_to_v2(tmp_path: Path, monkeypatch):
    """RunLoggerBase.event() also writes to v2.events when _v2 is attached."""
    monkeypatch.setenv("PG_HOST", "127.0.0.1")
    monkeypatch.setenv("PG_PORT", "1")
    # Build a legacy logger pointing at a temp logging dir.
    import sys as _sys
    _sys.path.insert(0, str(_ROOT / "connect_into_orcal"))
    from pipeline_logging.run_logger import RunLogger

    legacy_log_dir = tmp_path / "legacy_logs"
    legacy_log_dir.mkdir()
    monkeypatch.setattr(
        "connect_into_orcal.logging_setup.LOG_DIR", legacy_log_dir,
    )
    monkeypatch.setattr(
        "connect_into_orcal.logging_setup.EVENTS_CSV",
        legacy_log_dir / "events.csv",
    )
    from connect_into_orcal.logging_setup import RunLogger as LegacyLogger

    class LegacyTest(LegacyLogger):
        EVENTS_CSV = legacy_log_dir / "events.csv"
        CONN_CSV = legacy_log_dir / "conns.csv"
        QUERIES_CSV = legacy_log_dir / "queries.csv"

    legacy = LegacyTest("test_run")
    v2 = RunLogger(
        run_id="20260101_000000_mirror",
        env="dev", host="testhost", trigger="test",
        logs_root=tmp_path,
    )
    legacy._v2 = v2

    legacy.event("fetch_plan", table="playerbonus", batch=3, approx_rows=12345)
    v2.close()

    files = sorted((v2.run_dir / "events").glob("part_*.parquet"))
    assert files, "events shard not written"
    table = pq.read_table(files[0])
    assert table.num_rows == 1
    assert table.column("event").to_pylist()[0] == "fetch_plan"
    assert table.column("target").to_pylist()[0] == "playerbonus"
    assert table.column("batch_id").to_pylist()[0] == "3"
    fields = json.loads(table.column("fields").to_pylist()[0])
    assert fields["approx_rows"] == 12345


def test_legacy_query_mirror_to_v2(tmp_path: Path, monkeypatch):
    """QueryRecord.__exit__ also emits a v2 queries row when _v2 attached."""
    monkeypatch.setenv("PG_HOST", "127.0.0.1")
    monkeypatch.setenv("PG_PORT", "1")
    from pipeline_logging.run_logger import RunLogger

    legacy_log_dir = tmp_path / "legacy_logs"
    legacy_log_dir.mkdir()
    from connect_into_orcal.logging_setup import RunLogger as LegacyLogger

    class LegacyTest(LegacyLogger):
        EVENTS_CSV = legacy_log_dir / "events.csv"
        CONN_CSV = legacy_log_dir / "conns.csv"
        QUERIES_CSV = legacy_log_dir / "queries.csv"

    legacy = LegacyTest("test_run_q")
    v2 = RunLogger(
        run_id="20260101_000000_qmirror",
        env="dev", host="testhost", trigger="test",
        logs_root=tmp_path,
    )
    legacy._v2 = v2

    with legacy.query("SELECT 1 FROM DUAL", table="DUAL", batch=0,
                      env="dev", operation="probe") as q:
        q.set_rows(1)
    v2.close()

    files = sorted((v2.run_dir / "queries").glob("part_*.parquet"))
    assert files, "queries shard not written"
    table = pq.read_table(files[0])
    assert table.num_rows == 1
    assert table.column("system").to_pylist()[0] == "oracle"
    assert table.column("target").to_pylist()[0] == "DUAL"
    assert table.column("rows").to_pylist()[0] == 1
    assert table.column("operation").to_pylist()[0] == "probe"


def test_phases_dir_created_empty(tmp_path: Path, monkeypatch):
    """phases/ dir must exist after RunSink init even with no callsites yet."""
    monkeypatch.setenv("PG_HOST", "127.0.0.1")
    monkeypatch.setenv("PG_PORT", "1")
    from pipeline_logging.run_logger import RunLogger

    logger = RunLogger(
        run_id="20260101_000000_phases",
        env="dev", host="testhost", trigger="test",
        logs_root=tmp_path,
    )
    logger.close()

    assert (logger.run_dir / "phases").is_dir()
    # No shards (no callsites yet).
    assert not list((logger.run_dir / "phases").glob("*.parquet"))


def test_meta_status_at_close(tmp_path: Path, monkeypatch):
    """meta.json must contain status=running initially, status=ok at close."""
    monkeypatch.setenv("PG_HOST", "127.0.0.1")
    monkeypatch.setenv("PG_PORT", "1")
    from pipeline_logging.run_logger import RunLogger

    logger = RunLogger(
        run_id="20260101_000000_status",
        env="dev", host="testhost", trigger="test",
        logs_root=tmp_path,
    )
    meta = json.loads((logger.run_dir / "meta.json").read_text())
    assert meta["status"] == "running"
    assert meta["ended_at"] is None

    logger.close()
    meta = json.loads((logger.run_dir / "meta.json").read_text())
    assert meta["status"] == "ok"
    assert meta["ended_at"] is not None


def test_query_ctx_emits_query_row(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PG_HOST", "127.0.0.1")
    monkeypatch.setenv("PG_PORT", "1")
    from pipeline_logging.run_logger import RunLogger

    logger = RunLogger(
        run_id="20260101_000000_qtest",
        env="dev", host="testhost", trigger="test",
        logs_root=tmp_path,
    )
    with logger.query("SELECT 1 FROM DUAL", system="oracle",
                      target="DUAL", operation="probe") as q:
        q.set_rows(1)
    logger.close()

    files = sorted((logger.run_dir / "queries").glob("part_*.parquet"))
    assert files
    table = pq.read_table(files[0])
    assert table.num_rows == 1
    assert table.column("system").to_pylist()[0] == "oracle"
    assert table.column("rows").to_pylist()[0] == 1
    assert table.column("status").to_pylist()[0] == "ok"
