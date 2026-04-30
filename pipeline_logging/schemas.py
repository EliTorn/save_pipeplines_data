"""Arrow schemas for per-run parquet tables. See SPEC.md section 4."""
from __future__ import annotations

import pyarrow as pa

SCHEMA_VERSION = "1.0"

# Table names map directly to subdirs under logs/runs/<run_id>/<table>/
# `phases` has no callsites yet (Phase 2) but the dir is pre-created so the
# folder layout is consistent from the first v2 run.
TABLES = ("connections", "queries", "batches", "events", "errors", "phases")

_TS = pa.timestamp("ms", tz="UTC")

CONNECTIONS = pa.schema([
    ("run_id",      pa.string()),
    ("system",      pa.string()),
    ("target",      pa.string()),
    ("host",        pa.string()),
    ("port",        pa.int32()),
    ("user",        pa.string()),
    ("started_at",  _TS),
    ("ended_at",    _TS),
    ("duration_ms", pa.int32()),
    ("status",      pa.string()),
    ("error",       pa.string()),
    ("pid",         pa.int32()),
    ("thread",      pa.string()),
])

QUERIES = pa.schema([
    ("query_id",     pa.string()),
    ("run_id",       pa.string()),
    ("system",       pa.string()),
    ("target",       pa.string()),
    ("operation",    pa.string()),
    ("batch_id",     pa.string()),
    ("sql_hash",     pa.string()),
    ("sql_text",     pa.string()),
    ("params",       pa.string()),
    ("started_at",   _TS),
    ("ended_at",     _TS),
    ("duration_ms",  pa.int32()),
    ("rows",         pa.int64()),
    ("rows_per_sec", pa.float64()),
    ("status",       pa.string()),
    ("error",        pa.string()),
    ("owner",        pa.string()),
    ("thread",       pa.string()),
    ("pid",          pa.int32()),
])

BATCHES = pa.schema([
    ("batch_id",        pa.string()),
    ("run_id",          pa.string()),
    ("target",          pa.string()),
    ("operation",       pa.string()),
    ("window_from",     _TS),
    ("window_to",       _TS),
    ("id_from",         pa.int64()),
    ("id_to",           pa.int64()),
    ("started_at",      _TS),
    ("ended_at",        _TS),
    ("duration_ms",     pa.int32()),
    ("rows_oracle",     pa.int64()),
    ("rows_es",         pa.int64()),
    ("rows_changed",    pa.int64()),
    ("rows_missing",    pa.int64()),
    ("oracle_query_id", pa.string()),
    ("es_query_id",     pa.string()),
    ("status",          pa.string()),
    ("error",           pa.string()),
    ("worker_pid",      pa.int32()),
])

EVENTS = pa.schema([
    ("run_id",   pa.string()),
    ("ts",       _TS),
    ("level",    pa.string()),
    ("event",    pa.string()),
    ("target",   pa.string()),
    ("batch_id", pa.string()),
    ("query_id", pa.string()),
    ("message",  pa.string()),
    ("fields",   pa.string()),
    ("thread",   pa.string()),
    ("pid",      pa.int32()),
])

ERRORS = pa.schema([
    ("run_id",      pa.string()),
    ("ts",          _TS),
    ("where",       pa.string()),
    ("target",      pa.string()),
    ("batch_id",    pa.string()),
    ("query_id",    pa.string()),
    ("error_type",  pa.string()),
    ("error_msg",   pa.string()),
    ("traceback",   pa.string()),
    ("retried",     pa.bool_()),
    ("retry_count", pa.int32()),
    ("recovered",   pa.bool_()),
])

PHASES = pa.schema([
    ("run_id",      pa.string()),
    ("target",      pa.string()),
    ("phase",       pa.string()),
    ("batch_id",    pa.string()),
    ("started_at",  _TS),
    ("ended_at",    _TS),
    ("duration_ms", pa.int32()),
    ("rows_in",     pa.int64()),
    ("rows_out",    pa.int64()),
    ("status",      pa.string()),
    ("error",       pa.string()),
    ("worker_pid",  pa.int32()),
])

SCHEMAS: dict[str, pa.Schema] = {
    "connections": CONNECTIONS,
    "queries":     QUERIES,
    "batches":     BATCHES,
    "events":      EVENTS,
    "errors":      ERRORS,
    "phases":      PHASES,
}


def schema_for(table: str) -> pa.Schema:
    return SCHEMAS[table]
