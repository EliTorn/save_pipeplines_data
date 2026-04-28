"""Postgres-connector run logging — thin shim around _pipeline_logging.

Differs from oracle/es shims only in CONN_FIELDS layout, log prefix, and that
connection prints are gated on PIPELINE_VERBOSE.
"""
from __future__ import annotations

import sys
import threading
from datetime import datetime
from pathlib import Path

_PARENT = Path(__file__).resolve().parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from _pipeline_logging import (  # noqa: E402
    EVENT_FIELDS, QUERY_FIELDS, TIME_FIELDS,
    QueryRecord, QueueLogger, QueueQueryRecord, RunLoggerBase,
    _append_row, _sql_hash, _time_breakdown, _time_fields,
    start_log_listener,
)

LOG_DIR = Path(__file__).resolve().parent / "logging"
LOG_DIR.mkdir(exist_ok=True)
CONN_CSV = LOG_DIR / "connections.csv"
EVENTS_CSV = LOG_DIR / "events.csv"
QUERIES_CSV = LOG_DIR / "queries.csv"

CONN_FIELDS = [
    "run_id", *TIME_FIELDS,
    "hostname", "fqdn", "os_user", "local_ip",
    "public_ip", "country", "region", "city", "org", "geo_source",
    "platform", "python", "pid", "cwd",
    "pg_host", "pg_port", "pg_db", "pg_user", "pg_schema", "pg_sslmode",
    "query_timeout_ms", "connect_timeout",
]


class RunLogger(RunLoggerBase):
    CONN_CSV = CONN_CSV
    EVENTS_CSV = EVENTS_CSV
    QUERIES_CSV = QUERIES_CSV
    CONN_FIELDS = CONN_FIELDS
    PREFIX = "pg "
    CONN_PRINT_VERBOSE_ONLY = True


def get_run_logger(run_id: str | None = None) -> RunLogger:
    run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    return RunLogger(run_id)


# ---------------------------------------------------------------------------
# Module-shared logger so any caller of connect_to_postgres gets one logger
# per process (rather than a fresh run_id per query).
# ---------------------------------------------------------------------------

_default_lock = threading.Lock()
_default_logger: RunLogger | None = None


def default_logger() -> RunLogger:
    global _default_logger
    with _default_lock:
        if _default_logger is None:
            _default_logger = get_run_logger()
        return _default_logger
