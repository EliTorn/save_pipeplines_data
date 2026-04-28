"""Oracle-connector run logging — thin shim around _pipeline_logging."""
from __future__ import annotations

import sys
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
    "oracle_host", "oracle_port", "oracle_service", "oracle_user",
    "batch_size", "workers", "query_timeout_ms",
]


class RunLogger(RunLoggerBase):
    CONN_CSV = CONN_CSV
    EVENTS_CSV = EVENTS_CSV
    QUERIES_CSV = QUERIES_CSV
    CONN_FIELDS = CONN_FIELDS


def get_run_logger(run_id: str | None = None) -> RunLogger:
    run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    return RunLogger(run_id)
