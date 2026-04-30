"""Pipeline logging — v2 (parquet + DuckDB local, slim PG summary).

Off by default. Enable with ``PIPELINE_LOGGING_V2=1``.

Public API:

    from pipeline_logging import (
        is_enabled, get_run_logger, QueueLogger, start_listener,
        SCHEMA_VERSION,
    )

See SPEC.md for the full design.
"""
from __future__ import annotations

from _pipeline_env import env_truthy

from pipeline_logging.schemas import SCHEMA_VERSION
from pipeline_logging.run_logger import (
    RunLogger, get_run_logger, make_run_id,
)
from pipeline_logging.queue_logger import QueueLogger
from pipeline_logging.listener import start_listener, V2Listener
from pipeline_logging.parquet_sink import RunSink
from pipeline_logging import pg_summary


def is_enabled() -> bool:
    return env_truthy("PIPELINE_LOGGING_V2")


__all__ = [
    "SCHEMA_VERSION",
    "RunLogger",
    "get_run_logger",
    "make_run_id",
    "QueueLogger",
    "start_listener",
    "V2Listener",
    "RunSink",
    "pg_summary",
    "is_enabled",
]
