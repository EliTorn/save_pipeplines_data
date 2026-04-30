"""Oracle → Elasticsearch comparison pipeline entrypoint.

Per-event MODE dispatch (set in events.yaml / per-index config.yaml):
    time         - chunk by [START_TIME, END_TIME) in BANCH_VALUE-sized windows
    id_range     - chunk by PK; SQL has '-- @range' and '-- @batch' sections
    time_union   - run multiple `parts:` SQL files, concat shaped output, compare once

Special-purpose flag:
    --summary-only   read-only PostgreSQL report of pipeline_apply_batches +
                     pipeline_run_summary. Does NOT run any batches, does
                     NOT initialize anything else, opens exactly ONE PG
                     connection. See connect_into_postgres/summary_report.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT))


# ---------------------------------------------------------------------------
# Short-circuit BEFORE any heavy import (Oracle / ES / DuckDB / observability).
# Summary-only mode is purely a PG read; running heavy imports would open
# Oracle/ES connections and trigger observability writes — exactly what the
# user is trying to avoid when slot pressure is already high.
# ---------------------------------------------------------------------------
if "--summary-only" in sys.argv:
    from connect_into_postgres.summary_report import print_report
    sys.exit(print_report())


import uuid

sys.path.insert(0, str(_ROOT / "connect_into_orcal"))
sys.path.insert(0, str(_ROOT / "connect_into_es"))
sys.path.insert(0, str(_ROOT / "settings"))

from _pipeline_env import env_truthy
from connect_into_es.connect_to_es import (
    ES_URL, ES_USER, ES_VERIFY, PAGE_SIZE as ES_PAGE_SIZE,
    TIMEOUT_CONNECT as ES_TIMEOUT_CONNECT, TIMEOUT_READ as ES_TIMEOUT_READ,
)
from connect_into_orcal.connect_to_orcal import (
    ARRAYSIZE, DB_HOST, PORT, QUERY_TIMEOUT_MS, SERVICE_NAME, USERNAME, WORKERS,
)
from connect_into_orcal.geo_info import host_info
from connect_into_orcal.logging_setup import (
    CONN_CSV, EVENTS_CSV, QUERIES_CSV, get_run_logger,
)
from connect_into_postgres import run_summary
from connect_into_postgres import observability
from apply_changes import pg_tracking
from core.config import PIPELINE_WORKERS
from core.duckdb_catalog import init_catalog as init_duckdb_catalog
from core.runner import run_pipeline
import pipeline_logging


def main() -> None:
    run_id = uuid.uuid4().hex[:12]
    logger = get_run_logger(run_id)
    logger.connection(
        oracle_host=DB_HOST, oracle_port=PORT, oracle_service=SERVICE_NAME,
        oracle_user=USERNAME, batch_size=ARRAYSIZE, workers=WORKERS,
        query_timeout_ms=QUERY_TIMEOUT_MS,
        es_url=ES_URL, es_user=ES_USER, es_verify=ES_VERIFY,
        page_size=ES_PAGE_SIZE,
        timeout_connect=ES_TIMEOUT_CONNECT, timeout_read=ES_TIMEOUT_READ,
        **host_info(),
    )
    print(f"Run {run_id} | conn -> {CONN_CSV.name} | events -> {EVENTS_CSV.name} | queries -> {QUERIES_CSV.name}")
    print(f"Pipeline workers: {PIPELINE_WORKERS}")

    v2_logger = None
    if pipeline_logging.is_enabled():
        try:
            pipeline_logging.pg_summary.init_schema()
            v2_logger = pipeline_logging.get_run_logger(
                env="unknown", trigger="manual", run_id=run_id,
                args={"argv": sys.argv[1:]},
            )
            v2_logger.connection(
                system="oracle", target=SERVICE_NAME, host=DB_HOST,
                port=PORT, user=USERNAME,
            )
            v2_logger.connection(
                system="elasticsearch", target=ES_URL, host=ES_URL,
                user=ES_USER,
            )
            print(f"[v2] logging enabled -> {v2_logger.run_dir}", flush=True)
            logger._v2 = v2_logger  # piggyback on legacy logger object
        except Exception as e:
            print(f"[v2] init failed (continuing with legacy only): "
                  f"{type(e).__name__}: {e}", flush=True)
            v2_logger = None

    # Loop 6: legacy heavy tables are gone. We only ensure the active small
    # tables (run summary, observability logs, apply-batch tracking).
    if env_truthy("PIPELINE_PG_RESET"):
        print("[pg] PIPELINE_PG_RESET=1 has no effect after loop 6 "
              "(no legacy tables to truncate)", flush=True)
    for label, fn in (
        ("run-summary",   run_summary.init_schema),
        ("observability", observability.init_schema),
        ("pg-tracking",   pg_tracking.init_schema),
    ):
        try:
            fn()
        except Exception as e:
            print(f"[{label}] init_schema raised (continuing): "
                  f"{type(e).__name__}: {e}", flush=True)
    try:
        init_duckdb_catalog()
    except Exception as e:
        print(f"[duckdb] init_catalog raised (continuing): {type(e).__name__}: {e}",
              flush=True)

    fatal_err: Exception | None = None
    try:
        run_pipeline(run_id, logger)
    except Exception as e:
        fatal_err = e
        raise
    finally:
        if v2_logger is not None:
            if fatal_err is not None:
                try:
                    v2_logger._record_error(
                        where="main.run_pipeline", error=fatal_err,
                    )
                except Exception:
                    pass
            try:
                v2_logger.close(
                    final_error=f"{type(fatal_err).__name__}: {fatal_err}"
                    if fatal_err else None,
                )
            except Exception as e:
                print(f"[v2] close failed (silenced): "
                      f"{type(e).__name__}: {e}", flush=True)


if __name__ == "__main__":
    main()
