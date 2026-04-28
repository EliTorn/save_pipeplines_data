"""Oracle → Elasticsearch comparison pipeline entrypoint.

Per-event MODE dispatch (set in events.yaml / per-index config.yaml):
    time         - chunk by [START_TIME, END_TIME) in BANCH_VALUE-sized windows
    id_range     - chunk by PK; SQL has '-- @range' and '-- @batch' sections
    time_union   - run multiple `parts:` SQL files, concat shaped output, compare once
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT))
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
from core.config import PIPELINE_WORKERS
from core.runner import run_pipeline


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

    run_pipeline(run_id, logger)

    if not env_truthy("PIPELINE_SKIP_PG_SYNC"):
        try:
            from connect_into_postgres.sync_out import run_sync
            print("[pg-sync] mirroring out/ -> Postgres")
            run_sync()
        except Exception as e:
            print(f"[pg-sync] skipped: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
