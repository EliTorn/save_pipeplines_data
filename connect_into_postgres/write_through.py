"""Loop 6: emptied. The legacy heavy-table DDL (pipeline_changes /
pipeline_missing) is gone — those tables are dropped via
`drop_legacy_tables.py` and should NOT be re-created.

`init_pg_schema()` remains as a no-op shim for backward compatibility with
main.py callers; remove the call from main.py at your convenience.
"""
from __future__ import annotations


def init_pg_schema(reset: bool = False) -> None:
    """No-op since loop 6. Kept so existing main.py imports don't break.

    Active PG schema lives in:
      - connect_into_postgres.run_summary.init_schema()        (pipeline_run_summary)
      - connect_into_postgres.observability.init_schema()      (connection_log,
                                                                query_log,
                                                                batch_log)
      - apply_changes.pg_tracking implicit (pipeline_apply_batches —
        DDL is created by sync_out historically; if you start a fresh
        DB, see drop_legacy_tables.py for the canonical schema list).
    """
    if reset:
        print("[pg] PIPELINE_PG_RESET=1 ignored: no legacy tables to truncate "
              "after loop 6", flush=True)
    return
