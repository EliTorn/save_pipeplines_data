# Architecture

## Layout

```
.
├── main.py                       # entrypoint — sys.path setup, logger, run_pipeline()
├── app.py                        # Streamlit settings editor + runner UI
├── core/                         # generic pipeline machinery (no per-index code)
│   ├── adapter.py                #   IndexAdapter ABC
│   ├── adapter_loader.py         #   get_adapter(name), known_indexes()
│   ├── batch.py                  #   per-batch executors, Plan, sql/window helpers
│   ├── coerce.py                 #   coerce(value, kind), _LAMBDA_TYPE, field_types
│   ├── compare.py                #   transform_to_es_shape, compare_records
│   ├── config.py                 #   env-derived constants (workers, OUT_DIR, DIFF_MODE)
│   ├── csv_writer.py             #   strip_nl + per-batch save helpers
│   └── runner.py                 #   pool init, mode runners, run_pipeline()
├── settings/
│   ├── events.yaml               #   per-event references (IS_RUNNING, time window, index_config)
│   ├── loader.py                 #   resolve events.yaml + per-index config.yaml
│   ├── common_lambdas.py         #   shared value converters (date/int/bool/list/...)
│   ├── compare.py                #   shim re-exporting from core.compare
│   ├── utils_lambda.py           #   shim re-exporting from common_lambdas + indexes
│   └── indexes/                  #   per-index assets — adding one = adding a folder
│       ├── cardusers/
│       ├── loginlogoutinfo/
│       ├── playerbonus/
│       │   ├── config.yaml
│       │   ├── enums.yaml
│       │   ├── helpers.py
│       │   ├── lookup/menu_items.sql
│       │   ├── parts/{redeem,freespins,wheelspin,jackpot}/{sql.sql,mapping.csv}
│       │   └── schema.csv
│       └── playerinfoidx/
├── apply_changes/                # write the diffs back to ES
│   ├── apply_changes.py          #   CLI: --event --env --mode --dry; default --source duckdb
│   ├── es_schema.py              #   fetch + flatten + validate ES mapping
│   ├── fetch_schemas.py          #   one-shot per-index schema CSV refresh
│   ├── duckdb_source.py          #   read pending diffs from local Parquet via DuckDB
│   ├── pg_source.py              #   legacy fallback: read pending diffs from Postgres
│   └── pg_tracking.py            #   mark applied files in Postgres (small state)
├── connect_into_orcal/           # Oracle connection + run_tracked() instrumentation
├── connect_into_es/              # ES query helpers (fetch_range_df, fetch_terms_df)
├── connect_into_postgres/        # observability + summary writers
│   ├── _pg_cache.py              #   shared CachedConnection helper (sticky failure)
│   ├── connect_to_postgres.py    #   create_connection, run_query, execute
│   ├── observability.py          #   connection_log / query_log / batch_log inserts
│   ├── run_summary.py            #   pipeline_run_summary inserts
│   ├── write_through.py          #   legacy DDL only (CREATE IF NOT EXISTS)
│   ├── parity_check.py           #   compare PG vs DuckDB row counts
│   └── sync_out.py               #   LEGACY one-shot manual mirror (not in main flow)
├── duckdb_data/                  # local DuckDB catalog over Parquet/CSV
│   └── pipeline.duckdb           #   views only — safe to delete + reinit
└── out/                          # all pipeline output (per event / per env)
    └── <EVENT>/<env>/
        ├── changes/
        │   ├── changes_<stamp>.csv         # human-readable copy
        │   ├── changes_<stamp>.parquet     # DuckDB read target
        │   ├── missing_in_es_<stamp>.csv
        │   └── missing_in_es_<stamp>.parquet
        └── <EVENT>_oracle_<stamp>.csv     # only when PIPELINE_SAVE_FULL_CSV=1
```

## Data flow

```
                          settings/events.yaml
                                  │
                                  ▼
                      settings.loader.load_events()
                                  │
                                  ▼  (per event)
            ┌─────────────────────┴─────────────────────┐
            │                                           │
            ▼                                           ▼
       Oracle SELECT                              ES query
   (windowed by time / id)                  (matching window or ids)
            │                                           │
            ▼                                           │
   adapter.transform()                                  │
   (mapping.csv + lambdas)                              │
            │                                           │
            └──────────────► compare_records ◄──────────┘
                                  │
                                  ▼
                out/<EVENT>/<env>/changes/*.{csv,parquet}
                  (local files = source of truth)
                                  │
                                  ▼
                       DuckDB views (v_changes / v_missing)
                                  │
                                  ▼
                       apply_changes.apply_changes
                                  │       (default --source duckdb)
                                  ▼
                  adapter.coerce_for_es(field, value)
                  adapter.before_apply(doc)
                                  │
                                  ▼
                          Elasticsearch

  Side channel (small, async, best-effort, never blocks pipeline):

       Oracle / ES / PG queries  ─┐
       per-batch results         ─┼─►  connection_log  (PG)
       per-event totals          ─┤    query_log       (PG)
                                  └──► batch_log       (PG)
                                       pipeline_run_summary (PG)
```

## Mode dispatch

`MODE` in each index's `config.yaml` selects which `core.runner` runner
handles the event:

| MODE | Runner | When to use |
|------|--------|-------------|
| `time` | `run_event_time` | One SQL, windowed by `START_TIME`/`END_TIME` in `BANCH_VALUE` chunks. Default. |
| `id_range` | `run_event_id_range` | SQL has `-- @range` (returns MIN/MAX) and `-- @batch` (per-chunk) sections. Chunked by PK. |
| `time_union` | `run_event_time_union` | Multiple `parts:` SQL files; each part shapes its own rows; concat then compare once. |

## IndexAdapter contract

`core.adapter.IndexAdapter` is the only thing generic code touches.
Subclass per index in `settings/indexes/<X>/helpers.py`:

```python
class IndexAdapter:
    INDEX_NAME: str = ""

    # transform side
    def lambdas(self) -> dict[str, Callable]: ...      # used by transform_to_es_shape
    def transform(self, df, mapping): ...               # convenience wrapper

    # apply side
    def field_kind_overrides(self) -> dict[str, str]: ...  # e.g. {"parentId": "int_str"}
    def bind_field_types(self, types): ...                  # caller injects resolved types
    def coerce_for_es(self, field, value): ...              # uses bound types + core.coerce
    def before_apply(self, doc) -> dict: ...                # mutate before send (e.g. updateDate)
    def validate(self, doc) -> list[str]: ...               # per-doc rules
```

Lambda union for legacy callers: `core.compare.LAMBDAS` is the explicit
union of `settings.common_lambdas.COMMON_LAMBDAS` + every adapter's
`lambdas()`. Built once at import; **no auto-registration side-effects**.

## Multiprocessing

Per-batch work runs in a `multiprocessing.spawn` pool sized by
`PIPELINE_WORKERS`:

- Worker init opens its own Oracle connection + `QueueLogger`.
- Tasks are picklable dicts containing `mapping`, `adapter`, time/id
  bounds, plus mode-specific knobs.
- Worker imports `helpers.py` per-process, recreating any closures
  (e.g. enum lookups) locally — no cross-process lambda pickling.
- Logging events flow back via a queue to the parent's listener thread.

## PostgreSQL role (Phase D+)

PG holds **only** small operational metadata. Heavy data lives on disk and
is queried through DuckDB. Every PG insert is best-effort — pipeline never
blocks on PG availability.

### Active tables

| Table | Purpose |
|---|---|
| `pipeline_run_summary` | one row per (run_id, env, target_name, operation) |
| `connection_log` | per-connection timing/status (oracle / elasticsearch / postgres) |
| `query_log` | per Oracle/ES query timing + row counts (no result payloads) |
| `batch_log` | per pipeline batch (compare / apply_changes / apply_missing) |
| `pipeline_apply_batches` | per-CSV applied-marker, used by duckdb_source for filtering |

### Legacy tables (read-only after Phase C)

`pipeline_changes`, `pipeline_missing`, `pipeline_apply_audit`,
`pipeline_summary`, `pipeline_log_*`. Idempotent CREATE IF NOT EXISTS at
startup, but no new writes. `sync_out.py` is a manual one-shot tool to
backfill these from local files if needed; not invoked by the active flow.

### Apply path source priority

`apply_changes --source duckdb` (default) → reads Parquet via DuckDB,
filters out files in `pipeline_apply_batches`, pushes to ES.
`--source pg` → legacy `pipeline_changes` reads (only useful with old data).
`--source csv` → direct disk read of `changes_*.csv` files.
`--source auto` → tries `pg → duckdb → csv` in order.

### Failure mode

Every PG-touching module routes through `connect_into_postgres._pg_cache.
CachedConnection`: first failure prints one warning, subsequent calls
return None silently. Pipeline finishes the same speed PG-up or PG-down.
