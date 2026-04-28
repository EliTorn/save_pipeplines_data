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
│   ├── apply_changes.py          #   CLI: --event --env --mode --dry; uses adapter
│   ├── es_schema.py              #   fetch + flatten + validate ES mapping
│   ├── fetch_schemas.py          #   one-shot per-index schema CSV refresh
│   ├── pg_source.py              #   read pending diffs from Postgres
│   └── pg_tracking.py            #   mark applied files in Postgres
├── connect_into_orcal/           # Oracle connection + run_tracked() instrumentation
├── connect_into_es/              # ES query helpers (fetch_range_df, fetch_terms_df)
├── connect_into_postgres/        # PG connection + sync_out (mirror out/ → PG)
└── out/                          # all pipeline output (per event / per env)
    └── <EVENT>/<env>/
        ├── changes/
        │   ├── changes_<stamp>.csv
        │   └── missing_in_es_<stamp>.csv
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
                  out/<EVENT>/<env>/changes/*.csv
                                  │
                                  ▼
                  connect_into_postgres.sync_out
                  (mirror to pipeline_changes / pipeline_missing)
                                  │
                                  ▼
                       apply_changes.apply_changes
                                  │
                                  ▼
                  adapter.coerce_for_es(field, value)
                  adapter.before_apply(doc)
                                  │
                                  ▼
                          Elasticsearch
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

## Postgres as source of truth

`connect_into_postgres.sync_out` mirrors every `out/` artifact to PG:

- `pipeline_changes` — one row per field-level diff.
- `pipeline_missing` — one row per Oracle row missing from ES.
- `pipeline_apply_audit` — append-only log of every ES op.
- `pipeline_run` / `pipeline_event` / `pipeline_query` — instrumentation
  produced by the logger.

`apply_changes` reads `WHERE applied_ts IS NULL` from PG by default
(`--source pg|csv|auto`). CSV files in `out/` are the local cache; PG
rows are the durable plan.
