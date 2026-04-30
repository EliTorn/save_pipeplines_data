# Pipeline Logging — Design Spec

**Status:** approved (design phase, not yet implemented)
**Schema version:** 1.0
**Last updated:** 2026-04-29

---

## 1. Goals

Two-tier logging:

- **Postgres** — small, clean, business / high-level. Used for dashboards, alerts,
  monitoring. A few rows per run. Append + update only.
- **Local Parquet (+ DuckDB)** — heavy, detailed, process-level. Used for deep
  debugging and analysis. Per-run isolated.

Boundary is strict: **nothing detailed goes into Postgres, nothing
business-summary-only goes into Parquet**.

### Non-goals

- Real-time streaming to remote sinks.
- Replacing existing `out/` data outputs.
- Removing legacy PG tables in this phase (stop writing, leave for now).

---

## 2. Storage layout

### Postgres

Two tables only:

- `pipeline_run` — one row per run.
- `pipeline_run_target` — one row per `(run_id × target_name × operation)`.

Existing `connection_log`, `query_log`, `batch_log`, `pipeline_run_summary`
**stay in DB but receive no new writes**. Removal is a later cleanup step.

### Local filesystem

```
oracle-es-sync-pipeline/
├── logs/                                  # DATA — gitignored
│   ├── runs/
│   │   └── <run_id>/
│   │       ├── meta.json
│   │       ├── connections/part_NNNN.parquet
│   │       ├── queries/     part_NNNN.parquet
│   │       ├── batches/     part_NNNN.parquet
│   │       ├── events/      part_NNNN.parquet
│   │       ├── phases/      part_NNNN.parquet
│   │       └── errors/      part_NNNN.parquet
│   ├── _archive/<yyyymm>/<run_id>.tar.zst
│   └── duckdb/runs.duckdb
│
└── pipeline_logging/                      # CODE module
    ├── __init__.py
    ├── pg_summary.py
    ├── parquet_sink.py
    ├── listener.py
    ├── run_logger.py
    ├── phase.py
    ├── retention.py
    ├── schemas.py
    ├── duckdb_views.py
    └── SPEC.md  (this file)
```

`run_id` format: `<yyyymmdd>_<hhmmss>_<8-char-uuid>` — e.g.
`20260429_141522_a1b2c3d4`.

---

## 3. Postgres schemas

### 3.1 `pipeline_run`

One row per run. INSERT at run start (`status='running'`), UPDATE at run end.

| column              | type         | nullable | meaning |
|---------------------|--------------|----------|---------|
| run_id              | TEXT         | PK       | run identifier |
| env                 | TEXT         | NO       | `prod` / `stage` / `dev` |
| host                | TEXT         | NO       | machine hostname |
| trigger             | TEXT         | NO       | `manual` / `cron` / `app_ui` |
| schema_version      | TEXT         | NO       | logging schema version (e.g. `1.0`) |
| started_at          | TIMESTAMPTZ  | NO       | inserted at run start |
| ended_at            | TIMESTAMPTZ  | YES      | NULL while running |
| duration_ms         | BIGINT       | YES      | NULL while running |
| status              | TEXT         | NO       | `running` / `ok` / `partial` / `failed` |
| events_count        | INT          | YES      | how many events processed |
| total_rows_changed  | BIGINT       | YES      | sum of `rows_inserted+rows_updated+rows_deleted` across all targets |
| final_error         | TEXT         | YES      | last fatal error string if `failed` |
| created_at          | TIMESTAMPTZ  | NO       | `now()` |
| updated_at          | TIMESTAMPTZ  | NO       | touched on each update |

**Status values:**

- `running` — INSERT at run start.
- `ok` — all targets succeeded.
- `partial` — some targets failed but run completed.
- `failed` — fatal error stopped the run.

**Examples:**

```
run_id              | env  | host   | trigger | sv  | started_at         | ended_at           | dur_ms  | status  | events | total_rows | final_error
20260429_141522_a1b2| prod | eli-pc | manual  | 1.0 | 2026-04-29 14:15:22| 2026-04-29 14:38:07| 1365000 | ok      | 3      | 1892       | null
20260429_150000_c3d4| prod | eli-pc | cron    | 1.0 | 2026-04-29 15:00:00| null               | null    | running | 1      | null       | null
20260428_220000_e5f6| prod | eli-pc | cron    | 1.0 | 2026-04-28 22:00:00| 2026-04-28 22:04:11| 251000  | failed  | 3      | 0          | ORA-12541: TNS:no listener
```

### 3.2 `pipeline_run_target`

One row per `(run_id × target_name × operation)`. INSERT at target start
(`status='running'`), UPDATE at target end.

| column          | type         | nullable | meaning |
|-----------------|--------------|----------|---------|
| id              | BIGSERIAL    | PK       | |
| run_id          | TEXT         | NO       | FK → `pipeline_run.run_id` |
| source_system   | TEXT         | NO       | `oracle` / `local` / `elasticsearch` |
| target_system   | TEXT         | NO       | `elasticsearch` / `local` |
| target_name     | TEXT         | NO       | index/table name |
| operation       | TEXT         | NO       | `compare` / `apply_changes` / `apply_missing` |
| started_at      | TIMESTAMPTZ  | NO       | |
| ended_at        | TIMESTAMPTZ  | YES      | NULL while in-progress |
| duration_ms     | BIGINT       | YES      | |
| rows_source     | BIGINT       | YES      | rows pulled from source |
| rows_target     | BIGINT       | YES      | rows present in target |
| rows_inserted   | BIGINT       | YES      | new rows written |
| rows_updated    | BIGINT       | YES      | existing rows changed |
| rows_deleted    | BIGINT       | YES      | rows removed |
| rows_missing    | BIGINT       | YES      | source-not-in-target |
| rows_unchanged  | BIGINT       | YES      | identical rows |
| status          | TEXT         | NO       | `running` / `ok` / `failed` |
| error           | TEXT         | YES      | error string if `failed` |
| created_at      | TIMESTAMPTZ  | NO       | `now()` |

For `compare`: `rows_inserted/updated/deleted = 0` (compare detects only).
`rows_missing` is the count to be applied later.

For `apply_changes` / `apply_missing`: row counts reflect actual writes.

**Examples:**

```
run_id              | source | target_system | target_name | operation     | dur_ms | rows_src | rows_tgt | ins | upd  | del | missing | unchanged | status | error
20260429_141522_a1b2| oracle | elasticsearch | playerbonus | compare       | 406000 | 120440   | 120388   | 0   | 0    | 0   | 52      | 118548    | ok     | null
20260429_141522_a1b2| local  | elasticsearch | playerbonus | apply_changes | 198000 | 1840     | 1840     | 0   | 1840 | 0   | 0       | 0         | ok     | null
20260429_141522_a1b2| local  | elasticsearch | playerbonus | apply_missing | 11000  | 52       | 52       | 52  | 0    | 0   | 0       | 0         | ok     | null
```

### 3.3 Indexes

```sql
CREATE INDEX ix_pipeline_run_started ON pipeline_run (started_at DESC);
CREATE INDEX ix_pipeline_run_status  ON pipeline_run (status, started_at DESC);

CREATE INDEX ix_run_target_run    ON pipeline_run_target (run_id);
CREATE INDEX ix_run_target_target ON pipeline_run_target (target_name, started_at DESC);
```

### 3.4 What does NOT go in Postgres

- Individual SQL statements / ES request bodies
- Per-batch timings
- Shard / worker events
- Retries
- Per-query row counts
- Tracebacks
- Connection geo / OS / pid metadata
- Lookup queries (menu_items.sql, etc.)
- `fetch_plan`, `shard_start`, `batch_progress` milestones
- Anything emitted from worker processes outside run boundaries

All of the above → Parquet.

---

## 4. Parquet files

All under `logs/runs/<run_id>/<table>/part_NNNN.parquet`. Schema version
in `meta.json` only — not on every row.

### 4.1 `meta.json`

One per run. Atomic write via tmp-rename pattern (write to
`meta.json.tmp`, fsync, rename to `meta.json`). Written at run start and
rewritten at run end.

```json
{
  "run_id": "20260429_141522_a1b2c3d4",
  "schema_version": "1.0",
  "env": "prod",
  "host": "eli-pc",
  "trigger": "manual",
  "started_at": "2026-04-29T14:15:22.000+02:00",
  "ended_at": "2026-04-29T14:38:07.000+02:00",
  "events": ["playerbonus", "cardusers", "loginlogoutinfo"],
  "args": {"event": "playerbonus", "env": "prod", "mode": "compare"},
  "git_commit": "a8f12c3",
  "python": "3.11.9",
  "pid": 18472
}
```

### 4.2 `connections/`

Generated by every `connect_to_*` call (oracle pool init, ES client
init, PG pool init).

| col          | type                    | meaning |
|--------------|-------------------------|---------|
| run_id       | string                  | |
| system       | string                  | `oracle` / `elasticsearch` / `postgres` |
| target       | string                  | service / db / url |
| host         | string                  | |
| port         | int32                   | |
| user         | string                  | |
| started_at   | timestamp[ms,UTC]       | |
| ended_at     | timestamp[ms,UTC]       | |
| duration_ms  | int32                   | |
| status       | string                  | `ok` / `error` |
| error        | string (nullable)       | |
| pid          | int32                   | |
| thread       | string                  | |

```
run_id              | system        | target       | host                     | port | user      | dur_ms | status | error
20260429_141522_a1b2| oracle        | XEPDB1       | oradb01.internal         | 1521 | gth_read  | 370    | ok     | null
20260429_141522_a1b2| elasticsearch | playerbonus  | https://es.internal:9200 | 9200 | -         | 88     | ok     | null
20260429_141522_a1b2| postgres      | pipeline_db  | pg.internal              | 5432 | pg_writer | 42     | ok     | null
```

### 4.3 `queries/`

Generated by every `with logger.query(...)` block — wraps every Oracle
SQL, ES request, and PG query.

| col            | type                    | meaning |
|----------------|-------------------------|---------|
| query_id       | string                  | unique short id |
| run_id         | string                  | |
| system         | string                  | `oracle` / `elasticsearch` / `postgres` |
| target         | string                  | table / index name |
| operation      | string                  | `fetch_oracle` / `fetch_es` / `lookup` / `apply_changes` / `bulk_insert` |
| batch_id       | string (nullable)       | links to batches |
| sql_hash       | string                  | first 12 chars of normalized hash |
| sql_text       | string                  | full SQL or ES body, truncated to 8000 chars |
| params         | string (nullable)       | JSON |
| started_at     | timestamp[ms,UTC]       | |
| ended_at       | timestamp[ms,UTC]       | |
| duration_ms    | int32                   | |
| rows           | int64 (nullable)        | |
| rows_per_sec   | float64 (nullable)      | |
| status         | string                  | `ok` / `error` |
| error          | string (nullable)       | |
| owner          | string (nullable)       | Oracle schema owner |
| thread         | string                  | |
| pid            | int32                   | |

```
query_id   | system        | target      | operation    | batch_id                    | dur_ms | rows | status | error
q_3f9a12bc | oracle        | playerbonus | fetch_oracle | playerbonus#2026-04-01..02  | 2770   | 4820 | ok     | null
q_88fe2210 | elasticsearch | playerbonus | fetch_es     | playerbonus#2026-04-01..02  | 1100   | 4818 | ok     | null
q_77abc12  | oracle        | playerbonus | fetch_oracle | playerbonus#2026-04-15..16  | 30000  | null | error  | ORA-12541: TNS:no listener
```

### 4.4 `batches/`

Generated by each batch executor in `core/batch.py` once it finishes.

| col              | type                    | meaning |
|------------------|-------------------------|---------|
| batch_id         | string                  | `<target>#<window>` or `<target>#<id_lo>..<id_hi>` |
| run_id           | string                  | |
| target           | string                  | |
| operation        | string                  | |
| window_from      | timestamp (nullable)    | time mode |
| window_to        | timestamp (nullable)    | |
| id_from          | int64 (nullable)        | id_range mode |
| id_to            | int64 (nullable)        | |
| started_at       | timestamp               | |
| ended_at         | timestamp               | |
| duration_ms      | int32                   | |
| rows_oracle      | int64 (nullable)        | |
| rows_es          | int64 (nullable)        | |
| rows_changed     | int64 (nullable)        | |
| rows_missing     | int64 (nullable)        | |
| oracle_query_id  | string (nullable)       | FK → queries |
| es_query_id      | string (nullable)       | FK → queries |
| status           | string                  | |
| error            | string (nullable)       | |
| worker_pid       | int32                   | |

```
batch_id                          | target      | op      | dur_ms | ora  | es   | changed | missing | status
playerbonus#2026-04-01..02        | playerbonus | compare | 8420   | 4820 | 4818 | 12      | 2       | ok
playerbonus#2026-04-02..03        | playerbonus | compare | 8210   | 4944 | 4944 | 8       | 0       | ok
playerbonus#2026-04-15..16 (retry)| playerbonus | compare | 12880  | 5012 | 5012 | 4       | 0       | ok
```

### 4.5 `events/`

Catch-all for milestones not covered by queries / batches / phases.
Generated by existing `logger.event(...)` calls (`fetch_plan`,
`parallel_plan`, `csv_saved`, `parquet_saved`, `retry`, custom).

| col       | type                  | meaning |
|-----------|-----------------------|---------|
| run_id    | string                | |
| ts        | timestamp[ms,UTC]     | |
| level     | string                | `INFO` / `WARN` / `ERROR` |
| event     | string                | event name |
| target    | string (nullable)     | |
| batch_id  | string (nullable)     | |
| query_id  | string (nullable)     | |
| message   | string (nullable)     | human-readable |
| fields    | string                | JSON of extra fields |
| thread    | string                | |
| pid       | int32                 | |

```
ts                       | level | event       | target      | message                      | fields
2026-04-29T14:15:25.001Z | INFO  | fetch_plan  | playerbonus | null                         | {"approx_rows":120440,"workers":4,"batch_size":40000}
2026-04-29T14:18:46.220Z | INFO  | csv_saved   | playerbonus | out/playerbonus/prod/...     | {"rows":4820,"path":"..."}
2026-04-29T14:22:05.000Z | WARN  | retry       | playerbonus | retry 1/3 after ORA-12541    | {"attempt":1,"backoff_ms":2000}
```

**Rule:** anything with a duration → `phases` or `batches`. Anything that
is a SQL/HTTP call → `queries`. Anything else → `events`.

### 4.6 `phases/`

Generated by new `with logger.phase("transform"):` context manager
wrapping each pipeline phase in `core/runner.py`, `core/batch.py`,
`apply_changes/`.

| col          | type                  | meaning |
|--------------|-----------------------|---------|
| run_id       | string                | |
| target       | string                | |
| phase        | string                | `oracle_fetch` / `es_fetch` / `transform` / `compare` / `parquet_write` / `apply_es` |
| batch_id     | string (nullable)     | |
| started_at   | timestamp             | |
| ended_at     | timestamp             | |
| duration_ms  | int32                 | |
| rows_in      | int64 (nullable)      | |
| rows_out     | int64 (nullable)      | |
| status       | string                | |
| error        | string (nullable)     | |
| worker_pid   | int32                 | |

```
target      | phase         | batch_id                    | dur_ms | rows_in | rows_out | status
playerbonus | oracle_fetch  | playerbonus#2026-04-01..02  | 2770   | null    | 4820     | ok
playerbonus | transform     | playerbonus#2026-04-01..02  | 320    | 4820    | 4820     | ok
playerbonus | compare       | playerbonus#2026-04-01..02  | 880    | 4820    | 14       | ok
playerbonus | parquet_write | playerbonus#2026-04-01..02  | 110    | 14      | 14       | ok
playerbonus | apply_es      | (whole run)                 | 198000 | 1892    | 1892     | ok
```

### 4.7 `errors/`

Generated by every caught exception in pipeline + every
`logger.event(level="ERROR")`.

| col          | type                  | meaning |
|--------------|-----------------------|---------|
| run_id       | string                | |
| ts           | timestamp[ms,UTC]     | |
| where        | string                | code location (e.g. `oracle.run_tracked`) |
| target       | string (nullable)     | |
| batch_id     | string (nullable)     | |
| query_id     | string (nullable)     | |
| error_type   | string                | exception class |
| error_msg    | string                | `str(exc)` |
| traceback    | string                | full traceback |
| retried      | bool                  | was a retry attempted |
| retry_count  | int32                 | |
| recovered    | bool                  | did a retry eventually succeed |

```
ts                       | where               | target      | batch_id                  | error_type             | error_msg                  | retried | retry_count | recovered
2026-04-29T14:22:09.110Z | oracle.run_tracked  | playerbonus | playerbonus#2026-04-15..16| oracledb.DatabaseError | ORA-12541: TNS:no listener | true    | 2           | true
```

---

## 5. Write strategy

### 5.1 Per-batch shards (not single-file append)

Parquet is not appendable. Instead:

- Each `<table>/` is a directory.
- Listener buffers rows in memory.
- Buffer flushes to a new shard `part_NNNN.parquet` when:
  - row count ≥ `LOG_SHARD_ROWS` (default 5000), **or**
  - time since last flush ≥ `LOG_SHARD_SECONDS` (default 30), **or**
  - run end.
- DuckDB reads `<table>/*.parquet` as one logical dataset.

**Crash safety:** at most the current in-memory buffer is lost
(seconds of data, never the whole run).

### 5.2 Worker write path

```
worker proc          parent proc
-----------          -----------
logger.event() ─┐
logger.query() ─┼──► multiprocessing.Queue ──► listener thread ──► parquet shard writer
logger.phase() ─┘                                                ├─► PG summary writer
                                                                 └─► duckdb view register
```

Workers **never write parquet directly**. Workers only push events.

### 5.3 Atomic `meta.json`

```python
tmp = path.with_suffix(".json.tmp")
tmp.write_text(json.dumps(data, indent=2))
os.fsync(tmp)
tmp.replace(path)
```

### 5.4 PG write order

- Run start: `INSERT pipeline_run` (status=running).
- Target start: `INSERT pipeline_run_target` (status=running).
- Target end: `UPDATE pipeline_run_target` (status, totals, ended_at).
- Run end: `UPDATE pipeline_run` (status, totals, ended_at).

All PG writes best-effort via existing `_pg_cache.CachedConnection`. PG
unreachable → silent no-op, parquet still written. Pipeline never
blocks on PG.

---

## 6. Schema versioning

`schema_version` lives **once per run** in `meta.json` — not as a column.

**Policy (loose):**

- Additive change (new column) → compatible. Old runs missing the column
  read as NULL via DuckDB views with `COALESCE`.
- Breaking change (rename, type change, removal) → bump major version.
- Bump minor for additive, major for breaking.

DuckDB views in `pipeline_logging/duckdb_views.py` should handle missing
columns where possible.

---

## 7. Retention

Sweeper runs nightly (cron or at run-start). Configurable knobs:

| age                       | env var              | default  | action |
|---------------------------|----------------------|----------|--------|
| < hot                     | `LOG_HOT_DAYS`       | 30       | live shards in `logs/runs/<id>/` |
| hot ≤ age < warm          | `LOG_WARM_DAYS`      | 180      | compact: shards merged into single `<table>.parquet` per run, recompressed zstd |
| warm ≤ age < cold         | `LOG_COLD_DAYS`      | 365      | move to `logs/_archive/<yyyymm>/<run_id>.tar.zst` |
| ≥ delete (default off)    | `LOG_DELETE_DAYS`    | None     | delete archive |

PG `pipeline_run` + `pipeline_run_target` rows kept indefinitely
(small).

**Compaction (nightly, not at run-end):** merge shards
`<table>/part_*.parquet` → single `<table>.parquet`, then delete the
shard dir. DuckDB views read both layouts.

---

## 8. Module layout (code)

```
pipeline_logging/
├── __init__.py        # public API: get_run_logger, RunLogger
├── run_logger.py      # RunLogger class — emit + route to queue
├── parquet_sink.py    # buffered shard writer (per table)
├── pg_summary.py      # writes pipeline_run + pipeline_run_target
├── listener.py        # parent-process queue drain
├── phase.py           # `with logger.phase("name"):` ctx manager
├── retention.py       # nightly sweeper (compact / archive / delete)
├── schemas.py         # arrow schemas for each parquet table
├── duckdb_views.py    # registers views in logs/duckdb/runs.duckdb
└── SPEC.md            # this file
```

Old `_pipeline_logging.py` and each connector's `logging_setup.py`
shrink to thin shims importing from `pipeline_logging`.

---

## 9. Public API (preview)

```python
from pipeline_logging import get_run_logger

logger = get_run_logger(env="prod", trigger="manual")
# emits run start: PG INSERT pipeline_run + meta.json + logs/runs/<run_id>/

logger.connection(system="oracle", target="XEPDB1", ...)

with logger.target("playerbonus", "compare") as t:
    # emits PG INSERT pipeline_run_target (status=running)
    with logger.phase("oracle_fetch", batch_id="..."):
        with logger.query(sql, system="oracle", ...) as q:
            df = run(sql)
            q.set_rows(len(df))
    with logger.phase("transform", batch_id="..."):
        df2 = transform(df)
    t.set_rows(source=len(df), target=..., inserted=..., updated=...)
    # exits emit PG UPDATE pipeline_run_target

logger.close()
# emits PG UPDATE pipeline_run + meta.json final + flush all parquet shards
```

Worker variant: `pipeline_logging.QueueLogger` — same API, pushes to
multiprocessing queue. Listener thread in parent does all writes.

---

## 10. Full-run walkthrough

**Scenario:** `run_id=20260429_141522_a1b2`, prod, manual, single event
`playerbonus`, mode `time`, 3 batches, 1 transient ORA-12541 retried OK,
finished `compare` + `apply_changes`.

### Timeline

| t (s) | what                       | writes |
|-------|----------------------------|--------|
| 0     | run start                  | PG INSERT pipeline_run (running). FS meta.json |
| +0.4  | oracle/es/pg connects      | parquet connections/ |
| +1    | target start (compare)     | PG INSERT pipeline_run_target playerbonus/compare (running) |
| +3    | batch 1 fetch_oracle       | parquet queries/, phases/, events/ |
| +12   | batch 1 done               | parquet batches/ |
| +30   | batch 2 done               | parquet batches/ |
| +400  | batch 3 retry+done         | parquet errors/ (recovered=true), batches/ |
| +406  | compare done               | PG UPDATE pipeline_run_target (ok, totals) |
| +406  | target start (apply)       | PG INSERT pipeline_run_target playerbonus/apply_changes (running) |
| +604  | apply done                 | PG UPDATE pipeline_run_target (ok) |
| +1365 | run end                    | PG UPDATE pipeline_run (ok). FS meta.json finalized. flush shards |

### Final state

**PG (3 rows total):**

```
pipeline_run:
  20260429_141522_a1b2 | prod | eli-pc | manual | 1.0 | 14:15:22 | 14:38:07 | 1365000 | ok | 1 | 1892 | null

pipeline_run_target:
  ... | oracle | elasticsearch | playerbonus | compare       | 406000 | 120440 | 120388 | 0 | 0    | 0 | 52 | 118548 | ok | null
  ... | local  | elasticsearch | playerbonus | apply_changes | 198000 | 1840   | 1840   | 0 | 1840 | 0 | 0  | 0      | ok | null
```

**Parquet `logs/runs/20260429_141522_a1b2/`:**

```
meta.json                         (1 file)
connections/part_0001.parquet     (3 rows)
queries/part_0001.parquet         (~250 rows, possibly multiple shards)
batches/part_0001.parquet         (3 rows)
events/part_0001.parquet          (~40 rows)
phases/part_0001.parquet          (~12 rows)
errors/part_0001.parquet          (1 row, recovered=true)
```

### Debugging step-by-step

**Q: which run was slow yesterday?** → PG only.

```sql
SELECT run_id, duration_ms, total_rows_changed
FROM pipeline_run
WHERE started_at::date = '2026-04-28' ORDER BY duration_ms DESC;
```

**Q: which target took the longest?** → PG only.

```sql
SELECT run_id, target_name, operation, duration_ms
FROM pipeline_run_target
WHERE run_id='20260429_141522_a1b2' ORDER BY duration_ms DESC;
```

**Q: which Oracle queries were slow in that run?** → DuckDB.

```sql
SELECT query_id, target, batch_id, duration_ms, rows
FROM read_parquet('logs/runs/20260429_141522_a1b2/queries/*.parquet')
WHERE system='oracle' ORDER BY duration_ms DESC LIMIT 20;
```

**Q: did anything retry?** → DuckDB.

```sql
SELECT * FROM read_parquet('logs/runs/20260429_141522_a1b2/errors/*.parquet');
```

**Q: where did the time go in batch 3?** → DuckDB.

```sql
SELECT phase, duration_ms, rows_in, rows_out
FROM read_parquet('logs/runs/20260429_141522_a1b2/phases/*.parquet')
WHERE batch_id='playerbonus#2026-04-15..16' ORDER BY started_at;
```

**Q: full SQL of the slowest query?** → DuckDB.

```sql
SELECT sql_text, params, duration_ms
FROM read_parquet('logs/runs/20260429_141522_a1b2/queries/*.parquet')
WHERE query_id='q_3f9a12bc';
```

**Cross-tier join (PG run summary + parquet detail) via DuckDB
postgres extension:**

```sql
ATTACH 'host=pg.internal dbname=pipeline_db' AS pg (TYPE postgres);
SELECT r.run_id, r.status, t.target_name, t.duration_ms,
       (SELECT count(*) FROM read_parquet('logs/runs/' || r.run_id || '/queries/*.parquet')) AS query_count,
       (SELECT count(*) FROM read_parquet('logs/runs/' || r.run_id || '/errors/*.parquet')) AS error_count
FROM pg.pipeline_run r
JOIN pg.pipeline_run_target t USING (run_id)
WHERE r.started_at > now() - INTERVAL 7 DAY;
```

---

## 11. Migration / phasing

- **Phase 1:** new `pipeline_logging/` module + `pipeline_run` /
  `pipeline_run_target` PG tables + per-run parquet shards. Old CSV
  logs continue in parallel as safety net. Old PG tables receive no new
  writes.
- **Phase 2:** DuckDB views, retention sweeper, `logger.phase()` calls
  threaded through `core/runner.py` + `core/batch.py` +
  `apply_changes/`.
- **Phase 3:** delete legacy CSV writes + drop legacy PG tables once
  parquet stack proven.

---

## 12. Open items deferred to later phases

- DuckDB view DDL — concrete view names, column casts, per-version
  shims.
- Retention sweeper trigger — cron vs run-start.
- Streamlit `app.py` integration — read from new tables for the in-app
  run history page.
- PG attach in DuckDB — credential handling for cross-tier joins.
- Alerting rules — `pipeline_run.status='failed'` → Slack / email hook.
