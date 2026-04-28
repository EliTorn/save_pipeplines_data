"""Mirror local `out/` artifacts + connection logs into Postgres.

Tables (auto-created on first run):
  pipeline_changes         <- out/<EVENT>/<env>/changes/changes_*.csv
  pipeline_missing         <- out/<EVENT>/<env>/changes/missing_in_es_*.csv
  pipeline_apply_audit     <- out/_apply_log/<env>/<event>_<stamp>.jsonl
  pipeline_summary         <- out/summary_*.csv
  pipeline_log_connection  <- connect_into_{orcal,es}/logging/connections.csv
  pipeline_log_event       <- connect_into_{orcal,es}/logging/events.csv
  pipeline_log_query       <- connect_into_{orcal,es}/logging/queries.csv
  pipeline_log_offsets     incremental cursor per logging CSV

Behavior:
  - out/ tables: append every run (no dedupe). Re-running re-imports every file.
  - log tables: incremental — only rows past pipeline_log_offsets.rows_synced
    are inserted. UNIQUE(source_file, csv_row_no) + ON CONFLICT DO NOTHING is
    a safety net.
  - If Postgres is unreachable: print warning and exit 1 — never raises a traceback,
    so callers that chain this after main.py / apply_changes.py keep going.

Usage:
    python -m connect_into_postgres.sync_out
    python -m connect_into_postgres.sync_out --only changes,missing
    python -m connect_into_postgres.sync_out --only logs
    python -m connect_into_postgres.sync_out --dry
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from _pipeline_env import env_truthy  # noqa: E402

OUT_DIR = _ROOT / "out"
APPLY_LOG_DIR = OUT_DIR / "_apply_log"
YAML_PATH = _ROOT / "settings" / "events.yaml"

# Connection-logging CSVs from oracle + es + postgres modules.
LOG_SOURCES = [
    ("oracle",   _ROOT / "connect_into_orcal"    / "logging"),
    ("es",       _ROOT / "connect_into_es"       / "logging"),
    ("postgres", _ROOT / "connect_into_postgres" / "logging"),
]
LOG_FILES = ("connections.csv", "events.csv", "queries.csv")

_STAMP_RE = re.compile(r"_(\d{8})_(\d{6})")


# ---------------------------------------------------------------------------
# DDL — all tables append-only. `sync_ts` defaults to now() so re-runs append.
# ---------------------------------------------------------------------------

DDL = [
    """CREATE TABLE IF NOT EXISTS pipeline_changes (
        id BIGSERIAL PRIMARY KEY,
        sync_ts TIMESTAMP NOT NULL DEFAULT now(),
        source_file TEXT NOT NULL,
        file_run_ts TIMESTAMP,
        event TEXT NOT NULL,
        env TEXT,
        doc_id TEXT,
        field TEXT,
        oracle_value TEXT,
        es_value TEXT,
        status TEXT,
        applied_ts TIMESTAMP
    )""",
    "ALTER TABLE pipeline_changes ADD COLUMN IF NOT EXISTS applied_ts TIMESTAMP",
    """CREATE TABLE IF NOT EXISTS pipeline_missing (
        id BIGSERIAL PRIMARY KEY,
        sync_ts TIMESTAMP NOT NULL DEFAULT now(),
        source_file TEXT NOT NULL,
        file_run_ts TIMESTAMP,
        event TEXT NOT NULL,
        env TEXT,
        doc_id TEXT,
        payload JSONB NOT NULL,
        applied_ts TIMESTAMP
    )""",
    "ALTER TABLE pipeline_missing ADD COLUMN IF NOT EXISTS applied_ts TIMESTAMP",
    """CREATE TABLE IF NOT EXISTS pipeline_apply_batches (
        event TEXT NOT NULL,
        env TEXT NOT NULL,
        mode TEXT NOT NULL,
        source_file TEXT NOT NULL,
        applied_ts TIMESTAMP NOT NULL DEFAULT now(),
        run_id TEXT,
        docs_planned INT,
        es_updated INT,
        es_created INT,
        es_conflicts INT,
        es_failures INT,
        notes TEXT,
        PRIMARY KEY (event, env, mode, source_file)
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_apply_audit (
        id BIGSERIAL PRIMARY KEY,
        sync_ts TIMESTAMP NOT NULL DEFAULT now(),
        source_file TEXT NOT NULL,
        line_no INT NOT NULL,
        event_ts TEXT,
        record_type TEXT,
        env TEXT,
        event TEXT,
        index_name TEXT,
        doc_id TEXT,
        batch INT,
        raw JSONB NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_summary (
        id BIGSERIAL PRIMARY KEY,
        sync_ts TIMESTAMP NOT NULL DEFAULT now(),
        source_file TEXT NOT NULL,
        file_run_ts TIMESTAMP,
        event TEXT,
        env TEXT,
        field TEXT,
        total_issues INT,
        diff INT,
        row_missing_in_es INT,
        row_missing_in_oracle INT,
        es_value_blank INT,
        oracle_value_blank INT
    )""",
    # ---- connection-logging mirrors ----
    """CREATE TABLE IF NOT EXISTS pipeline_log_offsets (
        source_file TEXT PRIMARY KEY,
        rows_synced BIGINT NOT NULL DEFAULT 0,
        last_sync TIMESTAMP NOT NULL DEFAULT now()
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_log_connection (
        id BIGSERIAL PRIMARY KEY,
        sync_ts TIMESTAMP NOT NULL DEFAULT now(),
        source TEXT NOT NULL,
        source_file TEXT NOT NULL,
        csv_row_no BIGINT NOT NULL,
        run_id TEXT,
        ts TEXT, tz TEXT,
        hostname TEXT, fqdn TEXT, os_user TEXT, local_ip TEXT,
        public_ip TEXT, country TEXT, region TEXT, city TEXT, org TEXT,
        platform TEXT, python TEXT, pid INT, cwd TEXT,
        payload JSONB NOT NULL,
        UNIQUE (source_file, csv_row_no)
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_log_event (
        id BIGSERIAL PRIMARY KEY,
        sync_ts TIMESTAMP NOT NULL DEFAULT now(),
        source TEXT NOT NULL,
        source_file TEXT NOT NULL,
        csv_row_no BIGINT NOT NULL,
        run_id TEXT, query_id TEXT,
        ts TEXT, tz TEXT,
        level TEXT, thread TEXT, event TEXT,
        owner TEXT, "table" TEXT, batch INT, "offset" BIGINT, "limit" BIGINT,
        rows BIGINT, seconds NUMERIC, rows_per_sec NUMERIC,
        sql TEXT, sql_hash TEXT, error TEXT, path TEXT,
        payload JSONB NOT NULL,
        UNIQUE (source_file, csv_row_no)
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_log_query (
        id BIGSERIAL PRIMARY KEY,
        sync_ts TIMESTAMP NOT NULL DEFAULT now(),
        source TEXT NOT NULL,
        source_file TEXT NOT NULL,
        csv_row_no BIGINT NOT NULL,
        query_id TEXT, run_id TEXT, sql_hash TEXT,
        start_ts TEXT, end_ts TEXT, tz TEXT,
        seconds NUMERIC, rows BIGINT, rows_per_sec NUMERIC,
        status TEXT, error TEXT,
        owner TEXT, "table" TEXT, batch INT, thread TEXT,
        params TEXT, sql TEXT,
        payload JSONB NOT NULL,
        UNIQUE (source_file, csv_row_no)
    )""",
]


# ---------------------------------------------------------------------------
# Views — friendly read-only projections on top of the raw log tables.
# Re-applied on every sync via CREATE OR REPLACE so schema drift is auto-healed.
# ---------------------------------------------------------------------------

VIEWS = [
    # 1) Connections — successes (from connection log) UNION failures (from event log).
    """CREATE OR REPLACE VIEW v_pipeline_connections AS
    SELECT
        source,
        run_id,
        ts AS start_ts,
        COALESCE(payload->>'oracle_host', payload->>'es_url', payload->>'pg_host') AS host,
        COALESCE(payload->>'oracle_port', payload->>'pg_port') AS port,
        COALESCE(payload->>'oracle_service', payload->>'pg_db', payload->>'es_user') AS db,
        COALESCE(payload->>'oracle_user', payload->>'es_user', payload->>'pg_user') AS db_user,
        hostname, country, city,
        'success'::text AS status,
        NULL::text AS error
    FROM pipeline_log_connection
    UNION ALL
    SELECT
        source, run_id, ts AS start_ts,
        COALESCE(payload->>'pg_host', payload->>'oracle_host', payload->>'es_url') AS host,
        COALESCE(payload->>'pg_port', payload->>'oracle_port') AS port,
        COALESCE(payload->>'pg_db', payload->>'oracle_service') AS db,
        NULL::text AS db_user,
        NULL::text AS hostname, NULL::text AS country, NULL::text AS city,
        'failed'::text AS status,
        error
    FROM pipeline_log_event
    WHERE event LIKE '%connect_failed%' OR (level = 'ERROR' AND event LIKE 'pg_%')
    """,

    # 2) Connection overview — counts/avgs per source.
    """CREATE OR REPLACE VIEW v_pipeline_connection_overview AS
    SELECT
        source,
        COUNT(*) FILTER (WHERE status = 'success') AS successes,
        COUNT(*) FILTER (WHERE status = 'failed')  AS failures,
        COUNT(*)                                   AS total,
        MIN(start_ts) AS first_seen,
        MAX(start_ts) AS last_seen
    FROM v_pipeline_connections
    GROUP BY source
    ORDER BY source
    """,

    # 3) Query performance — readable per-query view with slow flag.
    """CREATE OR REPLACE VIEW v_pipeline_query_perf AS
    SELECT
        id,
        source,
        run_id,
        query_id,
        sql_hash,
        "table"  AS query_table,
        owner,
        batch,
        start_ts,
        end_ts,
        seconds,
        rows,
        rows_per_sec,
        status,
        error,
        (seconds IS NOT NULL AND seconds > 5)::boolean AS slow_query,
        sql
    FROM pipeline_log_query
    """,

    # 4) Slowest 50 queries (any source).
    """CREATE OR REPLACE VIEW v_pipeline_query_slowest AS
    SELECT id, source, run_id, query_table, owner, batch, seconds, rows,
           rows_per_sec, status, sql_hash, start_ts, error
    FROM v_pipeline_query_perf
    WHERE seconds IS NOT NULL
    ORDER BY seconds DESC
    LIMIT 50
    """,

    # 5) Per-sql_hash latency by hour-of-day — answers "for THIS query, what hour
    #    runs fastest?". Different queries (different sql_hash) stay separated.
    """CREATE OR REPLACE VIEW v_pipeline_query_by_hour AS
    SELECT
        source,
        sql_hash,
        MAX("table")                   AS query_table,
        (payload->>'start_hour')::int  AS hour_of_day,
        COUNT(*)                       AS runs,
        AVG(seconds)::numeric(10,3)    AS avg_seconds,
        MIN(seconds)::numeric(10,3)    AS min_seconds,
        MAX(seconds)::numeric(10,3)    AS max_seconds,
        SUM(rows)                      AS total_rows,
        AVG(rows_per_sec)::numeric(12,1) AS avg_rows_per_sec
    FROM pipeline_log_query
    WHERE seconds IS NOT NULL
      AND sql_hash IS NOT NULL
      AND payload->>'start_hour' IS NOT NULL
    GROUP BY source, sql_hash, (payload->>'start_hour')::int
    ORDER BY source, sql_hash, hour_of_day
    """,

    # 5b) Index of distinct sql_hashes — pick-list helper for the per-hash drilldown.
    """CREATE OR REPLACE VIEW v_pipeline_query_hashes AS
    SELECT
        sql_hash,
        source,
        MAX("table")                  AS query_table,
        COUNT(*)                       AS total_runs,
        AVG(seconds)::numeric(10,3)    AS avg_seconds,
        MAX(seconds)::numeric(10,3)    AS max_seconds,
        SUM(rows)                      AS total_rows,
        MIN(start_ts)                  AS first_seen,
        MAX(start_ts)                  AS last_seen,
        LEFT(MAX(sql), 120)            AS sql_preview
    FROM pipeline_log_query
    WHERE sql_hash IS NOT NULL AND seconds IS NOT NULL
    GROUP BY sql_hash, source
    ORDER BY total_runs DESC, avg_seconds DESC
    """,

    # 6) Per-run metrics — pipeline lifecycle summary per run_id.
    #    Joins connection (start), run_end event (finish + total_seconds),
    #    plus query aggregates per source within the run.
    """CREATE OR REPLACE VIEW v_pipeline_run_metrics AS
    WITH runs AS (
        SELECT DISTINCT run_id, source FROM pipeline_log_connection
        UNION SELECT DISTINCT run_id, source FROM pipeline_log_event WHERE run_id IS NOT NULL
    ),
    starts AS (
        SELECT run_id, MIN(ts) AS started_at
        FROM pipeline_log_connection
        GROUP BY run_id
    ),
    ends AS (
        SELECT run_id, MAX(ts) AS finished_at,
               MAX(NULLIF(payload->>'total_seconds','')::numeric) AS total_seconds_logged
        FROM pipeline_log_event
        WHERE event = 'run_end'
        GROUP BY run_id
    ),
    qstats AS (
        SELECT run_id, source,
               COUNT(*)                       AS query_count,
               SUM(seconds)::numeric(12,3)    AS total_seconds,
               MAX(seconds)::numeric(12,3)    AS max_seconds,
               SUM(rows)                      AS total_rows
        FROM pipeline_log_query
        WHERE run_id IS NOT NULL
        GROUP BY run_id, source
    ),
    errors AS (
        SELECT run_id, COUNT(*) AS error_count
        FROM pipeline_log_event
        WHERE level = 'ERROR' AND run_id IS NOT NULL
        GROUP BY run_id
    )
    SELECT
        r.run_id,
        r.source,
        s.started_at,
        e.finished_at,
        EXTRACT(EPOCH FROM (e.finished_at::timestamp - s.started_at::timestamp))::numeric(12,3) AS wall_seconds,
        e.total_seconds_logged,
        q.query_count,
        q.total_seconds AS source_query_seconds,
        q.max_seconds   AS slowest_query_seconds,
        q.total_rows,
        COALESCE(er.error_count, 0) AS error_count,
        CASE WHEN COALESCE(er.error_count, 0) > 0 THEN 'failed' ELSE 'ok' END AS status
    FROM runs r
    LEFT JOIN starts s ON s.run_id = r.run_id
    LEFT JOIN ends   e ON e.run_id = r.run_id
    LEFT JOIN qstats q ON q.run_id = r.run_id AND q.source = r.source
    LEFT JOIN errors er ON er.run_id = r.run_id
    ORDER BY s.started_at DESC NULLS LAST, r.source
    """,

    # 7) Pipeline event timeline — readable per-run lifecycle.
    """CREATE OR REPLACE VIEW v_pipeline_event_timeline AS
    SELECT
        source, run_id, ts, level, event,
        thread, "table" AS table_name, batch, rows, seconds,
        error, sql_hash
    FROM pipeline_log_event
    ORDER BY ts DESC
    """,

    # 8) Data quality summary — per (event, env, field) with apply progress.
    """CREATE OR REPLACE VIEW v_pipeline_data_quality AS
    SELECT
        event, env, field,
        SUM(total_issues)         AS total_issues,
        SUM(diff)                 AS diff_count,
        SUM(row_missing_in_es)    AS missing_in_es,
        SUM(row_missing_in_oracle) AS missing_in_oracle,
        SUM(es_value_blank)       AS es_blank,
        SUM(oracle_value_blank)   AS oracle_blank,
        MAX(file_run_ts)          AS last_run_ts
    FROM pipeline_summary
    GROUP BY event, env, field
    ORDER BY total_issues DESC NULLS LAST
    """,

    # 9) Apply progress — per (event, env): how many diff rows applied vs pending.
    """CREATE OR REPLACE VIEW v_pipeline_apply_progress AS
    SELECT
        event, env,
        COUNT(*)                                          AS total_diffs,
        COUNT(*) FILTER (WHERE status = 'applied')        AS applied,
        COUNT(*) FILTER (WHERE status IS DISTINCT FROM 'applied') AS pending,
        MAX(applied_ts) AS last_applied_ts
    FROM pipeline_changes
    GROUP BY event, env
    ORDER BY pending DESC, event, env
    """,
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_stamp(name: str) -> datetime | None:
    m = _STAMP_RE.search(name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
    except ValueError:
        return None


def _event_pks() -> dict[str, str]:
    """{event_name: pk_column} from events.yaml. Default 'id'."""
    if not YAML_PATH.is_file():
        return {}
    cfg = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8")) or {}
    out: dict[str, str] = {}
    for k, v in cfg.items():
        if isinstance(v, dict):
            out[k] = (v.get("PK") or "id").strip()
    return out


def _safe_str(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    s = str(v)
    if s.strip() == "" or s.lower() in ("nan", "none", "<na>", "null"):
        return None
    return s


def _to_int(v) -> int | None:
    s = _safe_str(v)
    if s is None:
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Per-source readers
# ---------------------------------------------------------------------------

def collect_changes(pks: dict[str, str]) -> tuple[list[tuple], list[Path]]:
    rows: list[tuple] = []
    files: list[Path] = []
    for f in sorted(OUT_DIR.glob("*/*/changes/changes_*.csv")):
        rel = f.relative_to(OUT_DIR).as_posix()
        parts = f.relative_to(OUT_DIR).parts
        event = parts[0]
        env = parts[1]
        run_ts = _parse_stamp(f.name)
        pk_col = pks.get(event, "id")
        try:
            df = pd.read_csv(f, dtype=str, keep_default_na=False)
        except Exception as e:
            print(f"  warn: failed to read {rel}: {e}")
            continue
        files.append(f)
        if df.empty:
            continue
        for _, r in df.iterrows():
            rows.append((
                rel, run_ts, event, env,
                _safe_str(r.get(pk_col) or r.get("id")),
                _safe_str(r.get("field")),
                _safe_str(r.get("oracle_value")),
                _safe_str(r.get("es_value")),
                _safe_str(r.get("status")),
            ))
    return rows, files


def collect_missing(pks: dict[str, str]) -> tuple[list[tuple], list[Path]]:
    rows: list[tuple] = []
    files: list[Path] = []
    for f in sorted(OUT_DIR.glob("*/*/changes/missing_in_es_*.csv")):
        rel = f.relative_to(OUT_DIR).as_posix()
        parts = f.relative_to(OUT_DIR).parts
        event = parts[0]
        env = parts[1]
        run_ts = _parse_stamp(f.name)
        pk_col = pks.get(event, "id")
        try:
            df = pd.read_csv(f, dtype=str, keep_default_na=False)
        except Exception as e:
            print(f"  warn: failed to read {rel}: {e}")
            continue
        files.append(f)
        if df.empty:
            continue
        records = df.to_dict(orient="records")
        for r in records:
            doc_id = _safe_str(r.get(pk_col) or r.get("id"))
            payload = {k: _safe_str(v) for k, v in r.items() if _safe_str(v) is not None}
            rows.append((rel, run_ts, event, env, doc_id, json.dumps(payload, ensure_ascii=False)))
    return rows, files


def collect_apply_audit() -> tuple[list[tuple], list[Path]]:
    """Walk out/_apply_log/<env>/*.jsonl. Each line is a JSON object."""
    rows: list[tuple] = []
    files: list[Path] = []
    if not APPLY_LOG_DIR.is_dir():
        return rows, files
    for f in sorted(APPLY_LOG_DIR.rglob("*.jsonl")):
        rel = f.relative_to(OUT_DIR).as_posix()
        ok = True
        try:
            with f.open(encoding="utf-8") as fp:
                for ln_no, raw in enumerate(fp, start=1):
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        obj = json.loads(raw)
                    except json.JSONDecodeError:
                        print(f"  warn: bad JSON at {rel}:{ln_no}")
                        continue
                    rows.append((
                        rel, ln_no,
                        _safe_str(obj.get("ts")),
                        _safe_str(obj.get("type")),
                        _safe_str(obj.get("env")),
                        _safe_str(obj.get("event")),
                        _safe_str(obj.get("index")),
                        _safe_str(obj.get("id")),
                        _to_int(obj.get("batch")),
                        json.dumps(obj, default=str, ensure_ascii=False),
                    ))
        except Exception as e:
            print(f"  warn: failed to read {rel}: {e}")
            ok = False
        if ok:
            files.append(f)
    return rows, files


def collect_summary() -> tuple[list[tuple], list[Path]]:
    rows: list[tuple] = []
    files: list[Path] = []
    for f in sorted(OUT_DIR.glob("summary_*.csv")):
        rel = f.relative_to(OUT_DIR).as_posix()
        run_ts = _parse_stamp(f.name)
        try:
            df = pd.read_csv(f, dtype=str, keep_default_na=False)
        except Exception as e:
            print(f"  warn: failed to read {rel}: {e}")
            continue
        files.append(f)
        for _, r in df.iterrows():
            rows.append((
                rel, run_ts,
                _safe_str(r.get("event")),
                _safe_str(r.get("env")),
                _safe_str(r.get("field")),
                _to_int(r.get("total_issues")),
                _to_int(r.get("diff")),
                _to_int(r.get("row_missing_in_es")),
                _to_int(r.get("row_missing_in_oracle")),
                _to_int(r.get("es_value_blank")),
                _to_int(r.get("oracle_value_blank")),
            ))
    return rows, files


# ---------------------------------------------------------------------------
# Connection-log collectors (incremental, offset-tracked)
# ---------------------------------------------------------------------------

def _read_log_csv(path: Path) -> pd.DataFrame:
    """Read a logging CSV. Tolerates trailing partial line and BOM."""
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig",
                           on_bad_lines="skip")
    except Exception as e:
        print(f"  warn: failed to read {path}: {e}")
        return pd.DataFrame()


def _row_payload(r: dict) -> str:
    """Drop empty values and JSON-encode the rest as the payload column."""
    cleaned = {k: v for k, v in r.items() if _safe_str(v) is not None}
    return json.dumps(cleaned, ensure_ascii=False, default=str)


def collect_log_files(offsets: dict[str, int]) -> dict[str, list[tuple]]:
    """Walk LOG_SOURCES, slice each CSV from its synced offset, build per-table rows.

    Returns mapping {table_name: list[tuple]} plus the special key
    '__offsets__' -> list[(source_file, new_total_rows)] for upsert after insert.
    """
    plan: dict[str, list[tuple]] = {
        "pipeline_log_connection": [],
        "pipeline_log_event": [],
        "pipeline_log_query": [],
    }
    new_offsets: list[tuple[str, int]] = []

    for source, base in LOG_SOURCES:
        if not base.is_dir():
            continue
        for fname in LOG_FILES:
            path = base / fname
            if not path.is_file():
                continue
            rel = f"{source}/{fname}"
            already = int(offsets.get(rel, 0))
            df = _read_log_csv(path)
            total = len(df)
            if total <= already:
                continue
            delta = df.iloc[already:].reset_index(drop=True)

            if fname == "connections.csv":
                for i, r in delta.iterrows():
                    rd = r.to_dict()
                    plan["pipeline_log_connection"].append((
                        source, rel, already + int(i) + 1,
                        _safe_str(rd.get("run_id")),
                        _safe_str(rd.get("ts")),
                        _safe_str(rd.get("tz")),
                        _safe_str(rd.get("hostname")),
                        _safe_str(rd.get("fqdn")),
                        _safe_str(rd.get("os_user")),
                        _safe_str(rd.get("local_ip")),
                        _safe_str(rd.get("public_ip")),
                        _safe_str(rd.get("country")),
                        _safe_str(rd.get("region")),
                        _safe_str(rd.get("city")),
                        _safe_str(rd.get("org")),
                        _safe_str(rd.get("platform")),
                        _safe_str(rd.get("python")),
                        _to_int(rd.get("pid")),
                        _safe_str(rd.get("cwd")),
                        _row_payload(rd),
                    ))
            elif fname == "events.csv":
                for i, r in delta.iterrows():
                    rd = r.to_dict()
                    plan["pipeline_log_event"].append((
                        source, rel, already + int(i) + 1,
                        _safe_str(rd.get("run_id")),
                        _safe_str(rd.get("query_id")),
                        _safe_str(rd.get("ts")),
                        _safe_str(rd.get("tz")),
                        _safe_str(rd.get("level")),
                        _safe_str(rd.get("thread")),
                        _safe_str(rd.get("event")),
                        _safe_str(rd.get("owner")),
                        _safe_str(rd.get("table")),
                        _to_int(rd.get("batch")),
                        _to_int(rd.get("offset")),
                        _to_int(rd.get("limit")),
                        _to_int(rd.get("rows")),
                        _safe_str(rd.get("seconds")),
                        _safe_str(rd.get("rows_per_sec")),
                        _safe_str(rd.get("sql")),
                        _safe_str(rd.get("sql_hash")),
                        _safe_str(rd.get("error")),
                        _safe_str(rd.get("path")),
                        _row_payload(rd),
                    ))
            elif fname == "queries.csv":
                for i, r in delta.iterrows():
                    rd = r.to_dict()
                    plan["pipeline_log_query"].append((
                        source, rel, already + int(i) + 1,
                        _safe_str(rd.get("query_id")),
                        _safe_str(rd.get("run_id")),
                        _safe_str(rd.get("sql_hash")),
                        _safe_str(rd.get("start_ts")),
                        _safe_str(rd.get("end_ts")),
                        _safe_str(rd.get("tz")),
                        _safe_str(rd.get("seconds")),
                        _to_int(rd.get("rows")),
                        _safe_str(rd.get("rows_per_sec")),
                        _safe_str(rd.get("status")),
                        _safe_str(rd.get("error")),
                        _safe_str(rd.get("owner")),
                        _safe_str(rd.get("table")),
                        _to_int(rd.get("batch")),
                        _safe_str(rd.get("thread")),
                        _safe_str(rd.get("params")),
                        _safe_str(rd.get("sql")),
                        _row_payload(rd),
                    ))
            new_offsets.append((rel, total))

    plan["__offsets__"] = new_offsets  # type: ignore[assignment]
    return plan


def fetch_offsets(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT source_file, rows_synced FROM pipeline_log_offsets")
        return {r[0]: int(r[1]) for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# Postgres writer
# ---------------------------------------------------------------------------

INSERTS = {
    "pipeline_changes": (
        "INSERT INTO pipeline_changes "
        "(source_file, file_run_ts, event, env, doc_id, field, oracle_value, es_value, status) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    ),
    "pipeline_missing": (
        "INSERT INTO pipeline_missing "
        "(source_file, file_run_ts, event, env, doc_id, payload) "
        "VALUES (%s, %s, %s, %s, %s, %s::jsonb)"
    ),
    "pipeline_apply_audit": (
        "INSERT INTO pipeline_apply_audit "
        "(source_file, line_no, event_ts, record_type, env, event, index_name, doc_id, batch, raw) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
    ),
    "pipeline_summary": (
        "INSERT INTO pipeline_summary "
        "(source_file, file_run_ts, event, env, field, total_issues, diff, "
        "row_missing_in_es, row_missing_in_oracle, es_value_blank, oracle_value_blank) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    ),
    "pipeline_log_connection": (
        "INSERT INTO pipeline_log_connection "
        "(source, source_file, csv_row_no, run_id, ts, tz, "
        "hostname, fqdn, os_user, local_ip, public_ip, country, region, city, org, "
        "platform, python, pid, cwd, payload) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s::jsonb) "
        "ON CONFLICT (source_file, csv_row_no) DO NOTHING"
    ),
    "pipeline_log_event": (
        "INSERT INTO pipeline_log_event "
        "(source, source_file, csv_row_no, run_id, query_id, ts, tz, level, thread, event, "
        "owner, \"table\", batch, \"offset\", \"limit\", rows, seconds, rows_per_sec, "
        "sql, sql_hash, error, path, payload) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s::jsonb) "
        "ON CONFLICT (source_file, csv_row_no) DO NOTHING"
    ),
    "pipeline_log_query": (
        "INSERT INTO pipeline_log_query "
        "(source, source_file, csv_row_no, query_id, run_id, sql_hash, start_ts, end_ts, tz, "
        "seconds, rows, rows_per_sec, status, error, "
        "owner, \"table\", batch, thread, params, sql, payload) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, %s, %s::jsonb) "
        "ON CONFLICT (source_file, csv_row_no) DO NOTHING"
    ),
}

UPSERT_OFFSET = (
    "INSERT INTO pipeline_log_offsets (source_file, rows_synced, last_sync) "
    "VALUES (%s, %s, now()) "
    "ON CONFLICT (source_file) DO UPDATE "
    "SET rows_synced = EXCLUDED.rows_synced, last_sync = EXCLUDED.last_sync"
)


# Map output table -> (post-insert verify SQL: per source_file row count).
_VERIFY_TABLE_SQL = {
    "pipeline_changes":     "SELECT COUNT(*) FROM pipeline_changes WHERE source_file = %s",
    "pipeline_missing":     "SELECT COUNT(*) FROM pipeline_missing WHERE source_file = %s",
    "pipeline_apply_audit": "SELECT COUNT(*) FROM pipeline_apply_audit WHERE source_file = %s",
    "pipeline_summary":     "SELECT COUNT(*) FROM pipeline_summary WHERE source_file = %s",
}

_LOG_TABLE_BY_FNAME = {
    "connections.csv": "pipeline_log_connection",
    "events.csv":      "pipeline_log_event",
    "queries.csv":     "pipeline_log_query",
}


def _log_path_from_rel(rel: str) -> Path | None:
    src, _, fname = rel.partition("/")
    for s, base in LOG_SOURCES:
        if s == src:
            return base / fname
    return None


def _is_apply_consumed(conn, mode: str, rel: str) -> bool:
    """True if pipeline_apply_batches records this CSV as already pushed to ES."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pipeline_apply_batches "
                "WHERE mode = %s AND source_file = %s LIMIT 1",
                (mode, rel),
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def _verify_and_cleanup_outputs(conn, files_by_table: dict[str, list[Path]],
                                expected_rows_by_file: dict[str, int],
                                say) -> tuple[int, int]:
    """Per-file: SELECT COUNT WHERE source_file = rel. If >= expected, delete file.
    Special-case changes_/missing_ CSVs: only delete if apply_changes has already
    pushed them to ES (i.e. row exists in pipeline_apply_batches).
    Returns (deleted, kept). Empty files (expected 0) deleted unconditionally."""
    deleted = kept = 0
    for table, files in files_by_table.items():
        verify_sql = _VERIFY_TABLE_SQL.get(table)
        if not verify_sql:
            continue
        for f in files:
            try:
                rel = f.relative_to(OUT_DIR).as_posix()
            except ValueError:
                rel = f.as_posix()
            expected = expected_rows_by_file.get(rel, 0)
            try:
                with conn.cursor() as cur:
                    cur.execute(verify_sql, (rel,))
                    actual = int(cur.fetchone()[0])
            except Exception as e:
                say(f"  verify failed for {rel}: {type(e).__name__}: {e}")
                kept += 1
                continue
            if actual < expected:
                say(f"  KEEP {rel}: PG has {actual} rows, expected ≥ {expected}")
                kept += 1
                continue

            # Gate: changes/missing CSVs are still needed by apply_changes
            # until they've been pushed to ES.
            if table == "pipeline_changes" and not _is_apply_consumed(conn, "changes", rel):
                say(f"  KEEP {rel}: not yet applied to ES (run apply_changes first)")
                kept += 1
                continue
            if table == "pipeline_missing" and not _is_apply_consumed(conn, "missing", rel):
                say(f"  KEEP {rel}: not yet applied to ES (run apply_changes first)")
                kept += 1
                continue

            try:
                f.unlink()
                deleted += 1
            except Exception as e:
                say(f"  delete failed for {rel}: {type(e).__name__}: {e}")
                kept += 1
    return deleted, kept


def _verify_and_cleanup_logs(conn, log_offsets_synced: list[tuple[str, int]],
                             say) -> tuple[int, int]:
    """For each (rel, total) we synced: verify PG has >= total rows for that source_file,
    then delete the file AND the offset row so a fresh file starts at 0."""
    deleted = kept = 0
    for rel, total in log_offsets_synced:
        _src, _, fname = rel.partition("/")
        table = _LOG_TABLE_BY_FNAME.get(fname)
        if table is None:
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE source_file = %s", (rel,))
                actual = int(cur.fetchone()[0])
        except Exception as e:
            say(f"  verify failed for log {rel}: {type(e).__name__}: {e}")
            kept += 1
            continue
        if actual < total:
            say(f"  KEEP log {rel}: PG has {actual}, expected ≥ {total}")
            kept += 1
            continue
        path = _log_path_from_rel(rel)
        if path is None or not path.exists():
            kept += 1
            continue
        try:
            path.unlink()
            with conn.cursor() as cur:
                cur.execute("DELETE FROM pipeline_log_offsets WHERE source_file = %s", (rel,))
            conn.commit()
            deleted += 1
        except Exception as e:
            say(f"  delete failed for log {rel}: {type(e).__name__}: {e}")
            try: conn.rollback()
            except Exception: pass
            kept += 1
    return deleted, kept


def _prune_empty_dirs(say) -> None:
    """Walk OUT_DIR bottom-up, remove empty dirs (but keep OUT_DIR itself)."""
    if not OUT_DIR.is_dir():
        return
    removed = 0
    for d in sorted((p for p in OUT_DIR.rglob("*") if p.is_dir()),
                    key=lambda p: len(p.parts), reverse=True):
        try:
            if d != OUT_DIR and not any(d.iterdir()):
                d.rmdir()
                removed += 1
        except Exception:
            pass
    if removed:
        say(f"  pruned {removed} empty dir(s) under out/")


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
    conn.commit()
    ensure_views(conn)


def ensure_views(conn) -> None:
    """CREATE OR REPLACE VIEWs. Failures of one view don't abort the rest."""
    for stmt in VIEWS:
        try:
            with conn.cursor() as cur:
                cur.execute(stmt)
            conn.commit()
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            head = stmt.strip().split("\n", 1)[0][:80]
            print(f"  warn: view failed: {head} -> {type(e).__name__}: {e}")


def insert_many(conn, sql: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        n = cur.rowcount
    conn.commit()
    return n if n is not None and n >= 0 else len(rows)


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

def run_sync(only: str = "changes,missing,apply_audit,summary,logs",
             dry: bool = False, quiet: bool = False) -> int:
    """Programmatic entry. Returns 0 on success, 1 on any failure.
    Never raises. Safe to call from main.py / apply_changes after a run.
    """
    def _say(msg: str) -> None:
        if not quiet:
            print(msg)

    from .logging_setup import default_logger
    log = default_logger()
    log.event("sync_start", only=only, dry=dry)

    selected = {s.strip() for s in only.split(",") if s.strip()}

    out_missing = not OUT_DIR.is_dir()
    if out_missing:
        _say(f"out/ not found at {OUT_DIR} — will still ensure tables exist")

    pks = _event_pks()
    plan: dict[str, list[tuple]] = {}
    files_by_table: dict[str, list[Path]] = {}
    if not out_missing:
        if "changes" in selected:
            rows, files = collect_changes(pks)
            plan["pipeline_changes"] = rows
            files_by_table["pipeline_changes"] = files
        if "missing" in selected:
            rows, files = collect_missing(pks)
            plan["pipeline_missing"] = rows
            files_by_table["pipeline_missing"] = files
        if "apply_audit" in selected:
            rows, files = collect_apply_audit()
            plan["pipeline_apply_audit"] = rows
            files_by_table["pipeline_apply_audit"] = files
        if "summary" in selected:
            rows, files = collect_summary()
            plan["pipeline_summary"] = rows
            files_by_table["pipeline_summary"] = files

    # Per-file expected counts (so verify_and_cleanup knows the bar to clear).
    expected_rows_by_file: dict[str, int] = {}
    for tbl, rows in plan.items():
        for row in rows:
            rel = row[0]  # source_file is always col 0 in our INSERT tuples
            expected_rows_by_file[rel] = expected_rows_by_file.get(rel, 0) + 1

    total = sum(len(v) for v in plan.values())
    _say("planned inserts:" if total else "no rows collected — will only ensure tables exist")
    for tbl, rows in plan.items():
        _say(f"  {tbl}: {len(rows)} rows")

    if dry:
        if "logs" in selected:
            _say("[DRY] log mirror needs DB-side offsets — skipped in dry mode")
        _say("[DRY] not writing to Postgres")
        return 0

    try:
        from connect_into_postgres import connect_to_postgres as pg
    except SystemExit as e:
        _say(f"WARN: postgres connector refused to load: {e}")
        return 1

    try:
        conn = pg.create_connection()
    except Exception as e:
        _say(f"WARN: postgres unreachable, skipping sync: {type(e).__name__}: {e}")
        return 1

    log_offsets_synced: list[tuple[str, int]] = []
    sync_ok = False
    try:
        ensure_tables(conn)
        _say("  tables ensured (CREATE TABLE IF NOT EXISTS)")
        log.event("ensure_tables", count=len(DDL))
        for tbl, rows in plan.items():
            n = insert_many(conn, INSERTS[tbl], rows)
            _say(f"  {tbl}: inserted {n}")
            log.event("insert", table=tbl, rows=n)

        if "logs" in selected:
            offsets = fetch_offsets(conn)
            log_plan = collect_log_files(offsets)
            log_offsets_synced = log_plan.pop("__offsets__")  # type: ignore[arg-type]
            log_total = sum(len(v) for v in log_plan.values())
            _say(f"log mirror: {log_total} new rows across {len(log_offsets_synced)} file(s)")
            log.event("log_mirror", total_rows=log_total, count=len(log_offsets_synced))
            for tbl, rows in log_plan.items():
                if rows:
                    n = insert_many(conn, INSERTS[tbl], rows)
                    _say(f"  {tbl}: inserted {n}")
                    log.event("insert", table=tbl, rows=n)
            if log_offsets_synced:
                with conn.cursor() as cur:
                    cur.executemany(UPSERT_OFFSET, log_offsets_synced)
                conn.commit()

        sync_ok = True
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        _say(f"WARN: postgres insert failed, partial run rolled back: {type(e).__name__}: {e}")
        log.event("sync_failed", level="ERROR", error=str(e))
        try: conn.close()
        except Exception: pass
        return 1

    # ---- Verify + cleanup ----
    keep_csv = env_truthy("PIPELINE_KEEP_CSV")
    if sync_ok and not keep_csv:
        try:
            d1, k1 = _verify_and_cleanup_outputs(conn, files_by_table,
                                                 expected_rows_by_file, _say)
            d2, k2 = _verify_and_cleanup_logs(conn, log_offsets_synced, _say)
            _prune_empty_dirs(_say)
            _say(f"cleanup: deleted {d1 + d2} file(s), kept {k1 + k2} file(s)")
            log.event("cleanup", level="INFO",
                      count=d1 + d2, total=d1 + d2 + k1 + k2)
        except Exception as e:
            _say(f"WARN: cleanup encountered an error (data IS in PG; files left alone): "
                 f"{type(e).__name__}: {e}")
            log.event("cleanup_failed", level="ERROR", error=str(e))
    elif keep_csv:
        _say("PIPELINE_KEEP_CSV=1 → leaving CSVs on disk")

    try:
        conn.close()
    except Exception:
        pass

    _say("done.")
    log.event("sync_end")
    return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--only", default="changes,missing,apply_audit,summary,logs",
                   help="comma list: any of changes/missing/apply_audit/summary/logs")
    p.add_argument("--dry", action="store_true", help="collect + count, do not write")
    p.add_argument("--quiet", action="store_true", help="suppress progress prints")
    args = p.parse_args()
    sys.exit(run_sync(only=args.only, dry=args.dry, quiet=args.quiet))


if __name__ == "__main__":
    main()
