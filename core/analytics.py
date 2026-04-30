"""DuckDB-equivalent SQL for Streamlit analytics panels.

Each function returns (df, source). DuckDB is preferred; PG is the fallback
when DuckDB has no data or fails. The `pg_module` + `pg_conn` arguments are
optional — when omitted, DuckDB-only mode is used.

Phase B: read parity with the PG `v_pipeline_*` views so app.py can switch
panels over without losing functionality. Heavy PG tables stay in place.
"""
from __future__ import annotations

from typing import Any, Optional

from core import duckdb_catalog


# ---------------------------------------------------------------------------
# DuckDB SQL — mirrors the PG views in connect_into_postgres.sync_out.VIEWS
# ---------------------------------------------------------------------------

# Connection log: numeric/date casts so DuckDB types match PG output where
# the UI expects them.
_DUCK_CONN_OVERVIEW = """
WITH conn AS (
    SELECT 'success'::VARCHAR AS status,
           regexp_extract(filename, 'connect_into_([^/\\\\]+)', 1) AS source
    FROM v_log_connections
),
fail AS (
    SELECT 'failed'::VARCHAR AS status,
           regexp_extract(filename, 'connect_into_([^/\\\\]+)', 1) AS source
    FROM v_log_events
    WHERE event LIKE '%connect_failed%' OR (level = 'ERROR' AND event LIKE 'pg_%')
)
SELECT source,
       COUNT(*) FILTER (WHERE status = 'success') AS successes,
       COUNT(*) FILTER (WHERE status = 'failed')  AS failures,
       COUNT(*)                                   AS total
FROM (SELECT * FROM conn UNION ALL SELECT * FROM fail) u
GROUP BY source
ORDER BY source
"""

_DUCK_CONNECTIONS_RECENT = """
WITH conn AS (
    SELECT regexp_extract(filename, 'connect_into_([^/\\\\]+)', 1) AS source,
           run_id, ts AS start_ts,
           hostname, country, city,
           'success'::VARCHAR AS status,
           NULL::VARCHAR AS error
    FROM v_log_connections
),
fail AS (
    SELECT regexp_extract(filename, 'connect_into_([^/\\\\]+)', 1) AS source,
           run_id, ts AS start_ts,
           NULL::VARCHAR AS hostname, NULL::VARCHAR AS country, NULL::VARCHAR AS city,
           'failed'::VARCHAR AS status, error
    FROM v_log_events
    WHERE event LIKE '%connect_failed%' OR (level = 'ERROR' AND event LIKE 'pg_%')
)
SELECT * FROM (SELECT * FROM conn UNION ALL SELECT * FROM fail) u
{where}
ORDER BY start_ts DESC NULLS LAST
LIMIT 200
"""

_DUCK_QUERY_SLOWEST = """
SELECT
    ROW_NUMBER() OVER () AS id,
    regexp_extract(filename, 'connect_into_([^/\\\\]+)', 1) AS source,
    run_id, "table" AS query_table, owner, batch,
    TRY_CAST(seconds AS DOUBLE) AS seconds,
    TRY_CAST(rows AS BIGINT) AS rows,
    TRY_CAST(rows_per_sec AS DOUBLE) AS rows_per_sec,
    status, sql_hash, start_ts, error
FROM v_log_queries
WHERE TRY_CAST(seconds AS DOUBLE) IS NOT NULL
ORDER BY TRY_CAST(seconds AS DOUBLE) DESC
LIMIT 50
"""

_DUCK_QUERY_HASHES = """
SELECT
    sql_hash,
    regexp_extract(any_value(filename), 'connect_into_([^/\\\\]+)', 1) AS source,
    MAX("table") AS query_table,
    COUNT(*) AS total_runs,
    ROUND(AVG(TRY_CAST(seconds AS DOUBLE)), 3) AS avg_seconds,
    ROUND(MAX(TRY_CAST(seconds AS DOUBLE)), 3) AS max_seconds,
    SUM(TRY_CAST(rows AS BIGINT)) AS total_rows,
    MIN(start_ts) AS first_seen,
    MAX(start_ts) AS last_seen,
    LEFT(MAX(sql), 120) AS sql_preview
FROM v_log_queries
WHERE sql_hash IS NOT NULL AND sql_hash <> ''
  AND TRY_CAST(seconds AS DOUBLE) IS NOT NULL
GROUP BY sql_hash
ORDER BY total_runs DESC, avg_seconds DESC
LIMIT 200
"""

# DuckDB query_by_hour: extract hour from start_ts.
_DUCK_QUERY_BY_HOUR = """
SELECT
    EXTRACT(hour FROM CAST(start_ts AS TIMESTAMP)) AS hour_of_day,
    COUNT(*) AS runs,
    ROUND(AVG(TRY_CAST(seconds AS DOUBLE)), 3) AS avg_seconds,
    ROUND(MIN(TRY_CAST(seconds AS DOUBLE)), 3) AS min_seconds,
    ROUND(MAX(TRY_CAST(seconds AS DOUBLE)), 3) AS max_seconds,
    SUM(TRY_CAST(rows AS BIGINT)) AS total_rows,
    ROUND(AVG(TRY_CAST(rows_per_sec AS DOUBLE)), 1) AS avg_rows_per_sec
FROM v_log_queries
WHERE sql_hash = ?
  AND TRY_CAST(seconds AS DOUBLE) IS NOT NULL
GROUP BY hour_of_day
ORDER BY hour_of_day
"""

_DUCK_QUERY_PERF_BASE = """
SELECT
    ROW_NUMBER() OVER () AS id,
    regexp_extract(filename, 'connect_into_([^/\\\\]+)', 1) AS source,
    run_id, "table" AS query_table, batch,
    start_ts,
    TRY_CAST(seconds AS DOUBLE) AS seconds,
    TRY_CAST(rows AS BIGINT) AS rows,
    TRY_CAST(rows_per_sec AS DOUBLE) AS rows_per_sec,
    status,
    (TRY_CAST(seconds AS DOUBLE) > 5) AS slow_query,
    sql_hash
FROM v_log_queries
{where}
ORDER BY start_ts DESC NULLS LAST
LIMIT 500
"""

_DUCK_RUN_METRICS = """
WITH starts AS (
    SELECT run_id, MIN(ts) AS started_at
    FROM v_log_connections
    WHERE run_id IS NOT NULL
    GROUP BY run_id
),
ends AS (
    SELECT run_id, MAX(ts) AS finished_at
    FROM v_log_events
    WHERE event = 'run_end' AND run_id IS NOT NULL
    GROUP BY run_id
),
qstats AS (
    SELECT run_id,
           regexp_extract(any_value(filename), 'connect_into_([^/\\\\]+)', 1) AS source,
           COUNT(*) AS query_count,
           ROUND(SUM(TRY_CAST(seconds AS DOUBLE)), 3) AS total_seconds,
           ROUND(MAX(TRY_CAST(seconds AS DOUBLE)), 3) AS max_seconds,
           SUM(TRY_CAST(rows AS BIGINT)) AS total_rows
    FROM v_log_queries
    WHERE run_id IS NOT NULL
    GROUP BY run_id
),
errors AS (
    SELECT run_id, COUNT(*) AS error_count
    FROM v_log_events
    WHERE level = 'ERROR' AND run_id IS NOT NULL
    GROUP BY run_id
)
SELECT
    s.run_id,
    q.source,
    s.started_at,
    e.finished_at,
    q.query_count,
    q.total_seconds AS source_query_seconds,
    q.max_seconds   AS slowest_query_seconds,
    q.total_rows,
    COALESCE(er.error_count, 0) AS error_count,
    CASE WHEN COALESCE(er.error_count, 0) > 0 THEN 'failed' ELSE 'ok' END AS status
FROM starts s
LEFT JOIN ends   e ON e.run_id = s.run_id
LEFT JOIN qstats q ON q.run_id = s.run_id
LEFT JOIN errors er ON er.run_id = s.run_id
ORDER BY s.started_at DESC NULLS LAST
LIMIT 100
"""

_DUCK_RECENT_ERRORS = """
SELECT
    regexp_extract(filename, 'connect_into_([^/\\\\]+)', 1) AS source,
    run_id, ts, event, "table" AS table_name, error
FROM v_log_events
WHERE level = 'ERROR'
ORDER BY ts DESC
LIMIT 50
"""

_DUCK_DATA_QUALITY = """
SELECT
    event, env, field,
    SUM(TRY_CAST(total_issues AS BIGINT))         AS total_issues,
    SUM(TRY_CAST(diff AS BIGINT))                 AS diff_count,
    SUM(TRY_CAST(row_missing_in_es AS BIGINT))    AS missing_in_es,
    SUM(TRY_CAST(row_missing_in_oracle AS BIGINT)) AS missing_in_oracle,
    SUM(TRY_CAST(es_value_blank AS BIGINT))       AS es_blank,
    SUM(TRY_CAST(oracle_value_blank AS BIGINT))   AS oracle_blank
FROM v_summary
GROUP BY event, env, field
ORDER BY total_issues DESC NULLS LAST
LIMIT 200
"""

# v_changes / v_missing carry filename — derive event/env from path. Layout:
# .../out/<EVENT>/<ENV>/changes/{changes,missing_in_es}_*.parquet
_DUCK_CHANGES_BROWSE = """
SELECT
    {pk_select} AS doc_id,
    field, oracle_value, es_value, status,
    regexp_extract(replace(filename, chr(92), '/'), '/out/([^/]+)/([^/]+)/changes/', 1) AS event,
    regexp_extract(replace(filename, chr(92), '/'), '/out/([^/]+)/([^/]+)/changes/', 2) AS env,
    filename AS source_file
FROM v_changes
{where}
LIMIT 1000
"""

_DUCK_MISSING_BROWSE = """
SELECT
    *,
    regexp_extract(replace(filename, chr(92), '/'), '/out/([^/]+)/([^/]+)/changes/', 1) AS event,
    regexp_extract(replace(filename, chr(92), '/'), '/out/([^/]+)/([^/]+)/changes/', 2) AS env
FROM v_missing
{where}
LIMIT 500
"""

_DUCK_CHANGES_DISTINCT_EVENTS = """
SELECT DISTINCT
    regexp_extract(replace(filename, chr(92), '/'), '/out/([^/]+)/([^/]+)/changes/', 1) AS event
FROM v_changes
WHERE event IS NOT NULL AND event <> ''
ORDER BY event
"""


# ---------------------------------------------------------------------------
# PG SQL — verbatim copies of what app.py used. Kept here so app.py can
# call one function instead of duplicating both.
# ---------------------------------------------------------------------------

_PG_RUN_METRICS = "SELECT * FROM v_pipeline_run_metrics LIMIT 100"
_PG_RECENT_ERRORS = (
    "SELECT source, run_id, ts, event, \"table\" AS table_name, error "
    "FROM pipeline_log_event WHERE level = 'ERROR' ORDER BY ts DESC LIMIT 50"
)
_PG_CONN_OVERVIEW = "SELECT * FROM v_pipeline_connection_overview"
_PG_QUERY_SLOWEST = "SELECT * FROM v_pipeline_query_slowest"
_PG_QUERY_HASHES = "SELECT * FROM v_pipeline_query_hashes LIMIT 200"
_PG_DATA_QUALITY = "SELECT * FROM v_pipeline_data_quality LIMIT 200"


# ---------------------------------------------------------------------------
# Public API — one function per panel.
# Each returns (df, source) where source in {'duckdb', 'pg', 'failed'}.
# ---------------------------------------------------------------------------

def run_metrics(pg_module=None, pg_conn=None):
    return duckdb_catalog.query_or_fallback(
        _DUCK_RUN_METRICS, None,
        _PG_RUN_METRICS, None, pg_module, pg_conn,
    )


def recent_errors(pg_module=None, pg_conn=None):
    return duckdb_catalog.query_or_fallback(
        _DUCK_RECENT_ERRORS, None,
        _PG_RECENT_ERRORS, None, pg_module, pg_conn,
    )


def connection_overview(pg_module=None, pg_conn=None):
    return duckdb_catalog.query_or_fallback(
        _DUCK_CONN_OVERVIEW, None,
        _PG_CONN_OVERVIEW, None, pg_module, pg_conn,
    )


def connections_recent(source: Optional[str] = None,
                       status: Optional[str] = None,
                       pg_module=None, pg_conn=None):
    duck_where_parts: list[str] = []
    duck_params: list[Any] = []
    if source:
        duck_where_parts.append("source = ?"); duck_params.append(source)
    if status:
        duck_where_parts.append("status = ?"); duck_params.append(status)
    duck_where = ("WHERE " + " AND ".join(duck_where_parts)) if duck_where_parts else ""
    duck_sql = _DUCK_CONNECTIONS_RECENT.format(where=duck_where)

    pg_where_parts: list[str] = []
    pg_params: list[Any] = []
    if source:
        pg_where_parts.append("source = %s"); pg_params.append(source)
    if status:
        pg_where_parts.append("status = %s"); pg_params.append(status)
    pg_where = ("WHERE " + " AND ".join(pg_where_parts)) if pg_where_parts else ""
    pg_sql = (f"SELECT * FROM v_pipeline_connections {pg_where} "
              f"ORDER BY start_ts DESC NULLS LAST LIMIT 200")
    return duckdb_catalog.query_or_fallback(
        duck_sql, tuple(duck_params) if duck_params else None,
        pg_sql,   tuple(pg_params)   if pg_params   else None,
        pg_module, pg_conn,
    )


def query_slowest(pg_module=None, pg_conn=None):
    return duckdb_catalog.query_or_fallback(
        _DUCK_QUERY_SLOWEST, None,
        _PG_QUERY_SLOWEST, None, pg_module, pg_conn,
    )


def query_hashes(pg_module=None, pg_conn=None):
    return duckdb_catalog.query_or_fallback(
        _DUCK_QUERY_HASHES, None,
        _PG_QUERY_HASHES, None, pg_module, pg_conn,
    )


def query_by_hour(sql_hash: str, source: Optional[str] = None,
                  pg_module=None, pg_conn=None):
    pg_sql = (
        "SELECT hour_of_day, runs, avg_seconds, min_seconds, max_seconds, "
        "total_rows, avg_rows_per_sec FROM v_pipeline_query_by_hour "
        "WHERE sql_hash = %s AND source = %s ORDER BY hour_of_day"
    )
    return duckdb_catalog.query_or_fallback(
        _DUCK_QUERY_BY_HOUR, (sql_hash,),
        pg_sql, (sql_hash, source) if source else (sql_hash, ""),
        pg_module, pg_conn,
    )


def query_perf(only_slow: bool = False, source: Optional[str] = None,
               pg_module=None, pg_conn=None):
    duck_where_parts: list[str] = []
    duck_params: list[Any] = []
    if only_slow:
        duck_where_parts.append("TRY_CAST(seconds AS DOUBLE) > 5")
    if source:
        duck_where_parts.append(
            "regexp_extract(filename, 'connect_into_([^/\\\\\\\\]+)', 1) = ?"
        )
        duck_params.append(source)
    duck_where = ("WHERE " + " AND ".join(duck_where_parts)) if duck_where_parts else ""
    duck_sql = _DUCK_QUERY_PERF_BASE.format(where=duck_where)

    pg_where_parts: list[str] = []
    pg_params: list[Any] = []
    if only_slow:
        pg_where_parts.append("slow_query = TRUE")
    if source:
        pg_where_parts.append("source = %s"); pg_params.append(source)
    pg_where = ("WHERE " + " AND ".join(pg_where_parts)) if pg_where_parts else ""
    pg_sql = (
        f"SELECT id, source, run_id, query_table, batch, start_ts, seconds, "
        f"rows, rows_per_sec, status, slow_query, sql_hash "
        f"FROM v_pipeline_query_perf {pg_where} "
        f"ORDER BY start_ts DESC NULLS LAST LIMIT 500"
    )
    return duckdb_catalog.query_or_fallback(
        duck_sql, tuple(duck_params) if duck_params else None,
        pg_sql, tuple(pg_params) if pg_params else None,
        pg_module, pg_conn,
    )


def data_quality(pg_module=None, pg_conn=None):
    return duckdb_catalog.query_or_fallback(
        _DUCK_DATA_QUALITY, None,
        _PG_DATA_QUALITY, None, pg_module, pg_conn,
    )


def changes_distinct_events(pg_module=None, pg_conn=None):
    pg_sql = "SELECT DISTINCT event FROM pipeline_changes ORDER BY event"
    return duckdb_catalog.query_or_fallback(
        _DUCK_CHANGES_DISTINCT_EVENTS, None,
        pg_sql, None, pg_module, pg_conn,
    )


def changes_browse(pk: str = "id",
                   event: Optional[str] = None,
                   env: Optional[str] = None,
                   status: Optional[str] = None,
                   pg_module=None, pg_conn=None):
    """Field-level diff browser. DuckDB reads from v_changes; PG fallback
    reads from pipeline_changes (which has filter columns directly)."""
    # Build DuckDB inline filter — event/env extracted from filename in select,
    # so we must use a CTE.
    duck_where_parts: list[str] = []
    duck_params: list[Any] = []
    if status:
        duck_where_parts.append("status = ?"); duck_params.append(status)
    base_where = ("WHERE " + " AND ".join(duck_where_parts)) if duck_where_parts else ""
    duck_sql = _DUCK_CHANGES_BROWSE.format(
        pk_select=f"COALESCE(CAST(\"{pk}\" AS VARCHAR), CAST(id AS VARCHAR))" if pk != "id"
                  else "CAST(id AS VARCHAR)",
        where=base_where,
    )
    # Wrap DuckDB SQL in outer filter on extracted event/env.
    outer_parts: list[str] = []
    outer_params: list[Any] = list(duck_params)
    if event:
        outer_parts.append("event = ?"); outer_params.append(event)
    if env:
        outer_parts.append("env = ?"); outer_params.append(env)
    if outer_parts:
        duck_sql = (f"SELECT * FROM ({duck_sql.replace('LIMIT 1000', '')}) "
                    f"WHERE {' AND '.join(outer_parts)} LIMIT 1000")

    # PG side — straightforward.
    pg_where_parts: list[str] = []
    pg_params: list[Any] = []
    if event:  pg_where_parts.append("event = %s");  pg_params.append(event)
    if env:    pg_where_parts.append("env = %s");    pg_params.append(env)
    if status: pg_where_parts.append("status = %s"); pg_params.append(status)
    pg_where = ("WHERE " + " AND ".join(pg_where_parts)) if pg_where_parts else ""
    pg_sql = (f"SELECT * FROM pipeline_changes {pg_where} "
              f"ORDER BY id DESC LIMIT 1000")
    return duckdb_catalog.query_or_fallback(
        duck_sql, tuple(outer_params) if outer_params else None,
        pg_sql, tuple(pg_params) if pg_params else None,
        pg_module, pg_conn,
    )


def missing_browse(event: Optional[str] = None,
                   env: Optional[str] = None,
                   pg_module=None, pg_conn=None):
    """Missing-rows browser. DuckDB reads payload columns directly."""
    duck_outer: list[str] = []
    duck_params: list[Any] = []
    if event:
        duck_outer.append("event = ?"); duck_params.append(event)
    if env:
        duck_outer.append("env = ?"); duck_params.append(env)
    inner = _DUCK_MISSING_BROWSE.format(where="")
    inner = inner.replace("LIMIT 500", "")
    if duck_outer:
        duck_sql = f"SELECT * FROM ({inner}) WHERE {' AND '.join(duck_outer)} LIMIT 500"
    else:
        duck_sql = inner + " LIMIT 500"

    pg_where_parts: list[str] = []
    pg_params: list[Any] = []
    if event: pg_where_parts.append("event = %s"); pg_params.append(event)
    if env:   pg_where_parts.append("env = %s");   pg_params.append(env)
    pg_where = ("WHERE " + " AND ".join(pg_where_parts)) if pg_where_parts else ""
    pg_sql = (f"SELECT id, sync_ts, event, env, doc_id, applied_ts, payload "
              f"FROM pipeline_missing {pg_where} "
              f"ORDER BY id DESC LIMIT 500")
    return duckdb_catalog.query_or_fallback(
        duck_sql, tuple(duck_params) if duck_params else None,
        pg_sql, tuple(pg_params) if pg_params else None,
        pg_module, pg_conn,
    )
