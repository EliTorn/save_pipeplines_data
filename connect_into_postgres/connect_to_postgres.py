"""Postgres connection layer — env-driven, mirrors connect_to_orcal.py.

Required env vars (set in .env or shell):
    PG_HOST       e.g. localhost / 192.168.x.y / RDS endpoint
    PG_DB         database name (e.g. etl_catalog)
    PG_USER
    PG_PASSWORD

Optional:
    PG_PORT       default 5432
    PG_SCHEMA     default 'catalog'  (used as search_path on connect)
    PG_SSLMODE    default 'prefer'   ('disable', 'require', 'verify-full', ...)
    PG_QUERY_TIMEOUT_MS  default 600000  (10 min statement timeout)

Install once:
    pip install "psycopg[binary]>=3.1"
"""
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

try:
    import psycopg
    _PSYCOPG_VERSION = 3
except ImportError:
    try:
        import psycopg2 as psycopg
        _PSYCOPG_VERSION = 2
    except ImportError:
        sys.exit("psycopg not installed. Run: pip install \"psycopg[binary]>=3.1\"")

load_dotenv()

_PARENT = Path(__file__).resolve().parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from _pipeline_host import host_info  # noqa: E402
from .logging_setup import default_logger  # noqa: E402

_logger = default_logger()

REQUIRED_VARS = ("PG_HOST", "PG_DB", "PG_USER")


def _cfg() -> dict:
    """Read env vars at call-time (so Streamlit etc. don't die at import)."""
    missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")
    return dict(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD", ""),
        schema=os.getenv("PG_SCHEMA", "public"),
        sslmode=os.getenv("PG_SSLMODE", "prefer"),
        timeout_ms=int(os.getenv("PG_QUERY_TIMEOUT_MS", "600000")),
    )


# Backwards-compat module-level constants (read once at import, may be empty in
# environments where .env isn't loaded yet — prefer _cfg() at runtime).
PG_HOST = os.getenv("PG_HOST")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")
PG_SSLMODE = os.getenv("PG_SSLMODE", "prefer")
PG_QUERY_TIMEOUT_MS = int(os.getenv("PG_QUERY_TIMEOUT_MS", "600000"))


def create_connection():
    """Open a new Postgres connection. Reads env at call-time. Raises on misconfig.

    Honors PG_CONNECT_TIMEOUT (default 5 sec) so misconfigured hosts fail fast
    instead of blocking the UI for ~75 sec on the kernel TCP timeout.
    """
    c = _cfg()
    connect_timeout = int(os.getenv("PG_CONNECT_TIMEOUT", "5"))
    try:
        conn = psycopg.connect(
            host=c["host"], port=c["port"], dbname=c["dbname"],
            user=c["user"], password=c["password"], sslmode=c["sslmode"],
            connect_timeout=connect_timeout,
            options=f"-c search_path={c['schema']},public -c statement_timeout={c['timeout_ms']}",
        )
    except Exception as e:
        _logger.event("pg_connect_failed", level="ERROR", error=str(e),
                      pg_host=c["host"], pg_port=c["port"], pg_db=c["dbname"])
        raise
    try:
        host_meta = host_info(with_geo=False)
    except Exception:
        host_meta = {}
    _logger.connection(
        pg_host=c["host"], pg_port=c["port"], pg_db=c["dbname"],
        pg_user=c["user"], pg_schema=c["schema"], pg_sslmode=c["sslmode"],
        query_timeout_ms=c["timeout_ms"], connect_timeout=connect_timeout,
        **host_meta,
    )
    return conn


def run_query(conn, sql: str, params=None, *, owner=None, table=None, batch=None) -> pd.DataFrame:
    """Run a SELECT and return rows as a DataFrame (column names from cursor.description)."""
    with _logger.query(sql, owner=owner, table=table, batch=batch, params=params) as q:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                q.set_rows(0)
                return pd.DataFrame()
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        q.set_rows(len(df))
    return df


def execute(conn, sql: str, params=None, *, owner=None, table=None, batch=None) -> int:
    """Run a non-SELECT (INSERT/UPDATE/DDL). Commits on success. Returns row count."""
    with _logger.query(sql, owner=owner, table=table, batch=batch, params=params) as q:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            n = cur.rowcount
        conn.commit()
        q.set_rows(n if n is not None and n >= 0 else 0)
    return n


def execute_many(conn, sql: str, rows, *, owner=None, table=None, batch=None) -> int:
    """Bulk insert/update. `rows` is a list of tuples or dicts."""
    with _logger.query(sql, owner=owner, table=table, batch=batch) as q:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
            n = cur.rowcount
        conn.commit()
        effective = n if n is not None and n >= 0 else len(rows)
        q.set_rows(effective)
    return n


def list_tables(conn, schema: str | None = None) -> pd.DataFrame:
    schema = schema or PG_SCHEMA
    return run_query(conn, """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name
    """, (schema,))


def ping(conn) -> str:
    """Smoke test: returns server version string."""
    df = run_query(conn, "SELECT version()")
    return df.iloc[0, 0] if not df.empty else "?"


if __name__ == "__main__":
    with create_connection() as c:
        print("connected:", ping(c))
        print("\ntables in", PG_SCHEMA, ":")
        print(list_tables(c).to_string(index=False))
