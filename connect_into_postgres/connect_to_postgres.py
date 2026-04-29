"""Postgres connection layer — env-driven, mirrors connect_to_orcal.py.

Required env vars (set in .env or shell):
    PG_HOST       e.g. localhost / 192.168.x.y / RDS endpoint
    PG_DB         database name (e.g. etl_catalog)
    PG_USER
    PG_PASSWORD

Optional:
    PG_PORT                          default 5432
    PG_SCHEMA                        default 'public' (used as search_path)
    PG_SSLMODE                       default 'prefer'
    PG_QUERY_TIMEOUT_MS              default 600000   (10 min statement timeout)
    PG_CONNECT_TIMEOUT               default 5        (seconds)
    PG_IDLE_IN_TXN_TIMEOUT_MS        default 60000    (1 min — server kicks
                                                       us if we sit idle in
                                                       a transaction)
    PG_KEEPALIVES_IDLE               default 60       (TCP keepalive idle, s)
    PG_KEEPALIVES_INTERVAL           default 10       (probe interval, s)
    PG_KEEPALIVES_COUNT              default 5        (probes before dead)
    PG_APPLICATION_NAME              default '<role>:<host>:<pid>'

Connection identification:
    Every connection sets `application_name` so a DBA can grep
    pg_stat_activity to find which process owns each slot. Combined with
    keepalives, dead clients are detected within ~minute and their slots
    reclaimed automatically.

Install once:
    pip install "psycopg[binary]>=3.1"
"""
import os
import socket
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
        idle_in_txn_ms=int(os.getenv("PG_IDLE_IN_TXN_TIMEOUT_MS", "60000")),
        keepalives_idle=int(os.getenv("PG_KEEPALIVES_IDLE", "60")),
        keepalives_interval=int(os.getenv("PG_KEEPALIVES_INTERVAL", "10")),
        keepalives_count=int(os.getenv("PG_KEEPALIVES_COUNT", "5")),
    )


def _default_application_name() -> str:
    """Identify the process that owns this PG slot. Shows up in
    pg_stat_activity.application_name so the DBA can find leaks."""
    role = os.getenv("PG_APPLICATION_ROLE")
    if not role:
        # Best-effort: figure out who we are from sys.argv[0].
        argv0 = (sys.argv[0] if sys.argv else "") or ""
        base = os.path.basename(argv0).lower()
        if "streamlit" in base or "app.py" in argv0:
            role = "streamlit"
        elif "main.py" in argv0:
            role = "pipeline"
        elif "apply_changes" in argv0:
            role = "apply"
        elif "parity_check" in argv0:
            role = "parity"
        elif "sync_out" in argv0:
            role = "sync_out"
        else:
            role = "py"
    try:
        host = socket.gethostname()[:24]
    except Exception:
        host = "host"
    pid = os.getpid()
    return os.getenv("PG_APPLICATION_NAME") or f"oraes:{role}:{host}:{pid}"


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


def create_connection(application_name: str | None = None):
    """Open a new Postgres connection.

    Connection settings (server-side timeouts + TCP keepalives) are tuned so
    that a dead/abandoned client releases its slot quickly:
      - statement_timeout            kills runaway queries
      - idle_in_transaction_timeout  kicks us if we hold a tx without work
      - TCP keepalives               PG detects dead clients in ~60+5*10s
      - application_name             visible in pg_stat_activity for triage

    Honors PG_CONNECT_TIMEOUT so misconfigured hosts fail fast instead of
    blocking ~75 sec on the kernel TCP timeout.
    """
    c = _cfg()
    connect_timeout = int(os.getenv("PG_CONNECT_TIMEOUT", "5"))
    app_name = application_name or _default_application_name()

    options = (
        f"-c search_path={c['schema']},public "
        f"-c statement_timeout={c['timeout_ms']} "
        f"-c idle_in_transaction_session_timeout={c['idle_in_txn_ms']}"
    )

    connect_kwargs = dict(
        host=c["host"], port=c["port"], dbname=c["dbname"],
        user=c["user"], password=c["password"], sslmode=c["sslmode"],
        connect_timeout=connect_timeout,
        application_name=app_name,
        keepalives=1,
        keepalives_idle=c["keepalives_idle"],
        keepalives_interval=c["keepalives_interval"],
        keepalives_count=c["keepalives_count"],
        options=options,
    )

    try:
        conn = psycopg.connect(**connect_kwargs)
    except Exception as e:
        _logger.event("pg_connect_failed", level="ERROR", error=str(e),
                      pg_host=c["host"], pg_port=c["port"], pg_db=c["dbname"],
                      pg_application_name=app_name)
        raise
    try:
        host_meta = host_info(with_geo=False)
    except Exception:
        host_meta = {}
    _logger.connection(
        pg_host=c["host"], pg_port=c["port"], pg_db=c["dbname"],
        pg_user=c["user"], pg_schema=c["schema"], pg_sslmode=c["sslmode"],
        query_timeout_ms=c["timeout_ms"], connect_timeout=connect_timeout,
        pg_application_name=app_name,
        pg_idle_in_txn_ms=c["idle_in_txn_ms"],
        **host_meta,
    )
    return conn


def run_query(conn, sql: str, params=None, *, owner=None, table=None, batch=None,
              env=None, operation: str = "postgres_select") -> pd.DataFrame:
    """Run a SELECT and return rows as a DataFrame (column names from cursor.description)."""
    with _logger.query(sql, owner=owner, table=table, batch=batch, params=params,
                       env=env, operation=operation) as q:
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


def execute(conn, sql: str, params=None, *, owner=None, table=None, batch=None,
            env=None, operation: str = "postgres_execute") -> int:
    """Run a non-SELECT (INSERT/UPDATE/DDL). Commits on success. Returns row count."""
    with _logger.query(sql, owner=owner, table=table, batch=batch, params=params,
                       env=env, operation=operation) as q:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            n = cur.rowcount
        conn.commit()
        q.set_rows(n if n is not None and n >= 0 else 0)
    return n


def execute_many(conn, sql: str, rows, *, owner=None, table=None, batch=None,
                 env=None, operation: str = "postgres_execute_many") -> int:
    """Bulk insert/update. `rows` is a list of tuples or dicts."""
    with _logger.query(sql, owner=owner, table=table, batch=batch,
                       env=env, operation=operation) as q:
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
