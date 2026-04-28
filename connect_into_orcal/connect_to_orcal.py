import math
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import oracledb
import pandas as pd
from dotenv import load_dotenv

from logging_setup import get_run_logger, CONN_CSV, EVENTS_CSV, QUERIES_CSV
from geo_info import host_info

load_dotenv()

REQUIRED_VARS = ("ORACLE_USERNAME", "ORACLE_PASSWORD", "ORACLE_DB_HOST", "ORACLE_SERVICE_NAME")
_missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
if _missing:
    sys.exit(f"Missing env vars: {', '.join(_missing)}")

USERNAME = os.getenv("ORACLE_USERNAME")
PASSWORD = os.getenv("ORACLE_PASSWORD")
DB_HOST = os.getenv("ORACLE_DB_HOST")
SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME")
PORT = int(os.getenv("ORACLE_PORT", "1521"))

QUERY_TIMEOUT_MS = int(os.getenv("ORACLE_QUERY_TIMEOUT_MS", "600000"))
WORKERS = int(os.getenv("ORACLE_WORKERS", "4"))
ARRAYSIZE = int(os.getenv("ORACLE_ARRAYSIZE", "10000"))
PREFETCH = int(os.getenv("ORACLE_PREFETCH", "10001"))


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def create_connection() -> oracledb.Connection:
    dsn = oracledb.makedsn(DB_HOST, PORT, service_name=SERVICE_NAME)
    return oracledb.connect(user=USERNAME, password=PASSWORD, dsn=dsn)


def run_query(conn: oracledb.Connection, sql: str, params: dict | None = None,
              timeout_ms: int = QUERY_TIMEOUT_MS) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.callTimeout = timeout_ms
        cur.arraysize = ARRAYSIZE
        cur.prefetchrows = PREFETCH
        cur.execute(sql, params or {})
        if cur.description is None:
            return pd.DataFrame()
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def run_tracked(conn, sql: str, params: dict | None, logger,
                owner: str | None = None, table: str | None = None,
                batch: int | None = None) -> tuple[pd.DataFrame, str]:
    with logger.query(sql, owner=owner, table=table, batch=batch, params=params) as q:
        df = run_query(conn, sql, params)
        q.set_rows(len(df))
    return df, q.query_id


def list_tables(conn, logger, owner: str | None = None, pattern: str = "%"):
    if owner:
        sql = ("SELECT owner, table_name FROM all_tables "
               "WHERE owner=:o AND table_name LIKE :p ORDER BY table_name")
        params = {"o": owner.upper(), "p": pattern}
    else:
        sql = ("SELECT owner, table_name FROM all_tables "
               "WHERE table_name LIKE :p ORDER BY owner, table_name")
        params = {"p": pattern}
    df, qid = run_tracked(conn, sql, params, logger, table="ALL_TABLES")
    return list(df.itertuples(index=False, name=None)), qid


def _fetch_shard(owner: str, table: str, worker_id: int, n_workers: int,
                 max_rows: int | None, logger) -> tuple[int, pd.DataFrame]:
    base = f"SELECT * FROM {qident(owner)}.{qident(table)} WHERE MOD(ORA_HASH(ROWID), :n) = :w"
    params: dict = {"n": n_workers, "w": worker_id}
    if max_rows:
        sql = base + " FETCH FIRST :mr ROWS ONLY"
        params["mr"] = max_rows
    else:
        sql = base

    logger.event("shard_start", owner=owner, table=table,
                 batch=worker_id, total=n_workers, sql=sql)
    t0 = time.perf_counter()
    try:
        with create_connection() as conn:
            df, qid = run_tracked(conn, sql, params, logger,
                                  owner=owner, table=table, batch=worker_id)
        dt = time.perf_counter() - t0
        logger.event("shard_done", query_id=qid, owner=owner, table=table,
                     batch=worker_id, rows=len(df), seconds=round(dt, 3),
                     rows_per_sec=round(len(df) / dt, 1) if dt > 0 else None)
        return worker_id, df
    except Exception as e:
        dt = time.perf_counter() - t0
        logger.event("shard_error", level="ERROR", owner=owner, table=table,
                     batch=worker_id, seconds=round(dt, 3), error=str(e))
        raise


def fetch_parallel(owner: str, table: str, logger,
                   workers: int = WORKERS,
                   max_rows_per_shard: int | None = None) -> pd.DataFrame:
    logger.event("parallel_plan", owner=owner, table=table,
                 workers=workers, total_batches=workers)
    parts: list[pd.DataFrame | None] = [None] * workers
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="orcl") as ex:
        futs = {ex.submit(_fetch_shard, owner, table, w, workers,
                          max_rows_per_shard, logger): w
                for w in range(workers)}
        done = 0
        for f in as_completed(futs):
            w, df = f.result()
            parts[w] = df
            done += 1
            logger.event("batch_progress", owner=owner, table=table,
                         completed=done, total=workers)
    dt = time.perf_counter() - t0
    result = pd.concat([p for p in parts if p is not None], ignore_index=True) if parts else pd.DataFrame()
    logger.event("parallel_done", owner=owner, table=table,
                 rows=len(result), seconds=round(dt, 3),
                 total_batches=workers,
                 rows_per_sec=round(len(result) / dt, 1) if dt > 0 else None)
    return result


def main() -> None:
    run_id = uuid.uuid4().hex[:12]
    logger = get_run_logger(run_id)

    host = host_info()
    logger.connection(
        oracle_host=DB_HOST, oracle_port=PORT, oracle_service=SERVICE_NAME,
        oracle_user=USERNAME, batch_size=ARRAYSIZE, workers=WORKERS,
        query_timeout_ms=QUERY_TIMEOUT_MS, **host,
    )
    print(f"Run {run_id} | conn -> {CONN_CSV.name} | events -> {EVENTS_CSV.name} | queries -> {QUERIES_CSV.name}")

    target = sys.argv[1] if len(sys.argv) > 1 else "--list"
    t_run = time.perf_counter()
    try:
        if target in ("--list", "list"):
            owner = sys.argv[2] if len(sys.argv) > 2 else None
            with create_connection() as conn:
                rows, qid = list_tables(conn, logger, owner)
            logger.event("list_tables", query_id=qid, count=len(rows))
            for o, t in rows:
                print(f"  {o}.{t}")
            return

        if "." not in target:
            sys.exit("Usage: py connect_to_orcal.py OWNER.TABLE [max_total_rows]  |  list [OWNER]")

        owner, table = target.split(".", 1)
        max_total = int(sys.argv[2]) if len(sys.argv) > 2 else None
        max_per_shard = math.ceil(max_total / WORKERS) if max_total else None

        logger.event("fetch_plan", owner=owner, table=table,
                     user_limit=max_total, total_to_fetch=max_total,
                     workers=WORKERS, batch_size=max_per_shard)

        df = fetch_parallel(owner, table, logger,
                            workers=WORKERS,
                            max_rows_per_shard=max_per_shard)

        if max_total and len(df) > max_total:
            df = df.head(max_total)

        out = f"{owner}_{table}.csv".lower()
        t0 = time.perf_counter()
        df.to_csv(out, index=False, encoding="utf-8-sig")
        logger.event("csv_saved", owner=owner, table=table,
                     path=os.path.abspath(out), rows=len(df),
                     seconds=round(time.perf_counter() - t0, 3))
        print(f"Saved -> {out} ({len(df)} rows)")

    except oracledb.DatabaseError as e:
        logger.event("oracle_error", level="ERROR", error=str(e))
        sys.exit(f"Oracle error: {e}")
    except Exception as e:
        logger.event("fatal", level="ERROR", error=str(e))
        raise
    finally:
        logger.event("run_end", total_seconds=round(time.perf_counter() - t_run, 3))


if __name__ == "__main__":
    main()
