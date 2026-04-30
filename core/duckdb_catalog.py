"""DuckDB catalog over local files.

Single catalog at `<repo>/duckdb/pipeline.duckdb`. Views point at globs
under `out/` so newly-written Parquet files appear automatically — DuckDB
re-resolves the glob on every query.

Phase A: read-only catalog of diffs/missing/full Oracle CSVs/log CSVs.
Streamlit + ad-hoc analytics will read from here in Phase B.

Best-effort: missing duckdb dep / I/O error -> warn + return None.
"""
from __future__ import annotations

import glob as _pyglob
from pathlib import Path
from typing import Optional

try:
    import duckdb
    _DUCKDB_OK = True
except ImportError:
    _DUCKDB_OK = False

_ROOT = Path(__file__).resolve().parent.parent
# Phase D loop 4 fix: directory name must NOT collide with the `duckdb`
# PyPI package, otherwise Python treats it as a namespace package and
# `import duckdb` resolves to this empty dir instead of the real lib —
# leading to `module 'duckdb' has no attribute 'connect'` at runtime.
DUCKDB_DIR = _ROOT / "duckdb_data"
DUCKDB_PATH = DUCKDB_DIR / "pipeline.duckdb"
OUT_DIR = _ROOT / "out"


def _glob(p: Path) -> str:
    """Forward-slash glob path safe for DuckDB on Windows."""
    return p.as_posix()


# Each spec describes a view over a glob. Built dynamically so that views
# referencing globs that match no files become empty placeholders instead
# of failing the whole catalog.
def _view_specs() -> list[tuple[str, str, str, list[str]]]:
    """Returns (view_name, glob_pattern, kind, columns) tuples.
    kind: 'parquet' | 'csv'. columns: schema for the empty placeholder."""
    return [
        ("v_changes",
         _glob(OUT_DIR / "*" / "*" / "changes" / "changes_*.parquet"),
         "parquet",
         ["id", "field", "oracle_value", "es_value", "status"]),
        ("v_missing",
         _glob(OUT_DIR / "*" / "*" / "changes" / "missing_in_es_*.parquet"),
         "parquet",
         ["id", "error"]),
        ("v_oracle_full",
         _glob(OUT_DIR / "*" / "*" / "*_oracle_*.csv"),
         "csv",
         ["id"]),
        ("v_summary",
         _glob(OUT_DIR / "summary_*.csv"),
         "csv",
         ["event", "env", "field", "total_issues",
          "diff", "row_missing_in_es", "row_missing_in_oracle",
          "es_value_blank", "oracle_value_blank"]),
        ("v_log_connections",
         _glob(_ROOT / "connect_into_*" / "logging" / "connections.csv"),
         "csv",
         ["run_id", "ts"]),
        ("v_log_events",
         _glob(_ROOT / "connect_into_*" / "logging" / "events.csv"),
         "csv",
         ["run_id", "ts", "event"]),
        ("v_log_queries",
         _glob(_ROOT / "connect_into_*" / "logging" / "queries.csv"),
         "csv",
         ["run_id", "sql_hash", "seconds"]),
    ]


def _empty_view_sql(name: str, columns: list[str]) -> str:
    cols = ", ".join(f"NULL::VARCHAR AS {c}" for c in columns) + ", NULL::VARCHAR AS filename"
    return f"CREATE OR REPLACE VIEW {name} AS SELECT {cols} WHERE FALSE"


def _real_view_sql(name: str, pattern: str, kind: str) -> str:
    if kind == "parquet":
        return (f"CREATE OR REPLACE VIEW {name} AS "
                f"SELECT * FROM read_parquet('{pattern}', filename=true, "
                f"union_by_name=true)")
    return (f"CREATE OR REPLACE VIEW {name} AS "
            f"SELECT * FROM read_csv('{pattern}', filename=true, "
            f"all_varchar=true, ignore_errors=true, union_by_name=true)")


def connect(read_only: bool = False):
    """Open a connection to the local DuckDB catalog. Returns None if duckdb
    isn't installed."""
    if not _DUCKDB_OK:
        return None
    DUCKDB_DIR.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


def init_catalog() -> Optional[Path]:
    """Create the catalog file + register all views. Idempotent. Returns
    catalog path on success, None on skip."""
    con = connect(read_only=False)
    if con is None:
        print("[duckdb] duckdb not installed, skipping catalog init", flush=True)
        return None
    try:
        live = empty = 0
        for name, pattern, kind, cols in _view_specs():
            has_files = bool(_pyglob.glob(pattern))
            sql = _real_view_sql(name, pattern, kind) if has_files \
                  else _empty_view_sql(name, cols)
            try:
                con.execute(sql)
                if has_files: live += 1
                else:         empty += 1
            except Exception as e:
                print(f"[duckdb] view {name} failed: {type(e).__name__}: {e}",
                      flush=True)
        con.commit()
        print(f"[duckdb] catalog at {DUCKDB_PATH} | live={live} empty={empty}",
              flush=True)
        return DUCKDB_PATH
    finally:
        try: con.close()
        except Exception: pass


def query(sql: str, params: tuple | list | None = None):
    """Convenience: open read-only, run sql, return DataFrame. None on no-duckdb."""
    con = connect(read_only=True)
    if con is None:
        return None
    try:
        cur = con.execute(sql, list(params)) if params else con.execute(sql)
        return cur.fetch_df()
    finally:
        try: con.close()
        except Exception: pass


def is_available() -> bool:
    """True if duckdb importable AND catalog file exists."""
    return _DUCKDB_OK and DUCKDB_PATH.is_file()


def query_or_fallback(duck_sql: str, duck_params=None,
                      pg_sql: str | None = None, pg_params=None,
                      pg_module=None, pg_conn=None):
    """Try DuckDB; on exception fall back to PG (if both supplied).
    Returns (df, source) where source in {'duckdb', 'pg', 'failed'}.
    df is None on total failure.

    Special case: if a DuckDB view points at a glob with no matching files,
    DuckDB raises IOException at query time. We auto-recover by re-initing
    the catalog (which replaces those views with empty placeholders) and
    retrying once before falling back."""
    if is_available():
        try:
            df = query(duck_sql, duck_params)
            if df is not None:
                return df, "duckdb"
        except Exception as e:
            msg = str(e)
            if "No files found that match the pattern" in msg:
                # File set changed since last init — refresh and retry once.
                try:
                    init_catalog()
                    df = query(duck_sql, duck_params)
                    if df is not None:
                        return df, "duckdb"
                except Exception as e2:
                    print(f"[duckdb] retry after re-init failed: "
                          f"{type(e2).__name__}: {e2}", flush=True)
            else:
                print(f"[duckdb] query failed, falling back to PG: "
                      f"{type(e).__name__}: {e}", flush=True)
    if pg_sql is not None and pg_module is not None and pg_conn is not None:
        try:
            return pg_module.run_query(pg_conn, pg_sql, pg_params), "pg"
        except Exception as e:
            print(f"[pg] fallback query failed: {type(e).__name__}: {e}", flush=True)
    return None, "failed"
