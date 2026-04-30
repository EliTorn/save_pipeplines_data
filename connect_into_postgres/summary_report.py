"""Read-only PostgreSQL summary of past batches.

Prints aggregate stats about `pipeline_apply_batches` (and, if present,
`pipeline_run_summary`) WITHOUT running, applying, or initializing
anything else. Exactly ONE PG connection is opened, queries run, then it
is closed in a `finally` block.

Behavior knobs:
  - Calls observability.disable() so any incidental query/connection log
    mirror that the connect layer might fire is silenced.
  - Skips cached connection helpers; opens a raw psycopg connection so
    the global module caches stay None for this process.
  - Adapts queries to whichever columns actually exist (queries
    information_schema first).

Entry points:
    python -m connect_into_postgres.summary_report
    python main.py --summary-only            (delegates here)

Lines are prefixed `[summary-only]` so they're easy to grep.
"""
from __future__ import annotations

import os
import sys
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

_PARENT = Path(__file__).resolve().parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

LOG = "[summary-only]"


def _say(msg: str = "") -> None:
    print(f"{LOG} {msg}" if msg else LOG, flush=True)


def _connect():
    """Open ONE raw psycopg connection. Bypasses CachedConnection,
    bypasses the run-logger so observability stays untouched.

    Reads the same env vars as `connect_to_postgres._cfg()` so config
    matches the rest of the pipeline."""
    from dotenv import load_dotenv
    load_dotenv()  # idempotent — same as the main pipeline does

    try:
        import psycopg
    except ImportError:
        try:
            import psycopg2 as psycopg  # type: ignore
        except ImportError:
            raise RuntimeError("psycopg not installed. "
                               "Run: pip install \"psycopg[binary]>=3.1\"")

    required = ("PG_HOST", "PG_DB", "PG_USER")
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    options = (
        f"-c search_path={os.getenv('PG_SCHEMA', 'public')},public "
        f"-c statement_timeout={int(os.getenv('PG_QUERY_TIMEOUT_MS', '600000'))} "
        f"-c idle_in_transaction_session_timeout="
        f"{int(os.getenv('PG_IDLE_IN_TXN_TIMEOUT_MS', '60000'))}"
    )

    return psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD", ""),
        sslmode=os.getenv("PG_SSLMODE", "prefer"),
        connect_timeout=int(os.getenv("PG_CONNECT_TIMEOUT", "5")),
        application_name="oraes:summary-only",
        keepalives=1,
        keepalives_idle=int(os.getenv("PG_KEEPALIVES_IDLE", "60")),
        keepalives_interval=int(os.getenv("PG_KEEPALIVES_INTERVAL", "10")),
        keepalives_count=int(os.getenv("PG_KEEPALIVES_COUNT", "5")),
        options=options,
    )


def _columns(cur, table: str) -> set[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = %s",
        (table,),
    )
    return {r[0] for r in cur.fetchall()}


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = current_schema() AND table_name = %s",
        (table,),
    )
    return cur.fetchone() is not None


def _fmt_ts(ts: Any) -> str:
    if ts is None:
        return "—"
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return str(ts)


def _fmt_dur(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    s = float(seconds)
    if s < 1:    return f"{s*1000:.0f} ms"
    if s < 60:   return f"{s:.2f} s"
    if s < 3600: return f"{s/60:.1f} min"
    return f"{s/3600:.2f} h"


def _fmt_int(n: Any) -> str:
    if n is None:
        return "—"
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return str(n)


def _print_section(title: str) -> None:
    print()
    print(f"{LOG} === {title} ===")


def _print_kv(rows: Iterable[tuple[str, Any]]) -> None:
    for k, v in rows:
        print(f"{LOG}   {k:32s} {v}")


# ---------------------------------------------------------------------------
# pipeline_apply_batches  (per-CSV applied marker — has rows columns +
# applied_ts, no started/ended_at)
# ---------------------------------------------------------------------------

def _report_apply_batches(cur) -> None:
    if not _table_exists(cur, "pipeline_apply_batches"):
        _say("pipeline_apply_batches: not present — skipped")
        return

    cols = _columns(cur, "pipeline_apply_batches")

    # Total / success / failed counts.
    cur.execute("SELECT COUNT(*) FROM pipeline_apply_batches")
    total = cur.fetchone()[0]
    if total == 0:
        _print_section("pipeline_apply_batches")
        _print_kv([("total batches", 0)])
        return

    has_failures = "es_failures" in cols
    has_conflicts = "es_conflicts" in cols
    has_updated = "es_updated" in cols
    has_created = "es_created" in cols

    if has_failures:
        cur.execute("SELECT COUNT(*) FROM pipeline_apply_batches "
                    "WHERE COALESCE(es_failures, 0) = 0")
        success = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM pipeline_apply_batches "
                    "WHERE COALESCE(es_failures, 0) > 0")
        failed = cur.fetchone()[0]
    else:
        success = total; failed = 0

    cur.execute("SELECT MIN(applied_ts), MAX(applied_ts) FROM pipeline_apply_batches")
    first, last = cur.fetchone()
    elapsed_s = None
    if first and last:
        elapsed_s = (last - first).total_seconds()
    avg_s = (elapsed_s / total) if (elapsed_s and total) else None

    rows_total = None
    if has_updated and has_created:
        cur.execute("SELECT SUM(COALESCE(es_updated, 0)) + SUM(COALESCE(es_created, 0)) "
                    "FROM pipeline_apply_batches")
        rows_total = cur.fetchone()[0]
    elif has_updated:
        cur.execute("SELECT SUM(COALESCE(es_updated, 0)) FROM pipeline_apply_batches")
        rows_total = cur.fetchone()[0]

    _print_section("pipeline_apply_batches")
    _print_kv([
        ("total batches",        _fmt_int(total)),
        ("successful",           _fmt_int(success)),
        ("failed",               _fmt_int(failed)),
        ("first applied_ts",     _fmt_ts(first)),
        ("last applied_ts",      _fmt_ts(last)),
        ("elapsed (last-first)", _fmt_dur(elapsed_s)),
        ("avg interval/batch",   _fmt_dur(avg_s)),
        ("rows updated+created", _fmt_int(rows_total)),
    ])

    # Per (event, env, mode) breakdown.
    breakdown_cols = []
    if "event" in cols: breakdown_cols.append("event")
    if "env" in cols:   breakdown_cols.append("env")
    if "mode" in cols:  breakdown_cols.append("mode")
    if breakdown_cols:
        sel = ", ".join(breakdown_cols)
        extras = []
        if has_updated:   extras.append("SUM(COALESCE(es_updated, 0))   AS rows_updated")
        if has_created:   extras.append("SUM(COALESCE(es_created, 0))   AS rows_created")
        if has_conflicts: extras.append("SUM(COALESCE(es_conflicts, 0)) AS conflicts")
        if has_failures:  extras.append("SUM(COALESCE(es_failures, 0))  AS failures")
        extras_sql = (", " + ", ".join(extras)) if extras else ""
        cur.execute(
            f"SELECT {sel}, COUNT(*) AS batches{extras_sql}, "
            f"       MIN(applied_ts) AS first_ts, MAX(applied_ts) AS last_ts "
            f"FROM pipeline_apply_batches "
            f"GROUP BY {sel} "
            f"ORDER BY batches DESC, {sel}"
        )
        rows = cur.fetchall()
        cols_meta = [d[0] for d in cur.description]
        if rows:
            _print_section(f"breakdown by {', '.join(breakdown_cols)}")
            header = "  ".join(f"{c:>14s}" for c in cols_meta)
            print(f"{LOG}   {header}")
            print(f"{LOG}   {'-' * len(header)}")
            for r in rows:
                cells = []
                for c, val in zip(cols_meta, r):
                    if c in ("first_ts", "last_ts"):
                        cells.append(f"{_fmt_ts(val):>14s}")
                    elif isinstance(val, (int, float)):
                        cells.append(f"{_fmt_int(val):>14s}")
                    else:
                        cells.append(f"{(str(val) if val is not None else '—'):>14s}")
                print(f"{LOG}   {'  '.join(cells)}")


# ---------------------------------------------------------------------------
# pipeline_run_summary  (has started_at + ended_at + status + rows_count)
# ---------------------------------------------------------------------------

def _report_run_summary(cur) -> None:
    if not _table_exists(cur, "pipeline_run_summary"):
        _say("pipeline_run_summary: not present — skipped")
        return

    cols = _columns(cur, "pipeline_run_summary")
    cur.execute("SELECT COUNT(*) FROM pipeline_run_summary")
    total = cur.fetchone()[0]
    if total == 0:
        _print_section("pipeline_run_summary")
        _print_kv([("total rows", 0)])
        return

    has_status   = "status"      in cols
    has_started  = "started_at"  in cols
    has_ended    = "ended_at"    in cols
    has_rows     = "rows_count"  in cols
    has_op       = "operation"   in cols
    has_target   = "target_name" in cols
    has_env      = "env"         in cols

    success = failed = pending = None
    if has_status:
        cur.execute("SELECT status, COUNT(*) FROM pipeline_run_summary "
                    "GROUP BY status")
        by_status = {(r[0] or "(null)"): int(r[1]) for r in cur.fetchall()}
        success = by_status.get("ok", 0)
        failed  = by_status.get("failed", 0)
        pending = sum(v for k, v in by_status.items()
                      if k not in ("ok", "failed"))

    first = last = elapsed_s = avg_s = None
    if has_started and has_ended:
        cur.execute("SELECT MIN(started_at), MAX(ended_at) FROM pipeline_run_summary")
        first, last = cur.fetchone()
        if first and last:
            elapsed_s = (last - first).total_seconds()

        # Average duration of one summary row, not wall-clock.
        cur.execute(
            "SELECT AVG(EXTRACT(EPOCH FROM (ended_at - started_at))) "
            "FROM pipeline_run_summary "
            "WHERE started_at IS NOT NULL AND ended_at IS NOT NULL"
        )
        a = cur.fetchone()[0]
        avg_s = float(a) if a is not None else None

    fastest_dur = slowest_dur = None
    fastest_meta = slowest_meta = None
    if has_started and has_ended:
        order_cols = ["target_name" if has_target else "''",
                      "operation"  if has_op     else "''",
                      "env"        if has_env    else "''"]
        cur.execute(
            f"SELECT EXTRACT(EPOCH FROM (ended_at - started_at)) AS dur, "
            f"       {', '.join(order_cols)} "
            f"FROM pipeline_run_summary "
            f"WHERE started_at IS NOT NULL AND ended_at IS NOT NULL "
            f"  AND ended_at >= started_at "
            f"ORDER BY dur ASC NULLS LAST LIMIT 1"
        )
        row = cur.fetchone()
        if row:
            fastest_dur = float(row[0]) if row[0] is not None else None
            fastest_meta = " / ".join(str(x) for x in row[1:] if x)
        cur.execute(
            f"SELECT EXTRACT(EPOCH FROM (ended_at - started_at)) AS dur, "
            f"       {', '.join(order_cols)} "
            f"FROM pipeline_run_summary "
            f"WHERE started_at IS NOT NULL AND ended_at IS NOT NULL "
            f"  AND ended_at >= started_at "
            f"ORDER BY dur DESC NULLS LAST LIMIT 1"
        )
        row = cur.fetchone()
        if row:
            slowest_dur = float(row[0]) if row[0] is not None else None
            slowest_meta = " / ".join(str(x) for x in row[1:] if x)

    rows_total = None
    if has_rows:
        cur.execute("SELECT SUM(COALESCE(rows_count, 0)) FROM pipeline_run_summary")
        rows_total = cur.fetchone()[0]

    _print_section("pipeline_run_summary")
    _print_kv([
        ("total rows",                 _fmt_int(total)),
        ("status=ok",                  _fmt_int(success)),
        ("status=failed",              _fmt_int(failed)),
        ("status=other (pending/etc)", _fmt_int(pending)),
        ("first started_at",           _fmt_ts(first)),
        ("last ended_at",              _fmt_ts(last)),
        ("elapsed (last-first)",       _fmt_dur(elapsed_s)),
        ("avg duration / row",         _fmt_dur(avg_s)),
        ("fastest",                    f"{_fmt_dur(fastest_dur)} ({fastest_meta or '—'})"),
        ("slowest",                    f"{_fmt_dur(slowest_dur)} ({slowest_meta or '—'})"),
        ("sum(rows_count)",            _fmt_int(rows_total)),
    ])

    # Per-target / per-operation breakdown.
    breakdown_cols = []
    if has_target: breakdown_cols.append("target_name")
    if has_op:     breakdown_cols.append("operation")
    if has_env:    breakdown_cols.append("env")
    if not breakdown_cols:
        return
    sel = ", ".join(breakdown_cols)
    extras = ["COUNT(*) AS runs"]
    if has_status:
        extras.append("COUNT(*) FILTER (WHERE status='ok')     AS ok")
        extras.append("COUNT(*) FILTER (WHERE status='failed') AS failed")
    if has_rows:
        extras.append("SUM(COALESCE(rows_count, 0))            AS rows")
    if has_started and has_ended:
        extras.append("ROUND(AVG(EXTRACT(EPOCH FROM (ended_at - started_at)))::numeric, 2) AS avg_seconds")
        extras.append("ROUND(MAX(EXTRACT(EPOCH FROM (ended_at - started_at)))::numeric, 2) AS max_seconds")
    cur.execute(
        f"SELECT {sel}, {', '.join(extras)} "
        f"FROM pipeline_run_summary "
        f"GROUP BY {sel} "
        f"ORDER BY runs DESC, {sel}"
    )
    rows = cur.fetchall()
    cols_meta = [d[0] for d in cur.description]
    if not rows:
        return
    _print_section(f"breakdown by {', '.join(breakdown_cols)}")
    widths = [max(14, len(c) + 2) for c in cols_meta]
    header = "  ".join(f"{c:>{w}s}" for c, w in zip(cols_meta, widths))
    print(f"{LOG}   {header}")
    print(f"{LOG}   {'-' * len(header)}")
    for r in rows:
        cells = []
        for (c, val), w in zip(zip(cols_meta, r), widths):
            if isinstance(val, (int, float)):
                cells.append(f"{_fmt_int(val) if isinstance(val, int) else f'{val}':>{w}s}")
            else:
                cells.append(f"{(str(val) if val is not None else '—'):>{w}s}")
        print(f"{LOG}   {'  '.join(cells)}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def print_report() -> int:
    """Open one PG conn, print the summary, close. Returns exit code."""
    # Disable observability writes BEFORE anything imports it lazily.
    try:
        from connect_into_postgres import observability
        observability.disable()
    except Exception:
        pass

    _say(f"start at {datetime.now().isoformat(timespec='seconds')}")

    try:
        conn = _connect()
    except Exception as e:
        _say(f"FAIL: cannot connect to PG: {type(e).__name__}: {e}")
        return 1

    try:
        with closing(conn), conn.cursor() as cur:
            _say(f"connected as application_name=oraes:summary-only")
            _report_apply_batches(cur)
            _report_run_summary(cur)
        _say("done.")
        return 0
    except Exception as e:
        _say(f"FAIL during query: {type(e).__name__}: {e}")
        try: conn.rollback()
        except Exception: pass
        return 1


if __name__ == "__main__":
    sys.exit(print_report())
