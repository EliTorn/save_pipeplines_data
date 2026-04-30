"""Diagnostic CLI: who is holding PG connections right now?

Queries pg_stat_activity to show every active session in the current
database, sorted by oldest first. Each row shows pid, application_name
(set by our connect_to_postgres.create_connection — lets you spot
oraes:streamlit:host:pid vs other clients), client_addr, state, idle time,
and the current query.

Usage:
    python -m connect_into_postgres.connection_audit
    python -m connect_into_postgres.connection_audit --me     # only our app
    python -m connect_into_postgres.connection_audit --idle   # idle > 5 min
    python -m connect_into_postgres.connection_audit --kill PID  # terminate
                                                                  one session

Prints a hint at the end about max_connections and current usage.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PARENT = Path(__file__).resolve().parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))


SQL_LIST = """
SELECT pid,
       application_name,
       usename,
       client_addr,
       state,
       backend_start,
       state_change,
       EXTRACT(EPOCH FROM (now() - state_change))::bigint AS idle_seconds,
       EXTRACT(EPOCH FROM (now() - backend_start))::bigint AS lifetime_seconds,
       wait_event_type,
       wait_event,
       LEFT(query, 200) AS query
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
{filter}
ORDER BY backend_start ASC
"""

SQL_TOTALS = """
SELECT
    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections')
        AS max_connections,
    (SELECT setting::int FROM pg_settings
        WHERE name = 'superuser_reserved_connections') AS superuser_reserved,
    (SELECT COUNT(*) FROM pg_stat_activity)
        AS total_sessions,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database())
        AS in_this_db,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'idle')
        AS idle,
    (SELECT COUNT(*) FROM pg_stat_activity
        WHERE state = 'idle in transaction') AS idle_in_txn,
    (SELECT COUNT(*) FROM pg_stat_activity
        WHERE application_name LIKE 'oraes:%') AS our_app
"""

SQL_TERMINATE = "SELECT pg_terminate_backend(%s)"


def list_sessions(only_ours: bool, only_idle_seconds: int = 0) -> int:
    try:
        from connect_into_postgres import connect_to_postgres as pg
    except (Exception, SystemExit) as e:
        print(f"FAIL: cannot import connect_to_postgres: {e}")
        return 1
    try:
        conn = pg.create_connection(application_name="oraes:audit")
    except Exception as e:
        print(f"FAIL: cannot connect to PG: {type(e).__name__}: {e}")
        return 1
    try:
        # Totals first.
        with conn.cursor() as cur:
            cur.execute(SQL_TOTALS)
            row = cur.fetchone()
            if row is not None:
                (max_c, super_res, total, in_db, idle, idle_txn, ours) = row
                print(f"max_connections={max_c}  reserved_for_superuser={super_res}  "
                      f"total={total}  in_this_db={in_db}  idle={idle}  "
                      f"idle_in_txn={idle_txn}  our_app(oraes:*)={ours}")
                used = total
                free = max_c - used
                if free <= super_res:
                    print(f"  WARNING: only {free} slot(s) free; superuser "
                          f"reserves {super_res}")
                print()

        # Filter clauses.
        clauses = []
        if only_ours:
            clauses.append("AND application_name LIKE 'oraes:%'")
        if only_idle_seconds > 0:
            clauses.append(f"AND state = 'idle' AND state_change < "
                           f"now() - interval '{only_idle_seconds} seconds'")
        sql = SQL_LIST.format(filter=" ".join(clauses))

        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

        if not rows:
            print("(no other sessions match)")
            return 0

        # Print compact, fixed-width.
        print(f"{'pid':>6} {'app_name':30s} {'user':12s} {'state':20s} "
              f"{'idle_s':>6} {'life_s':>7} {'query':s}")
        print("-" * 120)
        for r in rows:
            d = dict(zip(cols, r))
            print(f"{d['pid']:>6} {(d.get('application_name') or '')[:30]:30s} "
                  f"{(d.get('usename') or '')[:12]:12s} "
                  f"{(d.get('state') or '')[:20]:20s} "
                  f"{(d.get('idle_seconds') or 0):>6} "
                  f"{(d.get('lifetime_seconds') or 0):>7} "
                  f"{(d.get('query') or '').strip()[:80]}")
        return 0
    finally:
        try: conn.close()
        except Exception: pass


def kill_session(pid: int) -> int:
    """Terminate one session. Caller must have superuser rights or be the
    session owner."""
    try:
        from connect_into_postgres import connect_to_postgres as pg
        conn = pg.create_connection(application_name="oraes:audit-kill")
    except Exception as e:
        print(f"FAIL: cannot connect: {type(e).__name__}: {e}")
        return 1
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_TERMINATE, (pid,))
            ok = cur.fetchone()
        conn.commit()
        print(f"pg_terminate_backend({pid}) -> {ok}")
        return 0 if ok and ok[0] else 2
    finally:
        try: conn.close()
        except Exception: pass


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--me", action="store_true",
                   help="only show sessions tagged oraes:* (our app)")
    p.add_argument("--idle", type=int, default=0,
                   help="only sessions idle longer than N seconds")
    p.add_argument("--kill", type=int, metavar="PID",
                   help="pg_terminate_backend(pid). use carefully.")
    args = p.parse_args()
    if args.kill:
        sys.exit(kill_session(args.kill))
    sys.exit(list_sessions(only_ours=args.me, only_idle_seconds=args.idle))


if __name__ == "__main__":
    main()
