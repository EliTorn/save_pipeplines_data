"""DESTRUCTIVE migration: back up + drop the legacy heavy/observability
tables left over from before Phase D.

Tables touched (all DROP TABLE IF EXISTS):
    pipeline_changes
    pipeline_missing
    pipeline_apply_audit
    pipeline_summary
    pipeline_log_connection
    pipeline_log_event
    pipeline_log_query
    pipeline_log_offsets
    file_registry
    ingest_log

Tables KEPT:
    pipeline_run_summary       (active — run summary)
    connection_log             (active — observability)
    query_log                  (active — observability)
    batch_log                  (active — observability)
    pipeline_apply_batches     (active — apply state)

Default behavior is a DRY RUN: lists what would be dropped + their row
counts. To actually drop:
    1. Run with `--backup` to dump every legacy table to Parquet under
       `out/_legacy_backup/<timestamp>/<table>.parquet`. Skipped tables
       (already missing) are recorded in `_legacy_backup/skipped.txt`.
    2. Re-run with `--confirm` to execute the drops. The script refuses to
       drop unless a backup directory from this run has been created OR
       you explicitly add `--no-backup` to skip backup (NOT RECOMMENDED).

Examples:
    python -m connect_into_postgres.drop_legacy_tables           # dry run
    python -m connect_into_postgres.drop_legacy_tables --backup  # backup only
    python -m connect_into_postgres.drop_legacy_tables --backup --confirm
    python -m connect_into_postgres.drop_legacy_tables --no-backup --confirm
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

_PARENT = Path(__file__).resolve().parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

LEGACY_TABLES = (
    "pipeline_changes",
    "pipeline_missing",
    "pipeline_apply_audit",
    "pipeline_summary",
    "pipeline_log_connection",
    "pipeline_log_event",
    "pipeline_log_query",
    "pipeline_log_offsets",
    "file_registry",
    "ingest_log",
)

LEGACY_VIEWS = (
    "v_pipeline_connections",
    "v_pipeline_connection_overview",
    "v_pipeline_query_perf",
    "v_pipeline_query_slowest",
    "v_pipeline_query_by_hour",
    "v_pipeline_query_hashes",
    "v_pipeline_run_metrics",
    "v_pipeline_event_timeline",
    "v_pipeline_data_quality",
    "v_pipeline_apply_progress",
)

OUT_DIR = _PARENT / "out"
BACKUP_ROOT = OUT_DIR / "_legacy_backup"


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = current_schema() AND table_name = %s",
        (table,),
    )
    return cur.fetchone() is not None


def _row_count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    n = cur.fetchone()
    return int(n[0]) if n else 0


def _audit(conn) -> dict[str, int | None]:
    """{table: row_count or None if missing}."""
    out: dict[str, int | None] = {}
    with conn.cursor() as cur:
        for t in LEGACY_TABLES:
            if not _table_exists(cur, t):
                out[t] = None
                continue
            try:
                out[t] = _row_count(cur, t)
            except Exception as e:
                print(f"  WARN: cannot count {t}: {type(e).__name__}: {e}")
                out[t] = -1
    return out


def _backup(conn, ts: str) -> Path:
    """Dump every existing legacy table to Parquet under out/_legacy_backup/<ts>/."""
    import pandas as pd

    backup_dir = BACKUP_ROOT / ts
    backup_dir.mkdir(parents=True, exist_ok=True)
    skipped_log = backup_dir / "skipped.txt"
    skipped_log.write_text("", encoding="utf-8")

    print(f"\nBackup target: {backup_dir}\n")
    with conn.cursor() as cur:
        for t in LEGACY_TABLES:
            if not _table_exists(cur, t):
                msg = f"{t}: not present (skipped)"
                print(f"  - {msg}")
                with skipped_log.open("a", encoding="utf-8") as fp:
                    fp.write(msg + "\n")
                continue
            try:
                cur.execute(f"SELECT * FROM {t}")
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                df = pd.DataFrame(rows, columns=cols)
                # Coerce object cols to string so pyarrow doesn't choke on
                # mixed types / arbitrary jsonb dicts.
                for c in df.columns:
                    if df[c].dtype == "object":
                        df[c] = df[c].astype("string")
                out_path = backup_dir / f"{t}.parquet"
                df.to_parquet(out_path, engine="pyarrow", compression="zstd",
                              index=False)
                print(f"  + {t}: {len(df):>10,} rows -> {out_path.name}")
            except Exception as e:
                print(f"  WARN: backup failed for {t}: {type(e).__name__}: {e}")
    return backup_dir


def _drop(conn) -> None:
    """DROP TABLE IF EXISTS / DROP VIEW IF EXISTS for every legacy entity.
    Uses CASCADE so dependent views/constraints disappear too."""
    print("\nDropping…")
    with conn.cursor() as cur:
        for v in LEGACY_VIEWS:
            cur.execute(f"DROP VIEW IF EXISTS {v} CASCADE")
            print(f"  DROP VIEW IF EXISTS {v}")
        for t in LEGACY_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
            print(f"  DROP TABLE IF EXISTS {t}")
    conn.commit()
    print("Drop complete.")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--backup", action="store_true",
                   help="dump every legacy table to Parquet first")
    p.add_argument("--no-backup", action="store_true",
                   help="explicitly skip backup (use only if you have an "
                        "external dump)")
    p.add_argument("--confirm", action="store_true",
                   help="actually execute the drops. Without this flag, the "
                        "script only audits / backs up.")
    args = p.parse_args()

    if args.no_backup and args.backup:
        print("ERROR: choose --backup OR --no-backup, not both"); return 2

    print("=" * 78)
    print("LEGACY TABLE DROP — Phase D / loop 6")
    print("=" * 78)

    try:
        from connect_into_postgres import connect_to_postgres as pg
        conn = pg.create_connection(application_name="oraes:drop-legacy")
    except (Exception, SystemExit) as e:
        print(f"FAIL: cannot connect to PG: {type(e).__name__}: {e}")
        return 1

    try:
        # 1) Audit.
        print("\nAudit:")
        counts = _audit(conn)
        any_present = False
        for t, n in counts.items():
            if n is None:
                print(f"  {t:30s}  (missing — already dropped)")
            elif n < 0:
                print(f"  {t:30s}  ?? (unreadable)")
                any_present = True
            else:
                print(f"  {t:30s} {n:>10,} rows")
                any_present = True

        if not any_present:
            print("\nNothing to drop. Exiting.")
            return 0

        # 2) Backup.
        if args.backup:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = _backup(conn, ts)
            print(f"\nBackup written to {backup_dir}")
        elif not args.no_backup and args.confirm:
            print("\nABORT: refuse to --confirm without --backup or --no-backup")
            return 2

        # 3) Drop.
        if not args.confirm:
            print("\nDRY RUN — re-run with --confirm to actually drop.")
            print("(use --backup to also export each table to Parquet first)")
            return 0

        print("\n" + "!" * 78)
        print("ABOUT TO DROP TABLES + VIEWS (CASCADE).")
        print(f"Backup: {'YES — ' + str(BACKUP_ROOT) if args.backup else 'NO'}")
        print("Tables:", ", ".join(LEGACY_TABLES))
        print("Views: ", ", ".join(LEGACY_VIEWS))
        print("!" * 78)

        _drop(conn)
        return 0
    finally:
        try: conn.close()
        except Exception: pass


if __name__ == "__main__":
    sys.exit(main())
