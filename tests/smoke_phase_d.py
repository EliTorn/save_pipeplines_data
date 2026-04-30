"""Phase A-D smoke checks. Run: `python -m tests.smoke_phase_d`.

Designed to pass *without* a live PG instance. Verifies:
  1. All recent files parse + import.
  2. Parquet roundtrip works.
  3. DuckDB catalog initializes.
  4. run_summary.record_run is callable (no-ops cleanly when PG unreachable).
  5. observability.log_{connection,query,batch} are callable (same).
  6. Static check: csv_writer.save_diffs does NOT import write_through.
  7. Static check: legacy heavy insert helpers are gone.
  8. Static check: registry module is removed.
  9. Static check: sync_out cleanup helpers are gone.
"""
from __future__ import annotations

import ast
import importlib
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FAILURES: list[str] = []
PASSES: list[str] = []


def _ok(label: str) -> None:
    PASSES.append(label)
    print(f"  PASS  {label}")


def _fail(label: str, err: str) -> None:
    FAILURES.append(f"{label}: {err}")
    print(f"  FAIL  {label}: {err}")


def check_parses() -> None:
    print("[1] parse all changed files")
    files = [
        "_pipeline_logging.py",
        "main.py",
        "app.py",
        "core/csv_writer.py",
        "core/runner.py",
        "core/batch.py",
        "core/parquet_writer.py",
        "core/duckdb_catalog.py",
        "core/analytics.py",
        "connect_into_postgres/_pg_cache.py",
        "connect_into_postgres/run_summary.py",
        "connect_into_postgres/observability.py",
        "connect_into_postgres/write_through.py",
        "connect_into_postgres/connection_audit.py",
        "connect_into_postgres/drop_legacy_tables.py",
        "apply_changes/apply_changes.py",
        "apply_changes/pg_tracking.py",
        "apply_changes/duckdb_source.py",
        "connect_into_orcal/logging_setup.py",
        "connect_into_es/logging_setup.py",
        "connect_into_postgres/logging_setup.py",
    ]
    for f in files:
        path = ROOT / f
        if not path.is_file():
            _fail(f"parse {f}", "missing")
            continue
        try:
            ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as e:
            _fail(f"parse {f}", str(e))
            continue
        _ok(f"parse {f}")


def check_imports() -> None:
    print("[2] import key modules")
    for mod in [
        "core.parquet_writer",
        "core.duckdb_catalog",
        "core.analytics",
        "core.csv_writer",
        "connect_into_postgres.run_summary",
        "connect_into_postgres.observability",
        "connect_into_postgres.write_through",
        "apply_changes.duckdb_source",
    ]:
        try:
            importlib.import_module(mod)
            _ok(f"import {mod}")
        except Exception as e:
            _fail(f"import {mod}", f"{type(e).__name__}: {e}")


def check_parquet_roundtrip() -> None:
    print("[3] parquet write/read roundtrip")
    try:
        import pandas as pd
        from core.parquet_writer import save_parquet, parquet_path_for
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            df_in = pd.DataFrame([
                {"id": "1", "field": "name", "oracle_value": "a", "es_value": "b", "status": "diff"},
                {"id": "2", "field": "age",  "oracle_value": "10", "es_value": "11", "status": "diff"},
            ])
            pq = parquet_path_for(p)
            written = save_parquet(df_in, pq)
            assert written is not None and written.is_file(), "parquet not written"
            df_out = pd.read_parquet(written)
            assert len(df_out) == 2, f"expected 2 rows, got {len(df_out)}"
            assert list(df_out.columns) == list(df_in.columns), "columns differ"
        _ok("parquet roundtrip (2 rows, 5 cols)")
    except Exception as e:
        _fail("parquet roundtrip", f"{type(e).__name__}: {e}")


def check_duckdb_catalog_init() -> None:
    print("[4] duckdb catalog init")
    try:
        from core import duckdb_catalog
        path = duckdb_catalog.init_catalog()
        if path is None:
            _ok("init_catalog returned None (duckdb not available — acceptable)")
            return
        assert path.is_file(), f"catalog file missing at {path}"
        _ok(f"init_catalog -> {path.name}")
    except Exception as e:
        _fail("duckdb_catalog.init_catalog", f"{type(e).__name__}: {e}")


def check_run_summary_no_pg() -> None:
    print("[5] run_summary.record_run with PG unreachable")
    try:
        from connect_into_postgres import run_summary
        rid = run_summary.record_run(
            run_id="smoketest", env="stage", target_name="smoke",
            operation="compare", rows_count=0, started_at=datetime.now(timezone.utc),
            ended_at=datetime.now(timezone.utc), status="ok",
        )
        # No PG -> returns None, never raises.
        if rid is None:
            _ok("record_run returned None (PG unreachable, expected)")
        else:
            _ok(f"record_run inserted id={rid}")
    except Exception as e:
        _fail("run_summary.record_run", f"{type(e).__name__}: {e}")


def check_observability_no_pg() -> None:
    print("[6] observability.log_* with PG unreachable")
    try:
        from connect_into_postgres import observability
        now = datetime.now(timezone.utc)
        observability.log_connection(
            run_id="smoketest", env="stage", system_name="oracle",
            target_name="ORCL", host="orahost",
            started_at=now, ended_at=now, duration_ms=10, status="ok",
        )
        observability.log_query(
            run_id="smoketest", env="stage", batch_id="1",
            system_name="oracle", target_name="T", operation="select",
            started_at=now, ended_at=now, duration_ms=5,
            rows_returned=10, status="ok",
        )
        observability.log_batch(
            run_id="smoketest", env="stage", batch_id="1",
            target_name="T", operation="compare",
            started_at=now, ended_at=now, duration_ms=8,
            rows_returned=10, status="ok",
        )
        _ok("log_connection / log_query / log_batch (no exceptions)")
    except Exception as e:
        _fail("observability.log_*", f"{type(e).__name__}: {e}")


def check_save_diffs_does_not_import_write_through() -> None:
    print("[7] save_diffs does NOT import write_through")
    src = (ROOT / "core" / "csv_writer.py").read_text(encoding="utf-8")
    if "write_through" in src or "registry" in src:
        _fail("save_diffs imports", "csv_writer.py still references write_through/registry")
    else:
        _ok("csv_writer.py free of legacy heavy-write imports")


def check_legacy_helpers_gone() -> None:
    print("[8] legacy heavy insert helpers are gone")
    from connect_into_postgres import write_through
    for fn in ("insert_changes_df", "insert_missing_df",
               "init_worker_pg", "worker_pg_conn", "close_worker_pg",
               "_bulk_insert"):
        if hasattr(write_through, fn):
            _fail(f"write_through.{fn}", "still present (should be deleted)")
        else:
            _ok(f"write_through.{fn} removed")


def check_registry_module_gone() -> None:
    print("[9] registry module deleted")
    p = ROOT / "connect_into_postgres" / "registry.py"
    if p.exists():
        _fail("registry.py", "still present (should be deleted)")
    else:
        _ok("registry.py is gone")


def check_sync_out_deleted() -> None:
    print("[10] sync_out.py + parity_check.py are deleted (loop 6)")
    for f in ("connect_into_postgres/sync_out.py",
              "connect_into_postgres/parity_check.py"):
        if (ROOT / f).exists():
            _fail(f, "still present (should be deleted)")
        else:
            _ok(f"{f} removed")


def check_query_record_carries_env_operation() -> None:
    print("[11] QueryRecord builds row with env + operation")
    try:
        from _pipeline_logging import QueryRecord, QUERY_FIELDS

        class _DummyLogger:
            run_id = "smoketest"
            QUERIES_CSV = ROOT / "tests" / "_smoke_queries.csv"

        rec = QueryRecord(_DummyLogger(), "SELECT 1", env="stage",
                          operation="oracle_select", table="t", batch=1)
        rec.__enter__()
        rec.set_rows(7)
        row = rec._build_row(None)
        assert row.get("env") == "stage", f"env not set: {row.get('env')!r}"
        assert row.get("operation") == "oracle_select", \
            f"operation not set: {row.get('operation')!r}"
        assert "env" in QUERY_FIELDS and "operation" in QUERY_FIELDS, \
            "env/operation missing from QUERY_FIELDS"
        _ok("env=stage, operation=oracle_select round-trip through _build_row")
    except Exception as e:
        _fail("QueryRecord env/operation", f"{type(e).__name__}: {e}")


def check_observability_adapter_propagates() -> None:
    print("[12] observability.from_query_row carries env + operation")
    captured: list[dict] = []

    try:
        from connect_into_postgres import observability
        # Monkeypatch log_query to capture instead of trying PG.
        orig = observability.log_query
        def _capture(**kw): captured.append(kw)
        observability.log_query = _capture  # type: ignore
        try:
            row = {
                "run_id": "rsmoke", "env": "prod", "operation": "es_search",
                "start_ts": "2026-04-29T10:00:00Z", "end_ts": "2026-04-29T10:00:01.500Z",
                "seconds": 1.5, "rows": 42, "status": "ok",
                "sql": "{...}", "sql_hash": "h1", "table": "cardusers",
                "batch": 3, "owner": None,
            }
            observability.from_query_row(row, "elasticsearch")
        finally:
            observability.log_query = orig  # type: ignore

        assert len(captured) == 1, f"expected 1 captured insert, got {len(captured)}"
        kw = captured[0]
        assert kw["env"] == "prod", f"env not propagated: {kw.get('env')!r}"
        assert kw["operation"] == "es_search", f"operation not propagated: {kw.get('operation')!r}"
        assert kw["system_name"] == "elasticsearch"
        assert kw["target_name"] == "cardusers"
        assert kw["rows_returned"] == 42
        _ok("env+operation flow through from_query_row -> log_query")
    except Exception as e:
        _fail("from_query_row env/operation", f"{type(e).__name__}: {e}")


def check_call_sites_use_operation() -> None:
    print("[13] every active logger.query/run_tracked call site passes operation")

    expected = [
        ("connect_into_orcal/connect_to_orcal.py",
         'operation: str = "oracle_select"'),
        ("connect_into_es/connect_to_es.py",
         'operation="es_search"'),
        ("core/batch.py", 'operation="oracle_select"'),
        ("core/batch.py", 'operation="es_search"'),
        ("core/batch.py", 'operation="oracle_lookup"'),
        ("core/runner.py", 'operation="oracle_range_probe"'),
        ("apply_changes/apply_changes.py", 'operation="es_bulk_update"'),
        ("apply_changes/apply_changes.py", 'operation="es_bulk_create"'),
    ]
    for path, needle in expected:
        src = (ROOT / path).read_text(encoding="utf-8")
        if needle in src:
            _ok(f"{path}: contains {needle!r}")
        else:
            _fail(f"{path}: missing {needle!r}", "operation kwarg not found")


def check_pg_cache_parity() -> None:
    print("[14] every PG-touching module uses the shared CachedConnection")
    import importlib
    expected = [
        "connect_into_postgres.run_summary",
        "connect_into_postgres.observability",
        "apply_changes.pg_source",
        "apply_changes.pg_tracking",
        "apply_changes.duckdb_source",
    ]
    for modname in expected:
        try:
            mod = importlib.import_module(modname)
        except Exception as e:
            _fail(f"import {modname}", f"{type(e).__name__}: {e}")
            continue
        cache = getattr(mod, "_cache", None)
        if cache is None:
            _fail(f"{modname}._cache", "module-level _cache missing")
            continue
        from connect_into_postgres._pg_cache import CachedConnection
        if not isinstance(cache, CachedConnection):
            _fail(f"{modname}._cache", f"not CachedConnection: {type(cache).__name__}")
            continue
        if not callable(getattr(mod, "reset_state", None)):
            _fail(f"{modname}.reset_state", "missing reset_state()")
            continue
        _ok(f"{modname}: CachedConnection + reset_state()")


def check_pg_up_no_tables_scenario() -> None:
    """Regression: PG accepts connections but observability tables don't
    exist yet. log_query / log_batch must NOT crash the caller — they
    should print 'insert failed (silenced)' and return None."""
    print("[15] PG-up-no-tables: log_* never raises")
    try:
        from connect_into_postgres import observability
        # Build a fake conn that raises a UndefinedTable-style error on cursor.
        class _FakeCursor:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, *a, **k):
                raise RuntimeError('relation "connection_log" does not exist')
        class _FakeConn:
            def cursor(self): return _FakeCursor()
            def commit(self): pass
            def rollback(self): pass

        # Temporarily override the cache so _exec uses our fake conn.
        orig = observability._cache.get
        observability._cache.get = lambda: _FakeConn()  # type: ignore
        try:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            observability.log_query(
                run_id="rsmoke", env="stage", batch_id="1",
                system_name="oracle", target_name="t",
                operation="oracle_select",
                started_at=now, ended_at=now, duration_ms=1,
                rows_returned=0, status="ok",
            )
            observability.log_batch(
                run_id="rsmoke", env="stage", batch_id="1",
                target_name="t", operation="compare",
                started_at=now, ended_at=now, duration_ms=1,
                rows_returned=0, status="ok",
            )
        finally:
            observability._cache.get = orig  # type: ignore
        _ok("log_query / log_batch survive 'relation does not exist'")
    except Exception as e:
        _fail("PG-up-no-tables", f"{type(e).__name__}: {e}")


def check_legacy_ddl_gone_from_write_through() -> None:
    print("[18] write_through.py no longer recreates legacy heavy tables")
    src = (ROOT / "connect_into_postgres" / "write_through.py").read_text(encoding="utf-8")
    for needle in ("CREATE TABLE IF NOT EXISTS pipeline_changes",
                   "CREATE TABLE IF NOT EXISTS pipeline_missing"):
        if needle in src:
            _fail(f"write_through.py", f"still emits {needle}")
        else:
            _ok(f"write_through.py: no '{needle[:40]}…'")


def check_pg_tracking_init_schema() -> None:
    print("[19] pg_tracking owns pipeline_apply_batches DDL after loop 6")
    from apply_changes import pg_tracking
    if not callable(getattr(pg_tracking, "init_schema", None)):
        _fail("pg_tracking.init_schema", "missing")
        return
    src = (ROOT / "apply_changes" / "pg_tracking.py").read_text(encoding="utf-8")
    if "CREATE TABLE IF NOT EXISTS pipeline_apply_batches" in src:
        _ok("pg_tracking.py owns pipeline_apply_batches DDL")
    else:
        _fail("pg_tracking.py", "DDL for pipeline_apply_batches not found")


def check_summary_only_mode() -> None:
    print("[20] --summary-only short-circuits before heavy imports")
    main_src = (ROOT / "main.py").read_text(encoding="utf-8")

    # The flag must be parsed before any line that imports Oracle/ES/runner.
    flag_pos = main_src.find('"--summary-only"')
    heavy_imports = (
        "from connect_into_es.connect_to_es",
        "from connect_into_orcal.connect_to_orcal",
        "from core.runner import run_pipeline",
    )
    if flag_pos < 0:
        _fail("main.py", "--summary-only flag not found")
        return
    for needle in heavy_imports:
        pos = main_src.find(needle)
        if pos > 0 and pos < flag_pos:
            _fail("main.py", f"heavy import {needle!r} appears before --summary-only check")
            return
    _ok("--summary-only is checked before heavy imports")

    # Module imports cleanly without Oracle/ES env vars.
    try:
        from connect_into_postgres import summary_report
    except Exception as e:
        _fail("import summary_report", f"{type(e).__name__}: {e}")
        return
    for fn in ("print_report", "_connect", "_columns", "_table_exists",
               "_report_apply_batches", "_report_run_summary"):
        if not callable(getattr(summary_report, fn, None)):
            _fail(f"summary_report.{fn}", "missing")
            return
    _ok("summary_report exports the expected helpers")

    # Application name = oraes:summary-only
    src = (ROOT / "connect_into_postgres" / "summary_report.py").read_text(encoding="utf-8")
    if 'application_name="oraes:summary-only"' in src:
        _ok("summary_report tags its connection oraes:summary-only")
    else:
        _fail("summary_report", "application_name not set to oraes:summary-only")

    # observability.disable() called before any work
    if "observability.disable()" in src:
        _ok("summary_report calls observability.disable() to avoid mirror writes")
    else:
        _fail("summary_report", "missing observability.disable() guard")


def check_pg_connection_settings() -> None:
    """Static check: every PG connection MUST set application_name +
    keepalives + idle_in_transaction_session_timeout. Without these,
    abandoned clients hold their slot until the kernel TCP timeout fires
    (minutes). With them, PG reaps within ~60s."""
    print("[16] PG connection options include application_name + keepalives "
          "+ idle_in_txn timeout")
    src = (ROOT / "connect_into_postgres" / "connect_to_postgres.py").read_text(encoding="utf-8")
    for needle in (
        "application_name=",
        "keepalives=1",
        "keepalives_idle",
        "keepalives_interval",
        "keepalives_count",
        "idle_in_transaction_session_timeout",
        "_default_application_name",
    ):
        if needle in src:
            _ok(f"connect_to_postgres.py uses {needle}")
        else:
            _fail(f"connect_to_postgres.py: {needle}", "missing — slot leaks risk")


def check_atexit_cleanup() -> None:
    """CachedConnection must register an atexit handler so clean process
    exit releases its PG slot immediately."""
    print("[17] CachedConnection registers atexit cleanup")
    src = (ROOT / "connect_into_postgres" / "_pg_cache.py").read_text(encoding="utf-8")
    for needle in ("import atexit", "atexit.register", "_atexit_close"):
        if needle in src:
            _ok(f"_pg_cache.py: {needle}")
        else:
            _fail(f"_pg_cache.py: {needle}", "missing")


def main() -> int:
    print(f"running smoke_phase_d at {ROOT}\n")
    check_parses()
    check_imports()
    check_parquet_roundtrip()
    check_duckdb_catalog_init()
    check_run_summary_no_pg()
    check_observability_no_pg()
    check_save_diffs_does_not_import_write_through()
    check_legacy_helpers_gone()
    check_registry_module_gone()
    check_sync_out_deleted()
    check_legacy_ddl_gone_from_write_through()
    check_pg_tracking_init_schema()
    check_summary_only_mode()
    check_query_record_carries_env_operation()
    check_observability_adapter_propagates()
    check_call_sites_use_operation()
    check_pg_cache_parity()
    check_pg_up_no_tables_scenario()
    check_pg_connection_settings()
    check_atexit_cleanup()
    print()
    print(f"summary: {len(PASSES)} passed, {len(FAILURES)} failed")
    if FAILURES:
        for f in FAILURES:
            print("  -", f)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
