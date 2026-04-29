"""Apply pipeline diffs back into Elasticsearch.

Reads `out/<EVENT>/changes/`:
  - changes_*.csv      -> field-level diffs; runs _update_by_query (multi-field painless)
  - missing_in_es_*.csv -> full Oracle rows missing on ES; bulk _create (skip existing)

YAML drives everything (INDEX_NAME, parts → mapping CSVs for type coercion).

Usage:
    python -m apply_changes.apply_changes --event PLAYERBONUS --mode both --env prod
    python -m apply_changes.apply_changes --event PLAYERBONUS --mode changes --dry
    python -m apply_changes.apply_changes --event CARDUSERS --mode missing --env stage
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import sys as _sys
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))

YAML_PATH = _ROOT / "settings" / "events.yaml"
SETTINGS_DIR = _ROOT / "settings"
OUT_DIR = _ROOT / "out"
INDEXES_DIR = SETTINGS_DIR / "indexes"


def schema_csv_path(index: str) -> Path:
    """settings/indexes/<index>/schema.csv (per-index layout)."""
    return INDEXES_DIR / index / "schema.csv"

from _pipeline_env import env_truthy, normalize_es_env  # noqa: E402

from apply_changes import es_schema as es_schema_io
from apply_changes import pg_tracking
from apply_changes import pg_source
from apply_changes import duckdb_source
from connect_into_postgres import run_summary, observability
from core import duckdb_catalog
from core.adapter_loader import get_adapter
from core.coerce import field_types
from datetime import timezone as _tz
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
BATCH_SIZE = int(os.getenv("APPLY_BATCH_SIZE", "1000"))
POLL_INTERVAL = float(os.getenv("APPLY_POLL_INTERVAL_SEC", "2.0"))


# ---------------------------------------------------------------------------
# Audit log (per env). Records every insert/update applied to ES.
# ---------------------------------------------------------------------------

class AuditLog:
    """JSONL audit writer. One line per doc op (insert/update) + per-batch summary."""

    def __init__(self, env: str, event: str, index: str):
        self.env = env
        self.event = event
        self.index = index
        self.path: Path | None = None
        self.fp = None
        self.enabled = env_truthy("APPLY_LOG_ENABLED", default=True)
        if not self.enabled:
            return
        env_key = "STAGE" if env == "stage" else "PRODE"
        base = Path(os.getenv(f"APPLY_LOG_DIR_{env_key}") or f"out/_apply_log/{env}")
        if not base.is_absolute():
            base = _ROOT / base
        base.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.path = base / f"{event}_{stamp}.jsonl"
        self.fp = self.path.open("w", encoding="utf-8")
        self._write({"ts": _now(), "type": "run_start", "env": env,
                     "event": event, "index": index})

    def _write(self, obj: dict) -> None:
        if not self.fp:
            return
        self.fp.write(json.dumps(obj, default=str, ensure_ascii=False) + "\n")
        self.fp.flush()

    def update(self, doc_id: str, changes: dict, batch: int) -> None:
        # changes: {field: {"old": ..., "new": ...}}
        self._write({"ts": _now(), "type": "update", "env": self.env,
                     "event": self.event, "index": self.index,
                     "id": doc_id, "batch": batch, "changes": changes})

    def update_batch_result(self, batch: int, ids: list[str],
                            updated: int, conflicts: int, failures: int) -> None:
        self._write({"ts": _now(), "type": "update_batch_result",
                     "batch": batch, "ids_planned": len(ids),
                     "updated": updated, "conflicts": conflicts, "failures": failures})

    def insert(self, doc_id: str, body: dict, status: int, result: str,
               batch: int, error: str | None = None) -> None:
        self._write({"ts": _now(), "type": "insert", "env": self.env,
                     "event": self.event, "index": self.index,
                     "id": doc_id, "batch": batch,
                     "es_status": status, "result": result,
                     "error": error, "body": body})

    def close(self, summary: dict | None = None) -> None:
        if not self.fp:
            return
        if summary:
            self._write({"ts": _now(), "type": "run_end", **summary})
        self.fp.close()
        self.fp = None
        if self.path:
            print(f"audit log: {self.path}")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# YAML / mapping introspection
# ---------------------------------------------------------------------------

def load_event(event: str) -> dict:
    from settings.loader import load_events
    cfg = load_events()
    if event not in cfg:
        sys.exit(f"event '{event}' not in events.yaml; available: {list(cfg)}")
    return cfg[event]


def collect_mapping_rows(entry: dict) -> list[dict]:
    """Union of mapping CSV rows across all parts (or single VALUE_COLM)."""
    rows: list[dict] = []
    parts = entry.get("parts") or [entry]
    for p in parts:
        rel = p.get("VALUE_COLM")
        if not rel:
            continue
        path = SETTINGS_DIR / rel
        if not path.is_file():
            continue
        with open(path, encoding="utf-8", newline="") as f:
            rows.extend(csv.DictReader(f))
    return rows


# ---------------------------------------------------------------------------
# ES client (per env)
# ---------------------------------------------------------------------------

def es_client(env: str) -> tuple[str, "HTTPBasicAuth | None", bool]:
    if normalize_es_env(env) == "prod":
        url = (os.getenv("ES_URL_PRODE") or os.getenv("ES_URL_PROD") or os.getenv("ES_URL") or "").strip()
        user = os.getenv("ES_USER")
        pw = os.getenv("ES_PASS")
        if not (url and user and pw):
            sys.exit("env=prod requires ES_URL_PRODE + ES_USER + ES_PASS")
        return url, HTTPBasicAuth(user, pw), False
    url = (os.getenv("ES_URL_STAGE") or "").strip()
    if not url:
        sys.exit("env=stage requires ES_URL_STAGE")
    return url, None, False


def _post(url, path, body, auth, verify):
    r = requests.post(url + path, auth=auth, verify=verify, json=body,
                      headers=HEADERS, timeout=(5, 600))
    if not r.ok:
        raise requests.HTTPError(f"ES POST {path} -> {r.status_code} {r.text[:400]}")
    return r.json()


def _get(url, path, auth, verify):
    r = requests.get(url + path, auth=auth, verify=verify, headers=HEADERS, timeout=(5, 120))
    if not r.ok:
        raise requests.HTTPError(f"ES GET {path} -> {r.status_code} {r.text[:400]}")
    return r.json()


def wait_task(url, task_id, auth, verify) -> dict:
    while True:
        r = _get(url, f"/_tasks/{task_id}", auth, verify)
        if r.get("completed"):
            return r
        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Mode A: apply field-level diffs (changes_*.csv)
# ---------------------------------------------------------------------------

def apply_changes(changes_dir: Path, index: str, adapter, pk: str,
                  url: str, auth, verify: bool, dry: bool,
                  audit: "AuditLog | None" = None,
                  schema: dict[str, dict] | None = None,
                  event: str | None = None, env: str | None = None,
                  force: bool = False, run_id: str | None = None,
                  data_source: str = "duckdb") -> None:
    """data_source: 'pg'     = read pipeline_changes (applied_ts IS NULL) — legacy,
                    'duckdb' = read v_changes (Parquet) — default,
                    'csv'    = read changes_*.csv from disk (legacy),
                    'auto'   = try PG first, then DuckDB, then CSV."""
    started_at = datetime.now(_tz.utc)
    docs: dict[str, dict] = {}
    audit_changes: dict[str, dict] = {}
    file_doc_ids: dict[Path, set[str]] = {}
    chosen_source: str | None = None

    # Source priority: explicit choice wins; auto = pg -> duckdb -> csv.
    def _consume(rows: list[dict], label: str) -> None:
        nonlocal chosen_source
        chosen_source = label
        print(f"[apply] reading {len(rows)} pending diff row(s) from {label}")
        files_seen: dict[str, set[str]] = {}
        for r in rows:
            doc_id = (str(r.get(pk) or r.get("id") or "")).strip()
            field = (r.get("field") or "").strip()
            if not doc_id or not field or field == "*":
                continue
            new_val = adapter.coerce_for_es(field, r.get("oracle_value"))
            old_val = r.get("es_value")
            docs.setdefault(doc_id, {})[field] = new_val
            audit_changes.setdefault(doc_id, {})[field] = {"old": old_val, "new": new_val}
            src = r.get("source_file") or ""
            files_seen.setdefault(src, set()).add(doc_id)
        for src, ids in files_seen.items():
            # Always key by CSV-rel path so pg_tracking.mark_applied works.
            csv_rel = duckdb_source._parquet_to_csv_rel(src)
            file_doc_ids[_ROOT / "out" / csv_rel] = ids

    if event and env and data_source in ("pg", "auto") and pg_source.is_available():
        pg_rows = pg_source.load_pending_changes(event, env, pk=pk)
        if pg_rows or data_source == "pg":
            _consume(pg_rows, "postgres (pipeline_changes WHERE applied_ts IS NULL)")

    if chosen_source is None and event and env \
            and data_source in ("duckdb", "auto") \
            and duckdb_source.is_available():
        dk_rows = duckdb_source.load_pending_changes(event, env, pk=pk)
        if dk_rows or data_source == "duckdb":
            _consume(dk_rows, "duckdb (v_changes from local Parquet)")

    if chosen_source is None:
        chosen_source = "csv"
        # Legacy CSV path.
        files = sorted(changes_dir.glob("changes_*.csv"))
        if not files:
            print("no changes_*.csv files found"); return
        if event and env and not dry:
            files, skipped = pg_tracking.filter_unapplied(
                files, event, env, "changes", force=force)
            if skipped:
                print(f"skipping {len(skipped)} already-applied changes file(s) "
                      f"(use --force to re-apply)")
                for f in skipped:
                    print(f"  skip: {f.name}")
            if not files:
                print("nothing to apply (all changes files already applied)")
                return
        for f in files:
            df = pd.read_csv(f, dtype=str, keep_default_na=False)
            ids_here: set[str] = set()
            for _, row in df.iterrows():
                doc_id = (row.get(pk) or row.get("id") or "").strip()
                field = (row.get("field") or "").strip()
                if not doc_id or not field or field == "*":
                    continue
                new_val = adapter.coerce_for_es(field, row.get("oracle_value"))
                old_val = row.get("es_value")
                docs.setdefault(doc_id, {})[field] = new_val
                audit_changes.setdefault(doc_id, {})[field] = {"old": old_val, "new": new_val}
                ids_here.add(doc_id)
            file_doc_ids[f] = ids_here

    if not docs:
        print("no usable diffs")
        if event and env and run_id:
            run_summary.record_run(
                run_id=run_id, env=env, target_name=index, operation="apply_changes",
                rows_count=0, started_at=started_at,
                ended_at=datetime.now(_tz.utc), status="ok",
                error=None,
            )
        return
    print(f"docs to update: {len(docs)}  "
          f"(across {len(file_doc_ids)} source file(s), "
          f"source={chosen_source})")

    # Pre-flight schema validation across the whole plan (incl. adapter-added fields).
    if schema:
        preview = {did: adapter.before_apply(dict(fields)) for did, fields in docs.items()}
        errors: list[str] = []
        for did, fields in preview.items():
            errors.extend(es_schema_io.validate_doc(did, fields, schema))
            if len(errors) >= 50:
                break
        if errors:
            print(f"ABORT — schema validation failed for index '{index}' "
                  f"({len(errors)} error{'s' if len(errors)!=1 else ''}, showing up to 50):")
            for e in errors[:50]:
                print(f"  {e}")
            sys.exit(2)

    if dry:
        # show first 3 for sanity
        for k in list(docs)[:3]:
            print(f"  would update {k}: {docs[k]}")
        print(f"[DRY] {len(docs)} docs would be updated")
        return

    ids = list(docs.keys())
    updated = conflicts = failures = 0
    batches = (len(ids) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(ids), BATCH_SIZE):
        chunk_ids = ids[i:i + BATCH_SIZE]
        chunk_map: dict[str, dict] = {}
        for k in chunk_ids:
            before = dict(docs[k])
            after = adapter.before_apply(before)
            chunk_map[k] = after
            for added in set(after.keys()) - set(docs[k].keys()):
                audit_changes.setdefault(k, {})[added] = {"old": None, "new": after[added]}
        body = {
            "query": {"terms": {"_id": chunk_ids}},
            "script": {
                "lang": "painless",
                "source": (
                    "def m = params.m.get(ctx._id);"
                    "if (m != null) { for (entry in m.entrySet()) "
                    "{ ctx._source.put(entry.getKey(), entry.getValue()); } }"
                ),
                "params": {"m": chunk_map},
            },
        }
        idx_n = i // BATCH_SIZE + 1
        batch_started = datetime.now(_tz.utc)
        try:
            r = _post(url,
                      f"/{index}/_update_by_query?wait_for_completion=false&conflicts=proceed&refresh=false",
                      body, auth, verify)
            res = wait_task(url, r["task"], auth, verify).get("response", {})
            u = res.get("updated", 0)
            c = res.get("version_conflicts", 0)
            fcount = len(res.get("failures", []))
            updated += u; conflicts += c; failures += fcount
            print(f"  changes batch {idx_n}/{batches}: updated={u} conflicts={c} failures={fcount}")
            if audit:
                for did in chunk_ids:
                    audit.update(did, audit_changes.get(did, {}), idx_n)
                audit.update_batch_result(idx_n, chunk_ids, u, c, fcount)
            batch_ended = datetime.now(_tz.utc)
            duration_ms = int((batch_ended - batch_started).total_seconds() * 1000)
            try:
                observability.log_batch(
                    run_id=run_id, env=env, batch_id=str(idx_n),
                    target_name=index, operation="apply_changes",
                    source_system="elasticsearch",
                    rows_requested=len(chunk_ids), rows_returned=u,
                    rows_changed=u, rows_missing=None,
                    started_at=batch_started, ended_at=batch_ended,
                    duration_ms=duration_ms,
                    status="failed" if fcount > 0 else "ok",
                    error=(f"{fcount} failures, {c} conflicts" if fcount else None),
                )
                observability.log_query(
                    run_id=run_id, env=env, batch_id=str(idx_n),
                    system_name="elasticsearch", target_name=index,
                    operation="es_bulk_update",
                    query_text=json.dumps(body, default=str)[:8000],
                    started_at=batch_started, ended_at=batch_ended,
                    duration_ms=duration_ms,
                    rows_returned=u, rows_affected=u,
                    status="failed" if fcount > 0 else "ok",
                    error=(f"{fcount} failures, {c} conflicts" if fcount else None),
                )
            except Exception:
                pass
        except Exception as e:
            failures += len(chunk_ids)
            print(f"  changes batch {idx_n}/{batches} FAILED: {e}")
            if audit:
                audit.update_batch_result(idx_n, chunk_ids, 0, 0, len(chunk_ids))
            try:
                observability.log_batch(
                    run_id=run_id, env=env, batch_id=str(idx_n),
                    target_name=index, operation="apply_changes",
                    source_system="elasticsearch",
                    rows_requested=len(chunk_ids), rows_returned=0,
                    rows_changed=0, rows_missing=None,
                    started_at=batch_started, ended_at=datetime.now(_tz.utc),
                    duration_ms=None, status="failed",
                    error=str(e),
                )
                observability.log_query(
                    run_id=run_id, env=env, batch_id=str(idx_n),
                    system_name="elasticsearch", target_name=index,
                    operation="es_bulk_update",
                    query_text=json.dumps(body, default=str)[:8000],
                    started_at=batch_started, ended_at=datetime.now(_tz.utc),
                    duration_ms=None,
                    rows_returned=0, rows_affected=0,
                    status="failed", error=str(e),
                )
            except Exception:
                pass
    print(f"changes done: updated={updated} conflicts={conflicts} failures={failures}")

    if event and env and not dry:
        if failures > 0:
            print(f"[pg-tracking] failures>0 → not marking files as applied "
                  f"(re-run will retry; use --force to override)")
        else:
            marked = 0
            n_sources = len(file_doc_ids)
            for f, ids in file_doc_ids.items():
                if pg_tracking.mark_applied(
                    f, event=event, env=env, mode="changes", run_id=run_id,
                    docs_planned=len(ids),
                    es_updated=updated, es_conflicts=conflicts, es_failures=failures,
                    notes=f"aggregate stats across {n_sources} file(s) in this run",
                ):
                    marked += 1
            print(f"[pg-tracking] marked {marked}/{n_sources} file(s) as applied")

    if event and env and run_id:
        # one summary row per (run, env, index, operation)
        rep_src = next(iter(file_doc_ids), None)
        rep = str(rep_src.relative_to(_ROOT)) if rep_src else None
        run_summary.record_run(
            run_id=run_id, env=env, target_name=index, operation="apply_changes",
            rows_count=updated, source_file=rep if len(file_doc_ids) == 1 else None,
            started_at=started_at, ended_at=datetime.now(_tz.utc),
            status="failed" if failures > 0 else "ok",
            error=(f"{failures} failures, {conflicts} conflicts"
                   if failures > 0 else None),
        )


# ---------------------------------------------------------------------------
# Mode B: insert missing docs (missing_in_es_*.csv) — never overwrite
# ---------------------------------------------------------------------------

def apply_missing(changes_dir: Path, index: str, adapter, pk: str,
                  url: str, auth, verify: bool, dry: bool,
                  audit: "AuditLog | None" = None,
                  schema: dict[str, dict] | None = None,
                  event: str | None = None, env: str | None = None,
                  force: bool = False, run_id: str | None = None,
                  data_source: str = "duckdb") -> None:
    started_at = datetime.now(_tz.utc)
    rows_total: list[dict] = []
    file_row_counts: dict[Path, int] = {}
    chosen_source: str | None = None

    def _consume(rows: list[dict], label: str) -> None:
        nonlocal chosen_source
        chosen_source = label
        print(f"[apply] reading {len(rows)} pending missing row(s) from {label}")
        files_seen: dict[str, int] = {}
        for r in rows:
            src = r.pop("source_file", "") or ""
            if "error" in r:
                r.pop("error", None)
            rows_total.append(r)
            files_seen[src] = files_seen.get(src, 0) + 1
        for src, n in files_seen.items():
            csv_rel = duckdb_source._parquet_to_csv_rel(src)
            file_row_counts[_ROOT / "out" / csv_rel] = n

    if event and env and data_source in ("pg", "auto") and pg_source.is_available():
        pg_rows = pg_source.load_pending_missing(event, env, pk=pk)
        if pg_rows or data_source == "pg":
            _consume(pg_rows, "postgres (pipeline_missing WHERE applied_ts IS NULL)")

    if chosen_source is None and event and env \
            and data_source in ("duckdb", "auto") \
            and duckdb_source.is_available():
        dk_rows = duckdb_source.load_pending_missing(event, env, pk=pk)
        if dk_rows or data_source == "duckdb":
            _consume(dk_rows, "duckdb (v_missing from local Parquet)")

    if chosen_source is None:
        chosen_source = "csv"
        files = sorted(changes_dir.glob("missing_in_es_*.csv"))
        if not files:
            print("no missing_in_es_*.csv files found"); return
        if event and env and not dry:
            files, skipped = pg_tracking.filter_unapplied(
                files, event, env, "missing", force=force)
            if skipped:
                print(f"skipping {len(skipped)} already-applied missing file(s) "
                      f"(use --force to re-apply)")
                for f in skipped:
                    print(f"  skip: {f.name}")
            if not files:
                print("nothing to apply (all missing files already applied)")
                return
        for f in files:
            df = pd.read_csv(f, dtype=str, keep_default_na=False)
            if "error" in df.columns:
                df = df.drop(columns=["error"])
            recs = df.to_dict(orient="records")
            rows_total.extend(recs)
            file_row_counts[f] = len(recs)
    if not rows_total:
        print("no missing rows")
        if event and env and run_id:
            run_summary.record_run(
                run_id=run_id, env=env, target_name=index, operation="apply_missing",
                rows_count=0, started_at=started_at,
                ended_at=datetime.now(_tz.utc), status="ok", error=None,
            )
        return
    print(f"docs to insert: {len(rows_total)} "
          f"(across {len(file_row_counts)} source file(s), "
          f"source={chosen_source})")

    # Coerce per-field, drop empty values
    docs: list[tuple[str, dict]] = []
    for r in rows_total:
        doc_id = (r.get(pk) or r.get("id") or "").strip()
        if not doc_id or doc_id.lower() in ("none", "nan", "<na>", "null"):
            continue
        body = {}
        for field, raw in r.items():
            v = adapter.coerce_for_es(field, raw)
            if v is None:
                continue
            body[field] = v
        body = adapter.before_apply(body)
        docs.append((doc_id, body))

    if schema:
        errors: list[str] = []
        for did, body in docs:
            errors.extend(es_schema_io.validate_doc(did, body, schema))
            if len(errors) >= 50:
                break
        if errors:
            print(f"ABORT — schema validation failed for index '{index}' "
                  f"({len(errors)} error{'s' if len(errors)!=1 else ''}, showing up to 50):")
            for e in errors[:50]:
                print(f"  {e}")
            sys.exit(2)

    if dry:
        for k, b in docs[:3]:
            print(f"  would create _id={k}: {json.dumps(b, default=str)}")
        print(f"[DRY] {len(docs)} docs would be created (skips existing)")
        return

    created = exists = errors = 0
    batches = (len(docs) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(docs), BATCH_SIZE):
        chunk = docs[i:i + BATCH_SIZE]
        # _bulk with `create` action — fails per-row if id already exists (we want that)
        bulk_lines = []
        for doc_id, body in chunk:
            bulk_lines.append(json.dumps({"create": {"_index": index, "_id": doc_id}}))
            bulk_lines.append(json.dumps(body, default=str))
        bulk_body = "\n".join(bulk_lines) + "\n"
        idx_n = i // BATCH_SIZE + 1
        batch_started = datetime.now(_tz.utc)
        batch_created = batch_exists = batch_errors = 0
        try:
            r = requests.post(url + "/_bulk?refresh=false", auth=auth, verify=verify,
                              data=bulk_body,
                              headers={"Accept": "application/json", "Content-Type": "application/x-ndjson"},
                              timeout=(5, 600))
            r.raise_for_status()
            resp = r.json()
            items = resp.get("items", [])
            for j, item in enumerate(items):
                op = item.get("create", {})
                status = op.get("status")
                if status == 201:
                    created += 1; batch_created += 1; result = "created"; err = None
                elif status == 409:
                    exists += 1; batch_exists += 1; result = "exists"; err = None
                else:
                    errors += 1; batch_errors += 1; result = "error"
                    err = json.dumps(op.get("error"), default=str) if op.get("error") else None
                if audit and j < len(chunk):
                    did, body = chunk[j]
                    audit.insert(did, body, status or 0, result, idx_n, err)
            print(f"  missing batch {idx_n}/{batches}: created={created} skipped(exists)={exists} errors={errors}")
            batch_ended = datetime.now(_tz.utc)
            duration_ms = int((batch_ended - batch_started).total_seconds() * 1000)
            try:
                observability.log_batch(
                    run_id=run_id, env=env, batch_id=str(idx_n),
                    target_name=index, operation="apply_missing",
                    source_system="elasticsearch",
                    rows_requested=len(chunk), rows_returned=batch_created,
                    rows_changed=batch_created, rows_missing=batch_exists,
                    started_at=batch_started, ended_at=batch_ended,
                    duration_ms=duration_ms,
                    status="failed" if batch_errors > 0 else "ok",
                    error=(f"{batch_errors} errors" if batch_errors else None),
                )
                observability.log_query(
                    run_id=run_id, env=env, batch_id=str(idx_n),
                    system_name="elasticsearch", target_name=index,
                    operation="es_bulk_create",
                    query_text=f"_bulk create n={len(chunk)} target={index}",
                    started_at=batch_started, ended_at=batch_ended,
                    duration_ms=duration_ms,
                    rows_returned=batch_created, rows_affected=batch_created,
                    status="failed" if batch_errors > 0 else "ok",
                    error=(f"{batch_errors} errors" if batch_errors else None),
                )
            except Exception:
                pass
        except Exception as e:
            errors += len(chunk)
            print(f"  missing batch {idx_n}/{batches} FAILED: {e}")
            if audit:
                for did, body in chunk:
                    audit.insert(did, body, 0, "error", idx_n, str(e))
            try:
                end_ts = datetime.now(_tz.utc)
                observability.log_batch(
                    run_id=run_id, env=env, batch_id=str(idx_n),
                    target_name=index, operation="apply_missing",
                    source_system="elasticsearch",
                    rows_requested=len(chunk), rows_returned=0,
                    rows_changed=0, rows_missing=None,
                    started_at=batch_started, ended_at=end_ts,
                    duration_ms=None, status="failed",
                    error=str(e),
                )
                observability.log_query(
                    run_id=run_id, env=env, batch_id=str(idx_n),
                    system_name="elasticsearch", target_name=index,
                    operation="es_bulk_create",
                    query_text=f"_bulk create n={len(chunk)} target={index}",
                    started_at=batch_started, ended_at=end_ts,
                    duration_ms=None,
                    rows_returned=0, rows_affected=0,
                    status="failed", error=str(e),
                )
            except Exception:
                pass
    print(f"missing done: created={created} skipped(exists)={exists} errors={errors}")

    if event and env and not dry:
        if errors > 0:
            print(f"[pg-tracking] errors>0 → not marking files as applied "
                  f"(re-run will retry; use --force to override)")
        else:
            marked = 0
            n_sources = len(file_row_counts)
            for f, n in file_row_counts.items():
                if pg_tracking.mark_applied(
                    f, event=event, env=env, mode="missing", run_id=run_id,
                    docs_planned=n,
                    es_created=created, es_conflicts=exists, es_failures=errors,
                    notes=f"aggregate stats across {n_sources} file(s) in this run",
                ):
                    marked += 1
            print(f"[pg-tracking] marked {marked}/{n_sources} file(s) as applied")

    if event and env and run_id:
        rep_src = next(iter(file_row_counts), None)
        rep = str(rep_src.relative_to(_ROOT)) if rep_src else None
        run_summary.record_run(
            run_id=run_id, env=env, target_name=index, operation="apply_missing",
            rows_count=created, source_file=rep if len(file_row_counts) == 1 else None,
            started_at=started_at, ended_at=datetime.now(_tz.utc),
            status="failed" if errors > 0 else "ok",
            error=(f"{errors} errors, {exists} exists"
                   if errors > 0 else None),
        )


# ---------------------------------------------------------------------------
# Source diagnostics — answers "why did apply_changes find 0 rows?".
# ---------------------------------------------------------------------------

def _diagnose_sources(event: str, env: str, source: str, changes_dir: Path) -> None:
    """Print a short, actionable summary of every potential source so a
    0-row result is explainable. Runs before any data is consumed; cheap."""
    print()
    print("[diag] -------------------------------------------------------------")
    print(f"[diag] event={event} env={env} requested_source={source}")
    print(f"[diag] changes_dir: {changes_dir}")
    if not changes_dir.is_dir():
        print("[diag]   NOT A DIRECTORY — no local files for this (event, env)")
    else:
        ch_csv  = sorted(changes_dir.glob("changes_*.csv"))
        ch_pq   = sorted(changes_dir.glob("changes_*.parquet"))
        mi_csv  = sorted(changes_dir.glob("missing_in_es_*.csv"))
        mi_pq   = sorted(changes_dir.glob("missing_in_es_*.parquet"))
        print(f"[diag]   changes_*.csv     : {len(ch_csv)} file(s)")
        for f in ch_csv[:3]:
            print(f"[diag]     - {f.name}  ({f.stat().st_size:,} bytes)")
        if len(ch_csv) > 3: print(f"[diag]     ... +{len(ch_csv)-3} more")
        print(f"[diag]   changes_*.parquet : {len(ch_pq)} file(s)")
        for f in ch_pq[:3]:
            print(f"[diag]     - {f.name}  ({f.stat().st_size:,} bytes)")
        if len(ch_pq) > 3: print(f"[diag]     ... +{len(ch_pq)-3} more")
        print(f"[diag]   missing_in_es_*.csv     : {len(mi_csv)} file(s)")
        print(f"[diag]   missing_in_es_*.parquet : {len(mi_pq)} file(s)")

    # DuckDB side — show catalog path, view definitions, row counts,
    # sample doc_ids for this (event, env).
    try:
        from core import duckdb_catalog
        print(f"[diag] duckdb catalog: {duckdb_catalog.DUCKDB_PATH}  "
              f"exists={duckdb_catalog.DUCKDB_PATH.is_file()}")

        df = duckdb_catalog.query(
            "SELECT view_name, sql FROM duckdb_views "
            "WHERE view_name IN ('v_changes', 'v_missing')"
        )
        if df is not None and len(df) > 0:
            for _, r in df.iterrows():
                preview = (r["sql"] or "").split("\n", 1)[0][:160]
                print(f"[diag]   view {r['view_name']}: {preview}")

        # Total rows in each view.
        for view in ("v_changes", "v_missing"):
            try:
                d = duckdb_catalog.query(f"SELECT COUNT(*) AS n FROM {view}")
                n = int(d.iloc[0, 0]) if d is not None and len(d) > 0 else 0
                print(f"[diag]   {view}: {n} total row(s)")
            except Exception as e:
                print(f"[diag]   {view}: query failed: {type(e).__name__}: {e}")

        # Filter rows for this (event, env). Mirror the regex used by
        # duckdb_source so we see what apply will actually see.
        for view in ("v_changes", "v_missing"):
            try:
                sql = (
                    f"SELECT COUNT(*) AS n FROM {view} "
                    f"WHERE regexp_extract(replace(filename, chr(92), '/'), "
                    f"  '/out/([^/]+)/([^/]+)/changes/', 1) = ? "
                    f"  AND regexp_extract(replace(filename, chr(92), '/'), "
                    f"  '/out/([^/]+)/([^/]+)/changes/', 2) = ?"
                )
                d = duckdb_catalog.query(sql, (event, env))
                n = int(d.iloc[0, 0]) if d is not None and len(d) > 0 else 0
                print(f"[diag]   {view} WHERE event='{event}' env='{env}': {n} row(s)")
                if n > 0 and view == "v_changes":
                    sample = duckdb_catalog.query(
                        f"SELECT id, field, status, "
                        f"  regexp_extract(replace(filename, chr(92), '/'), "
                        f"  '/([^/]+\\.parquet)$', 1) AS file "
                        f"FROM {view} "
                        f"WHERE regexp_extract(replace(filename, chr(92), '/'), "
                        f"  '/out/([^/]+)/([^/]+)/changes/', 1) = ? "
                        f"  AND regexp_extract(replace(filename, chr(92), '/'), "
                        f"  '/out/([^/]+)/([^/]+)/changes/', 2) = ? "
                        f"LIMIT 5",
                        (event, env),
                    )
                    if sample is not None and len(sample) > 0:
                        print(f"[diag]   sample rows from {view}:")
                        for _, r in sample.iterrows():
                            print(f"[diag]     id={r['id']!r:<18} field={r['field']!r:<22} "
                                  f"status={r['status']!r:<12} file={r['file']}")
            except Exception as e:
                print(f"[diag]   filtered {view} query failed: "
                      f"{type(e).__name__}: {e}")
    except Exception as e:
        print(f"[diag] duckdb diagnostics failed: {type(e).__name__}: {e}")

    # PG side — only show if it could be a source for this run.
    if source in ("pg", "auto"):
        try:
            available = pg_source.is_available()
            print(f"[diag] pg_source.is_available(): {available}")
            if available:
                cnt = pg_source.pending_counts(env).get(event, {})
                print(f"[diag]   pipeline_changes/missing for {event}/{env}: {cnt}")
        except Exception as e:
            print(f"[diag] pg diagnostics failed: {type(e).__name__}: {e}")

    # pipeline_apply_batches: which CSVs are already marked applied?
    try:
        already = pg_tracking.fetch_applied_files(event, env, "changes")
        print(f"[diag] pipeline_apply_batches: {len(already)} 'changes' file(s) "
              f"already applied for {event}/{env}")
        if already:
            for s in list(already)[:3]:
                print(f"[diag]   already-applied: {s}")
            if len(already) > 3: print(f"[diag]   ... +{len(already)-3} more")
    except Exception as e:
        print(f"[diag] pg_tracking diagnostics failed: {type(e).__name__}: {e}")

    print("[diag] -------------------------------------------------------------")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--event", required=True, help="YAML event name (e.g. PLAYERBONUS)")
    p.add_argument("--mode", choices=["changes", "missing", "both"], default="both")
    p.add_argument("--env", choices=["stage", "prod"], default="stage")
    p.add_argument("--dry", action="store_true")
    p.add_argument("--no-refresh", action="store_true",
                   help="skip auto-refresh of the ES schema CSV before validation")
    p.add_argument("--force", action="store_true",
                   help="re-apply CSVs even if pipeline_apply_batches already records them")
    p.add_argument("--source", choices=["pg", "duckdb", "csv", "auto"],
                   default="duckdb",
                   help="where to read pending diffs/missing from "
                        "(default: duckdb. 'auto' = PG → DuckDB → CSV.)")
    args = p.parse_args()
    import uuid as _uuid
    run_id = _uuid.uuid4().hex[:12]

    entry = load_event(args.event)
    index = (entry.get("INDEX_NAME") or "").strip()
    if not index:
        sys.exit(f"event {args.event}: INDEX_NAME is empty")
    pk = entry.get("PK") or "id"
    mapping_rows = collect_mapping_rows(entry)

    adapter = get_adapter(index)
    overrides = {**adapter.field_kind_overrides(), **(entry.get("FIELD_KIND_OVERRIDES") or {})}
    types = field_types(mapping_rows, overrides=overrides)
    types[pk] = "str"  # PK as-is
    adapter.bind_field_types(types)

    changes_dir = OUT_DIR / args.event / args.env / "changes"
    if not changes_dir.is_dir() and args.source == "csv":
        sys.exit(f"changes dir not found: {changes_dir} "
                 f"(env={args.env}, --source=csv requires local files)")

    # CRITICAL: refresh the DuckDB catalog so v_changes / v_missing point at
    # the current set of parquet files. If the catalog was created earlier
    # when out/ was empty, the views were registered as empty placeholders
    # and would silently return 0 rows here.
    if args.source in ("duckdb", "auto"):
        try:
            duckdb_catalog.init_catalog()
        except Exception as e:
            print(f"[duckdb] init_catalog failed (continuing): "
                  f"{type(e).__name__}: {e}")

    url, auth, verify = es_client(args.env)
    print(f"event={args.event} index={index} pk={pk} env={args.env} url={url} dry={args.dry}")
    print(f"adapter={type(adapter).__module__}.{type(adapter).__name__}")
    print(f"changes dir: {changes_dir}")
    print(f"types known for {len(types)} fields")
    print(f"source priority: {args.source}")

    # Diagnostic dump of what's actually on disk + in DuckDB views, so 0-row
    # results are explainable without guesswork.
    _diagnose_sources(args.event, args.env, args.source, changes_dir)

    # Auto-refresh ES schema CSV from prod (always prod regardless of --env).
    schema_csv = schema_csv_path(index)
    schema_csv.parent.mkdir(parents=True, exist_ok=True)
    if not args.no_refresh:
        try:
            print(f"refreshing ES schema for '{index}' from prod...")
            prod_url = (os.getenv("ES_URL_PRODE") or os.getenv("ES_URL_PROD") or os.getenv("ES_URL") or "").strip()
            prod_user = os.getenv("ES_USER"); prod_pw = os.getenv("ES_PASS")
            if not (prod_url and prod_user and prod_pw):
                sys.exit("schema refresh needs ES_URL_PRODE + ES_USER + ES_PASS in .env "
                         "(use --no-refresh to bypass)")
            rows = es_schema_io.fetch_schema(index, prod_url, HTTPBasicAuth(prod_user, prod_pw), False)
            es_schema_io.save_schema_csv(rows, schema_csv)
            print(f"  wrote {len(rows)} fields -> {schema_csv.relative_to(_ROOT)}")
        except SystemExit:
            raise
        except Exception as e:
            sys.exit(f"schema refresh failed: {e} (use --no-refresh to bypass)")

    schema = es_schema_io.load_schema_csv(schema_csv)
    if not schema:
        sys.exit(f"no schema CSV at {schema_csv} — run without --no-refresh, "
                 f"or `python -m apply_changes.fetch_schemas`")

    audit = None if args.dry else AuditLog(args.env, args.event, index)
    try:
        if args.mode in ("changes", "both"):
            apply_changes(changes_dir, index, adapter, pk, url, auth, verify, args.dry, audit,
                          schema,
                          event=args.event, env=args.env, force=args.force, run_id=run_id,
                          data_source=args.source)
        if args.mode in ("missing", "both"):
            apply_missing(changes_dir, index, adapter, pk, url, auth, verify, args.dry, audit,
                          schema,
                          event=args.event, env=args.env, force=args.force, run_id=run_id,
                          data_source=args.source)
    finally:
        if audit:
            audit.close({"event": args.event, "env": args.env, "mode": args.mode})
        pg_tracking.close()
        pg_source.close()
        duckdb_source.close()

if __name__ == "__main__":
    main()
