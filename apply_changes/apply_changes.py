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
import ast
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import urllib3
import yaml
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
SCHEMA_DIR = SETTINGS_DIR / "es_schema"

from _pipeline_env import env_truthy, normalize_es_env  # noqa: E402

from apply_changes import es_schema as es_schema_io
from apply_changes import pg_tracking
from apply_changes import pg_source
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


def _now_es_iso() -> str:
    """ES `updateDate` format: yyyy-MM-dd'T'HH:mm:ss.SSS (matches PlayerBonusVO @Field DateFormat).
    Mirrors Java `playerBonusVO.setUpdateDate(Timestamp.valueOf(DBManager.nowLocalDateTime()))`."""
    n = datetime.now()
    return n.strftime("%Y-%m-%dT%H:%M:%S.") + f"{n.microsecond // 1000:03d}"


# ---------------------------------------------------------------------------
# YAML / mapping introspection
# ---------------------------------------------------------------------------

def load_event(event: str) -> dict:
    cfg = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
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
# Type-coercion table from mapping lambdas
# ---------------------------------------------------------------------------

_LAMBDA_TYPE = {
    "convert_int_to_string": "str_int",
    "convert_to_int_strict":  "int",
    "convert_int_or_zero":    "int",
    "convert_int_or_neg_one": "int",
    "constant_zero_int":      "int",
    "constant_zero_long":     "int",
    "convert_num_to_bool":    "bool",
    "convert_truthy_bool":    "bool",
    "constant_false_bool":    "bool",
    "constant_zero_float":    "float",
    "convert_str_to_list":    "list",
    "compose_jackpot_menu_items_ids": "list",
    "parse_menu_items_ids":   "list",
    "compose_chip_count_left_freespins": "int",
    "convert_time_to_string": "str",
    "convert_time_to_date_as_string": "str",
    "convert_yymmdd_to_iso":  "str",
    "compose_expiration_from_days":   "str",
}

# Per-field kind overrides applied after the lambda lookup. Defaults retained
# for backwards-compat with existing PLAYERBONUS docs whose ES mapping diverges
# from the template lambdas. Per-event YAML key `FIELD_KIND_OVERRIDES:` adds
# (or replaces) overrides for that event only.
_DEFAULT_FIELD_KIND_OVERRIDE = {
    "parentId":                 "int_str",  # ES=keyword, lambdas emit int
    "maxWin":                   "int_str",  # ES=keyword, lambdas emit int
    "triggeringTransactionId":  "int",      # ES=integer, mapping lambda empty -> str fallback
}


def field_types(mapping_rows: list[dict],
                overrides: dict[str, str] | None = None) -> dict[str, str]:
    """{filed_es -> coerce_kind} where kind is one of int/float/bool/list/str/str_int.

    When the same ES field is declared in multiple templates with different
    lambdas, prefer the strongest kind: never let a "str" fallback overwrite
    an already-seen typed kind (float/int/bool/list/...).
    """
    out: dict[str, str] = {}
    for r in mapping_rows:
        fe = (r.get("filed_es") or "").strip()
        if not fe:
            continue
        fn = (r.get("funciton_lambda") or "").strip()
        kind = _LAMBDA_TYPE.get(fn, "str")
        if kind != "str" or fe not in out:
            out[fe] = kind
    final_overrides = {**_DEFAULT_FIELD_KIND_OVERRIDE, **(overrides or {})}
    for fe, kind in final_overrides.items():
        if fe in out:
            out[fe] = kind
    return out


# ES date mapping uses "yyyy-MM-dd'T'HH:mm:ss.SSS"; pipeline lambdas emit
# "YYYY-MM-DD HH:MM:SS.fff" (space). Swap space → 'T' just before send.
_ISO_DT_SPACE_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$")


def coerce(value: str, kind: str):
    """String CSV value -> ES-typed value. Empty/None handled."""
    if value is None:
        return None
    s = value.strip() if isinstance(value, str) else value
    if s == "" or (isinstance(s, str) and s.lower() in ("none", "nan", "<na>", "null")):
        return None
    if kind in ("int", "str_int"):
        try:
            return int(float(s))
        except (TypeError, ValueError):
            return None
    if kind == "int_str":
        try:
            return str(int(float(s)))
        except (TypeError, ValueError):
            return None
    if kind == "float":
        try:
            return float(s)
        except (TypeError, ValueError):
            return None
    if kind == "bool":
        if isinstance(s, str):
            return s.strip().lower() in ("1", "true", "yes", "t", "y")
        return bool(s)
    if kind == "list":
        if isinstance(s, list):
            return s
        if isinstance(s, str) and s.startswith("[") and s.endswith("]"):
            try:
                v = ast.literal_eval(s)
                return list(v) if isinstance(v, (list, tuple)) else [v]
            except (ValueError, SyntaxError):
                return [p.strip() for p in s.strip("[]").split(",") if p.strip()]
        if isinstance(s, str) and "," in s:
            return [p.strip() for p in s.split(",") if p.strip()]
        return [s]
    out = str(s)
    if _ISO_DT_SPACE_RE.match(out):
        return out.replace(" ", "T", 1)
    return out


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

def apply_changes(changes_dir: Path, index: str, types: dict[str, str], pk: str,
                  url: str, auth, verify: bool, dry: bool,
                  audit: "AuditLog | None" = None,
                  stamp_update_date: bool = False,
                  schema: dict[str, dict] | None = None,
                  event: str | None = None, env: str | None = None,
                  force: bool = False, run_id: str | None = None,
                  data_source: str = "auto") -> None:
    """data_source: 'pg' = read pipeline_changes (applied_ts IS NULL),
                    'csv' = read changes_*.csv from disk (legacy),
                    'auto' = try PG first, fall back to CSV."""
    docs: dict[str, dict] = {}
    audit_changes: dict[str, dict] = {}
    file_doc_ids: dict[Path, set[str]] = {}
    use_pg = False

    # Try Postgres first unless explicitly forced to CSV.
    if event and env and data_source in ("pg", "auto") and pg_source.is_available():
        pg_rows = pg_source.load_pending_changes(event, env, pk=pk)
        if pg_rows or data_source == "pg":
            use_pg = True
            print(f"[apply] reading {len(pg_rows)} pending diff row(s) from Postgres "
                  f"(pipeline_changes WHERE applied_ts IS NULL)")
            files_seen: dict[str, set[str]] = {}
            for r in pg_rows:
                doc_id = (str(r.get(pk) or r.get("id") or "")).strip()
                field = (r.get("field") or "").strip()
                if not doc_id or not field or field == "*":
                    continue
                kind = types.get(field, "str")
                new_val = coerce(r.get("oracle_value"), kind)
                old_val = r.get("es_value")
                docs.setdefault(doc_id, {})[field] = new_val
                audit_changes.setdefault(doc_id, {})[field] = {"old": old_val, "new": new_val}
                src = r.get("source_file") or ""
                files_seen.setdefault(src, set()).add(doc_id)
            for src, ids in files_seen.items():
                file_doc_ids[_ROOT / "out" / src] = ids

    if not use_pg:
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
                kind = types.get(field, "str")
                new_val = coerce(row.get("oracle_value"), kind)
                old_val = row.get("es_value")
                docs.setdefault(doc_id, {})[field] = new_val
                audit_changes.setdefault(doc_id, {})[field] = {"old": old_val, "new": new_val}
                ids_here.add(doc_id)
            file_doc_ids[f] = ids_here

    if not docs:
        print("no usable diffs"); return
    print(f"docs to update: {len(docs)}  "
          f"(across {len(file_doc_ids)} source file(s), "
          f"source={'postgres' if use_pg else 'csv'})")

    # Pre-flight schema validation across the whole plan (incl. stamped updateDate).
    if schema:
        preview = {did: dict(fields) for did, fields in docs.items()}
        if stamp_update_date:
            sample_now = _now_es_iso()
            for d in preview.values():
                d["updateDate"] = sample_now
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
        chunk_map = {k: dict(docs[k]) for k in chunk_ids}
        if stamp_update_date:
            now_iso = _now_es_iso()
            for k in chunk_ids:
                chunk_map[k]["updateDate"] = now_iso
                audit_changes.setdefault(k, {})["updateDate"] = {"old": None, "new": now_iso}
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
        except Exception as e:
            failures += len(chunk_ids)
            print(f"  changes batch {idx_n}/{batches} FAILED: {e}")
            if audit:
                audit.update_batch_result(idx_n, chunk_ids, 0, 0, len(chunk_ids))
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


# ---------------------------------------------------------------------------
# Mode B: insert missing docs (missing_in_es_*.csv) — never overwrite
# ---------------------------------------------------------------------------

def apply_missing(changes_dir: Path, index: str, types: dict[str, str], pk: str,
                  url: str, auth, verify: bool, dry: bool,
                  audit: "AuditLog | None" = None,
                  stamp_update_date: bool = False,
                  schema: dict[str, dict] | None = None,
                  event: str | None = None, env: str | None = None,
                  force: bool = False, run_id: str | None = None,
                  data_source: str = "auto") -> None:
    rows_total: list[dict] = []
    file_row_counts: dict[Path, int] = {}
    use_pg = False

    if event and env and data_source in ("pg", "auto") and pg_source.is_available():
        pg_rows = pg_source.load_pending_missing(event, env, pk=pk)
        if pg_rows or data_source == "pg":
            use_pg = True
            print(f"[apply] reading {len(pg_rows)} pending missing row(s) from Postgres "
                  f"(pipeline_missing WHERE applied_ts IS NULL)")
            files_seen: dict[str, int] = {}
            for r in pg_rows:
                src = r.pop("source_file", "") or ""
                if "error" in r:
                    r.pop("error", None)
                rows_total.append(r)
                files_seen[src] = files_seen.get(src, 0) + 1
            for src, n in files_seen.items():
                file_row_counts[_ROOT / "out" / src] = n

    if not use_pg:
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
        print("no missing rows"); return
    print(f"docs to insert: {len(rows_total)} "
          f"(across {len(file_row_counts)} source file(s), "
          f"source={'postgres' if use_pg else 'csv'})")

    # Coerce per-field, drop empty values
    docs: list[tuple[str, dict]] = []
    for r in rows_total:
        doc_id = (r.get(pk) or r.get("id") or "").strip()
        if not doc_id or doc_id.lower() in ("none", "nan", "<na>", "null"):
            continue
        body = {}
        for field, raw in r.items():
            v = coerce(raw, types.get(field, "str"))
            if v is None:
                continue
            body[field] = v
        if stamp_update_date:
            body["updateDate"] = _now_es_iso()
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
                    created += 1; result = "created"; err = None
                elif status == 409:
                    exists += 1; result = "exists"; err = None
                else:
                    errors += 1; result = "error"
                    err = json.dumps(op.get("error"), default=str) if op.get("error") else None
                if audit and j < len(chunk):
                    did, body = chunk[j]
                    audit.insert(did, body, status or 0, result, idx_n, err)
            print(f"  missing batch {idx_n}/{batches}: created={created} skipped(exists)={exists} errors={errors}")
        except Exception as e:
            errors += len(chunk)
            print(f"  missing batch {idx_n}/{batches} FAILED: {e}")
            if audit:
                for did, body in chunk:
                    audit.insert(did, body, 0, "error", idx_n, str(e))
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
    p.add_argument("--source", choices=["pg", "csv", "auto"], default="auto",
                   help="where to read pending diffs/missing from (default: auto = "
                        "Postgres first, fall back to CSV)")
    args = p.parse_args()
    import uuid as _uuid
    run_id = _uuid.uuid4().hex[:12]

    entry = load_event(args.event)
    index = (entry.get("INDEX_NAME") or "").strip()
    if not index:
        sys.exit(f"event {args.event}: INDEX_NAME is empty")
    pk = entry.get("PK") or "id"
    mapping_rows = collect_mapping_rows(entry)
    overrides = entry.get("FIELD_KIND_OVERRIDES") or {}
    types = field_types(mapping_rows, overrides=overrides)
    types[pk] = "str"  # PK as-is

    changes_dir = OUT_DIR / args.event / args.env / "changes"
    if not changes_dir.is_dir() and args.source == "csv":
        sys.exit(f"changes dir not found: {changes_dir} "
                 f"(env={args.env}, --source=csv requires local files)")

    url, auth, verify = es_client(args.env)
    print(f"event={args.event} index={index} pk={pk} env={args.env} url={url} dry={args.dry}")
    print(f"changes dir: {changes_dir}")
    print(f"types known for {len(types)} fields")

    stamp_update_date = bool(entry.get("STAMP_UPDATE_DATE", False))
    if stamp_update_date:
        print(f"STAMP_UPDATE_DATE=on → every doc op will set updateDate=now()")

    # Auto-refresh ES schema CSV from prod (always prod regardless of --env).
    schema_csv = SCHEMA_DIR / f"{index}.csv"
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
            apply_changes(changes_dir, index, types, pk, url, auth, verify, args.dry, audit,
                          stamp_update_date, schema,
                          event=args.event, env=args.env, force=args.force, run_id=run_id,
                          data_source=args.source)
        if args.mode in ("missing", "both"):
            apply_missing(changes_dir, index, types, pk, url, auth, verify, args.dry, audit,
                          stamp_update_date, schema,
                          event=args.event, env=args.env, force=args.force, run_id=run_id,
                          data_source=args.source)
    finally:
        if audit:
            audit.close({"event": args.event, "env": args.env, "mode": args.mode})
        pg_tracking.close()
        pg_source.close()

    if not args.dry and not env_truthy("PIPELINE_SKIP_PG_SYNC"):
        try:
            from connect_into_postgres.sync_out import run_sync
            print("[pg-sync] mirroring out/ -> Postgres")
            run_sync(only="apply_audit,changes,missing,summary,logs")
        except Exception as e:
            print(f"[pg-sync] skipped: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
