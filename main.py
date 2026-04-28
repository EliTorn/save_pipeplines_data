"""Oracle → Elasticsearch comparison pipeline.

Per-event MODE dispatch (set in events.yaml):
    time         - chunk by [START_TIME, END_TIME) in BANCH_VALUE-sized windows
    id_range     - chunk by PK; SQL has '-- @range' and '-- @batch' sections
    time_union   - run multiple `parts:` SQL files, concat shaped output, compare once
"""
from __future__ import annotations

import multiprocessing as mp
import os
import re
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from tqdm import tqdm

_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "connect_into_orcal"))
sys.path.insert(0, str(_ROOT / "connect_into_es"))
sys.path.insert(0, str(_ROOT / "settings"))

from _pipeline_env import (
    DIFF_MODE_ALIASES, env_truthy, normalize_es_env, parse_ts,
)
from settings.loader import load_events, PIPELINE_SETTINGS
from settings.compare import compare_records, compare_shaped, transform_to_es_shape

_VERBOSE = env_truthy("PIPELINE_VERBOSE")
from connect_into_orcal.connect_to_orcal import (
    create_connection, run_tracked,
    USERNAME, DB_HOST, PORT, SERVICE_NAME,
    ARRAYSIZE, WORKERS, QUERY_TIMEOUT_MS,
)
from connect_into_orcal.logging_setup import (
    get_run_logger, CONN_CSV, EVENTS_CSV, QUERIES_CSV,
    QueueLogger, start_log_listener,
)
from connect_into_orcal.geo_info import host_info
from connect_into_es.connect_to_es import (
    fetch_range_df, fetch_terms_df, es_time_field,
    ES_URL, ES_USER, ES_VERIFY, PAGE_SIZE as ES_PAGE_SIZE,
    TIMEOUT_CONNECT as ES_TIMEOUT_CONNECT, TIMEOUT_READ as ES_TIMEOUT_READ,
)


OUT_DIR = _ROOT / "out"
OUT_DIR.mkdir(exist_ok=True)

_DEFAULT_PIPELINE_WORKERS = min(4, os.cpu_count() or 1)
PIPELINE_WORKERS = max(1, int(os.getenv("PIPELINE_WORKERS", str(_DEFAULT_PIPELINE_WORKERS))))
SAVE_FULL_CSV = env_truthy("PIPELINE_SAVE_FULL_CSV")

_diff_env = os.getenv("PIPELINE_DIFF_MODE", "").strip().lower()
DIFF_MODE = DIFF_MODE_ALIASES.get(_diff_env, PIPELINE_SETTINGS.get("PIPELINE_DIFF_MODE", "both")) \
    if _diff_env else PIPELINE_SETTINGS.get("PIPELINE_DIFF_MODE", "both")
SAVE_CHANGES = DIFF_MODE in ("changes", "both")
SAVE_MISSING = DIFF_MODE in ("missing", "both")

_UNIT = {
    "M": timedelta(minutes=1),  "MIN": timedelta(minutes=1),  "MINUTE": timedelta(minutes=1), "MINUTES": timedelta(minutes=1),
    "H": timedelta(hours=1),    "HR":  timedelta(hours=1),    "HOUR":   timedelta(hours=1),   "HOURS":   timedelta(hours=1),
    "D": timedelta(days=1),     "DAY": timedelta(days=1),     "DAYS":   timedelta(days=1),
    "W": timedelta(weeks=1),    "WEEK": timedelta(weeks=1),   "WEEKS":  timedelta(weeks=1),
}
_STEP_PATTERN = re.compile(r"\s*(\d+)\s*([A-Z]*)\s*")
_DEFAULT_BATCH_SIZE = 1000


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def parse_step(value) -> timedelta | None:
    """'HOUR' / 'DAY' / '30 DAYS' / '2h' / '15m' / '7' (=days) -> timedelta. Bad input -> None."""
    s = str(value).strip().upper()
    if not s:
        return None
    if s in _UNIT:
        return _UNIT[s]
    m = _STEP_PATTERN.fullmatch(s)
    if not m:
        return None
    unit = m.group(2) or "DAY"
    if unit not in _UNIT:
        return None
    return _UNIT[unit] * int(m.group(1))


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def windows(start: datetime, end: datetime, step: timedelta):
    cur = start
    while cur < end:
        nxt = min(cur + step, end)
        yield cur, nxt
        cur = nxt


def count_windows(start: datetime, end: datetime, step: timedelta) -> int:
    if step.total_seconds() <= 0 or end <= start:
        return 0
    sec_total = (end - start).total_seconds()
    sec_step = step.total_seconds()
    return int((sec_total + sec_step - 1) // sec_step)


def id_windows(min_id: int, max_id: int, step: int, limit: int | None = None):
    cur, n = min_id, 0
    while cur <= max_id:
        if limit is not None and n >= limit:
            return
        yield cur, cur + step
        cur += step
        n += 1


def count_id_windows(min_id: int, max_id: int, step: int, limit: int | None = None) -> int:
    if step <= 0 or max_id < min_id:
        return 0
    n = (max_id - min_id) // step + 1
    return min(n, limit) if limit is not None else n


_EMIT_PROGRESS_MARKERS = not sys.stdout.isatty()


def _emit_progress(event: str, batch: int, total: int) -> None:
    if _EMIT_PROGRESS_MARKERS:
        print(f"[PROGRESS] event={event} batch={batch}/{total}", flush=True)


def add_time_filter(sql: str, time_col: str) -> str:
    """Uncomment :from_ts/:to_ts marker lines if present, else inject WHERE before GROUP BY."""
    out, active = [], False
    for ln in sql.splitlines():
        if (":from_ts" in ln or ":to_ts" in ln) and "--" in ln:
            pre, _, tail = ln.partition("--")
            if tail.startswith(" "):
                tail = tail[1:]
            out.append(pre + tail)
            active = True
        else:
            if ":from_ts" in ln or ":to_ts" in ln:
                active = True
            out.append(ln)
    body = "\n".join(out).rstrip().rstrip(";")
    if active:
        return body
    where = (f"WHERE {time_col} >= TO_DATE(:from_ts, 'YYYY-MM-DD HH24:MI:SS')\n"
             f"  AND {time_col} <  TO_DATE(:to_ts,   'YYYY-MM-DD HH24:MI:SS')")
    idx = body.lower().rfind("group by")
    return f"{body}\n{where}" if idx == -1 else f"{body[:idx].rstrip()}\n{where}\n{body[idx:]}"


def parse_sql_sections(sql: str) -> dict[str, str]:
    """Split SQL by '-- @name' headers into {name: body}."""
    sections, cur = {}, None
    for ln in sql.splitlines():
        s = ln.strip()
        if s.startswith("-- @"):
            cur = s[4:].strip()
            sections[cur] = []
        elif cur is not None:
            sections[cur].append(ln)
    return {k: "\n".join(v).strip().rstrip(";") for k, v in sections.items()}


@contextmanager
def _timed():
    t0 = time.perf_counter()
    yield lambda: round(time.perf_counter() - t0, 3)


def _skip(logger, event: str, reason: str) -> None:
    print(f"[{event}] skipped: {reason}")
    logger.event("event_skipped", table=event, reason=reason)


# ---------------------------------------------------------------------------
# Mapping plan
# ---------------------------------------------------------------------------

@dataclass
class Plan:
    """Mapping rows + Oracle/ES column allowlists derived from FILED_THAT_RUN."""
    mapping: list[dict]
    ora_cols: list[str] | None
    es_cols: list[str] | None

    @classmethod
    def from_entry(cls, entry: dict, mapping: list[dict] | None = None) -> "Plan":
        mapping = entry["mapping"] if mapping is None else mapping
        allowed = entry.get("FILED_THAT_RUN") or []
        if not allowed:
            return cls(mapping=mapping, ora_cols=None, es_cols=None)
        keep = set(allowed) | {entry["PK"]}
        kept = [m for m in mapping
                if (m.get("filed_es") or "").strip() in keep
                or (m.get("filed_orcal") or "").strip() in keep]
        ora_cols: list[str] = []
        for m in kept:
            fo = (m.get("filed_orcal") or "").strip()
            for c in (fo.split("+") if "+" in fo else [fo]):
                c = c.strip()
                if c and c not in ora_cols:
                    ora_cols.append(c)
        es_cols = [(m.get("filed_es") or "").strip() for m in kept]
        return cls(mapping=kept, ora_cols=ora_cols, es_cols=es_cols)

    def filter_oracle(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.ora_cols is None:
            return df
        return df[[c for c in self.ora_cols if c in df.columns]]

    def filter_es(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.es_cols is None or df.empty:
            return df
        return df[[c for c in self.es_cols if c in df.columns]]


def _es_entry_for(entry: dict, mapping: list[dict] | None = None) -> dict:
    """Override TIME_DATE with ES_TIME so es_time_field resolves correctly."""
    es_time = (entry.get("ES_TIME") or "").strip()
    out = {**entry, "TIME_DATE": es_time} if es_time else dict(entry)
    if mapping is not None:
        out["mapping"] = mapping
    return out


# ---------------------------------------------------------------------------
# Per-batch I/O helpers
# ---------------------------------------------------------------------------

def _normalize_env(entry: dict) -> str:
    return normalize_es_env(entry.get("ES_ENV", "stage"))


def _event_dir(event: str, env: str) -> Path:
    d = OUT_DIR / event / env
    d.mkdir(parents=True, exist_ok=True)
    return d


def _summarize_event(event: str, env: str, event_dir: Path) -> None:
    """Print end-of-run summary: which output files (if any) were written."""
    changes_dir = event_dir / "changes"
    changes = list(changes_dir.glob("changes_*.csv")) if changes_dir.is_dir() else []
    missing = list(changes_dir.glob("missing_in_es_*.csv")) if changes_dir.is_dir() else []
    if not changes and not missing:
        print(f"[{event}] env={env} done: no changes found — nothing written "
              f"(Oracle == ES for this window)")
        return
    print(f"[{event}] env={env} done: changes_files={len(changes)} "
          f"missing_files={len(missing)} -> {changes_dir}")


# In-memory lookup-table cache, keyed by absolute SQL file path.
# Each value is a dict[str, str] mapping the first SQL column to the second.
_LOOKUP_CACHE: dict[str, dict[str, str]] = {}


def _load_lookup(conn, sql_file_rel: str, logger) -> dict[str, str]:
    abs_path = str((_ROOT / "settings" / sql_file_rel).resolve())
    if abs_path in _LOOKUP_CACHE:
        return _LOOKUP_CACHE[abs_path]
    sql = (_ROOT / "settings" / sql_file_rel).read_text(encoding="utf-8")
    df, _ = run_tracked(conn, sql, {}, logger, table=f"lookup::{sql_file_rel}", batch=0)
    if df.empty or len(df.columns) < 2:
        _LOOKUP_CACHE[abs_path] = {}
        return _LOOKUP_CACHE[abs_path]
    key_col, val_col = df.columns[0], df.columns[1]
    table = {}
    for k, v in zip(df[key_col], df[val_col]):
        if k is None:
            continue
        try:
            table[str(int(k))] = v
        except (TypeError, ValueError):
            table[str(k)] = v
    _LOOKUP_CACHE[abs_path] = table
    print(f"[lookup] {sql_file_rel}: cached {len(table)} entries")
    return table


def _apply_lookup(shaped: pd.DataFrame, df_raw: pd.DataFrame, part: dict, conn, logger) -> pd.DataFrame:
    sql_rel = part.get("LOOKUP_SQL")
    if not sql_rel:
        return shaped
    key_col = part.get("LOOKUP_KEY_COL")
    out_col = part.get("LOOKUP_OUTPUT_COL")
    key_type = (part.get("LOOKUP_KEY_TYPE") or "csv").strip().lower()
    if not (key_col and out_col):
        return shaped
    table = _load_lookup(conn, sql_rel, logger)
    if key_col not in df_raw.columns:
        shaped = shaped.copy()
        shaped[out_col] = None
        return shaped

    def _resolve(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        s = str(v).strip()
        if not s:
            return None
        if key_type == "single":
            if s.endswith(".0"):
                s = s[:-2]
            return table.get(s)
        # csv (Java parity: ',-1,' -> 'All', else split + filter numeric + lookup + ', ' join)
        if s == ",-1,":
            return "All"
        names = []
        for p in s.split(","):
            p = p.strip()
            if p.isdigit():
                name = table.get(p)
                if name:
                    names.append(name)
        return ", ".join(names)

    shaped = shaped.copy()
    shaped[out_col] = df_raw[key_col].map(_resolve).values
    return shaped


_NL_TRANS = str.maketrans({"\r": " ", "\n": " ", "\t": " "})


def _scrub(value):
    """Recursively replace newline/CR/tab in any string anywhere in the value."""
    if isinstance(value, str):
        return value.translate(_NL_TRANS)
    if isinstance(value, list):
        return [_scrub(x) for x in value]
    if isinstance(value, tuple):
        return tuple(_scrub(x) for x in value)
    if isinstance(value, dict):
        return {k: _scrub(v) for k, v in value.items()}
    return value


def _strip_nl(df: pd.DataFrame) -> pd.DataFrame:
    """Bulletproof: scrub newlines from any string in any cell so each CSV row is one line.
    Handles object/string/category dtypes plus nested lists/tuples/dicts."""
    if df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        kind = out[c].dtype
        if kind == "object" or pd.api.types.is_string_dtype(out[c]) or str(kind) == "category":
            try:
                out[c] = out[c].map(_scrub)
            except Exception:
                out[c] = out[c].astype(str).map(_scrub)
    return out


def _save_oracle_csv(df: pd.DataFrame, path: Path,
                     event: str, batch: int, logger, **extra) -> None:
    if not SAVE_FULL_CSV:
        return
    _strip_nl(df).to_csv(path, index=False, encoding="utf-8-sig")
    logger.event("csv_saved", table=event, batch=batch, source="oracle",
                 path=str(path), rows=len(df), **extra)


def _save_es_csv(df: pd.DataFrame, path: Path,
                 event: str, batch: int, logger) -> None:
    if not SAVE_FULL_CSV:
        return
    _strip_nl(df).to_csv(path, index=False, encoding="utf-8-sig")
    logger.event("csv_saved", table=event, batch=batch, source="es",
                 path=str(path), rows=len(df))


def _save_diffs(diffs: pd.DataFrame, event_dir: Path, stamp: str,
                event: str, batch: int, logger,
                shaped_ora: "pd.DataFrame | None" = None, pk: str = "id") -> None:
    if diffs.empty:
        if _VERBOSE:
            print(f"[{event}] batch {batch} diffs:  0 rows (skipped write)")
        return
    changes_dir = event_dir / "changes"
    changes_dir.mkdir(exist_ok=True)

    if SAVE_CHANGES:
        diff_only = diffs[diffs["status"] == "diff"]
        if not diff_only.empty:
            p = changes_dir / f"changes_{stamp}.csv"
            _strip_nl(diff_only).to_csv(p, index=False, encoding="utf-8-sig")
            logger.event("diffs_saved", table=event, batch=batch, path=str(p), rows=len(diff_only))
            if _VERBOSE:
                print(f"[{event}] batch {batch} diffs:        {len(diff_only)} rows -> {p}")

    if not SAVE_MISSING:
        return

    BAD_ID = {"", "none", "nan", "<na>", "null"}
    miss_es = diffs[diffs["status"] == "missing_in_es"]
    if not miss_es.empty and shaped_ora is not None and pk in shaped_ora.columns:
        ids = {i for i in miss_es[pk].astype(str).str.strip().unique()
               if i and i.lower() not in BAD_ID}
        if ids:
            sa = shaped_ora.copy()
            sa[pk] = sa[pk].astype(str).str.strip()
            sa = sa[~sa[pk].str.lower().isin(BAD_ID)]
            full = sa[sa[pk].isin(ids)].copy()
            if not full.empty:
                full["error"] = "missing_in_es"
                p = changes_dir / f"missing_in_es_{stamp}.csv"
                _strip_nl(full).to_csv(p, index=False, encoding="utf-8-sig")
                logger.event("missing_in_es_saved", table=event, batch=batch, path=str(p), rows=len(full))
                if _VERBOSE:
                    print(f"[{event}] batch {batch} missing_in_es: {len(full)} rows -> {p}")


# ---------------------------------------------------------------------------
# Multiprocessing workers (one Oracle conn + QueueLogger per process)
# ---------------------------------------------------------------------------

_W_CONN = None
_W_LOGGER = None


def _worker_init(log_queue, run_id: str) -> None:
    global _W_CONN, _W_LOGGER
    _W_CONN = create_connection()
    _W_LOGGER = QueueLogger(log_queue, run_id)
    import atexit
    atexit.register(_worker_cleanup)


def _worker_cleanup() -> None:
    global _W_CONN
    try:
        if _W_CONN is not None:
            _W_CONN.close()
    except Exception:
        pass
    _W_CONN = None


def _worker_batch(task: dict):
    kind = task["kind"]
    args = task["args"]
    if kind == "time":
        return _batch_time(_W_CONN, _W_LOGGER, **args)
    if kind == "id_range":
        return _batch_id_range(_W_CONN, _W_LOGGER, **args)
    if kind == "time_union":
        return _batch_time_union(_W_CONN, _W_LOGGER, **args)
    raise ValueError(f"unknown batch kind: {kind}")


def _execute_tasks(pool, tasks: list, event: str, total: int) -> None:
    if not tasks:
        return
    bar = tqdm(total=total, desc=event, unit="batch", disable=not sys.stderr.isatty())
    done = 0
    try:
        for result in pool.imap_unordered(_worker_batch, tasks):
            done += 1
            bar.update(1)
            _emit_progress(event, done, total)
            if _VERBOSE and isinstance(result, dict):
                bar.write(f"[{event}] batch {result.get('batch')}/{total} "
                          f"oracle={result.get('ora_rows')}r es={result.get('es_rows')}r")
    finally:
        bar.close()


# ---------------------------------------------------------------------------
# Per-batch helpers (one task = one window). Identical to the sequential
# inner loop bodies; just parameterized so a worker process can run them.
# ---------------------------------------------------------------------------

def _batch_time(conn, logger, *, event, entry_es, mapping, ora_cols, es_cols,
                sql, pk, index, event_dir_str, w_from, w_to, batch_idx):
    plan = Plan(mapping=mapping, ora_cols=ora_cols, es_cols=es_cols)
    event_dir = Path(event_dir_str)
    params = {"from_ts": fmt_ts(w_from), "to_ts": fmt_ts(w_to)}
    stamp = w_from.strftime("%Y%m%d_%H%M%S")

    df_ora, qid = run_tracked(conn, sql, params, logger, table=event, batch=batch_idx)
    df_ora = plan.filter_oracle(df_ora)
    shaped_ora = transform_to_es_shape(df_ora, plan.mapping)
    _save_oracle_csv(shaped_ora, event_dir / f"{event}_oracle_{stamp}.csv",
                     event, batch_idx, logger, query_id=qid)

    with logger.query(f"ES {index} {w_from}->{w_to}", table=index, batch=batch_idx) as q:
        df_es = fetch_range_df(index, entry_es, w_from, w_to)
        q.set_rows(len(df_es))
    df_es = plan.filter_es(df_es)
    _save_es_csv(df_es, event_dir / f"{event}_es_{stamp}.csv", event, batch_idx, logger)

    _save_diffs(compare_records(df_ora, df_es, plan.mapping, pk),
                event_dir, stamp, event, batch_idx, logger,
                shaped_ora=shaped_ora, pk=pk)
    return {"batch": batch_idx, "ora_rows": len(df_ora), "es_rows": len(df_es)}


def _batch_id_range(conn, logger, *, event, entry, mapping, ora_cols, es_cols,
                    batch_sql, pk, index, event_dir_str, from_id, to_id, batch_idx):
    plan = Plan(mapping=mapping, ora_cols=ora_cols, es_cols=es_cols)
    event_dir = Path(event_dir_str)
    params = {"from_id": from_id, "to_id": to_id}
    stamp = f"{from_id}_{to_id}"

    df_ora, qid = run_tracked(conn, batch_sql, params, logger, table=event, batch=batch_idx)
    df_ora = plan.filter_oracle(df_ora)
    shaped_ora = transform_to_es_shape(df_ora, plan.mapping)
    _save_oracle_csv(shaped_ora, event_dir / f"{event}_oracle_{stamp}.csv",
                     event, batch_idx, logger, query_id=qid)

    ids = df_ora[pk].tolist() if pk in df_ora.columns else []
    with logger.query(f"ES {index} terms {pk} n={len(ids)}", table=index, batch=batch_idx) as q:
        df_es = fetch_terms_df(index, pk, ids, entry)
        q.set_rows(len(df_es))
    df_es = plan.filter_es(df_es)
    _save_es_csv(df_es, event_dir / f"{event}_es_{stamp}.csv", event, batch_idx, logger)

    _save_diffs(compare_records(df_ora, df_es, plan.mapping, pk),
                event_dir, stamp, event, batch_idx, logger,
                shaped_ora=shaped_ora, pk=pk)
    return {"batch": batch_idx, "ora_rows": len(df_ora), "es_rows": len(df_es)}


def _batch_time_union(conn, logger, *, event, entry_es, parts_prepared,
                      allowed, pk, index, event_dir_str, w_from, w_to, batch_idx):
    event_dir = Path(event_dir_str)
    params = {"from_ts": fmt_ts(w_from), "to_ts": fmt_ts(w_to)}
    stamp = w_from.strftime("%Y%m%d_%H%M%S")
    keep_set = set(allowed) | {pk} if allowed else None

    shaped_parts: list[pd.DataFrame] = []
    total_raw = 0
    for j, prep in enumerate(parts_prepared, 1):
        df_raw, _ = run_tracked(conn, prep["sql"], params, logger,
                                table=f"{event}::part{j}", batch=batch_idx)
        total_raw += len(df_raw)
        shaped = transform_to_es_shape(df_raw, prep["mapping"])
        shaped = _apply_lookup(shaped, df_raw, prep["raw_part"], conn, logger)
        shaped_parts.append(shaped)

    df_ora_shaped = pd.concat(shaped_parts, ignore_index=True) if shaped_parts else pd.DataFrame()
    if keep_set is not None and not df_ora_shaped.empty:
        df_ora_shaped = df_ora_shaped[[c for c in df_ora_shaped.columns if c in keep_set]]
    _save_oracle_csv(df_ora_shaped, event_dir / f"{event}_oracle_{stamp}.csv",
                     event, batch_idx, logger, parts=len(parts_prepared), raw_rows=total_raw)

    with logger.query(f"ES {index} {w_from}->{w_to}", table=index, batch=batch_idx) as q:
        df_es = fetch_range_df(index, entry_es, w_from, w_to)
        q.set_rows(len(df_es))
    if keep_set is not None and not df_es.empty:
        df_es = df_es[[c for c in df_es.columns if c in keep_set or c == pk]]
    _save_es_csv(df_es, event_dir / f"{event}_es_{stamp}.csv", event, batch_idx, logger)

    fields = list(allowed) if allowed else None
    _save_diffs(compare_shaped(df_ora_shaped, df_es, pk, fields=fields),
                event_dir, stamp, event, batch_idx, logger,
                shaped_ora=df_ora_shaped, pk=pk)
    return {"batch": batch_idx, "ora_rows": len(df_ora_shaped), "es_rows": len(df_es)}


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------

def run_event(conn, event: str, entry: dict, logger, pool) -> None:
    if not entry.get("START_TIME") or not entry.get("END_TIME"):
        return _skip(logger, event, "missing START_TIME/END_TIME")
    step_td = parse_step(entry.get("BANCH_VALUE", "HOUR"))
    if step_td is None:
        return _skip(logger, event, f"bad BANCH_VALUE={entry.get('BANCH_VALUE')!r}")

    start, end = parse_ts(entry["START_TIME"]), parse_ts(entry["END_TIME"])
    plan = Plan.from_entry(entry)
    sql = add_time_filter(entry["scama"], entry["TIME_DATE"])
    index = entry["INDEX_NAME"].strip()
    env = _normalize_env(entry)
    event_dir = _event_dir(event, env)
    es_entry = _es_entry_for(entry)
    pk = entry["PK"]

    total = count_windows(start, end, step_td)
    print(f"[{event}] env={env} {fmt_ts(start)} -> {fmt_ts(end)} step={entry.get('BANCH_VALUE')} "
          f"index={index} es_field={es_time_field(es_entry)} batches={total} workers={PIPELINE_WORKERS}")

    tasks = [{
        "kind": "time",
        "args": {
            "event": event, "entry_es": es_entry,
            "mapping": plan.mapping, "ora_cols": plan.ora_cols, "es_cols": plan.es_cols,
            "sql": sql, "pk": pk, "index": index,
            "event_dir_str": str(event_dir),
            "w_from": w_from, "w_to": w_to, "batch_idx": i,
        },
    } for i, (w_from, w_to) in enumerate(windows(start, end, step_td), 1)]
    _execute_tasks(pool, tasks, event, total)
    _summarize_event(event, env, event_dir)


def run_event_id_range(conn, event: str, entry: dict, logger, pool) -> None:
    sections = parse_sql_sections(entry["scama"])
    if "range" not in sections or "batch" not in sections:
        raise ValueError(f"{event}: id_range mode needs '-- @range' and '-- @batch' sections in SQL")

    step = int(entry.get("BANCH_VALUE", _DEFAULT_BATCH_SIZE))
    limit = int(entry["LIMIT_BATCHES"]) if entry.get("LIMIT_BATCHES") is not None else None
    pk = entry["PK"]
    index = entry["INDEX_NAME"].strip()
    env = _normalize_env(entry)
    event_dir = _event_dir(event, env)
    plan = Plan.from_entry(entry)

    df_range, _ = run_tracked(conn, sections["range"], {}, logger, table=event, batch=0)
    if df_range.empty or df_range.iloc[0].isna().all():
        print(f"[{event}] empty range -> nothing to do")
        return
    row = df_range.iloc[0]
    min_id = int(row.get("MIN_ID") or row.get("min_id"))
    max_id = int(row.get("MAX_ID") or row.get("max_id"))
    total = count_id_windows(min_id, max_id, step, limit)
    print(f"[{event}] env={env} id_range min={min_id} max={max_id} step={step} limit={limit} "
          f"index={index} pk={pk} batches={total} workers={PIPELINE_WORKERS}")

    batch_sql = sections["batch"]
    tasks = [{
        "kind": "id_range",
        "args": {
            "event": event, "entry": entry,
            "mapping": plan.mapping, "ora_cols": plan.ora_cols, "es_cols": plan.es_cols,
            "batch_sql": batch_sql, "pk": pk, "index": index,
            "event_dir_str": str(event_dir),
            "from_id": from_id, "to_id": to_id, "batch_idx": i,
        },
    } for i, (from_id, to_id) in enumerate(id_windows(min_id, max_id, step, limit), 1)]
    _execute_tasks(pool, tasks, event, total)
    _summarize_event(event, env, event_dir)


def run_event_time_union(conn, event: str, entry: dict, logger, pool) -> None:
    parts = entry.get("parts") or []
    if not parts:
        raise ValueError(f"{event}: time_union needs `parts:` list with sql_file/VALUE_COLM/TIME_DATE per part")
    if not entry.get("START_TIME") or not entry.get("END_TIME"):
        return _skip(logger, event, "missing START_TIME/END_TIME")
    step_td = parse_step(entry.get("BANCH_VALUE", "DAY"))
    if step_td is None:
        raise ValueError(f"{event}: bad BANCH_VALUE {entry.get('BANCH_VALUE')!r}")

    start, end = parse_ts(entry["START_TIME"]), parse_ts(entry["END_TIME"])
    pk = entry["PK"]
    index = entry["INDEX_NAME"].strip()
    env = _normalize_env(entry)
    event_dir = _event_dir(event, env)
    allowed = entry.get("FILED_THAT_RUN") or []
    es_entry = _es_entry_for(entry, parts[0].get("mapping", []))

    parts_prepared = [{
        "sql": add_time_filter(part["scama"], part["TIME_DATE"]),
        "mapping": part["mapping"],
        "raw_part": part,
    } for part in parts]

    total = count_windows(start, end, step_td)
    print(f"[{event}] env={env} {fmt_ts(start)} -> {fmt_ts(end)} step={entry.get('BANCH_VALUE')} "
          f"index={index} parts={len(parts)} es_field={es_time_field(es_entry)} batches={total} "
          f"workers={PIPELINE_WORKERS}")

    tasks = [{
        "kind": "time_union",
        "args": {
            "event": event, "entry_es": es_entry,
            "parts_prepared": parts_prepared, "allowed": allowed,
            "pk": pk, "index": index,
            "event_dir_str": str(event_dir),
            "w_from": w_from, "w_to": w_to, "batch_idx": i,
        },
    } for i, (w_from, w_to) in enumerate(windows(start, end, step_td), 1)]
    _execute_tasks(pool, tasks, event, total)
    _summarize_event(event, env, event_dir)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

_RUNNERS = {
    "time": run_event,
    "id_range": run_event_id_range,
    "time_union": run_event_time_union,
}


def main() -> None:
    run_id = uuid.uuid4().hex[:12]
    logger = get_run_logger(run_id)
    logger.connection(
        oracle_host=DB_HOST, oracle_port=PORT, oracle_service=SERVICE_NAME,
        oracle_user=USERNAME, batch_size=ARRAYSIZE, workers=WORKERS,
        query_timeout_ms=QUERY_TIMEOUT_MS,
        es_url=ES_URL, es_user=ES_USER, es_verify=ES_VERIFY,
        page_size=ES_PAGE_SIZE,
        timeout_connect=ES_TIMEOUT_CONNECT, timeout_read=ES_TIMEOUT_READ,
        **host_info(),
    )
    print(f"Run {run_id} | conn -> {CONN_CSV.name} | events -> {EVENTS_CSV.name} | queries -> {QUERIES_CSV.name}")
    print(f"Pipeline workers: {PIPELINE_WORKERS}")

    ctx = mp.get_context("spawn")
    log_queue = ctx.Queue()
    listener = start_log_listener(log_queue, logger)
    pool = ctx.Pool(processes=PIPELINE_WORKERS,
                    initializer=_worker_init, initargs=(log_queue, run_id))

    t_run = time.perf_counter()
    pool_terminated = False
    try:
        with create_connection() as conn:
            for event, entry in load_events().items():
                if not entry.get("IS_RUNNING"):
                    _skip(logger, event, "IS_RUNNING=False")
                    continue
                mode = (entry.get("MODE") or "time").strip().lower()
                runner = _RUNNERS.get(mode, run_event)
                runner(conn, event, entry, logger, pool)
        pool.close()
    except Exception as e:
        logger.event("fatal", level="ERROR", error=str(e))
        pool.terminate()
        pool_terminated = True
        raise
    finally:
        if not pool_terminated:
            try:
                pool.close()
            except Exception:
                pass
        pool.join()
        log_queue.put(None)
        listener.join(timeout=10)
        logger.event("run_end", total_seconds=round(time.perf_counter() - t_run, 3))

    if not env_truthy("PIPELINE_SKIP_PG_SYNC"):
        try:
            from connect_into_postgres.sync_out import run_sync
            print("[pg-sync] mirroring out/ -> Postgres")
            run_sync()
        except Exception as e:
            print(f"[pg-sync] skipped: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
