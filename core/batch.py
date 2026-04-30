"""Per-batch executors + shared chunking/SQL/lookup helpers.

Each `_batch_*` runs in a worker process: one Oracle window -> compare vs ES -> save diffs.
"""
from __future__ import annotations

import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from _pipeline_env import normalize_es_env
from connect_into_es.connect_to_es import fetch_range_df, fetch_terms_df
from connect_into_orcal.connect_to_orcal import run_tracked
from core.compare import compare_records, compare_shaped, transform_to_es_shape
from core.config import OUT_DIR
from core.csv_writer import save_diffs, save_es_csv, save_oracle_csv

_UNIT = {
    "M": timedelta(minutes=1),  "MIN": timedelta(minutes=1),  "MINUTE": timedelta(minutes=1), "MINUTES": timedelta(minutes=1),
    "H": timedelta(hours=1),    "HR":  timedelta(hours=1),    "HOUR":   timedelta(hours=1),   "HOURS":   timedelta(hours=1),
    "D": timedelta(days=1),     "DAY": timedelta(days=1),     "DAYS":   timedelta(days=1),
    "W": timedelta(weeks=1),    "WEEK": timedelta(weeks=1),   "WEEKS":  timedelta(weeks=1),
}
_STEP_PATTERN = re.compile(r"\s*(\d+)\s*([A-Z]*)\s*")
DEFAULT_BATCH_SIZE = 1000


# ---------------------------------------------------------------------------
# Window / SQL helpers
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
def timed():
    import time as _time
    t0 = _time.perf_counter()
    yield lambda: round(_time.perf_counter() - t0, 3)


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


def es_entry_for(entry: dict, mapping: list[dict] | None = None) -> dict:
    """Override TIME_DATE with ES_TIME so es_time_field resolves correctly."""
    es_time = (entry.get("ES_TIME") or "").strip()
    out = {**entry, "TIME_DATE": es_time} if es_time else dict(entry)
    if mapping is not None:
        out["mapping"] = mapping
    return out


def normalize_env(entry: dict) -> str:
    return normalize_es_env(entry.get("ES_ENV", "stage"))


def event_dir(event: str, env: str) -> Path:
    d = OUT_DIR / event / env
    d.mkdir(parents=True, exist_ok=True)
    return d


def summarize_event(event: str, env: str, ev_dir: Path) -> None:
    """Print end-of-run summary: which output files (if any) were written."""
    changes_dir = ev_dir / "changes"
    changes = list(changes_dir.glob("changes_*.csv")) if changes_dir.is_dir() else []
    missing = list(changes_dir.glob("missing_in_es_*.csv")) if changes_dir.is_dir() else []
    if not changes and not missing:
        print(f"[{event}] env={env} done: no changes found — nothing written "
              f"(Oracle == ES for this window)")
        return
    print(f"[{event}] env={env} done: changes_files={len(changes)} "
          f"missing_files={len(missing)} -> {changes_dir}")


# ---------------------------------------------------------------------------
# Lookup table cache (per-process)
# ---------------------------------------------------------------------------

_LOOKUP_CACHE: dict[str, dict[str, str]] = {}
_SETTINGS_DIR = Path(__file__).resolve().parent.parent / "settings"


def _load_lookup(conn, sql_file_rel: str, logger) -> dict[str, str]:
    abs_path = str((_SETTINGS_DIR / sql_file_rel).resolve())
    if abs_path in _LOOKUP_CACHE:
        return _LOOKUP_CACHE[abs_path]
    sql = (_SETTINGS_DIR / sql_file_rel).read_text(encoding="utf-8")
    df, _ = run_tracked(conn, sql, {}, logger, table=f"lookup::{sql_file_rel}",
                        batch=0, operation="oracle_lookup")
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


# ---------------------------------------------------------------------------
# Per-batch executors
# ---------------------------------------------------------------------------

def batch_time(conn, logger, *, event, entry_es, mapping, ora_cols, es_cols,
               sql, pk, index, event_dir_str, w_from, w_to, batch_idx,
               env: str = "", adapter=None):
    started_at = datetime.now(timezone.utc)
    plan = Plan(mapping=mapping, ora_cols=ora_cols, es_cols=es_cols)
    ev_dir = Path(event_dir_str)
    params = {"from_ts": fmt_ts(w_from), "to_ts": fmt_ts(w_to)}
    stamp = w_from.strftime("%Y%m%d_%H%M%S")

    df_ora, qid = run_tracked(conn, sql, params, logger, table=event,
                              batch=batch_idx, env=env, operation="oracle_select")
    df_ora = plan.filter_oracle(df_ora)
    shaped_ora = transform_to_es_shape(df_ora, plan.mapping, adapter=adapter)
    save_oracle_csv(shaped_ora, ev_dir / f"{event}_oracle_{stamp}.csv",
                    event, batch_idx, logger, query_id=qid)

    with logger.query(f"ES {index} {w_from}->{w_to}", table=index,
                      batch=batch_idx, env=env, operation="es_search") as q:
        df_es = fetch_range_df(index, entry_es, w_from, w_to)
        q.set_rows(len(df_es))
    df_es = plan.filter_es(df_es)
    save_es_csv(df_es, ev_dir / f"{event}_es_{stamp}.csv", event, batch_idx, logger)

    diff_counts = save_diffs(
        compare_records(df_ora, df_es, plan.mapping, pk, adapter=adapter),
        ev_dir, stamp, event, batch_idx, logger,
        shaped_ora=shaped_ora, pk=pk,
    )
    return {"batch": batch_idx, "ora_rows": len(df_ora), "es_rows": len(df_es),
            "diff_counts": diff_counts,
            "started_at": started_at, "ended_at": datetime.now(timezone.utc),
            "window_from": w_from, "window_to": w_to,
            "batch_id": f"{index}#{stamp}"}


def batch_id_range(conn, logger, *, event, entry, mapping, ora_cols, es_cols,
                   batch_sql, pk, index, event_dir_str, from_id, to_id, batch_idx,
                   env: str = "", adapter=None):
    started_at = datetime.now(timezone.utc)
    plan = Plan(mapping=mapping, ora_cols=ora_cols, es_cols=es_cols)
    ev_dir = Path(event_dir_str)
    params = {"from_id": from_id, "to_id": to_id}
    stamp = f"{from_id}_{to_id}"

    df_ora, qid = run_tracked(conn, batch_sql, params, logger, table=event,
                              batch=batch_idx, env=env, operation="oracle_select")
    df_ora = plan.filter_oracle(df_ora)
    shaped_ora = transform_to_es_shape(df_ora, plan.mapping, adapter=adapter)
    save_oracle_csv(shaped_ora, ev_dir / f"{event}_oracle_{stamp}.csv",
                    event, batch_idx, logger, query_id=qid)

    ids = df_ora[pk].tolist() if pk in df_ora.columns else []
    with logger.query(f"ES {index} terms {pk} n={len(ids)}", table=index,
                      batch=batch_idx, env=env, operation="es_search") as q:
        df_es = fetch_terms_df(index, pk, ids, entry)
        q.set_rows(len(df_es))
    df_es = plan.filter_es(df_es)
    save_es_csv(df_es, ev_dir / f"{event}_es_{stamp}.csv", event, batch_idx, logger)

    diff_counts = save_diffs(
        compare_records(df_ora, df_es, plan.mapping, pk, adapter=adapter),
        ev_dir, stamp, event, batch_idx, logger,
        shaped_ora=shaped_ora, pk=pk,
    )
    return {"batch": batch_idx, "ora_rows": len(df_ora), "es_rows": len(df_es),
            "diff_counts": diff_counts,
            "started_at": started_at, "ended_at": datetime.now(timezone.utc),
            "id_from": int(from_id), "id_to": int(to_id),
            "batch_id": f"{index}#{stamp}"}


def batch_time_union(conn, logger, *, event, entry_es, parts_prepared,
                     allowed, pk, index, event_dir_str, w_from, w_to, batch_idx,
                     env: str = "", adapter=None):
    started_at = datetime.now(timezone.utc)
    ev_dir = Path(event_dir_str)
    params = {"from_ts": fmt_ts(w_from), "to_ts": fmt_ts(w_to)}
    stamp = w_from.strftime("%Y%m%d_%H%M%S")
    keep_set = set(allowed) | {pk} if allowed else None

    shaped_parts: list[pd.DataFrame] = []
    total_raw = 0
    for j, prep in enumerate(parts_prepared, 1):
        df_raw, _ = run_tracked(conn, prep["sql"], params, logger,
                                table=f"{event}::part{j}", batch=batch_idx,
                                env=env, operation="oracle_select")
        total_raw += len(df_raw)
        shaped = transform_to_es_shape(df_raw, prep["mapping"], adapter=adapter)
        shaped = _apply_lookup(shaped, df_raw, prep["raw_part"], conn, logger)
        shaped_parts.append(shaped)

    df_ora_shaped = pd.concat(shaped_parts, ignore_index=True) if shaped_parts else pd.DataFrame()
    if keep_set is not None and not df_ora_shaped.empty:
        df_ora_shaped = df_ora_shaped[[c for c in df_ora_shaped.columns if c in keep_set]]
    save_oracle_csv(df_ora_shaped, ev_dir / f"{event}_oracle_{stamp}.csv",
                    event, batch_idx, logger, parts=len(parts_prepared), raw_rows=total_raw)

    with logger.query(f"ES {index} {w_from}->{w_to}", table=index,
                      batch=batch_idx, env=env, operation="es_search") as q:
        df_es = fetch_range_df(index, entry_es, w_from, w_to)
        q.set_rows(len(df_es))
    if keep_set is not None and not df_es.empty:
        df_es = df_es[[c for c in df_es.columns if c in keep_set or c == pk]]
    save_es_csv(df_es, ev_dir / f"{event}_es_{stamp}.csv", event, batch_idx, logger)

    fields = list(allowed) if allowed else None
    diff_counts = save_diffs(
        compare_shaped(df_ora_shaped, df_es, pk, fields=fields),
        ev_dir, stamp, event, batch_idx, logger,
        shaped_ora=df_ora_shaped, pk=pk,
    )
    return {"batch": batch_idx, "ora_rows": len(df_ora_shaped), "es_rows": len(df_es),
            "diff_counts": diff_counts,
            "started_at": started_at, "ended_at": datetime.now(timezone.utc),
            "window_from": w_from, "window_to": w_to,
            "batch_id": f"{index}#{stamp}"}


def batch_id_range_union(conn, logger, *, event, entry_es, parts_prepared,
                         allowed, pk, index, event_dir_str, from_id, to_id,
                         batch_idx, env: str = "", adapter=None):
    """One numeric ID window, multiple parts. Each part runs its own @batch SQL
    with shared :from_id/:to_id, transforms with its mapping, then concatenated
    and compared against ES via terms-on-PK.

    Empty windows for any single part are silently OK — that part contributes
    zero rows and the batch continues.
    """
    started_at = datetime.now(timezone.utc)
    ev_dir = Path(event_dir_str)
    params = {"from_id": from_id, "to_id": to_id}
    stamp = f"{from_id}_{to_id}"
    keep_set = set(allowed) | {pk} if allowed else None

    shaped_parts: list[pd.DataFrame] = []
    total_raw = 0
    for j, prep in enumerate(parts_prepared, 1):
        df_raw, _ = run_tracked(conn, prep["batch_sql"], params, logger,
                                table=f"{event}::part{j}", batch=batch_idx,
                                env=env, operation="oracle_select")
        total_raw += len(df_raw)
        shaped = transform_to_es_shape(df_raw, prep["mapping"], adapter=adapter)
        if prep.get("raw_part"):
            shaped = _apply_lookup(shaped, df_raw, prep["raw_part"], conn, logger)
        shaped_parts.append(shaped)

    df_ora_shaped = pd.concat(shaped_parts, ignore_index=True) if shaped_parts else pd.DataFrame()
    if keep_set is not None and not df_ora_shaped.empty:
        df_ora_shaped = df_ora_shaped[[c for c in df_ora_shaped.columns if c in keep_set]]
    save_oracle_csv(df_ora_shaped, ev_dir / f"{event}_oracle_{stamp}.csv",
                    event, batch_idx, logger, parts=len(parts_prepared), raw_rows=total_raw)

    ids = df_ora_shaped[pk].tolist() if pk in df_ora_shaped.columns else []
    with logger.query(f"ES {index} terms {pk} n={len(ids)}", table=index,
                      batch=batch_idx, env=env, operation="es_search") as q:
        df_es = fetch_terms_df(index, pk, ids, entry_es)
        q.set_rows(len(df_es))
    if keep_set is not None and not df_es.empty:
        df_es = df_es[[c for c in df_es.columns if c in keep_set or c == pk]]
    save_es_csv(df_es, ev_dir / f"{event}_es_{stamp}.csv", event, batch_idx, logger)

    fields = list(allowed) if allowed else None
    diff_counts = save_diffs(
        compare_shaped(df_ora_shaped, df_es, pk, fields=fields),
        ev_dir, stamp, event, batch_idx, logger,
        shaped_ora=df_ora_shaped, pk=pk,
    )
    return {"batch": batch_idx, "ora_rows": len(df_ora_shaped), "es_rows": len(df_es),
            "diff_counts": diff_counts,
            "started_at": started_at, "ended_at": datetime.now(timezone.utc),
            "id_from": int(from_id), "id_to": int(to_id),
            "batch_id": f"{index}#{stamp}"}
