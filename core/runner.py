"""Pipeline runner: pool init, per-event dispatch, mode runners."""
from __future__ import annotations

import multiprocessing as mp
import sys
import time
from pathlib import Path

from connect_into_orcal.connect_to_orcal import create_connection
from connect_into_orcal.logging_setup import QueueLogger, start_log_listener
from connect_into_es.connect_to_es import es_time_field
from core.adapter_loader import get_adapter
from core.batch import (
    DEFAULT_BATCH_SIZE, Plan, add_time_filter, batch_id_range, batch_time, batch_time_union,
    count_id_windows, count_windows, es_entry_for, event_dir, fmt_ts, id_windows,
    normalize_env, parse_sql_sections, parse_step, summarize_event, windows,
)
from core.config import PIPELINE_WORKERS, VERBOSE
from settings.loader import load_events

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

_EMIT_PROGRESS_MARKERS = not sys.stdout.isatty()


def _emit_progress(event: str, batch: int, total: int) -> None:
    if _EMIT_PROGRESS_MARKERS:
        print(f"[PROGRESS] event={event} batch={batch}/{total}", flush=True)


def _skip(logger, event: str, reason: str) -> None:
    print(f"[{event}] skipped: {reason}")
    logger.event("event_skipped", table=event, reason=reason)


# ---------------------------------------------------------------------------
# Worker process: one Oracle connection + QueueLogger per worker.
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
        return batch_time(_W_CONN, _W_LOGGER, **args)
    if kind == "id_range":
        return batch_id_range(_W_CONN, _W_LOGGER, **args)
    if kind == "time_union":
        return batch_time_union(_W_CONN, _W_LOGGER, **args)
    raise ValueError(f"unknown batch kind: {kind}")


def _execute_tasks(pool, tasks: list, event: str, total: int) -> None:
    if not tasks:
        return
    bar = None
    if tqdm is not None:
        bar = tqdm(total=total, desc=event, unit="batch", disable=not sys.stderr.isatty())
    done = 0
    try:
        for result in pool.imap_unordered(_worker_batch, tasks):
            done += 1
            if bar is not None:
                bar.update(1)
            _emit_progress(event, done, total)
            if VERBOSE and isinstance(result, dict):
                msg = (f"[{event}] batch {result.get('batch')}/{total} "
                       f"oracle={result.get('ora_rows')}r es={result.get('es_rows')}r")
                if bar is not None:
                    bar.write(msg)
                else:
                    print(msg)
    finally:
        if bar is not None:
            bar.close()


# ---------------------------------------------------------------------------
# Mode runners
# ---------------------------------------------------------------------------

def run_event_time(conn, event: str, entry: dict, logger, pool) -> None:
    if not entry.get("START_TIME") or not entry.get("END_TIME"):
        return _skip(logger, event, "missing START_TIME/END_TIME")
    step_td = parse_step(entry.get("BANCH_VALUE", "HOUR"))
    if step_td is None:
        return _skip(logger, event, f"bad BANCH_VALUE={entry.get('BANCH_VALUE')!r}")

    from _pipeline_env import parse_ts
    start, end = parse_ts(entry["START_TIME"]), parse_ts(entry["END_TIME"])
    plan = Plan.from_entry(entry)
    sql = add_time_filter(entry["scama"], entry["TIME_DATE"])
    index = entry["INDEX_NAME"].strip()
    env = normalize_env(entry)
    ev_dir = event_dir(event, env)
    es_entry = es_entry_for(entry)
    pk = entry["PK"]

    adapter = get_adapter(index)
    total = count_windows(start, end, step_td)
    print(f"[{event}] env={env} {fmt_ts(start)} -> {fmt_ts(end)} step={entry.get('BANCH_VALUE')} "
          f"index={index} es_field={es_time_field(es_entry)} batches={total} workers={PIPELINE_WORKERS}")

    tasks = [{
        "kind": "time",
        "args": {
            "event": event, "entry_es": es_entry,
            "mapping": plan.mapping, "ora_cols": plan.ora_cols, "es_cols": plan.es_cols,
            "sql": sql, "pk": pk, "index": index,
            "event_dir_str": str(ev_dir),
            "w_from": w_from, "w_to": w_to, "batch_idx": i,
            "adapter": adapter,
        },
    } for i, (w_from, w_to) in enumerate(windows(start, end, step_td), 1)]
    _execute_tasks(pool, tasks, event, total)
    summarize_event(event, env, ev_dir)


def run_event_id_range(conn, event: str, entry: dict, logger, pool) -> None:
    sections = parse_sql_sections(entry["scama"])
    if "range" not in sections or "batch" not in sections:
        raise ValueError(f"{event}: id_range mode needs '-- @range' and '-- @batch' sections in SQL")

    from connect_into_orcal.connect_to_orcal import run_tracked
    step = int(entry.get("BANCH_VALUE", DEFAULT_BATCH_SIZE))
    limit = int(entry["LIMIT_BATCHES"]) if entry.get("LIMIT_BATCHES") is not None else None
    pk = entry["PK"]
    index = entry["INDEX_NAME"].strip()
    env = normalize_env(entry)
    ev_dir = event_dir(event, env)
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

    adapter = get_adapter(index)
    batch_sql = sections["batch"]
    tasks = [{
        "kind": "id_range",
        "args": {
            "event": event, "entry": entry,
            "mapping": plan.mapping, "ora_cols": plan.ora_cols, "es_cols": plan.es_cols,
            "batch_sql": batch_sql, "pk": pk, "index": index,
            "event_dir_str": str(ev_dir),
            "from_id": from_id, "to_id": to_id, "batch_idx": i,
            "adapter": adapter,
        },
    } for i, (from_id, to_id) in enumerate(id_windows(min_id, max_id, step, limit), 1)]
    _execute_tasks(pool, tasks, event, total)
    summarize_event(event, env, ev_dir)


def run_event_time_union(conn, event: str, entry: dict, logger, pool) -> None:
    parts = entry.get("parts") or []
    if not parts:
        raise ValueError(f"{event}: time_union needs `parts:` list with sql_file/VALUE_COLM/TIME_DATE per part")
    if not entry.get("START_TIME") or not entry.get("END_TIME"):
        return _skip(logger, event, "missing START_TIME/END_TIME")
    step_td = parse_step(entry.get("BANCH_VALUE", "DAY"))
    if step_td is None:
        raise ValueError(f"{event}: bad BANCH_VALUE {entry.get('BANCH_VALUE')!r}")

    from _pipeline_env import parse_ts
    start, end = parse_ts(entry["START_TIME"]), parse_ts(entry["END_TIME"])
    pk = entry["PK"]
    index = entry["INDEX_NAME"].strip()
    env = normalize_env(entry)
    ev_dir = event_dir(event, env)
    allowed = entry.get("FILED_THAT_RUN") or []
    es_entry = es_entry_for(entry, parts[0].get("mapping", []))

    parts_prepared = [{
        "sql": add_time_filter(part["scama"], part["TIME_DATE"]),
        "mapping": part["mapping"],
        "raw_part": part,
    } for part in parts]

    adapter = get_adapter(index)
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
            "event_dir_str": str(ev_dir),
            "w_from": w_from, "w_to": w_to, "batch_idx": i,
            "adapter": adapter,
        },
    } for i, (w_from, w_to) in enumerate(windows(start, end, step_td), 1)]
    _execute_tasks(pool, tasks, event, total)
    summarize_event(event, env, ev_dir)


_RUNNERS = {
    "time": run_event_time,
    "id_range": run_event_id_range,
    "time_union": run_event_time_union,
}


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------

def run_pipeline(run_id: str, logger) -> None:
    """Iterate enabled events, dispatch each to the right mode runner via worker pool."""
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
                runner = _RUNNERS.get(mode, run_event_time)
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
