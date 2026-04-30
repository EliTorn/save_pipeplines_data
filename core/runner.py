"""Pipeline runner: pool init, per-event dispatch, mode runners."""
from __future__ import annotations

import multiprocessing as mp
import sys
import time
from datetime import datetime, timezone

from connect_into_orcal.connect_to_orcal import create_connection
from connect_into_orcal.logging_setup import QueueLogger, start_log_listener
from connect_into_es.connect_to_es import es_time_field
from connect_into_postgres import run_summary, observability
from core.adapter_loader import get_adapter
from core.batch import (
    DEFAULT_BATCH_SIZE, Plan, add_time_filter, batch_id_range, batch_id_range_union,
    batch_time, batch_time_union,
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
    # Phase 1.5: emit a v2 connection row for this worker's oracle conn so
    # connections.parquet shows every worker, not just the parent.
    import os as _os
    if _os.getenv("PIPELINE_LOGGING_V2", "").strip().lower() in ("1", "true", "yes", "on"):
        try:
            from connect_into_orcal.connect_to_orcal import (
                DB_HOST, PORT, SERVICE_NAME, USERNAME,
            )
            now = datetime.now(timezone.utc)
            log_queue.put(("v2_row", {"table": "connections", "row": {
                "run_id":      run_id,
                "system":      "oracle",
                "target":      SERVICE_NAME,
                "host":        DB_HOST,
                "port":        int(PORT) if PORT else None,
                "user":        USERNAME,
                "started_at":  now,
                "ended_at":    now,
                "duration_ms": 0,
                "status":      "ok",
                "error":       None,
                "pid":         _os.getpid(),
                "thread":      "MainThread",
            }}))
        except Exception:
            pass
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
    if kind == "id_range_union":
        return batch_id_range_union(_W_CONN, _W_LOGGER, **args)
    raise ValueError(f"unknown batch kind: {kind}")


def _execute_tasks(pool, tasks: list, event: str, total: int,
                   *, run_id: str = "", env: str = "",
                   index: str = "", logger=None) -> dict:
    """Run all tasks; return aggregated counts {'changes': N, 'missing': M,
    'ora_rows': N, 'es_rows': N}. Per batch, also write a batch_log row
    (operation='compare') for observability."""
    agg = {"changes": 0, "missing": 0, "ora_rows": 0, "es_rows": 0}
    if not tasks:
        return agg
    v2 = getattr(logger, "_v2", None) if logger is not None else None
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
            if isinstance(result, dict):
                ora_rows = int(result.get("ora_rows") or 0)
                es_rows = int(result.get("es_rows") or 0)
                dc = result.get("diff_counts") or {}
                changes = int(dc.get("changes") or 0)
                missing = int(dc.get("missing") or 0)
                agg["ora_rows"] += ora_rows
                agg["es_rows"] += es_rows
                agg["changes"] += changes
                agg["missing"] += missing
                # batch_log: one row per finished batch.
                if run_id:
                    now = datetime.now(timezone.utc)
                    try:
                        observability.log_batch(
                            run_id=run_id, env=env,
                            batch_id=str(result.get("batch")),
                            target_name=index or event, operation="compare",
                            source_system="oracle+elasticsearch",
                            rows_requested=None,
                            rows_returned=ora_rows + es_rows,
                            rows_changed=changes,
                            rows_missing=missing,
                            started_at=now, ended_at=now, duration_ms=None,
                            status="ok",
                        )
                    except Exception:
                        pass
                # v2 batches.parquet — one row per finished batch
                if v2 is not None:
                    started_at = result.get("started_at") or datetime.now(timezone.utc)
                    ended_at = result.get("ended_at") or datetime.now(timezone.utc)
                    try:
                        v2.batch(
                            batch_id=result.get("batch_id") or f"{index or event}#{result.get('batch')}",
                            target=index or event, operation="compare",
                            started_at=started_at, ended_at=ended_at,
                            window_from=result.get("window_from"),
                            window_to=result.get("window_to"),
                            id_from=result.get("id_from"),
                            id_to=result.get("id_to"),
                            rows_oracle=ora_rows, rows_es=es_rows,
                            rows_changed=changes, rows_missing=missing,
                            status="ok",
                        )
                    except Exception as e:
                        print(f"[v2] batch emit failed (silenced): "
                              f"{type(e).__name__}: {e}", flush=True)
                if VERBOSE:
                    msg = (f"[{event}] batch {result.get('batch')}/{total} "
                           f"oracle={ora_rows}r es={es_rows}r")
                    if bar is not None:
                        bar.write(msg)
                    else:
                        print(msg)
    finally:
        if bar is not None:
            bar.close()
    return agg


def _record_compare_summary(run_id: str, event: str, env: str, index: str,
                            agg: dict, started_at: datetime,
                            error: Exception | None = None,
                            logger=None) -> None:
    """Insert one pipeline_run_summary row for a compare run. Best-effort.

    Also pipes counts into the active v2 target ctx (if any) attached to
    ``logger._v2_target`` — the parent run_pipeline loop sets this before
    each compare and clears it after.
    """
    if not run_id:
        return
    rows_count = (agg.get("changes") or 0) + (agg.get("missing") or 0)
    err_msg = None
    status = "ok"
    if error is not None:
        status = "failed"
        err_msg = f"{type(error).__name__}: {error}"
    try:
        run_summary.record_run(
            run_id=run_id, env=env, target_name=index, operation="compare",
            rows_count=rows_count,
            source_file=None,  # compare touches many files per event
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            status=status, error=err_msg,
        )
    except Exception as e:
        # Never let summary insertion break the run.
        print(f"[run-summary] record_run failed for {event}: "
              f"{type(e).__name__}: {e}", flush=True)
    # Pipe into v2 target if one is active.
    v2_target = getattr(logger, "_v2_target", None) if logger is not None else None
    if v2_target is not None:
        try:
            v2_target.set_rows(
                source=int(agg.get("ora_rows") or 0),
                target=int(agg.get("es_rows") or 0),
                inserted=0, updated=0, deleted=0,  # compare detects only
                missing=int(agg.get("missing") or 0),
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Mode runners
# ---------------------------------------------------------------------------

def run_event_time(conn, event: str, entry: dict, logger, pool,
                   run_id: str = "") -> None:
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
            "env": env, "adapter": adapter,
        },
    } for i, (w_from, w_to) in enumerate(windows(start, end, step_td), 1)]
    started = datetime.now(timezone.utc)
    err: Exception | None = None
    try:
        agg = _execute_tasks(pool, tasks, event, total,
                             run_id=run_id, env=env, index=index, logger=logger)
    except Exception as e:
        err = e
        agg = {"changes": 0, "missing": 0}
        raise
    finally:
        _record_compare_summary(run_id, event, env, index, agg, started, err,
                                logger=logger)
    summarize_event(event, env, ev_dir)


def run_event_id_range(conn, event: str, entry: dict, logger, pool,
                       run_id: str = "") -> None:
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

    df_range, _ = run_tracked(conn, sections["range"], {}, logger, table=event,
                               batch=0, env=env, operation="oracle_range_probe")
    if df_range.empty or df_range.iloc[0].isna().all():
        print(f"[{event}] empty range -> nothing to do")
        return
    row = df_range.iloc[0]
    min_id = int(row.get("MIN_ID") or row.get("min_id"))
    max_id = int(row.get("MAX_ID") or row.get("max_id"))
    # Optional event-level overrides (set via app.py batch range UI).
    eff_min = min_id
    eff_max = max_id
    if entry.get("BATCH_FROM_ID") is not None:
        eff_min = max(min_id, int(entry["BATCH_FROM_ID"]))
    if entry.get("BATCH_TO_ID") is not None:
        eff_max = min(max_id, int(entry["BATCH_TO_ID"]))
    if eff_min >= eff_max:
        print(f"[{event}] effective range empty after overrides "
              f"[{eff_min},{eff_max}] -> nothing to do")
        return
    total = count_id_windows(eff_min, eff_max, step, limit)
    print(f"[{event}] env={env} id_range min={min_id} max={max_id} "
          f"effective=[{eff_min},{eff_max}] step={step} limit={limit} "
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
            "env": env, "adapter": adapter,
        },
    } for i, (from_id, to_id) in enumerate(id_windows(eff_min, eff_max, step, limit), 1)]
    started = datetime.now(timezone.utc)
    err: Exception | None = None
    try:
        agg = _execute_tasks(pool, tasks, event, total,
                             run_id=run_id, env=env, index=index, logger=logger)
    except Exception as e:
        err = e
        agg = {"changes": 0, "missing": 0}
        raise
    finally:
        _record_compare_summary(run_id, event, env, index, agg, started, err,
                                logger=logger)
    summarize_event(event, env, ev_dir)


def run_event_time_union(conn, event: str, entry: dict, logger, pool,
                         run_id: str = "") -> None:
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
            "env": env, "adapter": adapter,
        },
    } for i, (w_from, w_to) in enumerate(windows(start, end, step_td), 1)]
    started = datetime.now(timezone.utc)
    err: Exception | None = None
    try:
        agg = _execute_tasks(pool, tasks, event, total,
                             run_id=run_id, env=env, index=index, logger=logger)
    except Exception as e:
        err = e
        agg = {"changes": 0, "missing": 0}
        raise
    finally:
        _record_compare_summary(run_id, event, env, index, agg, started, err,
                                logger=logger)
    summarize_event(event, env, ev_dir)


def run_event_id_range_union(conn, event: str, entry: dict, logger, pool,
                             run_id: str = "") -> None:
    """Numeric-id-range across multiple parts that share one ES index.

    Each part has its own SQL with `-- @range` and `-- @batch` sections plus
    its own VALUE_COLM mapping. Runner takes the outer envelope of all parts'
    MIN/MAX, then iterates fixed-size windows. Each window fires every part's
    @batch with the SAME :from_id/:to_id; results are concatenated and the
    combined Oracle df is compared once against ES (terms-on-PK).

    Optional event-level overrides (set via app.py batch range selector):
        BATCH_FROM_ID  — clamp the iteration window lower bound
        BATCH_TO_ID    — clamp the iteration window upper bound
    """
    parts = entry.get("parts") or []
    if not parts:
        raise ValueError(f"{event}: id_range_union needs `parts:` list with sql_file/VALUE_COLM per part")

    from connect_into_orcal.connect_to_orcal import run_tracked
    pk = entry["PK"]
    index = entry["INDEX_NAME"].strip()
    env = normalize_env(entry)
    ev_dir = event_dir(event, env)
    step = int(entry.get("BANCH_VALUE", DEFAULT_BATCH_SIZE))
    limit = int(entry["LIMIT_BATCHES"]) if entry.get("LIMIT_BATCHES") is not None else None

    # Per-part @range probe → outer envelope.
    parts_prepared = []
    mins: list[int] = []
    maxs: list[int] = []
    for j, part in enumerate(parts, 1):
        sections = parse_sql_sections(part["scama"])
        if "range" not in sections or "batch" not in sections:
            raise ValueError(f"{event}: part {j} ({part.get('sql_file')}) needs '-- @range' and '-- @batch' sections")
        df_range, _ = run_tracked(conn, sections["range"], {}, logger,
                                   table=f"{event}::part{j}", batch=0,
                                   env=env, operation="oracle_range_probe")
        if not df_range.empty and not df_range.iloc[0].isna().all():
            row = df_range.iloc[0]
            mn = row.get("MIN_ID") if "MIN_ID" in row.index else row.get("min_id")
            mx = row.get("MAX_ID") if "MAX_ID" in row.index else row.get("max_id")
            if mn is not None:
                mins.append(int(mn))
            if mx is not None:
                maxs.append(int(mx))
        parts_prepared.append({
            "batch_sql": sections["batch"],
            "mapping": part["mapping"],
            "raw_part": part,
        })

    if not mins or not maxs:
        print(f"[{event}] empty global range -> nothing to do")
        return

    global_min = min(mins)
    global_max = max(maxs)

    # Apply user overrides from events.yaml (set via app.py batch range UI).
    eff_min = global_min
    eff_max = global_max
    if entry.get("BATCH_FROM_ID") is not None:
        eff_min = max(global_min, int(entry["BATCH_FROM_ID"]))
    if entry.get("BATCH_TO_ID") is not None:
        eff_max = min(global_max, int(entry["BATCH_TO_ID"]))
    if eff_min >= eff_max:
        print(f"[{event}] effective range empty after overrides "
              f"[{eff_min},{eff_max}] -> nothing to do")
        return

    adapter = get_adapter(index)
    es_entry = es_entry_for(entry, parts_prepared[0]["mapping"])
    allowed = entry.get("FILED_THAT_RUN") or []
    total = count_id_windows(eff_min, eff_max, step, limit)
    print(f"[{event}] env={env} id_range_union global=[{global_min},{global_max}] "
          f"effective=[{eff_min},{eff_max}] step={step} limit={limit} "
          f"index={index} parts={len(parts)} batches={total} workers={PIPELINE_WORKERS}")

    tasks = [{
        "kind": "id_range_union",
        "args": {
            "event": event, "entry_es": es_entry,
            "parts_prepared": parts_prepared, "allowed": allowed,
            "pk": pk, "index": index,
            "event_dir_str": str(ev_dir),
            "from_id": from_id, "to_id": to_id, "batch_idx": i,
            "env": env, "adapter": adapter,
        },
    } for i, (from_id, to_id) in enumerate(id_windows(eff_min, eff_max, step, limit), 1)]
    started = datetime.now(timezone.utc)
    err: Exception | None = None
    try:
        agg = _execute_tasks(pool, tasks, event, total,
                             run_id=run_id, env=env, index=index, logger=logger)
    except Exception as e:
        err = e
        agg = {"changes": 0, "missing": 0}
        raise
    finally:
        _record_compare_summary(run_id, event, env, index, agg, started, err,
                                logger=logger)
    summarize_event(event, env, ev_dir)


_RUNNERS = {
    "time": run_event_time,
    "id_range": run_event_id_range,
    "time_union": run_event_time_union,
    "id_range_union": run_event_id_range_union,
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

    v2 = getattr(logger, "_v2", None)

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
                if v2 is not None:
                    target_name = (entry.get("INDEX_NAME") or event).strip()
                    with v2.target(target_name, "compare",
                                   source_system="oracle",
                                   target_system="elasticsearch") as t:
                        logger._v2_target = t
                        try:
                            runner(conn, event, entry, logger, pool, run_id=run_id)
                        finally:
                            logger._v2_target = None
                else:
                    runner(conn, event, entry, logger, pool, run_id=run_id)
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
