import csv
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, date, time
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")
sys.path.insert(0, str(Path(__file__).parent))

from _pipeline_env import DIFF_MODE_ALIASES, TS_FORMATS, normalize_es_env  # noqa: E402

_PROGRESS_RE = re.compile(r"^\[PROGRESS\]\s+event=(\S+)\s+batch=(\d+)/(\d+)")
_LOG_TAIL = 30

ROOT = Path(__file__).parent
SETTINGS_DIR = ROOT / "settings"
YAML_PATH = SETTINGS_DIR / "events.yaml"
MAIN_PY = ROOT / "main.py"
OUT_DIR = ROOT / "out"


def find_env_file() -> Path | None:
    candidates: list[Path] = []
    candidates.append(ROOT / ".env")
    candidates.append(Path.cwd() / ".env")
    candidates.extend(p / ".env" for p in ROOT.parents)
    candidates.extend(p / ".env" for p in Path.cwd().parents)
    for c in candidates:
        if c.is_file():
            return c.resolve()
    try:
        from dotenv import find_dotenv
        found = find_dotenv(usecwd=True)
        if found and Path(found).is_file():
            return Path(found).resolve()
    except ImportError:
        pass
    return None


def load_env_file() -> dict[str, str]:
    f = find_env_file()
    if f is None:
        return {}
    env: dict[str, str] = {}
    for raw in f.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.lower().startswith("export "):
            line = line[7:].lstrip()
        k, _, v = line.partition("=")
        v = v.strip().strip('"').strip("'")
        env[k.strip()] = v
    return env


def _run_stream(cmd: list[str], placeholder, progress_bar=None) -> tuple[int, str]:
    """Stream subprocess stdout. If progress_bar given, parse [PROGRESS] markers
    into st.progress and only show last _LOG_TAIL log lines in placeholder."""
    env = {**os.environ, **load_env_file()}
    proc = subprocess.Popen(
        cmd, cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace",
        bufsize=1,
    )
    lines: list[str] = []
    assert proc.stdout is not None
    for raw in proc.stdout:
        text = raw.rstrip("\n")
        m = _PROGRESS_RE.match(text) if progress_bar is not None else None
        if m:
            ev, cur, tot = m.group(1), int(m.group(2)), int(m.group(3))
            frac = min(cur / tot, 1.0) if tot > 0 else 0.0
            try:
                progress_bar.progress(frac, text=f"{ev}: batch {cur}/{tot}")
            except Exception:
                pass
            continue
        lines.append(text)
        if progress_bar is not None:
            placeholder.code("\n".join(lines[-_LOG_TAIL:]), language="text")
        else:
            placeholder.code("\n".join(lines), language="text")
    proc.wait()
    return proc.returncode, "\n".join(lines)


def run_main_py_stream(placeholder, progress_bar=None) -> tuple[int, str]:
    return _run_stream([sys.executable, "-u", str(MAIN_PY)], placeholder, progress_bar)


def run_apply_stream(event: str, mode: str, env_choice: str, dry: bool, placeholder) -> tuple[int, str]:
    cmd = [sys.executable, "-u", "-m", "apply_changes.apply_changes",
           "--event", event, "--mode", mode, "--env", env_choice]
    if dry:
        cmd.append("--dry")
    return _run_stream(cmd, placeholder)


def _changes_dir(event: str, env: str) -> Path:
    return OUT_DIR / event / env / "changes"


@st.cache_data(ttl=15, show_spinner=False)
def pg_pending_inventory() -> dict[str, dict[str, dict[str, int]]]:
    """{event: {env: {'changes': N, 'missing': M}}} from Postgres.
    Reads pipeline_changes / pipeline_missing WHERE applied_ts IS NULL.
    Returns empty dict if PG unreachable."""
    try:
        from connect_into_postgres import connect_to_postgres as pg
        conn = pg.create_connection()
    except (Exception, SystemExit):
        return {}
    out: dict[str, dict[str, dict[str, int]]] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT event, env, COUNT(*) FROM pipeline_changes "
                "WHERE applied_ts IS NULL "
                "  AND COALESCE(status, '') <> 'applied' "
                "GROUP BY event, env"
            )
            for ev, en, n in cur.fetchall():
                out.setdefault(ev, {}).setdefault(en, {"changes": 0, "missing": 0})["changes"] = int(n)
            cur.execute(
                "SELECT event, env, COUNT(*) FROM pipeline_missing "
                "WHERE applied_ts IS NULL "
                "GROUP BY event, env"
            )
            for ev, en, n in cur.fetchall():
                out.setdefault(ev, {}).setdefault(en, {"changes": 0, "missing": 0})["missing"] = int(n)
    except Exception as e:
        print(f"[app] pg_pending_inventory failed: {type(e).__name__}: {e}")
    finally:
        try: conn.close()
        except Exception: pass
    return out


def envs_with_changes(event: str) -> list[str]:
    """Which envs (stage/prod) have pending diffs/missing in Postgres for this event."""
    inv = pg_pending_inventory().get(event, {})
    out = [en for en in ("stage", "prod")
           if en in inv and (inv[en].get("changes", 0) + inv[en].get("missing", 0)) > 0]
    if out:
        return out
    # fallback: legacy CSV scan (only used if PG unreachable)
    legacy = []
    for env in ("stage", "prod"):
        ch = _changes_dir(event, env)
        if ch.is_dir() and (any(ch.glob("changes_*.csv")) or any(ch.glob("missing_in_es_*.csv"))):
            legacy.append(env)
    return legacy


def list_events_with_changes() -> list[str]:
    inv = pg_pending_inventory()
    pg_events = [
        ev for ev, envs in inv.items()
        if any((d.get("changes", 0) + d.get("missing", 0)) > 0 for d in envs.values())
    ]
    if pg_events:
        return sorted(pg_events)
    # fallback
    if not OUT_DIR.exists():
        return []
    return sorted(ev.name for ev in OUT_DIR.iterdir()
                  if ev.is_dir() and envs_with_changes(ev.name))


def list_out_tree() -> list[Path]:
    """All changes_/missing_in_es_ files anywhere under out/. Used by the
    cross-event summary builder."""
    if not OUT_DIR.exists():
        return []
    return sorted(p for p in OUT_DIR.rglob("*")
                  if p.is_file() and "changes" in p.parts)


def list_event_out_files(event: str, env: str) -> list[Path]:
    """Files under out/<event>/<env>/changes/ only — what the per-event
    page is allowed to surface."""
    base = OUT_DIR / event / env / "changes"
    if not base.exists():
        return []
    return sorted(p for p in base.iterdir()
                  if p.is_file() and (p.name.startswith("changes_")
                                      or p.name.startswith("missing_in_es_")))


def clear_out_dir() -> tuple[int, int]:
    files = dirs = 0
    if not OUT_DIR.exists():
        return 0, 0
    for child in OUT_DIR.iterdir():
        if child.is_file() or child.is_symlink():
            child.unlink()
            files += 1
        elif child.is_dir():
            shutil.rmtree(child)
            dirs += 1
    return files, dirs


def _is_blank(v) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in ("nan", "none", "<na>")


def build_changes_summary():
    files = list_out_tree()
    if not files:
        return None, pd.DataFrame(), pd.DataFrame()
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str, keep_default_na=False, na_filter=False)
        except Exception:
            continue
        if df.empty:
            continue
        try:
            parts = f.relative_to(OUT_DIR).parts
            event = parts[0]
            env = parts[1] if len(parts) >= 3 and parts[1] in ("stage", "prod") else "?"
        except Exception:
            event, env = "?", "?"
        df.insert(0, "event", event)
        df.insert(1, "env", env)
        df.insert(2, "source_file", str(f.relative_to(OUT_DIR)).replace("\\", "/"))
        frames.append(df)
    if not frames:
        return None, pd.DataFrame(), pd.DataFrame()
    big = pd.concat(frames, ignore_index=True)
    for col in ("field", "oracle_value", "es_value", "status"):
        if col not in big.columns:
            big[col] = ""

    def _toi(v) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            try:
                return int(float(v))
            except (TypeError, ValueError):
                return 0

    rows = []
    if "env" not in big.columns:
        big["env"] = "?"
    for (event, env, field), grp in big.groupby(["event", "env", "field"], dropna=False):
        diff_grp = grp[grp["status"] == "diff"]
        rows.append({
            "event": event,
            "env": env,
            "field": field,
            "total_issues": _toi(len(grp)),
            "diff": _toi((grp["status"] == "diff").sum()),
            "row_missing_in_es": _toi((grp["status"] == "missing_in_es").sum()),
            "row_missing_in_oracle": _toi((grp["status"] == "missing_in_oracle").sum()),
            "es_value_blank": _toi(diff_grp["es_value"].apply(_is_blank).sum()) if "es_value" in diff_grp.columns else 0,
            "oracle_value_blank": _toi(diff_grp["oracle_value"].apply(_is_blank).sum()) if "oracle_value" in diff_grp.columns else 0,
        })
    summary = pd.DataFrame(rows).sort_values(["event", "env", "total_issues"], ascending=[True, True, False])
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"summary_{stamp}.csv"
    OUT_DIR.mkdir(exist_ok=True)
    summary.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path, summary, big

st.set_page_config(page_title="events editor", layout="wide")


PG_SUBPAGES = [
    "Pipeline Health",
    "Connection Logs",
    "Query Performance",
    "Oracle vs ES Differences",
    "Missing Records",
    "Apply Audit",
    "Raw Tables",
]


def _df_or_warn(pg_conn, conn, sql: str, params=None):
    try:
        return pg_conn.run_query(conn, sql, params)
    except Exception as e:
        st.warning(f"query failed: {type(e).__name__}: {e}")
        st.code(sql, language="sql")
        return None


def _show_df(df, *, caption: str | None = None, download_name: str | None = None):
    if df is None:
        return
    if df.empty:
        st.info("no rows")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)
    if caption:
        st.caption(caption)
    if download_name:
        st.download_button(
            "Download as CSV",
            data=df.to_csv(index=False).encode("utf-8-sig"),
            file_name=download_name,
            mime="text/csv",
            key=f"_dl_{download_name}",
        )


def _pg_health(pg_conn, conn):
    st.subheader("Pipeline runs")
    df = _df_or_warn(pg_conn, conn,
                     "SELECT * FROM v_pipeline_run_metrics LIMIT 100")
    _show_df(df, caption="latest 100 runs from v_pipeline_run_metrics",
             download_name="pipeline_run_metrics.csv")

    st.subheader("Apply progress (per event/env)")
    df = _df_or_warn(pg_conn, conn, "SELECT * FROM v_pipeline_apply_progress")
    _show_df(df, caption="from v_pipeline_apply_progress",
             download_name="apply_progress.csv")

    st.subheader("Recent errors")
    df = _df_or_warn(pg_conn, conn, """
        SELECT source, run_id, ts, event, "table" AS table_name, error
        FROM pipeline_log_event
        WHERE level = 'ERROR'
        ORDER BY ts DESC
        LIMIT 50
    """)
    _show_df(df, caption="latest 50 ERROR events", download_name="recent_errors.csv")


def _pg_connection_logs(pg_conn, conn):
    st.subheader("Connection overview")
    df = _df_or_warn(pg_conn, conn, "SELECT * FROM v_pipeline_connection_overview")
    _show_df(df, caption="success/fail counts per source",
             download_name="connection_overview.csv")

    c1, c2 = st.columns([1, 1])
    with c1:
        source = st.selectbox("Source filter",
                              ["(all)", "oracle", "es", "postgres"], index=0)
    with c2:
        status = st.selectbox("Status", ["(all)", "success", "failed"], index=0)
    where = []
    params: list = []
    if source != "(all)":
        where.append("source = %s"); params.append(source)
    if status != "(all)":
        where.append("status = %s"); params.append(status)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    st.subheader("Recent connections")
    df = _df_or_warn(pg_conn, conn,
                     f"SELECT * FROM v_pipeline_connections {where_sql} "
                     f"ORDER BY start_ts DESC NULLS LAST LIMIT 200",
                     tuple(params) if params else None)
    _show_df(df, caption="from v_pipeline_connections",
             download_name="connections.csv")


def _pg_query_perf(pg_conn, conn):
    st.subheader("Slowest 50 queries (any source)")
    df = _df_or_warn(pg_conn, conn, "SELECT * FROM v_pipeline_query_slowest")
    _show_df(df, download_name="slowest_queries.csv")

    st.subheader("Best/worst hour for a specific query (sql_hash)")
    st.caption("Pick one query — same sql_hash means same SQL — to see how its "
               "latency varies hour-by-hour. Different queries are NOT averaged together.")

    hashes_df = _df_or_warn(pg_conn, conn, "SELECT * FROM v_pipeline_query_hashes LIMIT 200")
    if hashes_df is None or hashes_df.empty:
        st.info("no query history yet — run the pipeline first")
        return

    def _label(row) -> str:
        return (f"{row['sql_hash']}  ·  {row['source']}  ·  "
                f"{row.get('query_table') or '-'}  ·  "
                f"{row['total_runs']} runs · avg {row['avg_seconds']}s")

    options = [_label(r) for _, r in hashes_df.iterrows()]
    pick = st.selectbox("Pick sql_hash", options, index=0, key="qhash_pick")
    picked_hash = pick.split(" ", 1)[0]
    picked_source = hashes_df[hashes_df["sql_hash"] == picked_hash]["source"].iloc[0]

    meta = hashes_df[(hashes_df["sql_hash"] == picked_hash)
                     & (hashes_df["source"] == picked_source)].iloc[0]
    st.caption(f"**SQL preview:** `{meta.get('sql_preview') or '(empty)'}`  ·  "
               f"first_seen={meta['first_seen']}  ·  last_seen={meta['last_seen']}")

    by_hr = _df_or_warn(pg_conn, conn,
                        "SELECT hour_of_day, runs, avg_seconds, min_seconds, "
                        "max_seconds, total_rows, avg_rows_per_sec "
                        "FROM v_pipeline_query_by_hour "
                        "WHERE sql_hash = %s AND source = %s "
                        "ORDER BY hour_of_day",
                        (picked_hash, picked_source))
    _show_df(by_hr, caption=f"hour-by-hour timing for sql_hash={picked_hash}",
             download_name=f"query_by_hour_{picked_hash}.csv")

    if by_hr is not None and not by_hr.empty:
        try:
            chart_df = by_hr.set_index("hour_of_day")[["avg_seconds", "max_seconds"]]
            st.line_chart(chart_df)
        except Exception as e:
            st.caption(f"chart unavailable: {e}")
        try:
            best = by_hr.loc[by_hr["avg_seconds"].idxmin()]
            worst = by_hr.loc[by_hr["avg_seconds"].idxmax()]
            st.success(f"best hour: **{int(best['hour_of_day'])}:00** "
                       f"(avg {best['avg_seconds']}s over {int(best['runs'])} runs)")
            st.warning(f"worst hour: **{int(worst['hour_of_day'])}:00** "
                       f"(avg {worst['avg_seconds']}s over {int(worst['runs'])} runs)")
        except Exception:
            pass

    with st.expander("All query hashes (full list)"):
        _show_df(hashes_df, download_name="query_hashes.csv")

    st.subheader("All queries (slow flagged)")
    only_slow = st.checkbox("only slow_query=true", value=False)
    src = st.selectbox("source", ["(all)", "oracle", "es", "postgres"], index=0,
                       key="qperf_src")
    where = []
    params: list = []
    if only_slow:
        where.append("slow_query = TRUE")
    if src != "(all)":
        where.append("source = %s"); params.append(src)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    df = _df_or_warn(pg_conn, conn,
                     f"SELECT id, source, run_id, query_table, batch, "
                     f"start_ts, seconds, rows, rows_per_sec, status, slow_query, sql_hash "
                     f"FROM v_pipeline_query_perf {where_sql} "
                     f"ORDER BY start_ts DESC NULLS LAST LIMIT 500",
                     tuple(params) if params else None)
    _show_df(df, download_name="queries.csv")


def _pg_diffs(pg_conn, conn):
    st.subheader("Data quality (per event/env/field)")
    df = _df_or_warn(pg_conn, conn, "SELECT * FROM v_pipeline_data_quality LIMIT 200")
    _show_df(df, caption="from v_pipeline_data_quality",
             download_name="data_quality.csv")

    st.subheader("Field-level diffs")
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        ev_df = _df_or_warn(pg_conn, conn,
                            "SELECT DISTINCT event FROM pipeline_changes ORDER BY event")
        events_list = ev_df["event"].tolist() if (ev_df is not None and not ev_df.empty) else []
        ev = st.selectbox("event", ["(all)"] + events_list, key="diff_ev")
    with c2:
        en = st.selectbox("env", ["(all)", "stage", "prod"], key="diff_env")
    with c3:
        st_ = st.selectbox("status", ["(all)", "diff", "applied",
                                       "missing_in_es", "missing_in_oracle"], key="diff_st")
    where = []; params: list = []
    if ev != "(all)": where.append("event = %s"); params.append(ev)
    if en != "(all)": where.append("env = %s"); params.append(en)
    if st_ != "(all)": where.append("status = %s"); params.append(st_)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    df = _df_or_warn(pg_conn, conn,
                     f"SELECT * FROM pipeline_changes {where_sql} "
                     f"ORDER BY id DESC LIMIT 1000",
                     tuple(params) if params else None)
    _show_df(df, download_name="diffs.csv")


def _pg_missing(pg_conn, conn):
    st.subheader("Missing records (Oracle has it, ES doesn't)")
    df = _df_or_warn(pg_conn, conn, """
        SELECT event, env,
               COUNT(*)                                            AS total,
               COUNT(*) FILTER (WHERE applied_ts IS NOT NULL)      AS applied,
               COUNT(*) FILTER (WHERE applied_ts IS NULL)          AS pending
        FROM pipeline_missing
        GROUP BY event, env
        ORDER BY pending DESC, event, env
    """)
    _show_df(df, caption="counts per event/env", download_name="missing_summary.csv")

    st.subheader("Browse missing rows")
    c1, c2 = st.columns([1, 1])
    with c1:
        ev = st.text_input("event filter (exact)", "", key="mis_ev")
    with c2:
        en = st.selectbox("env", ["(all)", "stage", "prod"], key="mis_env")
    where = []; params: list = []
    if ev.strip(): where.append("event = %s"); params.append(ev.strip())
    if en != "(all)": where.append("env = %s"); params.append(en)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    df = _df_or_warn(pg_conn, conn,
                     f"SELECT id, sync_ts, event, env, doc_id, applied_ts, payload "
                     f"FROM pipeline_missing {where_sql} "
                     f"ORDER BY id DESC LIMIT 500",
                     tuple(params) if params else None)
    _show_df(df, download_name="missing_rows.csv")


def _pg_apply_audit(pg_conn, conn):
    st.subheader("Apply batches (which CSVs already pushed to ES)")
    df = _df_or_warn(pg_conn, conn, """
        SELECT event, env, mode, source_file, applied_ts, run_id,
               docs_planned, es_updated, es_created, es_conflicts, es_failures
        FROM pipeline_apply_batches
        ORDER BY applied_ts DESC LIMIT 500
    """)
    _show_df(df, download_name="apply_batches.csv")

    st.subheader("Audit log (raw events)")
    df = _df_or_warn(pg_conn, conn, """
        SELECT id, sync_ts, event, env, record_type, doc_id, batch, line_no, raw
        FROM pipeline_apply_audit
        ORDER BY id DESC LIMIT 500
    """)
    _show_df(df, download_name="apply_audit.csv")


def _pg_raw_tables(pg_conn, conn):
    """Original raw table browser — listing all tables with row counts + delete."""
    try:
        tables_df = pg_conn.list_tables(conn)
    except Exception as e:
        st.error(f"list_tables failed: {e}")
        return
    if tables_df.empty:
        st.info("No tables yet — run the pipeline first.")
        return

    tables = tables_df["table_name"].tolist()
    OUTPUT_ORDER = ["pipeline_changes", "pipeline_missing",
                    "pipeline_apply_audit", "pipeline_apply_batches",
                    "pipeline_summary"]
    LOG_ORDER = ["pipeline_log_connection", "pipeline_log_event",
                 "pipeline_log_query", "pipeline_log_offsets"]

    def _sort_key(name: str) -> tuple:
        if name in OUTPUT_ORDER:
            return (0, OUTPUT_ORDER.index(name))
        if name in LOG_ORDER:
            return (1, LOG_ORDER.index(name))
        return (2, name)
    tables.sort(key=_sort_key)

    @st.cache_data(ttl=15, show_spinner=False)
    def _row_counts(tbls: tuple[str, ...]) -> dict[str, int | None]:
        out: dict[str, int | None] = {}
        for t in tbls:
            try:
                df = pg_conn.run_query(conn, f"SELECT COUNT(*) AS n FROM {t}")
                out[t] = int(df.iloc[0, 0])
            except Exception:
                out[t] = None
        return out
    counts = _row_counts(tuple(tables))

    def _fmt(t: str) -> str:
        n = counts.get(t)
        return f"{t}  ({n:,} rows)" if isinstance(n, int) else f"{t}  (count: ?)"

    sel = st.selectbox("Table", tables, index=0, format_func=_fmt)

    confirm_key = f"_pg_truncate_confirm_{sel}"
    dc1, dc2, dc3 = st.columns([1, 1, 4])
    with dc1:
        if st.button("🗑 Delete all rows", key=f"_pg_truncate_btn_{sel}"):
            st.session_state[confirm_key] = True
    if st.session_state.get(confirm_key):
        with dc2:
            if st.button(f"Confirm TRUNCATE `{sel}`",
                         key=f"_pg_truncate_yes_{sel}", type="primary"):
                try:
                    pg_conn.execute(conn, f"TRUNCATE TABLE {sel} RESTART IDENTITY")
                    st.success(f"truncated `{sel}` (schema kept)")
                    st.session_state[confirm_key] = False
                    _row_counts.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"truncate failed: {type(e).__name__}: {e}")
        with dc3:
            if st.button("Cancel", key=f"_pg_truncate_no_{sel}"):
                st.session_state[confirm_key] = False
                st.rerun()
        st.warning(f"This will permanently delete **all rows** in `{sel}`. "
                   f"Schema stays. Identity (id) restarts at 1.")

    # Probe columns + types now (used by Delete-keep-these and Delete-WHERE).
    try:
        cols_meta_df = pg_conn.run_query(conn, """
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            ORDER BY ordinal_position
        """, (sel,))
        col_types: dict[str, str] = dict(
            zip(cols_meta_df["column_name"], cols_meta_df["data_type"])
        )
    except Exception as e:
        st.warning(f"could not read columns: {e}")
        col_types = {}
    cols = list(col_types.keys())

    _NUMERIC_TYPES = {"smallint", "integer", "bigint", "numeric",
                      "real", "double precision"}

    def _is_string_col(t: str) -> bool:
        return t not in _NUMERIC_TYPES and not t.startswith("timestamp") \
               and not t.startswith("date") and t != "boolean"

    # ---- Delete all EXCEPT these IDs (keep-list) ----
    with st.expander(f"⚠️ Delete all EXCEPT (keep specific rows in `{sel}`)"):
        st.caption("Pick a column, paste the value(s) to **keep**. "
                   "Everything else in the table gets deleted.")
        if not cols:
            st.warning("no columns probed — cannot show this control.")
        else:
            default_col = "doc_id" if "doc_id" in cols else \
                          ("id" if "id" in cols else cols[0])
            kc1, kc2 = st.columns([1, 3])
            with kc1:
                keep_col = st.selectbox("Match column", cols,
                                        index=cols.index(default_col),
                                        key=f"_pg_keepcol_{sel}")
            with kc2:
                keep_vals_raw = st.text_input(
                    f"Value(s) to keep (comma-separated, no quotes needed)",
                    value="", key=f"_pg_keepvals_{sel}",
                    help="e.g. `2493840_3` or `5, 7, 12`. "
                         "String values auto-quoted based on column type.",
                )

            keep_vals = [v.strip() for v in keep_vals_raw.split(",") if v.strip()]
            ctype = col_types.get(keep_col, "text")
            quote = _is_string_col(ctype)

            def _quote(v: str) -> str:
                return "'" + v.replace("'", "''") + "'" if quote else v

            if keep_vals:
                in_list = ", ".join(_quote(v) for v in keep_vals)
                cond = f"{keep_col} NOT IN ({in_list})"
                st.code(f"DELETE FROM {sel} WHERE {cond}", language="sql")

                kp_a, kp_b, kp_c = st.columns([1, 1, 4])
                with kp_a:
                    if st.button("Preview count", key=f"_pg_keepprev_{sel}"):
                        try:
                            df_n = pg_conn.run_query(
                                conn, f"SELECT COUNT(*) AS n FROM {sel} WHERE {cond}")
                            st.session_state[f"_pg_keepcnt_{sel}"] = int(df_n.iloc[0, 0])
                            st.session_state[f"_pg_keepcond_{sel}"] = cond
                        except Exception as e:
                            st.error(f"preview failed: {type(e).__name__}: {e}")
                            st.session_state.pop(f"_pg_keepcnt_{sel}", None)

                prev_n = st.session_state.get(f"_pg_keepcnt_{sel}")
                prev_cond = st.session_state.get(f"_pg_keepcond_{sel}")
                if isinstance(prev_n, int) and prev_cond == cond:
                    with kp_b:
                        st.metric("rows that will be deleted", f"{prev_n:,}")
                    with kp_c:
                        if prev_n == 0:
                            st.info("Nothing to delete — kept value(s) cover everything.")
                        else:
                            confirm_text = st.text_input(
                                f"Type **DELETE {prev_n}** to confirm",
                                value="", key=f"_pg_keepconfirm_{sel}",
                            )
                            if confirm_text == f"DELETE {prev_n}":
                                if st.button("🗑 Execute DELETE",
                                             type="primary",
                                             key=f"_pg_keepgo_{sel}"):
                                    try:
                                        deleted = pg_conn.execute(
                                            conn, f"DELETE FROM {sel} WHERE {cond}")
                                        st.success(f"deleted {deleted} row(s) "
                                                   f"from `{sel}` (kept "
                                                   f"{len(keep_vals)} row(s))")
                                        st.session_state.pop(
                                            f"_pg_keepcnt_{sel}", None)
                                        st.session_state.pop(
                                            f"_pg_keepcond_{sel}", None)
                                        _row_counts.clear()
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"delete failed: "
                                                 f"{type(e).__name__}: {e}")

    # ---- Delete WHERE (partial delete with custom condition) ----
    with st.expander(f"⚠️ Delete WHERE (partial delete on `{sel}`)"):
        st.caption("Examples:  `id <> 3433423`  ·  `event = 'PLAYERBONUS'`  ·  "
                   "`applied_ts IS NULL`  ·  `sync_ts < now() - interval '7 days'`")
        del_where = st.text_input("WHERE condition (required, no semicolons)",
                                  value="", key=f"_pg_delwhere_{sel}")
        BAD_TOKENS = (";", "--", "/*", "drop ", "alter ", "truncate ",
                      "insert ", "update ", "create ", "grant ", "revoke ")
        cond = del_where.strip()
        invalid_reason: str | None = None
        if not cond:
            invalid_reason = "empty WHERE — refusing (use TRUNCATE button above for full wipe)"
        else:
            low = cond.lower()
            for tok in BAD_TOKENS:
                if tok in low:
                    invalid_reason = f"contains forbidden token `{tok.strip()}`"
                    break

        col_a, col_b, col_c = st.columns([1, 1, 4])
        with col_a:
            if st.button("Preview count", key=f"_pg_delprev_{sel}",
                         disabled=invalid_reason is not None):
                try:
                    df_n = pg_conn.run_query(
                        conn, f"SELECT COUNT(*) AS n FROM {sel} WHERE {cond}")
                    n = int(df_n.iloc[0, 0])
                    st.session_state[f"_pg_delcnt_{sel}"] = n
                    st.session_state[f"_pg_delcond_{sel}"] = cond
                except Exception as e:
                    st.error(f"preview failed: {type(e).__name__}: {e}")
                    st.session_state.pop(f"_pg_delcnt_{sel}", None)

        if invalid_reason and cond:
            st.error(f"Refused: {invalid_reason}")

        prev_n = st.session_state.get(f"_pg_delcnt_{sel}")
        prev_cond = st.session_state.get(f"_pg_delcond_{sel}")
        if isinstance(prev_n, int) and prev_cond == cond and not invalid_reason:
            with col_b:
                st.metric("rows matched", f"{prev_n:,}")
            with col_c:
                if prev_n == 0:
                    st.info("Nothing to delete — no rows match.")
                else:
                    confirm_text = st.text_input(
                        f"Type **DELETE {prev_n}** to confirm",
                        value="", key=f"_pg_delconfirm_{sel}",
                    )
                    if confirm_text == f"DELETE {prev_n}":
                        if st.button("🗑 Execute DELETE", type="primary",
                                     key=f"_pg_delgo_{sel}"):
                            try:
                                deleted = pg_conn.execute(
                                    conn, f"DELETE FROM {sel} WHERE {cond}")
                                st.success(f"deleted {deleted} row(s) from `{sel}`")
                                st.session_state.pop(f"_pg_delcnt_{sel}", None)
                                st.session_state.pop(f"_pg_delcond_{sel}", None)
                                _row_counts.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"delete failed: {type(e).__name__}: {e}")

    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    with c1:
        limit = st.number_input("Limit", min_value=1, max_value=100000, value=1000, step=100)
    with c2:
        offset_v = st.number_input("Offset", min_value=0, value=0, step=100)
    with c3:
        newest_first = st.checkbox("Newest first", value=True,
                                   help="ORDER BY id DESC if the table has an `id` column.")
    with c4:
        where_clause = st.text_input("WHERE (optional)", value="",
                                     help="Raw SQL fragment, e.g. `event = 'PLAYERBONUS'`. Read-only.")

    has_id = "id" in cols
    order_clause = "ORDER BY id DESC" if (newest_first and has_id) else ""

    where_sql = ""
    if where_clause.strip():
        if any(tok in where_clause.lower()
               for tok in (";", "drop ", "delete ", "update ", "insert ", "alter ", "truncate ")):
            st.error("WHERE rejected: contains a write/DDL keyword.")
            return
        where_sql = f"WHERE {where_clause}"

    try:
        cnt_df = pg_conn.run_query(conn, f"SELECT COUNT(*) AS n FROM {sel} {where_sql}")
        total = int(cnt_df.iloc[0, 0])
        st.caption(f"`{sel}` total rows{' (filtered)' if where_sql else ''}: "
                   f"**{total:,}**  ·  cols: {len(cols)}")
    except Exception as e:
        st.warning(f"count failed: {e}")
        total = None

    sql = (f"SELECT * FROM {sel} {where_sql} {order_clause} "
           f"LIMIT {int(limit)} OFFSET {int(offset_v)}")
    try:
        df = pg_conn.run_query(conn, sql)
    except Exception as e:
        st.error(f"query failed: {e}")
        st.code(sql, language="sql")
        return

    _show_df(df, caption=f"showing {len(df)} rows  ·  `{sql}`",
             download_name=f"{sel}.csv")
    if total is not None and offset_v + len(df) < total:
        st.caption("more rows available — increase Limit or use Offset to page through.")


_PG_RENDERERS = {
    "Pipeline Health": _pg_health,
    "Connection Logs": _pg_connection_logs,
    "Query Performance": _pg_query_perf,
    "Oracle vs ES Differences": _pg_diffs,
    "Missing Records": _pg_missing,
    "Apply Audit": _pg_apply_audit,
    "Raw Tables": _pg_raw_tables,
}


def _index_to_event_map() -> dict[str, list[str]]:
    """Resolve `{INDEX_NAME -> [event_name, ...]}` from events.yaml + per-index configs."""
    from settings.loader import load_events
    events = load_events()
    out: dict[str, list[str]] = {}
    for ev_name, entry in events.items():
        idx = (entry.get("INDEX_NAME") or "").strip()
        if idx:
            out.setdefault(idx, []).append(ev_name)
    return out


def render_apply_page():
    """Run apply_changes for one (index, env) selection. No hardcoded indexes —
    dropdown comes from core.adapter_loader.known_indexes()."""
    st.title("Apply diffs to Elasticsearch")
    st.caption("Pushes pending diffs (Postgres `pipeline_changes` / `pipeline_missing`) "
               "back into ES via `apply_changes`. Subprocess-isolated; safe to cancel by closing the tab.")

    try:
        from core.adapter_loader import get_adapter, known_indexes
    except Exception as e:
        st.error(f"failed to import adapter loader: {type(e).__name__}: {e}")
        return

    idx_to_events = _index_to_event_map()
    indexes = [i for i in known_indexes() if i in idx_to_events]
    orphan = [i for i in known_indexes() if i not in idx_to_events]

    if not indexes:
        st.warning("No indexes have an events.yaml entry. Add one before running apply.")
        if orphan:
            st.caption(f"Indexes with adapter but no event: {', '.join(orphan)}")
        return

    c1, c2 = st.columns([2, 1])
    with c1:
        index = st.selectbox("Index", indexes, key="apply_index",
                             help="Index to apply diffs for. List comes from settings/indexes/.")
    with c2:
        env_choice = st.radio("Environment", ["stage", "prod"], horizontal=True,
                              index=0, key="apply_env_radio",
                              help="prod requires ES_USER + ES_PASS + ES_URL_PRODE in .env.")

    events_for_index = idx_to_events.get(index, [])
    if len(events_for_index) > 1:
        event = st.selectbox("Event", events_for_index, key="apply_event_pick",
                             help=f"Multiple events map to index `{index}`. Pick one.")
    else:
        event = events_for_index[0]
        st.caption(f"event = `{event}`")

    try:
        adapter = get_adapter(index)
        st.caption(f"adapter = `{type(adapter).__module__}.{type(adapter).__name__}`")
    except Exception as e:
        st.error(f"failed to load adapter for {index!r}: {type(e).__name__}: {e}")
        return

    mc1, mc2, mc3 = st.columns(3)
    with mc1:
        mode = st.radio("Mode", ["both", "changes", "missing"], horizontal=True,
                        index=0, key="apply_mode_radio")
    with mc2:
        dry = st.checkbox("Dry run", value=True, key="apply_dry",
                          help="Print what would happen, no writes.")
    with mc3:
        no_refresh = st.checkbox("Skip schema refresh", value=False, key="apply_no_refresh",
                                 help="Skip pulling fresh ES mapping from prod before validation.")

    if env_choice == "prod" and not dry:
        st.warning("Real write to **prod**. Double-check the index, mode, and event before running.")

    run_clicked = st.button(
        f"Apply ({mode}) → {env_choice}",
        type="primary",
        use_container_width=True,
        key="apply_run_btn",
    )

    log_box = st.empty()
    last = st.session_state.get("apply_page_last")
    if run_clicked:
        log_box.code(f"starting apply_changes index={index} event={event} env={env_choice} "
                     f"mode={mode} dry={dry} no_refresh={no_refresh}...", language="text")
        cmd = [sys.executable, "-u", "-m", "apply_changes.apply_changes",
               "--event", event, "--mode", mode, "--env", env_choice]
        if dry:
            cmd.append("--dry")
        if no_refresh:
            cmd.append("--no-refresh")
        rc, full = _run_stream(cmd, log_box)
        st.session_state["apply_page_last"] = {
            "rc": rc, "out": full,
            "index": index, "event": event, "env": env_choice,
            "mode": mode, "dry": dry,
        }
        last = st.session_state["apply_page_last"]

    if last:
        tag = (f"index={last['index']} event={last['event']} env={last['env']} "
               f"mode={last['mode']} dry={last['dry']}")
        if last["rc"] == 0:
            st.success(f"apply_changes exit=0 ({tag})")
        else:
            st.error(f"apply_changes exit={last['rc']} ({tag})")
        if not run_clicked:
            tail = "\n".join((last["out"] or "").splitlines()[-_LOG_TAIL:]) or "(empty)"
            log_box.code(tail, language="text")
        with st.expander("Full log"):
            st.code(last["out"] or "(empty)", language="text")


def render_postgres_page():
    """Postgres dashboards (read-only). 7 sub-pages selectable from sidebar."""
    st.title("Postgres viewer")

    host = os.getenv("PG_HOST"); port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB"); schema = os.getenv("PG_SCHEMA", "public")
    if not (host and db):
        st.error("PG_HOST / PG_DB not set in .env")
        return
    st.caption(f"`{host}:{port}/{db}` schema=`{schema}`")

    try:
        from connect_into_postgres import connect_to_postgres as pg_conn
    except Exception as e:
        st.error(f"failed to import postgres connector: {type(e).__name__}: {e}")
        return

    rb1, rb2 = st.columns([1, 4])
    with rb1:
        if st.button("↻ Refresh data from Postgres",
                     help="Re-read from PG. Read-only — does not run pipeline or sync."):
            st.cache_resource.clear()
            st.cache_data.clear()
            st.rerun()
    with rb2:
        st.caption("Read-only viewer. Pipeline (`main.py` / `apply_changes`) writes to Postgres.")

    @st.cache_resource(show_spinner="connecting to postgres…")
    def _conn():
        return pg_conn.create_connection()
    try:
        conn = _conn()
    except Exception as e:
        st.error(f"connect failed: {type(e).__name__}: {e}")
        return

    sub = st.sidebar.radio("Postgres section", PG_SUBPAGES, index=0, key="pg_subpage")
    st.markdown(f"### {sub}")
    _PG_RENDERERS[sub](pg_conn, conn)


PAGE = st.sidebar.radio("Page", ["Pipeline", "Apply", "Postgres"], index=0, key="page_selector")
if PAGE == "Apply":
    render_apply_page()
    st.stop()
if PAGE == "Postgres":
    render_postgres_page()
    st.stop()

st.title("events.yaml editor")
st.caption(str(YAML_PATH))


def load_yaml() -> dict:
    with open(YAML_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def dump_yaml(data: dict) -> None:
    with open(YAML_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True, default_flow_style=False)


def resolve_entry(entry: dict) -> dict:
    """Read-only merge of indexes/<X>/config.yaml referenced by `index_config`.
    Event-level keys override per-index defaults (parity with settings.loader)."""
    ref = entry.get("index_config")
    if not ref:
        return dict(entry)
    cfg_path = SETTINGS_DIR / ref
    if not cfg_path.is_file():
        return dict(entry)
    base = cfg_path.parent
    icfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    parts = icfg.pop("parts", None)
    if parts:
        rewritten = []
        for p in parts:
            p = dict(p)
            for k in ("sql_file", "VALUE_COLM", "LOOKUP_SQL"):
                rel = p.get(k)
                if rel:
                    try:
                        p[k] = (base / rel).resolve().relative_to(SETTINGS_DIR.resolve()).as_posix()
                    except ValueError:
                        pass
            rewritten.append(p)
        icfg["parts"] = rewritten
    else:
        for k in ("sql_file", "VALUE_COLM", "LOOKUP_SQL"):
            rel = icfg.get(k)
            if rel:
                try:
                    icfg[k] = (base / rel).resolve().relative_to(SETTINGS_DIR.resolve()).as_posix()
                except ValueError:
                    pass
    out = {**icfg, **{k: v for k, v in entry.items() if k != "index_config"}}
    return out


def load_csv_fields(rel: str) -> list[str]:
    path = SETTINGS_DIR / rel
    if not path.is_file():
        return []
    with open(path, encoding="utf-8", newline="") as f:
        return [(r.get("filed_es") or "").strip() for r in csv.DictReader(f)
                if (r.get("filed_es") or "").strip()]


def parse_selected(value) -> set[str]:
    if not value:
        return set()
    return {p.strip() for p in str(value).split(",") if p.strip()}


def to_datetime(v) -> datetime | None:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime.combine(v, time(0, 0))
    s = str(v).strip()
    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


cfg = load_yaml()
if not cfg:
    st.warning("YAML is empty.")
    st.stop()

DIFF_MODE_OPTIONS = ["changes", "missing", "both"]
_TOP_LEVEL_KEYS = {"PIPELINE_DIFF_MODE"}

cur_diff = DIFF_MODE_ALIASES.get(
    str(cfg.get("PIPELINE_DIFF_MODE", "both")).strip().lower(), "both"
)
st.sidebar.markdown("**Pipeline diff mode**")
new_diff = st.sidebar.radio(
    "PIPELINE_DIFF_MODE",
    DIFF_MODE_OPTIONS,
    index=DIFF_MODE_OPTIONS.index(cur_diff),
    key="diff_mode",
    help="changes = field diffs only; missing = rows missing in ES only; both = save both.",
)
if new_diff != cur_diff:
    cfg["PIPELINE_DIFF_MODE"] = new_diff
    dump_yaml(cfg)
    st.sidebar.success(f"Saved PIPELINE_DIFF_MODE={new_diff}")
    st.rerun()

events = [k for k in cfg.keys() if k not in _TOP_LEVEL_KEYS]
sel = st.sidebar.radio("Event", events, index=0)
if st.sidebar.button("Reload from disk"):
    st.rerun()

entry = resolve_entry(cfg[sel])
st.subheader(sel)

is_running = st.toggle("IS_RUNNING", value=bool(entry.get("IS_RUNNING", False)),
                       key=f"run_{sel}")

ES_ENV_OPTIONS = ["stage", "prod"]
cur_env = normalize_es_env(entry.get("ES_ENV", "stage"))
es_env = st.radio("ES_ENV", ES_ENV_OPTIONS, index=ES_ENV_OPTIONS.index(cur_env),
                  horizontal=True, key=f"es_env_{sel}")

has_limit = "LIMIT_BATCHES" in entry
new_limit = None
if has_limit:
    new_limit = st.number_input(
        "LIMIT_BATCHES",
        value=int(entry.get("LIMIT_BATCHES") or 0),
        min_value=0, step=1,
        key=f"lim_{sel}",
        help="Max number of batches to run (0 = no limit).",
    )

cur_banch = entry.get("BANCH_VALUE")
new_banch = None
if cur_banch is not None:
    new_banch = st.text_input(
        "BANCH_VALUE",
        value=str(cur_banch),
        key=f"banch_{sel}",
        help="Window size: 'HOUR'/'DAY'/'WEEK', '30 DAYS', '3 HOURS', '15m', or int (id_range chunk size).",
    )

has_times = "START_TIME" in entry or "END_TIME" in entry
new_start = new_end = None
if has_times:
    st.markdown("**Time window**")
    tc1, tc2, tc3, tc4 = st.columns(4)
    s_dt = to_datetime(entry.get("START_TIME")) or datetime(2026, 1, 1)
    e_dt = to_datetime(entry.get("END_TIME"))   or datetime(2026, 1, 2)
    with tc1:
        s_d = st.date_input("START date", value=s_dt.date(), key=f"sd_{sel}")
    with tc2:
        s_t = st.time_input("START time", value=s_dt.time(), key=f"st_{sel}", step=60)
    with tc3:
        e_d = st.date_input("END date",   value=e_dt.date(), key=f"ed_{sel}")
    with tc4:
        e_t = st.time_input("END time",   value=e_dt.time(), key=f"et_{sel}", step=60)
    new_start = datetime.combine(s_d, s_t)
    new_end   = datetime.combine(e_d, e_t)
    if new_end <= new_start:
        st.error("END_TIME must be after START_TIME")

st.markdown("**FILED_THAT_RUN** — toggle fields to include")
parts = entry.get("parts") or []
if parts:
    csv_path_rel = ", ".join(p.get("VALUE_COLM", "") for p in parts if p.get("VALUE_COLM"))
    seen, csv_fields = set(), []
    for p in parts:
        for f in load_csv_fields(p.get("VALUE_COLM", "")):
            if f not in seen:
                seen.add(f); csv_fields.append(f)
else:
    csv_path_rel = f"mappings/{sel}.csv"
    csv_fields = load_csv_fields(csv_path_rel)
current = parse_selected(entry.get("FILED_THAT_RUN", ""))

# include any FILED_THAT_RUN fields that aren't in the CSV (constants from mapping)
for f in current:
    if f not in csv_fields:
        csv_fields.append(f)

universe = list(csv_fields)

state_key = f"sel_{sel}"
if state_key not in st.session_state:
    st.session_state[state_key] = set(current)

c1, c2, c3 = st.columns(3)
with c1:
    if st.button("All"):
        st.session_state[state_key] = set(universe)
        st.rerun()
with c2:
    if st.button("None"):
        st.session_state[state_key] = set()
        st.rerun()
with c3:
    if st.button("Reset to saved"):
        st.session_state[state_key] = set(current)
        st.rerun()

selected = st.session_state[state_key]
cols = st.columns(4)
new_selected = set()
for i, fld in enumerate(universe):
    with cols[i % 4]:
        if st.checkbox(fld, value=(fld in selected), key=f"chk_{sel}_{fld}"):
            new_selected.add(fld)
st.session_state[state_key] = new_selected

if not csv_fields:
    st.info(f"`{csv_path_rel}` not found or has no fields.")
st.caption(f"Selected: {len(new_selected)} / {len(universe)} (source: `{csv_path_rel}`)")

st.markdown("---")
if st.button("Save to YAML", type="primary"):
    ordered = [f for f in universe if f in new_selected]
    cfg[sel]["IS_RUNNING"] = bool(is_running)
    cfg[sel]["ES_ENV"] = es_env
    cfg[sel]["FILED_THAT_RUN"] = ",".join(ordered)
    if has_times and new_start is not None and new_end is not None:
        cfg[sel]["START_TIME"] = new_start.strftime("%Y-%m-%d %H:%M:%S")
        cfg[sel]["END_TIME"]   = new_end.strftime("%Y-%m-%d %H:%M:%S")
    if has_limit:
        cfg[sel]["LIMIT_BATCHES"] = int(new_limit)
    if new_banch is not None and new_banch != "":
        v = new_banch.strip()
        try:
            cfg[sel]["BANCH_VALUE"] = int(v)
        except ValueError:
            cfg[sel]["BANCH_VALUE"] = v
    dump_yaml(cfg)
    st.success(f"Saved {sel}")
    st.rerun()

with st.expander("Raw YAML on disk"):
    st.code(YAML_PATH.read_text(encoding="utf-8"), language="yaml")

st.markdown("---")
st.subheader("Pipeline run")
env_file_loaded = load_env_file()
env_file_path = find_env_file()
if env_file_path is not None:
    st.caption(f"Loaded {len(env_file_loaded)} env vars from `{env_file_path}`: {', '.join(sorted(env_file_loaded))}")
else:
    debug_paths = [
        f"app.py ROOT = {ROOT}",
        f"cwd       = {os.getcwd()}",
        f"tried     = {ROOT / '.env'} exists={(ROOT / '.env').is_file()}",
    ]
    st.error("No `.env` found.\n\n" + "\n\n".join(debug_paths))
    st.caption("Subprocess uses parent env only.")
run_col, refresh_col, clear_col = st.columns([1, 1, 1])
with run_col:
    run_clicked = st.button("Run main.py", type="primary", use_container_width=True)
with refresh_col:
    if st.button("Refresh out/ list", use_container_width=True):
        st.rerun()
with clear_col:
    confirm = st.checkbox("Confirm wipe", key="confirm_clear")
    if st.button("Clear out/", disabled=not confirm, use_container_width=True):
        nf, nd = clear_out_dir()
        st.toast(f"Deleted {nf} files + {nd} folders", icon="✅")
        st.rerun()

st.markdown("**Progress**")
progress_box = st.progress(0.0, text="idle")
st.markdown(f"**Live output** (last {_LOG_TAIL} lines)")
log_box = st.empty()

if run_clicked:
    log_box.code("starting main.py...", language="text")
    progress_box.progress(0.0, text="starting...")
    rc, full = run_main_py_stream(log_box, progress_box)
    progress_box.progress(1.0, text=f"done (exit={rc})")
    st.session_state["last_run"] = {"rc": rc, "out": full}
    try:
        sp, sdf, _ = build_changes_summary()
        st.session_state["last_summary"] = {"path": str(sp) if sp else None,
                                            "rows": int(len(sdf)) if sdf is not None else 0}
    except Exception as e:
        import traceback
        st.session_state["last_summary_err"] = f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"

last = st.session_state.get("last_run")
if last:
    if last["rc"] == 0:
        st.success("main.py exit=0")
    else:
        st.error(f"main.py exit={last['rc']}")
    if not run_clicked:
        tail = "\n".join((last["out"] or "").splitlines()[-_LOG_TAIL:]) or "(empty)"
        log_box.code(tail, language="text")
    with st.expander("Full log"):
        st.code(last["out"] or "(empty)", language="text")

st.markdown("---")
st.subheader("Changes summary")
sum_col1, sum_col2 = st.columns([1, 3])
with sum_col1:
    if st.button("Generate summary now", use_container_width=True):
        try:
            sp, sdf, _ = build_changes_summary()
            st.session_state["last_summary"] = {"path": str(sp) if sp else None,
                                                "rows": int(len(sdf)) if sdf is not None else 0}
        except Exception as e:
            st.session_state["last_summary_err"] = str(e)
        st.rerun()
with sum_col2:
    ls = st.session_state.get("last_summary")
    if ls and ls.get("path"):
        st.caption(f"Latest: `{Path(ls['path']).name}` — {ls['rows']} (event,field) rows")
    elif st.session_state.get("last_summary_err"):
        st.error("summary error")
        st.code(st.session_state['last_summary_err'], language="text")
    else:
        st.caption("No summary yet.")

ls = st.session_state.get("last_summary")
if ls and ls.get("path") and Path(ls["path"]).is_file():
    sdf = pd.read_csv(ls["path"])
    if not sdf.empty:
        for col in ("total_issues", "diff", "row_missing_in_es", "row_missing_in_oracle",
                    "es_value_blank", "oracle_value_blank"):
            if col in sdf.columns:
                sdf[col] = pd.to_numeric(sdf[col], errors="coerce").fillna(0).astype(int)

        def _safe_sum(col: str) -> int:
            return int(sdf[col].sum()) if col in sdf.columns else 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total diffs", _safe_sum("diff"))
        m2.metric("ES rows missing", _safe_sum("row_missing_in_es"))
        m3.metric("Oracle rows missing", _safe_sum("row_missing_in_oracle"))
        m4.metric("Fields w/ issues", int(len(sdf)))

        m5, m6, m7 = st.columns(3)
        m5.metric("ES value blank (in diffs)", _safe_sum("es_value_blank"))
        m6.metric("Oracle value blank (in diffs)", _safe_sum("oracle_value_blank"))
        m7.metric("Events", int(sdf["event"].nunique()) if "event" in sdf.columns else 0)

        st.dataframe(sdf, use_container_width=True, hide_index=True)
        st.caption(f"Saved at `{ls['path']}`")
        with open(ls["path"], "rb") as fh:
            st.download_button("Download summary CSV", data=fh, file_name=Path(ls["path"]).name,
                               mime="text/csv")
    else:
        st.info("Summary CSV is empty (no changes files found).")

st.markdown("---")
st.subheader("Apply to Elasticsearch")
st.caption("Push **pending** diffs and missing rows from Postgres "
           "(`pipeline_changes` / `pipeline_missing` WHERE `applied_ts IS NULL`) "
           "to ES. Stage data → stage cluster only; prod data → prod cluster only. "
           "Local CSVs only used as fallback if PG is unreachable.")

apply_events = list_events_with_changes()
if not apply_events:
    st.info("No event has pending diffs/missing in Postgres "
            "(`pipeline_changes` / `pipeline_missing` with `applied_ts IS NULL`). "
            "Run `main.py` first to generate diffs, or check that sync_out completed.")
else:
    ac1, ac2, ac3 = st.columns([2, 2, 1])
    with ac1:
        apply_event = st.selectbox("Event", apply_events, key="apply_event")
    available_envs = envs_with_changes(apply_event)  # only envs that have data
    with ac2:
        if not available_envs:
            apply_env = None
            st.error(f"`{apply_event}` has no env folder with data.")
        else:
            yaml_env = normalize_es_env((cfg.get(apply_event) or {}).get("ES_ENV", "stage"))
            default_env = yaml_env if yaml_env in available_envs else available_envs[0]
            apply_env = st.radio("ES env (source folder + target cluster)",
                                 available_envs,
                                 index=available_envs.index(default_env),
                                 horizontal=True, key="apply_env",
                                 help="Only envs with data in `out/<EVENT>/<env>/changes/` shown.")
    with ac3:
        apply_dry = st.checkbox("Dry run", value=True, key="apply_dry",
                                help="Preview only — no writes to ES.")

    if apply_env is None:
        _n_changes = _n_missing = 0
    else:
        inv = pg_pending_inventory().get(apply_event, {}).get(apply_env, {})
        _n_changes = int(inv.get("changes", 0))
        _n_missing = int(inv.get("missing", 0))
        if _n_changes == 0 and _n_missing == 0:
            # PG empty → fallback to CSV file count (in case PG was unreachable)
            _ev_dir = _changes_dir(apply_event, apply_env)
            _n_changes = len(list(_ev_dir.glob("changes_*.csv"))) if _ev_dir.is_dir() else 0
            _n_missing = len(list(_ev_dir.glob("missing_in_es_*.csv"))) if _ev_dir.is_dir() else 0
            st.caption(f"`{apply_event}/{apply_env}`: PG has 0 pending — falling back to "
                       f"local CSVs ({_n_changes} changes, {_n_missing} missing).")
        else:
            st.caption(f"`{apply_event}/{apply_env}`: **{_n_changes:,}** pending diff row(s), "
                       f"**{_n_missing:,}** pending missing row(s)  ·  source=Postgres  "
                       f"·  target cluster=`{apply_env}`")

    if apply_env == "prod" and not apply_dry:
        st.warning("Target = **prod** with **dry-run OFF**. Writes will hit production ES.")

    _no_env = apply_env is None
    bcol1, bcol2, bcol3 = st.columns(3)
    with bcol1:
        apply_changes_clicked = st.button("Apply field diffs (changes_*)",
                                          disabled=(_no_env or _n_changes == 0),
                                          use_container_width=True, key="btn_apply_changes")
    with bcol2:
        apply_missing_clicked = st.button("Apply missing rows (missing_in_es_*)",
                                          disabled=(_no_env or _n_missing == 0),
                                          use_container_width=True, key="btn_apply_missing")
    with bcol3:
        apply_both_clicked = st.button("Apply BOTH",
                                       disabled=(_no_env or (_n_changes == 0 and _n_missing == 0)),
                                       use_container_width=True, key="btn_apply_both")

    apply_log = st.empty()

    _mode = None
    if apply_changes_clicked:
        _mode = "changes"
    elif apply_missing_clicked:
        _mode = "missing"
    elif apply_both_clicked:
        _mode = "both"

    if _mode is not None:
        apply_log.code(f"starting apply_changes mode={_mode} event={apply_event} env={apply_env} dry={apply_dry}...",
                       language="text")
        rc, full = run_apply_stream(apply_event, _mode, apply_env, apply_dry, apply_log)
        st.session_state["last_apply"] = {"rc": rc, "out": full,
                                          "event": apply_event, "env": apply_env,
                                          "mode": _mode, "dry": apply_dry}

    last_apply = st.session_state.get("last_apply")
    if last_apply and _mode is None:
        tag = f"event={last_apply['event']} env={last_apply['env']} mode={last_apply['mode']} dry={last_apply['dry']}"
        if last_apply["rc"] == 0:
            st.success(f"apply_changes exit=0 ({tag})")
        else:
            st.error(f"apply_changes exit={last_apply['rc']} ({tag})")
        apply_log.code(last_apply["out"] or "(empty)", language="text")

st.markdown("---")
_event_out_dir = OUT_DIR / sel / es_env / "changes"
st.markdown(f"**Files for this event** (`out/{sel}/{es_env}/changes/`)")
files = list_event_out_files(sel, es_env)
if not files:
    st.info("No output file found for this run.")
    st.stop()
else:
    rows = []
    for f in files:
        rows.append({
            "path": f.name,
            "size": f.stat().st_size,
            "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })
    st.dataframe(rows, use_container_width=True, hide_index=True)
    st.caption(f"{len(files)} file(s) in {_event_out_dir}")

    show_files = st.checkbox("Show per-file contents (raw rows)", value=False,
                             help="Off = summary only. On = expand each changes_*.csv below.")
    if not show_files:
        st.stop()
    show_all = st.checkbox("Auto-expand every file", value=False)
    max_bytes = st.number_input("Max bytes per file", min_value=1024, max_value=5_000_000,
                                value=200_000, step=10_000)
    for f in files:
        rel = f.name
        size = f.stat().st_size
        with st.expander(f"{rel}  —  {size} bytes", expanded=show_all):
            if size == 0:
                st.text("(empty file)")
                continue
            try:
                if f.suffix.lower() == ".csv":
                    df = pd.read_csv(f, nrows=5000)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.caption(f"showing {len(df)} rows")
                else:
                    raw = f.read_text(encoding="utf-8", errors="replace")
                    truncated = len(raw.encode("utf-8")) > max_bytes
                    if truncated:
                        raw = raw[:max_bytes] + f"\n\n... [truncated at {max_bytes} bytes]"
                    st.code(raw, language="text")
            except Exception as e:
                st.error(f"read error: {e}")
