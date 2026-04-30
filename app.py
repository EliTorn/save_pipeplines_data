import csv
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, date, time
from pathlib import Path
from time import monotonic as _now_mono

import pandas as pd
import streamlit as st
import yaml
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")
sys.path.insert(0, str(Path(__file__).parent))

from _pipeline_env import DIFF_MODE_ALIASES, TS_FORMATS, normalize_es_env  # noqa: E402
from core import duckdb_catalog  # noqa: E402
from connect_into_postgres import run_summary as _run_summary  # noqa: E402
from connect_into_postgres import observability as _observability  # noqa: E402
from apply_changes import pg_tracking as _pg_tracking  # noqa: E402

# Refresh DuckDB catalog so views reflect the current set of out/*.parquet
# files at app start. Cheap; tolerates no files / no duckdb.
try:
    duckdb_catalog.init_catalog()
except Exception as _e:
    print(f"[app] duckdb_catalog.init_catalog skipped: {type(_e).__name__}: {_e}")

# Ensure active PG tables exist. Best-effort; silent no-op when PG unreachable.
for _label, _fn in (
    ("run_summary",   _run_summary.init_schema),
    ("observability", _observability.init_schema),
    ("pg_tracking",   _pg_tracking.init_schema),
):
    try:
        _fn()
    except Exception as _e:
        print(f"[app] {_label}.init_schema skipped: {type(_e).__name__}: {_e}")

_PROGRESS_RE = re.compile(r"^\[PROGRESS\]\s+event=(\S+)\s+batch=(\d+)/(\d+)")
_LOG_TAIL = 30

# Whitelist of stdout patterns to surface in the UI as curated events.
# Anything not matched is silently discarded (pipe still drained).
_FILTERS: list[tuple[re.Pattern, object]] = [
    (re.compile(r"^Run\s+(\S+)\s*\|"),
     lambda m: f"Connected (run {m.group(1)})"),
    (re.compile(r"^\[(\w+)\]\s+env=(\w+)\s+id_range\s+min=(\S+)\s+max=(\S+)\s+step=\S+\s+limit=\S+\s+index=(\S+).*?batches=(\d+)\s+workers=(\d+)"),
     lambda m: f"{m.group(1)} starting on {m.group(2)} (index={m.group(5)}, id_range, {m.group(6)} batches × {m.group(7)} workers)"),
    (re.compile(r"^\[(\w+)\]\s+env=(\w+).*?index=(\S+).*?batches=(\d+)\s+workers=(\d+)"),
     lambda m: f"{m.group(1)} starting on {m.group(2)} (index={m.group(3)}, {m.group(4)} batches × {m.group(5)} workers)"),
    (re.compile(r"^\[(\w+)\]\s+skipped:\s+(.+)$"),
     lambda m: f"{m.group(1)} skipped ({m.group(2)})"),
    (re.compile(r"^\[(\w+)\]\s+env=\w+\s+done:\s+changes_files=(\d+)\s+missing_files=(\d+)"),
     lambda m: f"{m.group(1)} done: {m.group(2)} diff file(s), {m.group(3)} missing file(s)"),
    (re.compile(r"^\[(\w+)\]\s+env=\w+\s+done:\s+no changes"),
     lambda m: f"{m.group(1)} done: no changes"),
    (re.compile(r"^event=(\S+)\s+index=(\S+)\s+pk=\S+\s+env=(\w+)\s+url=\S+\s+dry=(\w+)"),
     lambda m: f"Apply {m.group(1)} on {m.group(3)} (index={m.group(2)}, dry={m.group(4)})"),
    (re.compile(r"^\[apply\]\s+reading\s+(\d+)\s+pending\s+diff"),
     lambda m: f"Found {m.group(1)} changed row(s) to apply"),
    (re.compile(r"^\[apply\]\s+reading\s+(\d+)\s+pending\s+missing"),
     lambda m: f"Found {m.group(1)} missing row(s) to insert"),
    (re.compile(r"^docs to update:\s+(\d+)"),
     lambda m: f"Updating {m.group(1)} doc(s) in Elasticsearch"),
    (re.compile(r"^docs to insert:\s+(\d+)"),
     lambda m: f"Inserting {m.group(1)} doc(s) into Elasticsearch"),
    (re.compile(r"^\[DRY\]\s+(\d+)\s+docs would be updated"),
     lambda m: f"Dry run: {m.group(1)} doc(s) would update"),
    (re.compile(r"^\[DRY\]\s+(\d+)\s+docs would be created"),
     lambda m: f"Dry run: {m.group(1)} doc(s) would create"),
    (re.compile(r"^\s*changes batch\s+(\d+)/(\d+):\s+updated=(\d+)\s+conflicts=(\d+)\s+failures=(\d+)"),
     lambda m: f"apply batch {m.group(1)}/{m.group(2)}: updated={m.group(3)} conflicts={m.group(4)} failures={m.group(5)}"),
    (re.compile(r"^\s*missing batch\s+(\d+)/(\d+):\s+created=(\d+)\s+skipped\(exists\)=(\d+)\s+errors=(\d+)"),
     lambda m: f"insert batch {m.group(1)}/{m.group(2)}: created={m.group(3)} exists={m.group(4)} errors={m.group(5)}"),
    (re.compile(r"^changes done:\s+updated=(\d+)\s+conflicts=(\d+)\s+failures=(\d+)"),
     lambda m: f"Apply complete: {m.group(1)} updated, {m.group(2)} conflicts, {m.group(3)} failures"),
    (re.compile(r"^missing done:\s+created=(\d+)\s+skipped\(exists\)=(\d+)\s+errors=(\d+)"),
     lambda m: f"Insert complete: {m.group(1)} created, {m.group(2)} already existed, {m.group(3)} errors"),
    (re.compile(r"^\[pg\]\s+(.+)$"),
     lambda m: f"Postgres: {m.group(1)}"),
    (re.compile(r"^\[pg-sync\]\s+mirroring"),
     lambda m: "Syncing to Postgres…"),
    (re.compile(r"^\[pg-sync\]\s+skipped:\s+(.+)$"),
     lambda m: f"Postgres sync skipped: {m.group(1)}"),
    (re.compile(r"^done\.\s*$"),
     lambda m: "Postgres sync complete"),
    (re.compile(r"^ABORT\s+—\s+(.+)$"),
     lambda m: f"ABORT: {m.group(1)}"),
    (re.compile(r"^WARN:\s+(.+)$"),
     lambda m: f"warn: {m.group(1)}"),
]


def _filter_line(text: str) -> str | None:
    for pat, fmt in _FILTERS:
        m = pat.match(text)
        if m:
            try:
                return fmt(m)
            except Exception:
                return None
    return None

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


def _render_events(placeholder, events: list[str]) -> None:
    if not events:
        placeholder.markdown("_waiting…_")
        return
    placeholder.markdown("\n".join(f"- {e}" for e in events[-_LOG_TAIL:]))


def _run_stream(cmd: list[str], placeholder, progress_bar=None) -> tuple[int, list[str]]:
    """Run cmd, drain stdout always, surface only whitelisted events to UI.

    Pipe is fully drained so the child never blocks on a backed-up stdout.
    Lines that don't match `_FILTERS` are discarded. UI updates are throttled
    to ~200ms so Streamlit doesn't repaint per-line."""
    env = {**os.environ, **load_env_file()}
    proc = subprocess.Popen(
        cmd, cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace",
        bufsize=1,
    )
    events: list[str] = []
    assert proc.stdout is not None
    last_render = 0.0
    for raw in proc.stdout:
        text = raw.rstrip("\n")
        if progress_bar is not None:
            pm = _PROGRESS_RE.match(text)
            if pm:
                ev, cur, tot = pm.group(1), int(pm.group(2)), int(pm.group(3))
                frac = min(cur / tot, 1.0) if tot > 0 else 0.0
                try:
                    progress_bar.progress(frac, text=f"{ev}: batch {cur}/{tot}")
                except Exception:
                    pass
                continue
        msg = _filter_line(text)
        if msg is None:
            continue
        events.append(msg)
        now = _now_mono()
        if now - last_render >= 0.2:
            _render_events(placeholder, events)
            last_render = now
    proc.wait()
    _render_events(placeholder, events)
    return proc.returncode, events


def run_main_py_stream(placeholder, progress_bar=None) -> tuple[int, list[str]]:
    return _run_stream([sys.executable, "-u", str(MAIN_PY)], placeholder, progress_bar)


def run_apply_stream(event: str, mode: str, env_choice: str, dry: bool, placeholder) -> tuple[int, list[str]]:
    cmd = [sys.executable, "-u", "-m", "apply_changes.apply_changes",
           "--event", event, "--mode", mode, "--env", env_choice]
    if dry:
        cmd.append("--dry")
    return _run_stream(cmd, placeholder)


def _changes_dir(event: str, env: str) -> Path:
    return OUT_DIR / event / env / "changes"


@st.cache_data(ttl=30, show_spinner=False)
def pg_pending_inventory() -> dict[str, dict[str, dict[str, int]]]:
    """{event: {env: {'changes': N, 'missing': M}}} of files on disk.

    Phase C+: heavy data lives in local Parquet, so this counts files via
    DuckDB views (v_changes / v_missing). PG legacy tables are queried only
    if DuckDB has nothing to show — useful when looking at historical
    backlog from before Phase C.

    The legacy PG fallback REUSES duckdb_source._cache (the long-lived
    CachedConnection) instead of opening a fresh conn every 30s — that
    avoids needless connection churn when PG is healthy.

    Returns empty dict if neither source has data.
    """
    out: dict[str, dict[str, dict[str, int]]] = {}

    # Primary: DuckDB views over local Parquet.
    from apply_changes import duckdb_source
    try:
        for env in ("stage", "prod"):
            counts = duckdb_source.pending_counts(env) or {}
            for ev, c in counts.items():
                out.setdefault(ev, {}).setdefault(env, {"changes": 0, "missing": 0})
                out[ev][env]["changes"] = int(c.get("changes", 0))
                out[ev][env]["missing"] = int(c.get("missing", 0))
    except Exception as e:
        print(f"[app] duckdb pending_counts failed: {type(e).__name__}: {e}")

    if any(any((d.get("changes", 0) + d.get("missing", 0)) > 0 for d in envs.values())
           for envs in out.values()):
        return out

    # Fallback: legacy PG tables. Reuse duckdb_source's cached connection so
    # we don't churn a fresh connect every cache miss.
    conn = duckdb_source._cache.get()
    if conn is None:
        return out
    try:
        with duckdb_source._cache.lock, conn.cursor() as cur:
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
        print(f"[app] pg_pending_inventory legacy fallback failed: {type(e).__name__}: {e}")
        try: conn.rollback()
        except Exception: pass
    return out


def envs_with_changes(event: str) -> list[str]:
    """Which envs (stage/prod) have pending diffs/missing for this event.
    Checks DuckDB inventory first, then a local file glob (CSV + Parquet)."""
    inv = pg_pending_inventory().get(event, {})
    out = [en for en in ("stage", "prod")
           if en in inv and (inv[en].get("changes", 0) + inv[en].get("missing", 0)) > 0]
    if out:
        return out
    # Local-disk fallback: any artifact file present (CSV or Parquet).
    on_disk: list[str] = []
    for env in ("stage", "prod"):
        ch = _changes_dir(event, env)
        if not ch.is_dir():
            continue
        if any(ch.glob("changes_*.csv")) or any(ch.glob("changes_*.parquet")) \
           or any(ch.glob("missing_in_es_*.csv")) or any(ch.glob("missing_in_es_*.parquet")):
            on_disk.append(env)
    return on_disk


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
        log_box.markdown(f"_starting apply_changes for `{event}` on `{env_choice}` (mode={mode}, dry={dry})…_")
        cmd = [sys.executable, "-u", "-m", "apply_changes.apply_changes",
               "--event", event, "--mode", mode, "--env", env_choice]
        if dry:
            cmd.append("--dry")
        if no_refresh:
            cmd.append("--no-refresh")
        rc, events = _run_stream(cmd, log_box)
        st.session_state["apply_page_last"] = {
            "rc": rc, "events": events,
            "index": index, "event": event, "env": env_choice,
            "mode": mode, "dry": dry,
        }
        last = st.session_state["apply_page_last"]

    if last:
        tag = (f"index={last['index']} event={last['event']} env={last['env']} "
               f"mode={last['mode']} dry={last['dry']}")
        if last["rc"] == 0:
            st.success(f"Done ({tag})")
        else:
            st.error(f"Failed exit={last['rc']} ({tag})")
        if not run_clicked:
            _render_events(log_box, last.get("events") or [])


_PG_LIMIT_OPTIONS = [100, 500, 1000]


def _pg_table_tab(pg_conn, conn, *, title: str, description: str, sql: str,
                  limit_key: str, download_name: str) -> None:
    """One tab: header + Limit selector + dataframe + CSV download."""
    st.subheader(title)
    st.caption(description)
    limit = st.selectbox("Limit", _PG_LIMIT_OPTIONS, index=0, key=limit_key)
    df = _df_or_warn(pg_conn, conn, f"{sql}\nLIMIT {int(limit)}")
    _show_df(df, download_name=download_name)


def render_postgres_page():
    """Postgres logging tables viewer (read-only).

    Shows the four tables the pipeline writes for run / observability tracking.
    Newest rows first; pick a Limit and download as CSV.
    """
    st.title("Postgres logging tables")

    host = os.getenv("PG_HOST"); port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB"); schema = os.getenv("PG_SCHEMA", "public")
    if not (host and db):
        st.error("PG_HOST / PG_DB not set in .env")
        return
    st.caption(f"`{host}:{port}/{db}` schema=`{schema}`  ·  read-only viewer")

    try:
        from connect_into_postgres import connect_to_postgres as pg_conn
    except Exception as e:
        st.error(f"failed to import postgres connector: {type(e).__name__}: {e}")
        return

    if st.button("↻ Refresh", help="Re-read from PG. Read-only."):
        st.cache_resource.clear()
        st.cache_data.clear()
        st.rerun()

    @st.cache_resource(show_spinner="connecting to postgres…")
    def _conn():
        return pg_conn.create_connection()
    try:
        conn = _conn()
    except Exception as e:
        st.error(f"connect failed: {type(e).__name__}: {e}")
        return

    tab_run, tab_conn, tab_query, tab_batch = st.tabs([
        "pipeline_run_summary",
        "connection_log",
        "query_log",
        "batch_log",
    ])

    with tab_run:
        _pg_table_tab(
            pg_conn, conn,
            title="pipeline_run_summary",
            description="One row per (run_id × env × target × operation). "
                        "Source of truth for run history.",
            sql="""
                SELECT id, run_id, env, target_name, operation,
                       rows_count, source_file,
                       started_at, ended_at,
                       EXTRACT(EPOCH FROM (ended_at - started_at))::numeric(12,3) AS seconds,
                       status, error
                FROM pipeline_run_summary
                ORDER BY started_at DESC NULLS LAST
            """,
            limit_key="pg_lim_run_summary",
            download_name="pipeline_run_summary.csv",
        )

    with tab_conn:
        _pg_table_tab(
            pg_conn, conn,
            title="connection_log",
            description="One row per connection attempt to Oracle / Elasticsearch / Postgres.",
            sql="""
                SELECT id, run_id, env, system_name, target_name, host,
                       started_at, ended_at, duration_ms, status, error
                FROM connection_log
                ORDER BY started_at DESC NULLS LAST
            """,
            limit_key="pg_lim_connection_log",
            download_name="connection_log.csv",
        )

    with tab_query:
        _pg_table_tab(
            pg_conn, conn,
            title="query_log",
            description="One row per Oracle SQL / ES request / PG query. "
                        "`duration_ms` is in milliseconds.",
            sql="""
                SELECT id, run_id, env, batch_id, system_name, target_name,
                       operation, query_hash, duration_ms,
                       rows_returned, rows_affected, status, error,
                       started_at
                FROM query_log
                ORDER BY started_at DESC NULLS LAST
            """,
            limit_key="pg_lim_query_log",
            download_name="query_log.csv",
        )

    with tab_batch:
        _pg_table_tab(
            pg_conn, conn,
            title="batch_log",
            description="One row per pipeline batch (compare / apply_changes / apply_missing).",
            sql="""
                SELECT id, run_id, env, batch_id, target_name, operation,
                       source_system, rows_returned, rows_changed, rows_missing,
                       duration_ms, status, error, started_at
                FROM batch_log
                ORDER BY started_at DESC NULLS LAST
            """,
            limit_key="pg_lim_batch_log",
            download_name="batch_log.csv",
        )


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

mode = (entry.get("MODE") or "time").strip().lower()
mode_color = {
    "time": "blue", "id_range": "violet",
    "time_union": "green", "id_range_union": "orange",
}.get(mode, "grey")
mc1, mc2, mc3 = st.columns([1, 1, 2])
with mc1:
    st.markdown(f"**MODE:** :{mode_color}[{mode}]")
with mc2:
    st.markdown(f"**INDEX_NAME:** `{entry.get('INDEX_NAME', '?')}`")
with mc3:
    if mode == "id_range":
        st.markdown(
            f"**PK:** `{entry.get('PK', '?')}` &nbsp;·&nbsp; "
            f"**ID_COLUMN:** `{entry.get('ID_COLUMN', '?')}`"
        )
    elif mode == "id_range_union":
        parts_n = len(entry.get("parts") or [])
        ids = " · ".join(p.get("ID_COLUMN", "?") for p in (entry.get("parts") or []))
        st.markdown(
            f"**PK:** `{entry.get('PK', '?')}` &nbsp;·&nbsp; "
            f"**parts:** {parts_n} &nbsp;·&nbsp; **id cols:** `{ids}`"
        )
    elif mode == "time_union":
        st.markdown(f"**parts:** {len(entry.get('parts') or [])}")

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
    if mode in ("id_range", "id_range_union"):
        banch_help = "Chunk size in IDs (int). 5000 = 5000-id windows."
    else:
        banch_help = "Window size: 'HOUR'/'DAY'/'WEEK', '30 DAYS', '3 HOURS', '15m'."
    new_banch = st.text_input(
        "BANCH_VALUE",
        value=str(cur_banch),
        key=f"banch_{sel}",
        help=banch_help,
    )

# ---------------------------------------------------------------------------
# Batch range selector (id_range / id_range_union only)
# ---------------------------------------------------------------------------

new_batch_from = entry.get("BATCH_FROM_ID")
new_batch_to = entry.get("BATCH_TO_ID")
clear_batch_overrides = False
range_dirty = False  # any change to from/to during this render

if mode in ("id_range", "id_range_union"):
    st.markdown("**Batch range** — pick a window or batch index range; default runs everything")

    range_key = f"range_{sel}"
    if range_key not in st.session_state:
        st.session_state[range_key] = {"global_min": None, "global_max": None,
                                       "per_part": []}

    rc1, rc2 = st.columns([1, 4])
    with rc1:
        if st.button("Compute range from Oracle", key=f"probe_{sel}"):
            try:
                from connect_into_orcal.connect_to_orcal import create_connection
                from core.batch import parse_sql_sections as _parse_sections

                if mode == "id_range_union":
                    parts_for_probe = entry.get("parts") or []
                    sql_paths = [(p.get("ID_COLUMN", "?"),
                                  SETTINGS_DIR / p["sql_file"]) for p in parts_for_probe]
                else:
                    sql_paths = [(entry.get("ID_COLUMN", "?"),
                                  SETTINGS_DIR / entry["sql_file"])]
                mins, maxs, per_part = [], [], []
                with st.spinner("Probing Oracle..."):
                    with create_connection() as _conn:
                        with _conn.cursor() as _cur:
                            for id_col, p in sql_paths:
                                sections = _parse_sections(p.read_text(encoding="utf-8"))
                                if "range" not in sections:
                                    per_part.append((id_col, None, None))
                                    continue
                                _cur.execute(sections["range"])
                                row = _cur.fetchone()
                                if row and row[0] is not None and row[1] is not None:
                                    mn, mx = int(row[0]), int(row[1])
                                    mins.append(mn); maxs.append(mx)
                                    per_part.append((id_col, mn, mx))
                                else:
                                    per_part.append((id_col, None, None))
                st.session_state[range_key] = {
                    "global_min": min(mins) if mins else None,
                    "global_max": max(maxs) if maxs else None,
                    "per_part": per_part,
                }
                st.success("Range probed.")
            except Exception as _e:
                st.error(f"Probe failed: {type(_e).__name__}: {_e}")

    rng = st.session_state.get(range_key, {})
    g_min = rng.get("global_min")
    g_max = rng.get("global_max")
    try:
        step_int = int(new_banch) if new_banch is not None else int(cur_banch or 0)
    except (ValueError, TypeError):
        step_int = 0
    total_global = ((g_max - g_min + step_int - 1) // step_int) if (g_min is not None and g_max is not None and step_int > 0) else None

    with rc2:
        if g_min is None:
            st.caption("Range not yet probed.")
        else:
            for id_col, mn, mx in rng.get("per_part", []):
                st.caption(
                    f"`{id_col}`: min={mn} max={mx}"
                    if mn is not None else f"`{id_col}`: empty"
                )
            st.markdown(
                f"**globalMin:** `{g_min}` &nbsp;·&nbsp; "
                f"**globalMax:** `{g_max}` &nbsp;·&nbsp; "
                f"**total batches @ step={step_int}:** `{total_global}`"
            )

    # Determine current selection mode from saved entry.
    if entry.get("BATCH_FROM_ID") is not None or entry.get("BATCH_TO_ID") is not None:
        default_mode = "Window"
    else:
        default_mode = "All"
    sel_mode_key = f"selmode_{sel}"
    if sel_mode_key not in st.session_state:
        st.session_state[sel_mode_key] = default_mode

    sel_mode = st.radio(
        "Run scope",
        ["All", "Window", "Batch index range"],
        index=["All", "Window", "Batch index range"].index(st.session_state[sel_mode_key]),
        horizontal=True,
        key=f"selmoderadio_{sel}",
    )
    st.session_state[sel_mode_key] = sel_mode

    if sel_mode == "All":
        # Mark that any prior overrides should be removed on Save.
        if entry.get("BATCH_FROM_ID") is not None or entry.get("BATCH_TO_ID") is not None:
            st.caption("On Save, BATCH_FROM_ID / BATCH_TO_ID will be cleared.")
            clear_batch_overrides = True
        new_batch_from = None
        new_batch_to = None
    elif sel_mode == "Window":
        wc1, wc2 = st.columns(2)
        with wc1:
            new_batch_from = st.number_input(
                "BATCH_FROM_ID",
                value=int(entry.get("BATCH_FROM_ID") if entry.get("BATCH_FROM_ID") is not None
                          else (g_min if g_min is not None else 0)),
                step=step_int or 1, key=f"bfrom_{sel}",
                help="Inclusive lower bound for the iteration window.",
            )
        with wc2:
            new_batch_to = st.number_input(
                "BATCH_TO_ID",
                value=int(entry.get("BATCH_TO_ID") if entry.get("BATCH_TO_ID") is not None
                          else (g_max if g_max is not None else 1)),
                step=step_int or 1, key=f"bto_{sel}",
                help="Exclusive upper bound for the iteration window.",
            )
        if new_batch_to <= new_batch_from:
            st.error("BATCH_TO_ID must be > BATCH_FROM_ID")
        else:
            n_batches = ((int(new_batch_to) - int(new_batch_from) + step_int - 1) // step_int) if step_int > 0 else 0
            st.caption(f"Selected window covers ~`{n_batches}` batch(es) at step={step_int}.")
        range_dirty = True
    else:  # Batch index range
        if g_min is None or step_int <= 0:
            st.warning("Click 'Compute range from Oracle' first to enable batch-index selection.")
            new_batch_from = entry.get("BATCH_FROM_ID")
            new_batch_to = entry.get("BATCH_TO_ID")
        else:
            bc1, bc2 = st.columns(2)
            with bc1:
                bi_from = st.number_input(
                    "Batch index from (1-based)",
                    value=1, min_value=1, max_value=int(total_global or 1),
                    step=1, key=f"bifrom_{sel}",
                )
            with bc2:
                bi_to = st.number_input(
                    "Batch index to (1-based, inclusive)",
                    value=int(total_global or 1), min_value=1, max_value=int(total_global or 1),
                    step=1, key=f"bito_{sel}",
                )
            if bi_to < bi_from:
                st.error("'to' must be >= 'from'")
            else:
                new_batch_from = int(g_min) + (int(bi_from) - 1) * step_int
                new_batch_to = min(int(g_min) + int(bi_to) * step_int, int(g_max))
                st.caption(
                    f"Translates to BATCH_FROM_ID=`{new_batch_from}`, BATCH_TO_ID=`{new_batch_to}` "
                    f"(`{int(bi_to) - int(bi_from) + 1}` batch(es))"
                )
        range_dirty = True


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
    csv_path_rel = entry.get("VALUE_COLM") or f"mappings/{sel}.csv"
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
    # Batch range overrides (id_range / id_range_union)
    if mode in ("id_range", "id_range_union"):
        if clear_batch_overrides:
            cfg[sel].pop("BATCH_FROM_ID", None)
            cfg[sel].pop("BATCH_TO_ID", None)
        elif range_dirty and new_batch_from is not None and new_batch_to is not None:
            cfg[sel]["BATCH_FROM_ID"] = int(new_batch_from)
            cfg[sel]["BATCH_TO_ID"] = int(new_batch_to)
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
    log_box.markdown("_starting main.py…_")
    progress_box.progress(0.0, text="starting...")
    rc, events = run_main_py_stream(log_box, progress_box)
    progress_box.progress(1.0, text=f"done (exit={rc})")
    st.session_state["last_run"] = {"rc": rc, "events": events}
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
        st.success("Done")
    else:
        st.error(f"Failed (exit={last['rc']})")
    if not run_clicked:
        _render_events(log_box, last.get("events") or [])

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
            "Run `main.py` first to generate diffs.")
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
        apply_log.markdown(f"_starting apply_changes mode={_mode} event={apply_event} env={apply_env} dry={apply_dry}…_")
        rc, events = run_apply_stream(apply_event, _mode, apply_env, apply_dry, apply_log)
        st.session_state["last_apply"] = {"rc": rc, "events": events,
                                          "event": apply_event, "env": apply_env,
                                          "mode": _mode, "dry": apply_dry}

    last_apply = st.session_state.get("last_apply")
    if last_apply and _mode is None:
        tag = f"event={last_apply['event']} env={last_apply['env']} mode={last_apply['mode']} dry={last_apply['dry']}"
        if last_apply["rc"] == 0:
            st.success(f"Done ({tag})")
        else:
            st.error(f"Failed exit={last_apply['rc']} ({tag})")
        _render_events(apply_log, last_apply.get("events") or [])

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
