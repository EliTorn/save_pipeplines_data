import json, os, sys, time, uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

_PARENT = Path(__file__).resolve().parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from logging_setup import get_run_logger, CONN_CSV, EVENTS_CSV, QUERIES_CSV
from geo_info import host_info
from _pipeline_env import env_truthy, normalize_es_env

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_VERBOSE = env_truthy("PIPELINE_VERBOSE")

_env = lambda n, d="": (os.getenv(n) or "").strip() or d
ES_URL_STAGE = _env("ES_URL_STAGE") or None
ES_URL_PRODE = _env("ES_URL_PRODE") or _env("ES_URL_PROD") or _env("ES_URL") or None
ES_USER, ES_PASS = os.getenv("ES_USER"), os.getenv("ES_PASS")
ES_VERIFY = _env("ES_VERIFY", "false").lower() == "true"
PAGE_SIZE = int(_env("ES_PAGE_SIZE", "10000"))
TIMEOUT_CONNECT = int(_env("ES_TIMEOUT_CONNECT", "5"))
TIMEOUT_READ = int(_env("ES_TIMEOUT_READ", "300"))
if not (ES_URL_STAGE or ES_URL_PRODE):
    sys.exit("Missing env vars: at least one of ES_URL_STAGE or ES_URL_PRODE must be set")
ES_URL = ES_URL_PRODE or ES_URL_STAGE
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
SESSION = requests.Session()


def es_target(es_env=None):
    if normalize_es_env(es_env) == "prod":
        if not ES_URL_PRODE: raise RuntimeError("ES_ENV=prod but ES_URL_PRODE not set")
        if not (ES_USER and ES_PASS): raise RuntimeError("ES_ENV=prod but ES_USER/ES_PASS not set")
        return ES_URL_PRODE, HTTPBasicAuth(ES_USER, ES_PASS), ES_VERIFY
    if not ES_URL_STAGE: raise RuntimeError("ES_ENV=stage but ES_URL_STAGE not set")
    return ES_URL_STAGE, None, False


_entry_env = lambda e: es_target(e.get("ES_ENV") if isinstance(e, dict) else e)
es_iso = lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"
build_range_query = lambda f, a, b: {"range": {f: {"gte": es_iso(a), "lt": es_iso(b)}}}


def es_time_field(entry):
    tc = entry["TIME_DATE"]
    for r in entry.get("mapping", []):
        if (r.get("filed_orcal") or "").strip().lower() == tc.strip().lower():
            f = (r.get("filed_es") or "").strip()
            if f: return f
    return tc[:1].lower() + tc[1:]


def _post(url, path, body, auth, verify):
    r = SESSION.post(url + path, auth=auth, verify=verify,
                     timeout=(TIMEOUT_CONNECT, TIMEOUT_READ), json=body, headers=HEADERS)
    if not r.ok:
        raise requests.HTTPError(
            f"ES POST {path} -> {r.status_code} body={json.dumps(body, default=str)} resp={r.text}", response=r)
    return r.json()


def _paginate(url, auth, verify, index, query, max_docs=None):
    body = {"size": PAGE_SIZE, "query": query, "sort": [{"_doc": "asc"}], "track_total_hits": True}
    out, sa = [], None
    while True:
        b = {**body, **({"search_after": sa} if sa else {})}
        hits = _post(url, f"/{index}/_search", b, auth, verify).get("hits", {}).get("hits", [])
        if not hits: break
        out.extend(hits)
        if max_docs and len(out) >= max_docs: return out[:max_docs]
        if len(hits) < PAGE_SIZE: break
        sa = hits[-1].get("sort")
        if sa is None: break
    return out


def _terms_values(values):
    seen, out_s, out_i = set(), [], []
    for v in values:
        if v is None: continue
        s = str(v)
        if s not in seen: seen.add(s); out_s.append(s)
        try:
            i = int(v)
            if i not in seen: seen.add(i); out_i.append(i)
        except (TypeError, ValueError): pass
    return out_s + out_i


def fetch_range_df(index, entry, w_from, w_to):
    url, auth, verify = _entry_env(entry)
    field = es_time_field(entry)
    if _VERBOSE:
        print(f"[ES] {url} {index} range {field} {w_from}..{w_to} auth={'on' if auth else 'off'}")
    return hits_to_df(_paginate(url, auth, verify, index, build_range_query(field, w_from, w_to)))


def fetch_terms_df(index, field, values, entry=None):
    if not values: return pd.DataFrame()
    url, auth, verify = _entry_env(entry)
    terms = _terms_values(values)
    if _VERBOSE:
        print(f"[ES] {url} {index} terms {field} n={len(terms)} sample={terms[:3]} auth={'on' if auth else 'off'}")
    return hits_to_df(_paginate(url, auth, verify, index, {"terms": {field: terms}}))


def _req(method, path, body=None, entry=None):
    url, auth, verify = _entry_env(entry)
    r = SESSION.request(method, url + path, auth=auth, verify=verify,
                        timeout=(TIMEOUT_CONNECT, TIMEOUT_READ), json=body, headers=HEADERS)
    if not r.ok:
        bs = json.dumps(body, default=str) if body else None
        raise requests.HTTPError(f"ES {method} {path} -> {r.status_code}\n  request: {bs}\n  response: {r.text}", response=r)
    return r.json() if r.content else {}


es_post = lambda p, b: _req("POST", p, b)
es_get = lambda p: _req("GET", p)


def run_tracked(path, body, logger, index=None, batch=None):
    sql = json.dumps(body, default=str) if body else path
    with logger.query(sql, owner=None, table=index, batch=batch, params=None) as q:
        resp = es_post(path, body) if body is not None else es_get(path)
        hits = resp.get("hits", {}).get("hits", []) if isinstance(resp, dict) else []
        total = resp.get("hits", {}).get("total", {}) if isinstance(resp, dict) else 0
        if isinstance(total, dict): total = total.get("value", 0)
        q.set_rows(len(hits) if hits else (total or 0))
    return resp, q.query_id


def fetch_all(index, logger, query=None, page_size=PAGE_SIZE, max_docs=None, sort_field="_doc"):
    body = {"size": page_size, "query": query or {"match_all": {}},
            "sort": [{sort_field: "asc"}], "track_total_hits": True}
    logger.event("fetch_plan", table=index, batch_size=page_size, user_limit=max_docs)
    out, sa, batch, t0 = [], None, 0, time.perf_counter()
    while True:
        b = {**body, **({"search_after": sa} if sa else {})}
        resp, qid = run_tracked(f"/{index}/_search", b, logger, index=index, batch=batch)
        hits = resp.get("hits", {}).get("hits", [])
        if not hits: break
        out.extend(hits)
        logger.event("batch_progress", query_id=qid, table=index, batch=batch, rows=len(hits), total=len(out))
        batch += 1
        if max_docs and len(out) >= max_docs: out = out[:max_docs]; break
        sa = hits[-1].get("sort")
        if sa is None or len(hits) < page_size: break
    dt = time.perf_counter() - t0
    logger.event("fetch_done", table=index, rows=len(out), seconds=round(dt, 3),
                 rows_per_sec=round(len(out) / dt, 1) if dt > 0 else None)
    return out


def list_indices(logger, pattern="*"):
    p = f"/_cat/indices/{pattern}?format=json&h=index"
    with logger.query(p, owner=None, table=None) as q:
        d = es_get(p); q.set_rows(len(d))
    return sorted(x["index"] for x in d if not x["index"].startswith("."))


def hits_to_df(hits):
    return pd.DataFrame([{"_index": h.get("_index"), "_id": h.get("_id"), **h.get("_source", {})} for h in hits])


def main():
    rid = uuid.uuid4().hex[:12]
    lg = get_run_logger(rid)
    lg.connection(es_url=ES_URL, es_user=ES_USER, es_verify=ES_VERIFY, page_size=PAGE_SIZE,
                  timeout_connect=TIMEOUT_CONNECT, timeout_read=TIMEOUT_READ, **host_info())
    print(f"Run {rid} | conn -> {CONN_CSV.name} | events -> {EVENTS_CSV.name} | queries -> {QUERIES_CSV.name}")
    target = sys.argv[1] if len(sys.argv) > 1 else "loginlogoutinfo"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    t_run = time.perf_counter()
    try:
        if target in ("--list", "list"):
            for i in list_indices(lg): print(f"  {i}")
            return
        probe, qid = run_tracked(f"/{target}/_search", {"size": 0, "track_total_hits": True}, lg, index=target)
        total = probe.get("hits", {}).get("total", {})
        if isinstance(total, dict): total = total.get("value", 0)
        lg.event("probe_done", query_id=qid, table=target, rows=total)
        df = hits_to_df(fetch_all(target, lg, max_docs=limit))
        out = f"{target.replace('*', 'all').replace('/', '_')}.csv"
        t0 = time.perf_counter()
        df.to_csv(out, index=False, encoding="utf-8-sig")
        lg.event("csv_saved", table=target, path=os.path.abspath(out), rows=len(df),
                 seconds=round(time.perf_counter() - t0, 3))
        print(f"Saved -> {out} ({len(df)} rows)")
    except requests.HTTPError as e:
        lg.event("es_http_error", level="ERROR", error=str(e)); sys.exit(f"ES HTTP error: {e}")
    except Exception as e:
        lg.event("fatal", level="ERROR", error=str(e)); raise
    finally:
        lg.event("run_end", total_seconds=round(time.perf_counter() - t_run, 3))


if __name__ == "__main__":
    main()
