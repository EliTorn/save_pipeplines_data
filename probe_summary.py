"""Cross-check bonusName/currency/prizeWon between ES and Oracle live, by bonus type."""
from __future__ import annotations
import json, os
from collections import Counter
from pathlib import Path

import oracledb
import requests
import urllib3
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def conn():
    dsn = oracledb.makedsn(os.environ["ORACLE_DB_HOST"],
                           int(os.environ.get("ORACLE_PORT", "1521")),
                           service_name=os.environ["ORACLE_SERVICE_NAME"])
    return oracledb.connect(user=os.environ["ORACLE_USERNAME"],
                            password=os.environ["ORACLE_PASSWORD"], dsn=dsn)


def fetch(c, sql, params=None):
    with c.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def es_search(body):
    url = os.environ.get("ES_URL_PRODE") or os.environ["ES_URL"]
    auth = HTTPBasicAuth(os.environ["ES_USER"], os.environ["ES_PASS"])
    r = requests.post(f"{url}/playerbonus/_search",
                      auth=auth, verify=False, timeout=60, json=body)
    r.raise_for_status()
    return r.json()


def main():
    out: dict = {}

    # 1) bonusName per bonus type — sample 200 each, count ES null
    out["bonusName_es_null_by_type"] = {}
    for bt in ("RedeemEligble", "FreeSpins", "FreeChips", "WheelSpin", "JackpotWheel"):
        resp = es_search({
            "size": 500,
            "query": {"term": {"bonusType": bt}},
            "_source": ["bonusName"]
        })
        hits = resp.get("hits", {}).get("hits", [])
        nulls = sum(1 for h in hits if h.get("_source", {}).get("bonusName") in (None, ""))
        out["bonusName_es_null_by_type"][bt] = {"sampled": len(hits), "es_null": nulls}

    # 2) currency per bonus type
    out["currency_es_null_by_type"] = {}
    for bt in ("RedeemEligble", "FreeSpins", "FreeChips", "WheelSpin", "JackpotWheel"):
        resp = es_search({
            "size": 500,
            "query": {"term": {"bonusType": bt}},
            "_source": ["currency"]
        })
        hits = resp.get("hits", {}).get("hits", [])
        nulls = sum(1 for h in hits if h.get("_source", {}).get("currency") in (None, ""))
        out["currency_es_null_by_type"][bt] = {"sampled": len(hits), "es_null": nulls}

    # 3) prizeWon: sample jackpot diffs. Get Oracle JWB rows + ES docs.
    with conn() as c:
        jwb = fetch(c,
            "SELECT USERBONUSID, AMOUNTWON, STATUSID FROM GAMER.JACKPOT_WHEEL_BONUSES "
            "WHERE CREATEDDATE >= TRUNC(SYSDATE)-7 "
            "FETCH FIRST 500 ROWS ONLY")
        ws = fetch(c,
            "SELECT BONUSWHEELSPINID, PRIZEWON, WHEELSPINSTATUSID FROM GAMER.WHEELSPIN_BONUSES "
            "WHERE CREATEDDATE >= TRUNC(SYSDATE)-7 "
            "FETCH FIRST 500 ROWS ONLY")

    jwb_ids = [f"{r['USERBONUSID']}_3" for r in jwb]
    ws_ids = [f"{r['BONUSWHEELSPINID']}_2" for r in ws]
    es_pw = {}
    for ids in (jwb_ids, ws_ids):
        for i in range(0, len(ids), 100):
            chunk = ids[i:i+100]
            resp = es_search({"size": len(chunk),
                              "query": {"ids": {"values": chunk}},
                              "_source": ["prizeWon", "amountWon", "statusId", "bonusType"]})
            for h in resp.get("hits", {}).get("hits", []):
                es_pw[h["_id"]] = h.get("_source", {})

    jwb_status = Counter()
    jwb_diff = Counter()
    jwb_samples = []
    for r in jwb:
        did = f"{r['USERBONUSID']}_3"
        es = es_pw.get(did)
        if not es:
            continue
        ora = float(r["AMOUNTWON"] or 0)
        es_val = float(es.get("prizeWon") or 0)
        st = int(r["STATUSID"] or 0)
        jwb_status[st] += 1
        if abs(ora - es_val) > 1e-9:
            jwb_diff[st] += 1
            if len(jwb_samples) < 10:
                jwb_samples.append({"id": did, "ora_AMOUNTWON": ora, "es_prizeWon": es_val,
                                    "es_amountWon": es.get("amountWon"), "status": st})
    out["jackpot_prizeWon_diff_by_status"] = {
        "total_by_status": dict(jwb_status),
        "diff_by_status": dict(jwb_diff),
        "samples": jwb_samples,
    }

    ws_status = Counter()
    ws_diff = Counter()
    ws_samples = []
    for r in ws:
        did = f"{r['BONUSWHEELSPINID']}_2"
        es = es_pw.get(did)
        if not es:
            continue
        ora = float(r["PRIZEWON"] or 0)
        es_val = float(es.get("prizeWon") or 0)
        st = int(r["WHEELSPINSTATUSID"] or 0)
        ws_status[st] += 1
        if abs(ora - es_val) > 1e-9:
            ws_diff[st] += 1
            if len(ws_samples) < 5:
                ws_samples.append({"id": did, "ora": ora, "es": es_val, "status": st,
                                   "es_amountWon": es.get("amountWon")})
    out["wheelspin_prizeWon_diff_by_status"] = {
        "total_by_status": dict(ws_status),
        "diff_by_status": dict(ws_diff),
        "samples": ws_samples,
    }

    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
