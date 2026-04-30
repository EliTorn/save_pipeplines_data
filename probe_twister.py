"""Probe v2: PRIZETYPE distribution + verify rule across true/false twisterPrizeWon.

Fetches a batch of recent JackpotWheel rows from Oracle, joins prize desc,
fetches matching ES docs, applies the candidate rule, and reports mismatches.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import oracledb
import requests
import urllib3
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def oracle_conn():
    dsn = oracledb.makedsn(os.environ["ORACLE_DB_HOST"],
                           int(os.environ.get("ORACLE_PORT", "1521")),
                           service_name=os.environ["ORACLE_SERVICE_NAME"])
    return oracledb.connect(user=os.environ["ORACLE_USERNAME"],
                            password=os.environ["ORACLE_PASSWORD"], dsn=dsn)


def fetch(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def es_post_search(body):
    url = os.environ.get("ES_URL_PRODE") or os.environ["ES_URL"]
    auth = HTTPBasicAuth(os.environ["ES_USER"], os.environ["ES_PASS"])
    r = requests.post(f"{url}/playerbonus/_search",
                      auth=auth, verify=False, timeout=60, json=body)
    r.raise_for_status()
    return r.json()


def candidate_rule(row):
    """Return computed twisterPrizeWon per Java logic."""
    if int(row.get("STATUSID") or 0) != 4:
        return False
    pt = int(row.get("PRIZETYPE") or 0)
    aw = float(row.get("AMOUNTWON") or 0)
    fs = int(row.get("FREESPINS") or 0)
    if pt == 1:                     # FREE_SPINS
        return fs > 0
    if pt in (2, 4, 5, 6):          # CASH_BONUS, MEGA/MIDI/MINI_JACKPOT
        return aw > 0
    if pt in (9, 10):               # BINGO_TICKET, SCRATCH_CARD (proxy on AMOUNTWON)
        return aw > 0
    return False                     # NO_PRIZE, LEVEL_UP, TICKET_TO_MEGA_LOTTERY, default


def main():
    with oracle_conn() as conn:
        # Distribution
        dist = fetch(conn,
            "SELECT JWPD.PRIZETYPE, COUNT(*) AS CNT "
            "FROM GAMER.JACKPOT_WHEEL_BONUSES JWB "
            "LEFT JOIN GAMER.JACKPOT_WHEEL_PRIZES_DESC JWPD ON JWPD.PRIZEID = JWB.PRIZEID "
            "WHERE JWB.CREATEDDATE >= TRUNC(SYSDATE)-7 "
            "GROUP BY JWPD.PRIZETYPE ORDER BY CNT DESC")
        print("PRIZETYPE dist last 7 days:", dist)

        # Sample 200 closed jackpot bonuses
        rows = fetch(conn,
            "SELECT JWB.USERBONUSID, JWB.STATUSID, JWB.AMOUNTWON, "
            "       JWB.PRIZEID, JWPD.PRIZETYPE, JWPD.FREESPINS, JWPD.AMOUNT "
            "FROM GAMER.JACKPOT_WHEEL_BONUSES JWB "
            "LEFT JOIN GAMER.JACKPOT_WHEEL_PRIZES_DESC JWPD ON JWPD.PRIZEID = JWB.PRIZEID "
            "WHERE JWB.CREATEDDATE >= TRUNC(SYSDATE)-30 "
            "  AND JWB.STATUSID = 4 "
            "FETCH FIRST 300 ROWS ONLY")
        print("sample rows:", len(rows))

    # Fetch ES twisterPrizeWon for same IDs
    ids = [f"{r['USERBONUSID']}_3" for r in rows]
    by_id = {}
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        resp = es_post_search({"size": len(chunk),
                               "query": {"ids": {"values": chunk}},
                               "_source": ["twisterPrizeWon", "amountWon", "statusId"]})
        for h in resp.get("hits", {}).get("hits", []):
            by_id[h["_id"]] = h.get("_source", {})

    mismatches = []
    matched = 0
    for r in rows:
        did = f"{r['USERBONUSID']}_3"
        es = by_id.get(did)
        if not es:
            continue
        es_val = bool(es.get("twisterPrizeWon"))
        ora_val = candidate_rule(r)
        if es_val == ora_val:
            matched += 1
        else:
            mismatches.append({**r, "es_twisterPrizeWon": es_val, "computed": ora_val})

    print(f"matched={matched} mismatched={len(mismatches)} total_with_es={matched + len(mismatches)}")
    print(f"mismatch sample (first 10):")
    print(json.dumps(mismatches[:10], default=str, indent=2))


if __name__ == "__main__":
    main()
