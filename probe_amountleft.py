"""Probe amountLeft drift for sample redeem-eligible bonus IDs."""
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

SAMPLES = [61029992, 61028272, 61028226, 61024738]


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


def es_get(doc_id):
    url = os.environ.get("ES_URL_PRODE") or os.environ["ES_URL"]
    auth = HTTPBasicAuth(os.environ["ES_USER"], os.environ["ES_PASS"])
    r = requests.get(f"{url}/playerbonus/_doc/{doc_id}",
                     auth=auth, verify=False, timeout=30)
    return r.json()


def main():
    out = {"oracle": [], "es": [], "isu_columns": []}
    with conn() as c:
        out["isu_columns"] = fetch(c,
            "SELECT column_name, data_type FROM all_tab_columns "
            "WHERE owner='GAMER' AND table_name='IR_SYS_USERSBONUSES' "
            "ORDER BY column_id")
        out["oracle"] = fetch(c,
            "SELECT USERBONUSID, USERID, BONUSID, AMOUNT, AMOUNTLEFT, "
            "       STATUSID, CREATEDDATE, "
            "       (SELECT STATUSNAME FROM GAMER.BONUSSTATUSES WHERE STATUSID = ISU.STATUSID) AS STATUSNAME "
            "FROM GAMER.IR_SYS_USERSBONUSES ISU "
            "WHERE USERBONUSID IN (:1, :2, :3, :4)",
            SAMPLES)

    for ub in SAMPLES:
        d = es_get(f"{ub}_0")
        src = d.get("_source", {})
        out["es"].append({
            "id": f"{ub}_0", "found": d.get("found"),
            "amount": src.get("amount"),
            "amountLeft": src.get("amountLeft"),
            "statusId": src.get("statusId"),
            "status": src.get("status"),
            "createdDate": src.get("createdDate"),
            "updateDate": src.get("updateDate"),
        })

    print(json.dumps(out, indent=2, default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
