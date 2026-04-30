"""Probe parentId/externalParentId for sample diff IDs.
Compare ES doc (frozen at event time) vs current Oracle join (live).
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

SAMPLES = [107442943, 107442138]


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


def es_get(doc_id):
    url = os.environ.get("ES_URL_PRODE") or os.environ["ES_URL"]
    auth = HTTPBasicAuth(os.environ["ES_USER"], os.environ["ES_PASS"])
    r = requests.get(f"{url}/playerbonus/_doc/{doc_id}",
                     auth=auth, verify=False, timeout=30)
    return r.json()


def main():
    out = {"freespins_rows": [], "user_parent_rows": [],
           "user_parent_columns": [], "es_docs": []}

    with oracle_conn() as conn:
        # 1) Free-spins row + current join values
        # 0) USER_PARENT columns first
        cols = fetch(conn,
            "SELECT column_name, data_type FROM all_tab_columns "
            "WHERE owner = 'GAMER' AND table_name = 'USER_PARENT' "
            "ORDER BY column_id")
        out["user_parent_columns"] = cols
        date_cols = [c["COLUMN_NAME"] for c in cols if "DATE" in c["COLUMN_NAME"]]
        date_select = ", ".join(f"UP.{c}" for c in date_cols) or "NULL AS NO_DATE"

        rows = fetch(conn,
            f"SELECT FS.FREEBONUSID, FS.USERID, FS.CREATEDATE, "
            f"       UD.USER_PARENT_ID, UP.EXTERNAL_PARENT_ID, {date_select} "
            f"FROM GAMER.FREESPINS_BONUSES FS "
            f"JOIN GAMER.USERDETAILS2 UD ON UD.USERID = FS.USERID "
            f"LEFT JOIN GAMER.USER_PARENT UP ON UP.USER_PARENT_ID = UD.USER_PARENT_ID "
            f"WHERE FS.FREEBONUSID IN (:1, :2)",
            SAMPLES)
        out["freespins_rows"] = rows

    # 3) ES docs
    for fb in SAMPLES:
        doc_id = f"{fb}_1"
        d = es_get(doc_id)
        src = d.get("_source", {})
        out["es_docs"].append({
            "id": doc_id, "found": d.get("found", False),
            "userId": src.get("userId"),
            "parentId": src.get("parentId"),
            "externalParentId": src.get("externalParentId"),
            "createdDate": src.get("createdDate"),
            "status": src.get("status"),
        })

    print(json.dumps(out, indent=2, default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
