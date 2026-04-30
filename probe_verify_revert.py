"""Verify post-revert: run real pipeline transform on Oracle jackpot rows
and compare emitted values to ES. Confirm prizeWon diffs gone, freeSpinsLeft
fix still active, no new diffs.
"""
from __future__ import annotations
import csv
import json
import os
from collections import Counter
from pathlib import Path

import oracledb
import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import sys
sys.path.insert(0, str(ROOT))
from core.adapter_loader import get_adapter
from core.compare import transform_to_es_shape


def conn():
    dsn = oracledb.makedsn(os.environ["ORACLE_DB_HOST"],
                           int(os.environ.get("ORACLE_PORT", "1521")),
                           service_name=os.environ["ORACLE_SERVICE_NAME"])
    return oracledb.connect(user=os.environ["ORACLE_USERNAME"],
                            password=os.environ["ORACLE_PASSWORD"], dsn=dsn)


def fetch_df(c, sql, params=None):
    with c.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def es_search(body):
    url = os.environ.get("ES_URL_PRODE") or os.environ["ES_URL"]
    auth = HTTPBasicAuth(os.environ["ES_USER"], os.environ["ES_PASS"])
    r = requests.post(f"{url}/playerbonus/_search",
                      auth=auth, verify=False, timeout=60, json=body)
    r.raise_for_status()
    return r.json()


def load_mapping(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def apply_pipeline(df_raw: pd.DataFrame, mapping: list[dict]) -> pd.DataFrame:
    adapter = get_adapter("playerbonus")
    return transform_to_es_shape(df_raw, mapping, adapter=adapter)


def compare_field(shaped: pd.DataFrame, es_by_id: dict, field: str, id_col: str = "id"):
    diffs = []
    for _, r in shaped.iterrows():
        did = r.get(id_col)
        if did is None:
            continue
        es = es_by_id.get(did)
        if not es:
            continue
        ora = r.get(field)
        es_v = es.get(field)
        # treat 0 vs None equal (mirror compare logic)
        def norm(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            try:
                f = float(v)
                return 0.0 if f == 0.0 else f
            except (TypeError, ValueError):
                return v
        if norm(ora) != norm(es_v):
            diffs.append({"id": did, "ora": ora, "es": es_v})
    return diffs


def main():
    # JACKPOT
    jackpot_sql = """
        SELECT JWB.USERBONUSID, JWB.BONUSID, JWB.USERID, JWB.CREATEDDATE, JWB.STATUSID,
               JWB.CURRENCYID, JWB.MENU_ITEM_ID, JWB.AMOUNTWON, JWB.BONUSCODE, JWB.PRIZEID,
               JWPD.PRIZETYPE, JWPD.FREESPINS AS PRIZE_FREESPINS,
               JWB.TRIGGERING_TRANSACTIONID, JWB.TRIGGERING_CAUSE,
               JWB.CREATEDDATE + B.EXPIERY_DAYS AS EXPIRATIONDATE,
               B.BONUSNAME, B.ACTIVEDAYS, B.EXPIERY_DAYS, B.DESCRIPTION, B.SPECIALBONUSTYPEID,
               CUR.CURRENCY AS CURRENCY_ISO,
               UD.EXTERNALUSERID, UD.USER_PARENT_ID,
               UA.INTERNALACCOUNT AS INTERNAL_ACCOUNT,
               UP.EXTERNAL_PARENT_ID AS EXTERNAL_PARENT_USERID,
               CU.SKINID, SK.SKIN AS SKIN_NAME, SK.SKINORIGIN AS SKIN_ORIGIN_ID,
               SGS.GROUPID AS SKIN_GROUP_ID
        FROM GAMER.JACKPOT_WHEEL_BONUSES JWB
        JOIN GAMER.BONUSES B ON JWB.BONUSID = B.BONUSID
        JOIN GAMER.CURRENCIES CUR ON CUR.CURRENCYID = JWB.CURRENCYID
        LEFT JOIN GAMER.JACKPOT_WHEEL_PRIZES_DESC JWPD ON JWPD.PRIZEID = JWB.PRIZEID
        JOIN GAMER.USERDETAILS2 UD ON UD.USERID = JWB.USERID
        JOIN CASINO.USERS CU ON CU.USERID = JWB.USERID
        LEFT JOIN GAMER.IR_SYS_USERACCOUNTS UA ON UA.USERID = JWB.USERID
        LEFT JOIN GAMER.USER_PARENT UP ON UP.USER_PARENT_ID = UD.USER_PARENT_ID
        JOIN GAMER.SKINS SK ON SK.SKINID = CU.SKINID
        LEFT JOIN GAMER.SKINGROUPSKINS SGS ON SGS.SKINID = CU.SKINID
        WHERE JWB.CREATEDDATE >= TRUNC(SYSDATE)-7
        FETCH FIRST 500 ROWS ONLY
    """
    freespins_sql = """
        SELECT FS.USERID, FS.FREEBONUSID, FS.BONUSID, FS.FREESPINS, FS.FREESPINS_LEFT,
               FS.DENOMINATION, FS.LINES, FS.COINS, FS.MULTIPLIER, FS.MAXWIN, FS.AMOUNT_WON,
               FS.FREESPINS_STATUSID, FS.SESSIONID, FS.CREATEDATE AS CREATEDDATE,
               CAST(FS.MENUITEMIDS AS VARCHAR2(4000)) AS MENUITEMIDS,
               CAST(FS.PROMOCODE AS VARCHAR2(200)) AS FSPROMOCODE,
               FS.TRIGGERING_TRANSACTIONID, FS.TRIGGERING_CAUSE,
               B.BONUSNAME, B.ACTIVEDAYS, B.EXPIERY_DAYS, B.DESCRIPTION, B.SPECIALBONUSTYPEID,
               CUR.CURRENCY AS CURRENCY_ISO,
               UD.EXTERNALUSERID, UD.USER_PARENT_ID,
               UA.INTERNALACCOUNT AS INTERNAL_ACCOUNT,
               UP.EXTERNAL_PARENT_ID AS EXTERNAL_PARENT_USERID,
               CU.SKINID, SK.SKIN AS SKIN_NAME, SK.SKINORIGIN AS SKIN_ORIGIN_ID,
               SGS.GROUPID AS SKIN_GROUP_ID
        FROM GAMER.FREESPINS_BONUSES FS
        JOIN GAMER.BONUSES B ON FS.BONUSID = B.BONUSID
        JOIN GAMER.CURRENCIES CUR ON CUR.CURRENCYID = FS.CURRENCYID
        JOIN GAMER.USERDETAILS2 UD ON UD.USERID = FS.USERID
        JOIN CASINO.USERS CU ON CU.USERID = FS.USERID
        LEFT JOIN GAMER.IR_SYS_USERACCOUNTS UA ON UA.USERID = FS.USERID
        LEFT JOIN GAMER.USER_PARENT UP ON UP.USER_PARENT_ID = UD.USER_PARENT_ID
        JOIN GAMER.SKINS SK ON SK.SKINID = CU.SKINID
        LEFT JOIN GAMER.SKINGROUPSKINS SGS ON SGS.SKINID = CU.SKINID
        WHERE FS.CREATEDATE >= TRUNC(SYSDATE)-7
        FETCH FIRST 500 ROWS ONLY
    """

    with conn() as c:
        df_jwb = fetch_df(c, jackpot_sql)
        df_fs = fetch_df(c, freespins_sql)

    print(f"jackpot rows: {len(df_jwb)}, freespins rows: {len(df_fs)}")

    map_jwb = load_mapping(ROOT / "settings/indexes/playerbonus/parts/jackpot/mapping.csv")
    map_fs = load_mapping(ROOT / "settings/indexes/playerbonus/parts/freespins/mapping.csv")

    shaped_jwb = apply_pipeline(df_jwb, map_jwb)
    shaped_fs = apply_pipeline(df_fs, map_fs)

    # Distinct sample sanity for jackpot prizeWon (should all be 0)
    pw_vals = shaped_jwb["prizeWon"].astype(float).tolist() if "prizeWon" in shaped_jwb else []
    print(f"jackpot prizeWon non-zero count in shaped: {sum(1 for v in pw_vals if v != 0.0)} / {len(pw_vals)}")

    # Distribution of freeSpinsLeft for FreeChips (sid=4) in shaped should be None
    fc_mask = df_fs["SPECIALBONUSTYPEID"] == 4
    if "freeSpinsLeft" in shaped_fs.columns:
        fc_fsl = shaped_fs.loc[fc_mask.values, "freeSpinsLeft"].tolist()
        non_null = sum(1 for v in fc_fsl if v is not None and not pd.isna(v))
        print(f"FreeChips freeSpinsLeft non-null count in shaped: {non_null} / {len(fc_fsl)} (expect 0)")
    fs_mask = df_fs["SPECIALBONUSTYPEID"] != 4
    if "freeSpinsLeft" in shaped_fs.columns:
        fsplain = shaped_fs.loc[fs_mask.values, "freeSpinsLeft"].tolist()
        non_null = sum(1 for v in fsplain if v is not None and not pd.isna(v))
        print(f"FreeSpins freeSpinsLeft non-null count in shaped: {non_null} / {len(fsplain)}")

    # Fetch ES docs for these IDs
    jwb_ids = shaped_jwb["id"].dropna().astype(str).tolist()
    fs_ids = shaped_fs["id"].dropna().astype(str).tolist()
    es_by_id = {}
    for ids in (jwb_ids, fs_ids):
        for i in range(0, len(ids), 100):
            chunk = ids[i:i + 100]
            resp = es_search({"size": len(chunk),
                              "query": {"ids": {"values": chunk}},
                              "_source": True})
            for h in resp.get("hits", {}).get("hits", []):
                es_by_id[h["_id"]] = h.get("_source", {})

    # Compare prizeWon, twisterPrizeWon, freeSpinsLeft
    print("\n--- jackpot prizeWon diffs (post-revert) ---")
    diffs = compare_field(shaped_jwb, es_by_id, "prizeWon")
    print(f"diffs: {len(diffs)} (expect ~0)")
    for d in diffs[:5]:
        print(" ", d)

    print("\n--- jackpot twisterPrizeWon diffs (fix still active) ---")
    diffs = compare_field(shaped_jwb, es_by_id, "twisterPrizeWon")
    print(f"diffs: {len(diffs)} (expect ~0)")
    for d in diffs[:5]:
        print(" ", d)

    print("\n--- freespins/chips freeSpinsLeft diffs ---")
    diffs = compare_field(shaped_fs, es_by_id, "freeSpinsLeft")
    print(f"diffs: {len(diffs)} (expect partial-spin drift only)")
    for d in diffs[:5]:
        print(" ", d)

    print("\n--- freespins chipCountLeft diffs ---")
    diffs = compare_field(shaped_fs, es_by_id, "chipCountLeft")
    print(f"diffs: {len(diffs)}")
    for d in diffs[:5]:
        print(" ", d)


if __name__ == "__main__":
    main()
