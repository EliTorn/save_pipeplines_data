"""Audit columns + sample diffs for bonusName/currency/prizeWon/wheelspin."""
from __future__ import annotations
import json, os
from pathlib import Path
import oracledb
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")


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


def main():
    out = {}
    with conn() as c:
        for t in ("WHEELSPIN_BONUSES", "WHEEL_SPIN_BONUSES", "BONUSWHEELSPINS"):
            try:
                rows = fetch(c,
                    "SELECT column_name, data_type FROM all_tab_columns "
                    "WHERE owner='GAMER' AND table_name=:t ORDER BY column_id",
                    {"t": t})
                if rows:
                    out[t] = rows
                    break
            except Exception as e:
                out[f"{t}_err"] = str(e)
        out["FREESPINS_BONUSES"] = fetch(c,
            "SELECT column_name, data_type FROM all_tab_columns "
            "WHERE owner='GAMER' AND table_name='FREESPINS_BONUSES' "
            "AND column_name LIKE '%WON%' OR column_name LIKE '%PRIZE%' "
            "AND owner='GAMER' AND table_name='FREESPINS_BONUSES'")
        # Find any tables with PRIZEWON or AMOUNTWON
        out["any_prizewon"] = fetch(c,
            "SELECT owner, table_name, column_name FROM all_tab_columns "
            "WHERE column_name = 'PRIZEWON' AND owner IN ('GAMER','CASINO') "
            "ORDER BY owner, table_name FETCH FIRST 30 ROWS ONLY")
        out["any_amountwon"] = fetch(c,
            "SELECT owner, table_name, column_name FROM all_tab_columns "
            "WHERE column_name = 'AMOUNTWON' AND owner IN ('GAMER','CASINO') "
            "ORDER BY owner, table_name FETCH FIRST 30 ROWS ONLY")

    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
