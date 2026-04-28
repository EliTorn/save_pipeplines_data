"""One-shot: refresh ES schema CSVs for every index referenced by events.yaml.

Pulls mappings + array detection from ES prod (always), writes one CSV per index
to `settings/es_schema/<index>.csv`.

Usage:
    python -m apply_changes.fetch_schemas                # all indexes from events.yaml
    python -m apply_changes.fetch_schemas playerbonus    # specific index(es)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import urllib3
import yaml
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

from apply_changes import es_schema

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCHEMA_DIR = _ROOT / "settings" / "es_schema"
YAML_PATH = _ROOT / "settings" / "events.yaml"


def discover_indexes() -> list[str]:
    """Distinct INDEX_NAME values from events.yaml. Skips the top-level non-event keys."""
    if not YAML_PATH.is_file():
        return []
    cfg = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8")) or {}
    seen: list[str] = []
    for key, entry in cfg.items():
        if not isinstance(entry, dict):
            continue
        idx = (entry.get("INDEX_NAME") or "").strip()
        if idx and idx not in seen:
            seen.append(idx)
    return seen


def prod_client() -> tuple[str, "HTTPBasicAuth | None", bool]:
    url = (os.getenv("ES_URL_PRODE") or os.getenv("ES_URL_PROD") or os.getenv("ES_URL") or "").strip()
    user = os.getenv("ES_USER")
    pw = os.getenv("ES_PASS")
    if not (url and user and pw):
        sys.exit("prod schema fetch requires ES_URL_PRODE + ES_USER + ES_PASS in .env")
    return url, HTTPBasicAuth(user, pw), False


def refresh_one(index: str, url: str, auth, verify: bool) -> Path:
    rows = es_schema.fetch_schema(index, url, auth, verify)
    out = SCHEMA_DIR / f"{index}.csv"
    es_schema.save_schema_csv(rows, out)
    n_arr = sum(1 for r in rows if r["is_array"] == "true")
    print(f"  {index}: {len(rows)} fields ({n_arr} array) -> {out.relative_to(_ROOT)}")
    return out


def main():
    indexes = sys.argv[1:] or discover_indexes()
    if not indexes:
        sys.exit(f"no indexes to refresh — pass index name(s) on the CLI or set INDEX_NAME in {YAML_PATH}")
    url, auth, verify = prod_client()
    print(f"prod ES = {url}")
    print(f"writing to {SCHEMA_DIR}")
    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
    for idx in indexes:
        refresh_one(idx, url, auth, verify)
    print("done.")


if __name__ == "__main__":
    main()
