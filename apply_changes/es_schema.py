"""ES schema i/o + pre-write type validation for apply_changes.

Pulls the live mapping from ES prod for each index we update, flattens it to a CSV
(`field,type,format,full_path,is_array`) under `settings/indexes/<index>/schema.csv`, and
provides `validate_doc()` so apply_changes can refuse to write a doc whose values
don't match the ES schema.

Array detection: ES mapping doesn't carry list-vs-scalar info, so we probe a sample
of live docs and mark `is_array=true` for any path observed as a list.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Optional

import requests

HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
SAMPLE_SIZE_FOR_ARRAY_DETECT = 100

# Fields excluded from schema CSV (Spring/legacy artifacts the ETL never writes).
_EXCLUDE_FIELDS = {"_class"}

# ES core types whose values we know how to validate. Unknown types are accepted as-is.
_TYPE_COMPAT: dict[str, tuple] = {
    "keyword":     (str,),
    "text":        (str,),
    "boolean":     (bool,),
    "byte":        (int,),
    "short":       (int,),
    "integer":     (int,),
    "long":        (int,),
    "float":       (int, float),
    "half_float":  (int, float),
    "double":      (int, float),
    "scaled_float":(int, float),
    "date":        (str, int),       # ISO string OR epoch_millis
    "object":      (dict,),
    "nested":      (dict, list),
    "ip":          (str,),
    "binary":      (str,),
}


def _flatten(props: dict, prefix: str = "") -> list[dict]:
    rows: list[dict] = []
    for name, spec in props.items():
        if name in _EXCLUDE_FIELDS:
            continue
        full = f"{prefix}.{name}" if prefix else name
        nested = spec.get("properties")
        if nested:
            rows.append({
                "field": name, "type": spec.get("type", "object"), "format": "",
                "full_path": full, "is_array": "false",
            })
            rows.extend(_flatten(nested, full))
            continue
        rows.append({
            "field":     name,
            "type":      spec.get("type", ""),
            "format":    spec.get("format", ""),
            "full_path": full,
            "is_array":  "false",
        })
    return rows


def _detect_arrays(index: str, url: str, auth, verify: bool) -> set[str]:
    body = {"size": SAMPLE_SIZE_FOR_ARRAY_DETECT, "query": {"match_all": {}}}
    r = requests.post(f"{url}/{index}/_search", auth=auth, verify=verify, json=body,
                      headers=HEADERS, timeout=(5, 60))
    r.raise_for_status()
    hits = r.json().get("hits", {}).get("hits", [])
    arrays: set[str] = set()

    def walk(path: str, val):
        if isinstance(val, list):
            arrays.add(path)
            for el in val:
                if isinstance(el, dict):
                    walk(path, el)
        elif isinstance(val, dict):
            for k, v in val.items():
                walk(f"{path}.{k}" if path else k, v)

    for h in hits:
        src = h.get("_source") or {}
        for k, v in src.items():
            walk(k, v)
    return arrays


def fetch_schema(index: str, url: str, auth, verify: bool) -> list[dict]:
    """Hit ES, return flattened rows with array detection filled in."""
    r = requests.get(f"{url}/{index}/_mapping", auth=auth, verify=verify,
                     headers=HEADERS, timeout=(5, 60))
    r.raise_for_status()
    body = r.json()
    if not body:
        return []
    # Resolve alias -> concrete index
    real_idx = next(iter(body))
    props = body[real_idx].get("mappings", {}).get("properties", {})
    rows = _flatten(props)

    try:
        arrays = _detect_arrays(index, url, auth, verify)
    except Exception as e:
        print(f"  warn: array detection failed for {index}: {e}")
        arrays = set()
    for row in rows:
        row["is_array"] = "true" if row["full_path"] in arrays else "false"
    return rows


def save_schema_csv(rows: list[dict], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["field", "type", "format", "full_path", "is_array"])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def load_schema_csv(dest: Path) -> dict[str, dict]:
    if not dest.is_file():
        return {}
    out: dict[str, dict] = {}
    with open(dest, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            out[r["full_path"]] = r
    return out


def validate_value(field: str, value: Any, spec: dict) -> Optional[str]:
    """Return error string if value is incompatible with the ES type, else None."""
    if value is None:
        return None
    es_type = (spec.get("type") or "").strip().lower()
    if not es_type or es_type == "object":
        return None
    accepted = _TYPE_COMPAT.get(es_type)
    if accepted is None:
        return None  # unknown type -> skip

    is_array = (spec.get("is_array") or "").strip().lower() == "true"
    if isinstance(value, list):
        if not is_array:
            return f"{field}: list value where ES schema is scalar ({es_type})"
        for i, el in enumerate(value):
            if el is None:
                continue
            err = _check_scalar(field, el, es_type, accepted, idx=i)
            if err:
                return err
        return None
    return _check_scalar(field, value, es_type, accepted)


def _check_scalar(field: str, value: Any, es_type: str, accepted: tuple, idx: int | None = None) -> Optional[str]:
    # Python bool is a subclass of int — treat strictly: only allow on boolean fields
    if isinstance(value, bool) and es_type != "boolean":
        suffix = f"[{idx}]" if idx is not None else ""
        return f"{field}{suffix}: bool value not allowed for ES type {es_type}"
    if not isinstance(value, accepted):
        suffix = f"[{idx}]" if idx is not None else ""
        return (f"{field}{suffix}: value {value!r} (python {type(value).__name__}) "
                f"not compatible with ES type {es_type}")
    return None


def validate_doc(doc_id: str, doc: dict, schema: dict[str, dict]) -> list[str]:
    """Validate every field of `doc`. Returns list of error strings (empty = ok)."""
    errors: list[str] = []
    for field, val in doc.items():
        spec = schema.get(field)
        if spec is None:
            errors.append(f"{doc_id}.{field}: field not in ES mapping")
            continue
        err = validate_value(field, val, spec)
        if err:
            errors.append(f"{doc_id}.{err}")
    return errors
