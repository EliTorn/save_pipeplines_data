"""Parquet writer — additive sibling to csv_writer.

Writes a DataFrame to <stem>.parquet next to the CSV. Failures are
non-fatal (caller logs); CSV remains the legacy path during Phase A.

Schema is inferred from the DataFrame. All-null columns get cast to
string so pyarrow doesn't choke on object dtype with no observed values.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd

try:
    import pyarrow.parquet as pq  # noqa: F401  (presence check; df.to_parquet uses pyarrow engine)
    _PYARROW_OK = True
except ImportError:
    _PYARROW_OK = False


def parquet_path_for(csv_path: Path) -> Path:
    return csv_path.with_suffix(".parquet")


def _coerce_object_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == "object":
            out[c] = out[c].astype("string")
    return out


def save_parquet(df: pd.DataFrame, path: Path,
                 compression: str = "zstd") -> Path | None:
    """Write df to `path` (atomic via tmp + rename). Returns final path or None
    if pyarrow missing / df empty."""
    if not _PYARROW_OK or df.empty:
        return None
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    safe = _coerce_object_cols(df)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", suffix=".parquet", dir=str(path.parent))
    os.close(fd)
    tmp_path = Path(tmp)
    try:
        safe.to_parquet(tmp_path, engine="pyarrow", compression=compression, index=False)
        os.replace(tmp_path, path)
    except Exception:
        try: tmp_path.unlink(missing_ok=True)
        except Exception: pass
        raise
    return path
