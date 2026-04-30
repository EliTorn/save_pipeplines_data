"""Per-table buffered parquet shard writer.

Owned by the parent-process listener thread. Workers never call this
directly — they push events via the multiprocessing queue and the
listener routes rows here.

Crash-safety: each shard `part_NNNN.parquet` is immutable. Buffer flushes
when row count >= LOG_SHARD_ROWS, age >= LOG_SHARD_SECONDS, or close().
DuckDB reads `<table>/*.parquet` as one logical dataset.
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline_logging.schemas import SCHEMAS


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


SHARD_ROWS = _env_int("LOG_SHARD_ROWS", 5000)
SHARD_SECONDS = _env_int("LOG_SHARD_SECONDS", 30)


def _coerce_ts(v: Any):
    """Coerce ISO string / datetime / None to tz-aware UTC datetime."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        s = str(v).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


class TableSink:
    """Buffered shard writer for one parquet table inside one run dir."""

    def __init__(self, table: str, dir_path: Path):
        self.table = table
        self.schema = SCHEMAS[table]
        self.dir = dir_path
        self.dir.mkdir(parents=True, exist_ok=True)
        self._buf: list[dict] = []
        self._buf_started_at: float | None = None
        self._shard_idx = self._scan_existing_shards()
        self._lock = threading.Lock()

    def _scan_existing_shards(self) -> int:
        """Resume shard numbering past whatever already exists."""
        max_idx = 0
        for p in self.dir.glob("part_*.parquet"):
            try:
                idx = int(p.stem.split("_")[1])
                if idx > max_idx:
                    max_idx = idx
            except (ValueError, IndexError):
                continue
        return max_idx

    def append(self, row: dict) -> None:
        with self._lock:
            self._buf.append(row)
            if self._buf_started_at is None:
                self._buf_started_at = time.monotonic()
            should_flush = (
                len(self._buf) >= SHARD_ROWS
                or (time.monotonic() - self._buf_started_at) >= SHARD_SECONDS
            )
        if should_flush:
            self.flush()

    def flush(self) -> Path | None:
        with self._lock:
            if not self._buf:
                self._buf_started_at = None
                return None
            rows = self._buf
            self._buf = []
            self._buf_started_at = None
            self._shard_idx += 1
            shard_idx = self._shard_idx
        try:
            return self._write_shard(rows, shard_idx)
        except Exception as e:
            # Roll the shard counter back so we don't leave a gap.
            with self._lock:
                self._shard_idx -= 1
            print(f"[parquet_sink:{self.table}] flush failed "
                  f"({type(e).__name__}: {e}); dropping {len(rows)} rows",
                  flush=True)
            return None

    def _write_shard(self, rows: list[dict], shard_idx: int) -> Path:
        cols: dict[str, list] = {f.name: [] for f in self.schema}
        ts_fields = {
            f.name for f in self.schema
            if pa.types.is_timestamp(f.type)
        }
        for r in rows:
            for name in cols:
                v = r.get(name)
                if name in ts_fields:
                    v = _coerce_ts(v)
                cols[name].append(v)
        table = pa.table(cols, schema=self.schema)
        path = self.dir / f"part_{shard_idx:04d}.parquet"
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(
            table, tmp,
            compression="zstd",
            use_dictionary=True,
            data_page_size=1 * 1024 * 1024,
        )
        os.replace(tmp, path)
        return path

    def close(self) -> None:
        self.flush()


class RunSink:
    """All five tables for one run, sharing one root dir."""

    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.tables: dict[str, TableSink] = {
            t: TableSink(t, run_dir / t) for t in SCHEMAS
        }

    def append(self, table: str, row: dict) -> None:
        sink = self.tables.get(table)
        if sink is None:
            print(f"[parquet_sink] unknown table {table!r}", flush=True)
            return
        sink.append(row)

    def flush_all(self) -> None:
        for s in self.tables.values():
            s.flush()

    def close(self) -> None:
        for s in self.tables.values():
            s.close()
