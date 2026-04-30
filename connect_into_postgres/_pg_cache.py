"""Shared cached-PG-connection helper used by every PG-touching module.

Each consumer (run_summary, observability, pg_source, pg_tracking,
duckdb_source) holds one CachedConnection so failure is sticky for the
process lifetime — preventing per-call retry storms when PG is down.

Process-exit cleanup: every CachedConnection registers an atexit handler so
clean shutdowns release their slot back to PG immediately. Without this,
PG only reaps slots after the TCP keepalive timeout (~minutes).

Usage:
    from connect_into_postgres._pg_cache import CachedConnection
    _cache = CachedConnection("run-summary")
    conn = _cache.get()      # None on failure (silent after first warning)
    _cache.reset()           # force retry next call
    _cache.close()           # explicit close
"""
from __future__ import annotations

import atexit
import threading
import weakref


class CachedConnection:
    """Lazy module-scoped PG connection. First failure prints one warning;
    subsequent calls return None silently. Thread-safe via internal lock.
    Auto-closes at process exit so the slot is released immediately."""

    def __init__(self, name: str):
        self.name = name
        self._conn = None
        self._failed = False
        self._warned = False
        self._lock = threading.Lock()
        # atexit handler closes the connection on clean shutdown. Use a
        # weakref so the handler doesn't pin the cache alive past use.
        atexit.register(_atexit_close, weakref.ref(self))

    def get(self):
        """Return cached PG connection or None. None means PG is unreachable
        in this process — caller should silently no-op."""
        if self._conn is not None or self._failed:
            return self._conn
        try:
            from connect_into_postgres import connect_to_postgres as pg
            self._conn = pg.create_connection()
        except (Exception, SystemExit) as e:
            self._failed = True
            if not self._warned:
                print(f"[{self.name}] PG unreachable; calls silenced this "
                      f"process: {type(e).__name__}: {e}", flush=True)
                self._warned = True
        return self._conn

    @property
    def failed(self) -> bool:
        return self._failed

    @property
    def lock(self) -> threading.Lock:
        return self._lock

    def reset(self) -> None:
        """Clear cached state so the next get() retries. Useful in long-lived
        Streamlit sessions when PG comes back."""
        with self._lock:
            if self._conn is not None:
                try: self._conn.close()
                except Exception: pass
            self._conn = None
            self._failed = False
            self._warned = False

    def close(self) -> None:
        """Close the connection without resetting the failure flag."""
        with self._lock:
            if self._conn is not None:
                try: self._conn.close()
                except Exception: pass
                self._conn = None


def _atexit_close(weak_cache) -> None:
    """atexit handler that closes a CachedConnection if it's still alive."""
    cache = weak_cache()
    if cache is None:
        return
    try:
        cache.close()
    except Exception:
        pass
