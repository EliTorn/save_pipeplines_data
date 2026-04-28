"""ES-connector geo/host metadata — thin shim around _pipeline_host."""
from __future__ import annotations

import sys
from pathlib import Path

_PARENT = Path(__file__).resolve().parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from _pipeline_host import host_info as _host_info, local_ip, public_geo  # noqa: E402

_USER_AGENT = "es-connector/1.0"


def host_info() -> dict:
    return _host_info(user_agent=_USER_AGENT)
