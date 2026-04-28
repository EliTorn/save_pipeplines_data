"""Host + geo metadata used by every connector's connection-log row."""
from __future__ import annotations

import getpass
import json
import os
import platform
import socket
import urllib.request


def local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "unknown"
    finally:
        s.close()


def public_geo(timeout: float = 3.0, user_agent: str = "pipeline-connector/1.0") -> dict:
    endpoints = ("https://ipapi.co/json/", "https://ipinfo.io/json")
    for url in endpoints:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": user_agent})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                d = json.loads(r.read().decode("utf-8"))
            return {
                "public_ip": d.get("ip"),
                "country": d.get("country_name") or d.get("country"),
                "region": d.get("region") or d.get("region_name"),
                "city": d.get("city"),
                "org": d.get("org") or d.get("asn"),
                "geo_source": url,
            }
        except Exception:
            continue
    return {"public_ip": None, "country": None, "region": None, "city": None,
            "org": None, "geo_source": None}


def host_info(*, with_geo: bool = True, geo_timeout: float = 3.0,
              user_agent: str = "pipeline-connector/1.0") -> dict:
    info = {
        "hostname": socket.gethostname(),
        "fqdn": socket.getfqdn(),
        "os_user": getpass.getuser(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "pid": os.getpid(),
        "cwd": os.getcwd(),
        "local_ip": local_ip(),
    }
    if with_geo:
        info.update(public_geo(timeout=geo_timeout, user_agent=user_agent))
    return info
