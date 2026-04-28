"""LOGINLOGOUTINFO index adapter. Uses only common_lambdas; no extras."""
from __future__ import annotations

from core.adapter import IndexAdapter


class Adapter(IndexAdapter):
    INDEX_NAME = "loginlogoutinfo"
