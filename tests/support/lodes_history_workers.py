"""Picklable worker stand-ins for the LODES history loader's process tests.

Spawned workers import these by name, so they live in a module rather than
inside a test.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any

from data_ingestion_toolbox.census_lodes.history import SliceResult


def no_database() -> Callable[[], Any]:
    def refuse() -> Any:
        raise AssertionError("the stand-in work opens no connection")

    return refuse


def loads(
    _factory: Callable[[], Any], state: str, year: int, _run: str | None
) -> SliceResult:
    return SliceResult(state, year, "silver_ready", rows=1, seconds=0.0)


def az_hangs_ar_dies(
    factory: Callable[[], Any], state: str, year: int, run: str | None
) -> SliceResult:
    if state == "az":
        time.sleep(3600)
    if state == "ar":
        os._exit(3)
    return loads(factory, state, year, run)
