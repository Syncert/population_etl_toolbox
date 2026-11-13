"""The state tile-wait script's plan, without a stack (place tiles by state).

Covers: MARTIN-013
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "measure_state_tile_wait.py"
_spec = importlib.util.spec_from_file_location("measure_state_tile_wait", SCRIPT)
wait = importlib.util.module_from_spec(_spec)
sys.modules["measure_state_tile_wait"] = wait
_spec.loader.exec_module(wait)


def test_the_tiles_measured_are_the_ones_under_the_maps_opening_canvas() -> None:
    """Covers: MARTIN-013 — Texas at z6 in the 1100 x 360 canvas is three by two 512 px tiles."""
    tiles = wait.viewport_tiles(-99.3, 31.2, 6, 1100, 360)
    assert tiles == [(6, x, y) for x in (13, 14, 15) for y in (25, 26)]
    # The centre tile contains the point it is centred on.
    x, y = wait.world_pixel(-99.3, 31.2, 6)
    assert (6, int(x // 512), int(y // 512)) in tiles
    # A canvas at the world's edge never asks for a tile off it.
    assert all(
        0 <= tx < 2 and 0 <= ty < 2
        for _, tx, ty in wait.viewport_tiles(-179.9, 84.0, 1, 1100, 360)
    )


def test_each_set_names_the_vintage_and_only_one_names_the_state() -> None:
    """Covers: MARTIN-013 — the comparison differs only in ?state=, and a run is measured cold."""
    before = wait.tile_url("http://h/tiles/", (6, 14, 26), 2025, None, "abc")
    after = wait.tile_url("http://h/tiles", (6, 14, 26), 2025, "48", "abc")
    assert before == "http://h/tiles/places/6/14/26?vintage=2025&_m=abc"
    assert after == "http://h/tiles/places/6/14/26?vintage=2025&state=48&_m=abc"
    assert wait.tile_url("http://h/tiles", (6, 1, 2), 2025, "48", None).endswith(
        "?vintage=2025&state=48"
    )


def test_the_report_compares_both_sets_and_fails_on_a_failed_tile(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Covers: MARTIN-013 — bytes and wall time saved are reported; a failed tile exits 1."""

    def fake_fetch(url: str) -> dict:
        size = 100 if "state=48" in url else 400
        return {
            "url": url,
            "status": 200,
            "bytes": size,
            "ms": 10.0,
            "cache_control": None,
        }

    monkeypatch.setattr(wait, "_fetch", fake_fetch)
    assert wait.main(["--tiles-url", "http://h/tiles"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["all_states"]["tiles"] == report["one_state"]["tiles"] == 6
    assert report["saved"]["bytes"] == 6 * 300
    assert report["cold"] is True

    monkeypatch.setattr(
        wait,
        "_fetch",
        lambda url: {
            "url": url,
            "status": 500,
            "bytes": 0,
            "ms": 1.0,
            "cache_control": None,
        },
    )
    assert wait.main(["--tiles-url", "http://h/tiles", "--warm"]) == 1
    assert json.loads(capsys.readouterr().out)["one_state"]["failed"]
