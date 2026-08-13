"""Overall application coverage ratchet used by CI and unit tests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def required_percentage(baseline: dict[str, object]) -> float:
    recorded = float(baseline["overall_percent"])
    allowed_drop = float(baseline["maximum_drop_points"])
    absolute_floor = float(baseline["absolute_floor_percent"])
    return max(absolute_floor, recorded - allowed_drop)


def enforce_coverage(coverage: dict[str, object], baseline: dict[str, object]) -> float:
    totals = coverage.get("totals")
    if not isinstance(totals, dict) or "percent_covered" not in totals:
        raise ValueError("coverage JSON is missing totals.percent_covered")
    actual = float(totals["percent_covered"])
    required = required_percentage(baseline)
    if actual < required:
        raise RuntimeError(
            f"overall coverage {actual:.2f}% is below ratchet {required:.2f}%"
        )
    return actual


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    args = parser.parse_args()
    coverage = json.loads(args.coverage.read_text(encoding="utf-8"))
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    actual = enforce_coverage(coverage, baseline)
    print(
        f"Overall coverage ratchet passed: {actual:.2f}% >= "
        f"{required_percentage(baseline):.2f}%"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
