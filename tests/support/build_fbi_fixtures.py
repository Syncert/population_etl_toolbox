"""Build the reviewed FBI CDE test fixtures from captured provider payloads.

This is a maintenance utility, not a test. It reads payloads captured from the
official Crime Data Explorer API and writes the bounded fixtures under
``tests/fixtures/fbi_ucr``. Real provider bytes are preserved wherever they
were captured; derived scenario fixtures are written only for cases the live
source does not currently exhibit, and every derivation is recorded in
``tests/fixtures/fbi_ucr/SOURCE_NOTES.md``.

Usage::

    python -m tests.support.build_fbi_fixtures <captured-payload-directory> [...]

Each directory may hold ``summarized_*``/``agency_byStateAbbr_*`` JSON files.
Directories are searched in order, so a directory holding responses already
captured for the registered window takes precedence over one holding a wider
window that must be trimmed.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = REPOSITORY_ROOT / "tests/fixtures/fbi_ucr"

PERIODS = ("01-2023", "02-2023", "03-2023", "04-2023", "05-2023", "06-2023")
NATIONAL = "United States"
STATE = "Wisconsin"

#: Reviewed agency sample: ORI -> (published name, county keys, scenario).
AGENCIES: dict[str, tuple[str, tuple[str, ...], str]] = {
    "WI0130000": ("Dane County Sheriff's Office", ("DANE",), "captured"),
    "WI0137000": ("Fitchburg Police Department", ("DANE",), "full"),
    "WI0540300": ("Edgerton Police Department", ("DANE, ROCK",), "full"),
    "WI0050700": ("University of Wisconsin: Green Bay", ("BROWN",), "reported_zero"),
    "WI0400100": ("Menominee Tribal", ("NOT SPECIFIED",), "missing_report"),
    "WIWSP0000": ("Wisconsin State Patrol", ("NOT SPECIFIED",), "full"),
}


def _trim_months(node: Any) -> Any:
    if isinstance(node, dict):
        if (
            node
            and all(isinstance(key, str) for key in node)
            and any(key in PERIODS for key in node)
        ):
            return {period: node[period] for period in PERIODS if period in node}
        return {key: _trim_months(value) for key, value in node.items()}
    return node


def _load(name: str, directories: list[Path]) -> dict[str, Any] | None:
    for directory in directories:
        candidate = directory / f"{name}.json"
        if candidate.is_file():
            return json.loads(candidate.read_text(encoding="utf-8"))
    return None


def _write(name: str, document: Any) -> None:
    path = FIXTURE_ROOT / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(f"wrote {path.relative_to(REPOSITORY_ROOT)}")


def _series(values: list[int | None]) -> dict[str, int]:
    return {
        period: value
        for period, value in zip(PERIODS, values, strict=True)
        if value is not None
    }


def _derived_agency(ori: str, template: dict[str, Any]) -> dict[str, Any]:
    name, _counties, scenario = AGENCIES[ori]
    offenses = {
        "full": [7, 9, 6, 11, 8, 10],
        "reported_zero": [0, 1, 0, 2, 0, 1],
        "missing_report": [3, 4, None, None, 5, 6],
    }[scenario]
    clearances = {
        "full": [3, 4, 2, 5, 4, 4],
        "reported_zero": [0, 0, 0, 1, 0, 0],
        "missing_report": [1, 2, None, None, 2, 3],
    }[scenario]
    population = 30000
    participated = {
        period: (0 if offenses[index] is None else population)
        for index, period in enumerate(PERIODS)
    }
    document = json.loads(json.dumps(template))
    document["offenses"]["actuals"] = {
        f"{name} Offenses": _series(offenses),
        f"{name} Clearances": _series(clearances),
    }
    rates = {
        key: value
        for key, value in document["offenses"]["rates"].items()
        if key.startswith((NATIONAL, STATE))
    }
    rates[f"{name} Offenses"] = {
        period: round(value * 100000 / population, 2)
        for period, value in _series(offenses).items()
    }
    rates[f"{name} Clearances"] = {
        period: round(value * 100000 / population, 2)
        for period, value in _series(clearances).items()
    }
    document["offenses"]["rates"] = rates
    populations = document["populations"]
    for section in ("population", "participated_population"):
        populations[section] = {
            key: value
            for key, value in populations[section].items()
            if key in (NATIONAL, STATE)
        }
    populations["population"][name] = dict.fromkeys(PERIODS, population)
    populations["participated_population"][name] = participated
    return document


def main(argv: list[str]) -> int:
    directories = [Path(item) for item in argv[1:]]
    if not directories:
        print(__doc__)
        return 2

    national = _trim_months(_load("summarized_national_V", directories))
    state = _trim_months(_load("summarized_state_WI_V", directories))
    directory = _load("agency_byStateAbbr_WI", directories)
    if national is None or state is None or directory is None:
        print("missing captured national, state, or agency-directory payload")
        return 1

    _write("summarized_national_V", national)
    _write("summarized_state_WI_V", state)

    trimmed_directory: dict[str, list[dict[str, Any]]] = {}
    for ori, (_name, counties, _scenario) in AGENCIES.items():
        for county in counties:
            entries = [row for row in directory.get(county, []) if row["ori"] == ori]
            if not entries:
                print(f"agency {ori} is absent from county key {county}")
                return 1
            trimmed_directory.setdefault(county, []).extend(entries)
    _write(
        "agency_directory_WI",
        {key: trimmed_directory[key] for key in sorted(trimmed_directory)},
    )

    template: dict[str, Any] | None = None
    for ori in AGENCIES:
        captured = _trim_months(_load(f"summarized_agency_{ori}_V", directories))
        if captured is not None:
            if template is None:
                template = captured
            _write(f"summarized_agency_{ori}_V", captured)
    if template is None:
        print("no captured agency payload is available to derive scenarios from")
        return 1
    for ori in AGENCIES:
        if (FIXTURE_ROOT / f"summarized_agency_{ori}_V.json").is_file():
            continue
        _write(f"summarized_agency_{ori}_V", _derived_agency(ori, template))

    # Revision case: the same request fingerprint answered by a later refresh
    # with a corrected value.
    revised = json.loads(json.dumps(national))
    revised["cde_properties"]["last_refresh_date"]["UCR"] = "09/15/2026"
    offenses = revised["offenses"]["actuals"][f"{NATIONAL} Offenses"]
    offenses[PERIODS[0]] = offenses[PERIODS[0]] + 25
    _write("summarized_national_V_revised", revised)

    # api.data.gov answers a rejected request with a structured error document.
    _write(
        "provider_error_body",
        {
            "error": {
                "code": "API_KEY_MISSING",
                "message": (
                    "An API key was not provided. Please get one at "
                    "https://api.data.gov/signup/"
                ),
            }
        },
    )

    for extra in ("arrest_state_WI_ASS", "shr_state_WI"):
        payload = _load(extra, directories)
        if payload is not None:
            _write(extra, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
