"""Census variables that join an ACS series (decennial-2000-and-1990-history).

A census dataset is requested only for the variables `decennial_crosswalk.json`
names, and each one is stored under the ACS variable it measures, with its
own dataset code and its source variable kept beside it. Nothing is matched
by label at load time: a pairing is a reviewed declaration, not an inference.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from .config import CENSUS_DATASETS, CONFIG

CROSSWALK_PATH = Path(__file__).with_name("decennial_crosswalk.json")


@dataclass(frozen=True)
class CrosswalkEntry:
    dataset: str
    source_variable: str
    acs_variable: str
    label: str
    dollars_of: int | None = None
    note: str | None = None


def parse_crosswalk(document: dict) -> dict[str, dict[str, CrosswalkEntry]]:
    """Validate the crosswalk document and index it by dataset and source variable.

    Every dataset must be a census dataset the registry names (one without
    margins); every target must be an ACS estimate (`_NNNE`) of a curated
    table; and no two source variables of a dataset may claim one target.
    """
    curated = set(CONFIG.curated_tables)
    index: dict[str, dict[str, CrosswalkEntry]] = {}
    for dataset, variables in document["datasets"].items():
        spec = CENSUS_DATASETS.get(dataset)
        if spec is None or spec.margins:
            raise ValueError(
                f"crosswalk names {dataset!r}, which is not a census dataset"
            )
        entries: dict[str, CrosswalkEntry] = {}
        for source, item in variables.items():
            target = item["acs"]
            table, _, cell = target.partition("_")
            if table not in curated or not (cell[:-1].isdigit() and cell.endswith("E")):
                raise ValueError(
                    f"{dataset} {source} names {target!r}, not a curated ACS estimate"
                )
            entries[source] = CrosswalkEntry(
                dataset=dataset,
                source_variable=source,
                acs_variable=target,
                label=item["label"],
                dollars_of=item.get("dollars_of"),
                note=item.get("note"),
            )
        targets = [entry.acs_variable for entry in entries.values()]
        if len(set(targets)) != len(targets):
            raise ValueError(f"{dataset} maps two source variables to one ACS variable")
        index[dataset] = entries
    return index


@lru_cache(maxsize=1)
def crosswalk() -> dict[str, dict[str, CrosswalkEntry]]:
    return parse_crosswalk(json.loads(CROSSWALK_PATH.read_text(encoding="utf-8")))


def requested_variables(dataset: str) -> list[str]:
    """The source variables a census dataset is asked for, sorted."""
    return sorted(crosswalk().get(dataset, {}))


def acs_variable_for(dataset: str, source_variable: str) -> str:
    """The ACS variable a census value is stored under; refuses an unnamed one."""
    try:
        return crosswalk()[dataset][source_variable].acs_variable
    except KeyError:
        raise ValueError(
            f"{dataset} {source_variable} is not in the decennial crosswalk"
        ) from None
