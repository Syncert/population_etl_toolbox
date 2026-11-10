"""The reviewed per-capita and share pairings (ADR-0009).

The registry is ``docs/semantics/derived_ratios.json`` (schema
``derived_ratio.schema.json``): one entry per derived view, binding a
numerator metric to a denominator metric -- or, where the provider publishes
the rate itself, to that rate -- with the view's kind and scale, the universe
the result describes, owner, reviewer, status and citations. Nothing is
paired by inference from names or units.

An ACS entry is written once with a ``{dataset}`` placeholder in both codes
and expands to one pairing per listed dataset, ``<ratio_id>:<dataset>``, so a
5-year count is only ever divided by a 5-year total.

A ``ratio`` entry divides one measure by another of the same release that
it is not a part of -- median real estate taxes over median home value --
with the Bureau's ratio formula for its margin; it is neither a share of a
universe nor a value per resident.

An ``agency_rate`` entry (ADR-0012) is a county crime rate computed the
FBI's way, in gold, from agency reports: it names the count it divides, the
FBI rate it reads beside (``reads_beside``, published only for states and the
nation) and the one geography level it describes. It recomputes no rate the
provider publishes at that level, so ADR-0009 rule 5 is not at stake.

A ``time_cost`` entry (ADR-0009 amendment of 2026-11-06) is minutes of work:
a price divided by an hourly wage, the wage being a weekly wage over
``hours_per_week``. The price and the wage are different programs and
usually different areas -- the price is BLS's for a Census region or the
nation, the wage QCEW's for the reader's own county, state or nation -- so
rule 2 is replaced for this kind by: the wage's area is named by the reader
and stated beside the value, the wage's level is one of ``wage_geo_levels``,
and the wage's calendar year matches the price's (the newest wage year when
the price's year has none yet, and the row says so). An ``{area}``
placeholder in the numerator expands to each listed BLS area code, as
``<ratio_id>:<area>``.

A county childcare price (Nick, 2026-11-08) is the other time cost: the
Women's Bureau's weekly price is published for the county itself, so it
names no ``{area}`` and its wage is that county's own (``wage_geo_levels``
is ``COUNTY`` only). A HUD Fair Market Rent (Nick, 2026-11-10) is the same
shape: HUD sets the rent for the county's HUD area and repeats it for each
county in it, and the wage is the county's own.

Only an ``approved`` pairing is served. ``DERIVED_RATIOS_SERVE_DRAFTS=1``
lets a local stack serve drafts too, each marked with its status, so the
owner can review them on the running site before approving them.
"""

from __future__ import annotations

import json
from decimal import Decimal
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

KINDS = frozenset(
    {"share", "per_capita", "ratio", "provider_rate", "agency_rate", "time_cost"}
)
STATUSES = frozenset({"draft", "approved", "deprecated", "needs_review"})
#: 60 is a time cost's: minutes in an hour of work.
SCALES = frozenset({1, 60, 100, 1000, 100000})
DATASET_PLACEHOLDER = "{dataset}"
AREA_PLACEHOLDER = "{area}"
#: The levels QCEW publishes a wage for (bls-qcew-county-wages).
TIME_COST_WAGE_LEVELS = frozenset({"COUNTY", "STATE", "NATIONAL"})
#: The price programs a time cost divides: BLS's region and nation prices,
#: each over its listed areas, and the Women's Bureau's county childcare
#: prices and HUD's Fair Market Rents, each over the county's own wage.
TIME_COST_PRICES = {"BLS": "areas", "DOL_CHILDCARE": "county", "HUD_FMR_IL": "county"}

REGISTRY_PATH = (
    Path(__file__).resolve().parents[3] / "docs" / "semantics" / "derived_ratios.json"
)

#: Set to ``1`` on a local stack to serve draft pairings, each row marked
#: ``pairing_status: draft``. Off by default: ADR-0009 serves only approved
#: pairings. Listed in ``infra/docker/stack.env.example``.
SERVE_DRAFTS_ENV = "DERIVED_RATIOS_SERVE_DRAFTS"


def serve_drafts() -> bool:
    return os.environ.get(SERVE_DRAFTS_ENV, "").strip().lower() in {"1", "true", "yes"}


class RatioRegistryError(ValueError):
    """The registry breaks its own contract."""


@dataclass(frozen=True)
class RatioPairing:
    ratio_id: str
    kind: str
    scale: int
    unit: str
    label: str
    numerator: str
    denominator: str | None
    provider_rate: str | None
    dataset: str | None
    denominator_label: str
    universe: str
    numerator_is_subset: bool
    controlled_denominator_levels: frozenset[str]
    status: str
    reviewer: str
    version: int
    limitations: tuple[str, ...]
    citations: tuple[str, ...]
    #: ``agency_rate`` only: the provider rate this reads beside, and the
    #: levels it is computed for.
    reads_beside: str | None = None
    geo_levels: tuple[str, ...] = ()
    #: ``time_cost`` only: hours in the week the wage pays for, the levels
    #: the wage may be read at, and the price's BLS area code.
    hours_per_week: int | None = None
    wage_geo_levels: tuple[str, ...] = ()
    area: str | None = None
    #: ``share`` only: further lines of the numerator's own ACS table added
    #: to it before dividing, as the Bureau's profile tables do (DP03's
    #: "all parents in the labor force" is B23008_004 + _010 + _013).
    numerator_addends: tuple[str, ...] = ()

    @property
    def derived(self) -> bool:
        """True when the API computes the value; a provider rate is the provider's."""
        return self.kind != "provider_rate"

    def is_served(self, drafts: bool | None = None) -> bool:
        if self.status == "approved":
            return True
        allow = serve_drafts() if drafts is None else drafts
        return allow and self.status == "draft"

    def refusal(self) -> str | None:
        """Why this pairing is not served, or ``None`` when it is."""
        if self.is_served():
            return None
        return (
            f"pairing '{self.ratio_id}' is {self.status}, not approved; only an "
            "approved pairing is served (ADR-0009)"
        )


def _program(code: str) -> tuple[str, ...]:
    """The program a metric code belongs to: its source, and the ACS dataset."""
    parts = code.split(":")
    return tuple(parts[:2]) if parts[0] == "CENSUS_ACS" else (parts[0],)


def _table(code: str) -> str:
    """The ACS table a variable code names: ``B23008`` of ``...:B23008_004``."""
    return code.rsplit(":", 1)[-1].split("_", 1)[0]


def _expand(entry: dict) -> list[tuple[str, str, str | None, str | None]]:
    """(ratio_id, numerator, denominator, dataset or area) for each listed one."""
    areas = entry.get("areas")
    if bool(areas) != (AREA_PLACEHOLDER in entry["numerator"]):
        raise RatioRegistryError(
            f"{entry['ratio_id']}: `areas` and the {AREA_PLACEHOLDER} "
            "placeholder come together"
        )
    if areas:
        return [
            (
                f"{entry['ratio_id']}:{area}",
                entry["numerator"].replace(AREA_PLACEHOLDER, area),
                entry.get("denominator"),
                area,
            )
            for area in areas
        ]
    datasets = entry.get("datasets")
    numerator = entry["numerator"]
    denominator = entry.get("denominator")
    templated = DATASET_PLACEHOLDER in numerator or DATASET_PLACEHOLDER in (
        denominator or ""
    )
    if bool(datasets) != templated:
        raise RatioRegistryError(
            f"{entry['ratio_id']}: `datasets` and the {DATASET_PLACEHOLDER} "
            "placeholder come together"
        )
    if not datasets:
        return [(entry["ratio_id"], numerator, denominator, None)]
    return [
        (
            f"{entry['ratio_id']}:{dataset}",
            numerator.replace(DATASET_PLACEHOLDER, dataset),
            (denominator or "").replace(DATASET_PLACEHOLDER, dataset) or None,
            dataset,
        )
        for dataset in datasets
    ]


def parse_registry(document: dict) -> dict[str, RatioPairing]:
    """Each pairing by its expanded id, or RatioRegistryError naming the broken rule."""
    pairings: dict[str, RatioPairing] = {}
    for entry in document.get("entries", []):
        rid = entry["ratio_id"]
        kind = entry["kind"]
        if kind not in KINDS or entry["status"] not in STATUSES:
            raise RatioRegistryError(f"{rid} names an unknown kind or status")
        if entry["scale"] not in SCALES:
            raise RatioRegistryError(f"{rid} has scale {entry['scale']!r}")
        provider = kind == "provider_rate"
        agency = kind == "agency_rate"
        if agency:
            if (
                entry.get("denominator")
                or entry.get("provider_rate")
                or not entry.get("reads_beside")
                or entry.get("geo_levels") != ["COUNTY"]
                or entry["scale"] != 100000
                or not entry["numerator"].startswith("FBI_UCR:")
            ):
                raise RatioRegistryError(
                    f"{rid}: an agency_rate divides an FBI count by its own "
                    "agencies' populations, per 100,000, for counties only; it "
                    "names the FBI rate it reads beside and no denominator "
                    "(ADR-0012)"
                )
        elif provider != bool(entry.get("provider_rate")) or provider == bool(
            entry.get("denominator")
        ):
            raise RatioRegistryError(
                f"{rid}: a provider_rate names the provider's rate and no "
                "denominator; any other kind names a denominator"
            )
        if kind == "share" and (
            entry.get("numerator_is_subset") is not True or entry["scale"] != 100
        ):
            raise RatioRegistryError(
                f"{rid}: a share's numerator is a subset of its universe and "
                "it is stated in percent (ADR-0009 rule 3)"
            )
        time_cost = kind == "time_cost"
        if time_cost != bool(entry.get("hours_per_week")) or (
            time_cost
            and (
                entry["scale"] != 60
                or entry.get("datasets")
                or entry.get("numerator_is_subset")
                or not entry.get("wage_geo_levels")
                or not set(entry["wage_geo_levels"]) <= TIME_COST_WAGE_LEVELS
                or _program(entry["numerator"])[0] not in TIME_COST_PRICES
                or (
                    TIME_COST_PRICES[_program(entry["numerator"])[0]] == "areas"
                    and not entry.get("areas")
                )
                or (
                    TIME_COST_PRICES[_program(entry["numerator"])[0]] == "county"
                    and (entry.get("areas") or entry["wage_geo_levels"] != ["COUNTY"])
                )
                or not entry["denominator"].startswith("BLS_QCEW:")
            )
        ):
            raise RatioRegistryError(
                f"{rid}: a time_cost divides a BLS price over its listed areas, "
                "or a county childcare price or HUD rent over the county's own "
                "wage, by a "
                "QCEW weekly wage over hours_per_week, in minutes (scale 60), "
                "at the wage levels QCEW publishes; only a time_cost names "
                "hours_per_week (ADR-0009 amendment, 2026-11-06)"
            )
        if kind == "ratio" and entry.get("numerator_is_subset"):
            raise RatioRegistryError(
                f"{rid}: a ratio divides one measure by another it is not a "
                "part of; a part of its denominator is a share"
            )
        if entry["status"] == "approved" and "pending" in entry["reviewer"].lower():
            raise RatioRegistryError(f"{rid} is approved without a reviewer")
        if not entry.get("citations"):
            raise RatioRegistryError(f"{rid} cites nothing")
        addends = list(entry.get("numerator_addends", ()))
        if addends and (
            kind != "share"
            or not entry["numerator"].startswith("CENSUS_ACS:")
            or len(set(addends)) != len(addends)
            or entry["numerator"] in addends
            or any(_table(a) != _table(entry["numerator"]) for a in addends)
        ):
            raise RatioRegistryError(
                f"{rid}: numerator_addends are further lines of the "
                "numerator's own ACS table, each named once, added to it in a "
                "share (ADR-0009 amendment, 2026-11-10)"
            )
        for ratio_id, numerator, denominator, variant in _expand(entry):
            if ratio_id in pairings:
                raise RatioRegistryError(f"{ratio_id} has two entries")
            other = (
                denominator or entry.get("provider_rate") or entry.get("reads_beside")
            )
            # A time cost's two programs are its point (the amendment above).
            if not time_cost and _program(numerator) != _program(other):
                raise RatioRegistryError(
                    f"{ratio_id} pairs {numerator} with {other}: numerator and "
                    "denominator come from the same program and dataset "
                    "(ADR-0009 rule 2)"
                )
            pairings[ratio_id] = RatioPairing(
                ratio_id=ratio_id,
                kind=kind,
                scale=int(entry["scale"]),
                unit=entry["unit"],
                label=entry["label"],
                numerator=numerator,
                denominator=denominator,
                provider_rate=entry.get("provider_rate"),
                dataset=None if time_cost else variant,
                denominator_label=entry["denominator_label"],
                universe=entry["universe"],
                numerator_is_subset=bool(entry.get("numerator_is_subset")),
                controlled_denominator_levels=frozenset(
                    entry.get("controlled_denominator_levels", ())
                ),
                status=entry["status"],
                reviewer=entry["reviewer"],
                version=int(entry["version"]),
                limitations=tuple(entry.get("limitations", ())),
                citations=tuple(entry["citations"]),
                reads_beside=entry.get("reads_beside"),
                geo_levels=tuple(entry.get("geo_levels", ())),
                hours_per_week=entry.get("hours_per_week"),
                wage_geo_levels=tuple(entry.get("wage_geo_levels", ())),
                area=variant if time_cost else None,
                numerator_addends=tuple(
                    a.replace(DATASET_PLACEHOLDER, variant or "") for a in addends
                ),
            )
    rated = {p.numerator for p in pairings.values() if p.kind == "provider_rate"}
    for pairing in pairings.values():
        # An agency rate is computed only where the provider publishes no
        # rate (ADR-0012), so it recomputes nothing beside the provider's.
        if (
            pairing.derived
            and pairing.kind not in {"agency_rate", "time_cost"}
            and pairing.numerator in rated
        ):
            raise RatioRegistryError(
                f"{pairing.ratio_id} recomputes a rate its provider publishes "
                f"for {pairing.numerator} (ADR-0009 rule 5)"
            )
    return pairings


@lru_cache(maxsize=1)
def load_registry(path: Path = REGISTRY_PATH) -> dict[str, RatioPairing]:
    return parse_registry(json.loads(path.read_text(encoding="utf-8")))


def pairing_for(ratio_id: str) -> RatioPairing | None:
    return load_registry().get(ratio_id)


def pairings_for_numerator(metric_code: str) -> list[RatioPairing]:
    return [p for p in load_registry().values() if p.numerator == metric_code]


def derived_margin(
    estimate: Decimal,
    numerator_moe: Decimal,
    denominator: Decimal,
    denominator_moe: Decimal,
    subset: bool,
) -> tuple[Decimal, str]:
    """The Bureau's margin of error for ``estimate = X / Y`` (unscaled), and its method."""
    numerator_term = numerator_moe * numerator_moe
    denominator_term = estimate * estimate * denominator_moe * denominator_moe
    if subset:
        radicand = numerator_term - denominator_term
        if radicand >= 0:
            return radicand.sqrt() / denominator, "proportion"
        return (numerator_term + denominator_term).sqrt() / denominator, (
            "ratio_fallback"
        )
    return (numerator_term + denominator_term).sqrt() / denominator, "ratio"
