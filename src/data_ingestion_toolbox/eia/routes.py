"""The registry of EIA API v2 routes the warehouse reads (home-heating-fuel-prices).

One generalisation of the adapter serves every route: each entry names the
route, the facets it asks for, the data column, the frequency, the first
period a full history read asks for, how many periods a routine read goes
back, the units EIA states, and the citation. Retail gasoline is the first
entry and keeps its request exactly, so its captures, fingerprints and
replay are unchanged (``gasoline_request`` below reproduces the request the
adapter made before the registry). The other entries land in
``silver_eia.fact_energy_price``; gasoline keeps ``fact_retail_price``.

Every metric is one EIA product (and, where the route has one, process) at
every area EIA publishes it for. Areas are read from their codes only:
``US``/``NUS`` the nation, a USPS code (``WI``) or ``S`` and a USPS code
(``SWI``) a state, ``R`` codes PADDs and ``Y`` codes cities (EIA's own
provider areas). Nothing is matched by name.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from .registry import PROCESS as GASOLINE_PROCESS
from .registry import PRODUCTS as GASOLINE_PRODUCTS

#: The route key every gasoline read carries, and the default of
#: ``control.eia_read.route``.
GASOLINE = "gasoline"


@dataclass(frozen=True)
class EnergyMetric:
    """One served measure: a product (and process) at every area."""

    #: The publisher's key; the catalog code is ``EIA:<key>``.
    key: str
    #: The product facet value that names it in an answer row.
    product: str
    #: The process facet value, where the route has one.
    process: str | None
    title: str
    #: EIA's unit text as it appears in the answer.
    source_unit: str
    #: The plain unit the catalog states.
    unit: str


@dataclass(frozen=True)
class EiaRoute:
    key: str
    #: The API v2 route, without ``/data/``.
    route: str
    #: ``weekly`` or ``monthly``.
    frequency: str
    #: The data column the value is in (``value``; electricity's ``price``).
    data_column: str
    #: The answer's column naming the area (``duoarea``; electricity's ``stateid``).
    area_column: str
    #: The answer's column naming the product (``product``; electricity's ``sectorid``).
    product_column: str
    #: The answer's column naming the process, or None.
    process_column: str | None
    #: The answer's column holding the unit text.
    unit_column: str
    #: The answer's column holding EIA's series id, or None where the route
    #: carries none (the series id is then composed from the route, area and
    #: product, so it is stable across reads).
    series_column: str | None
    #: The facets every request asks for, in request order.
    facets: tuple[tuple[str, tuple[str, ...]], ...]
    metrics: tuple[EnergyMetric, ...]
    history_start: date
    #: How many periods a routine read goes back, so a revision is read again.
    refresh_periods: int
    #: Months EIA surveys; a value outside them is a data-quality warning.
    survey_months: tuple[int, ...] = tuple(range(1, 13))
    citation: str = ""
    reference_url: str = ""
    #: The provider dataset name in ``silver_ref.geography_resolution``.
    dataset: str = ""

    @property
    def data_route(self) -> str:
        return f"{self.route}/data/"

    @property
    def parser_contract_version(self) -> str:
        return f"eia_v2_{self.route.replace('/', '_')}_json:v1"

    def metric_for(
        self, product: str | None, process: str | None
    ) -> EnergyMetric | None:
        for metric in self.metrics:
            if metric.product == product and (
                metric.process is None or metric.process == process
            ):
                return metric
        return None


ROUTES: dict[str, EiaRoute] = {
    route.key: route
    for route in (
        EiaRoute(
            key=GASOLINE,
            route="petroleum/pri/gnd",
            frequency="weekly",
            data_column="value",
            area_column="duoarea",
            product_column="product",
            process_column="process",
            unit_column="units",
            series_column="series",
            facets=(
                ("product", tuple(sorted(GASOLINE_PRODUCTS))),
                ("process", (GASOLINE_PROCESS,)),
            ),
            metrics=tuple(
                EnergyMetric(
                    code,
                    code,
                    GASOLINE_PROCESS,
                    title,
                    "$/GAL",
                    "U.S. dollars per gallon",
                )
                for code, title in sorted(GASOLINE_PRODUCTS.items())
            ),
            history_start=date(2015, 1, 5),
            refresh_periods=8,
            citation=(
                "Source: U.S. Energy Information Administration, Gasoline and "
                "Diesel Fuel Update (EIA-878), weekly retail gasoline prices"
            ),
            reference_url="https://www.eia.gov/petroleum/gasdiesel/",
            dataset="petroleum_pri_gnd",
        ),
        EiaRoute(
            key="diesel",
            route="petroleum/pri/gnd",
            frequency="weekly",
            data_column="value",
            area_column="duoarea",
            product_column="product",
            process_column="process",
            unit_column="units",
            series_column="series",
            facets=(("product", ("EPD2D",)), ("process", ("PTE",))),
            metrics=(
                EnergyMetric(
                    "EPD2D",
                    "EPD2D",
                    "PTE",
                    "On-highway diesel fuel",
                    "$/GAL",
                    "U.S. dollars per gallon",
                ),
            ),
            history_start=date(2015, 1, 5),
            refresh_periods=8,
            citation=(
                "Source: U.S. Energy Information Administration, Gasoline and "
                "Diesel Fuel Update (EIA-888), weekly retail on-highway diesel prices"
            ),
            reference_url="https://www.eia.gov/petroleum/gasdiesel/",
            dataset="petroleum_pri_gnd",
        ),
        EiaRoute(
            key="res_electricity",
            route="electricity/retail-sales",
            frequency="monthly",
            data_column="price",
            area_column="stateid",
            product_column="sectorid",
            process_column=None,
            unit_column="price-units",
            series_column=None,
            facets=(("sectorid", ("RES",)),),
            metrics=(
                EnergyMetric(
                    "ELEC_RES",
                    "RES",
                    None,
                    "Residential electricity",
                    "cents per kilowatt-hour",
                    "cents per kilowatt-hour",
                ),
            ),
            history_start=date(2001, 1, 1),
            refresh_periods=14,
            citation=(
                "Source: U.S. Energy Information Administration, Electric Power "
                "Monthly (Form EIA-861M), average retail price of electricity to "
                "the residential sector"
            ),
            reference_url="https://www.eia.gov/electricity/monthly/",
            dataset="electricity_retail_sales",
        ),
        EiaRoute(
            key="res_natural_gas",
            route="natural-gas/pri/sum",
            frequency="monthly",
            data_column="value",
            area_column="duoarea",
            product_column="product",
            process_column="process",
            unit_column="units",
            series_column="series",
            facets=(("process", ("PRS",)),),
            metrics=(
                EnergyMetric(
                    "NG_RES",
                    "EPG0",
                    "PRS",
                    "Residential natural gas",
                    "$/MCF",
                    "U.S. dollars per thousand cubic feet",
                ),
            ),
            history_start=date(1973, 1, 1),
            refresh_periods=14,
            citation=(
                "Source: U.S. Energy Information Administration, Natural Gas "
                "Monthly, price of natural gas delivered to residential consumers"
            ),
            reference_url="https://www.eia.gov/naturalgas/monthly/",
            dataset="natural_gas_pri_sum",
        ),
        EiaRoute(
            key="heating_fuels",
            route="petroleum/pri/wfr",
            frequency="weekly",
            data_column="value",
            area_column="duoarea",
            product_column="product",
            process_column="process",
            unit_column="units",
            series_column="series",
            facets=(("product", ("EPD2F", "EPLLPA")), ("process", ("PRS",))),
            metrics=(
                EnergyMetric(
                    "EPD2F",
                    "EPD2F",
                    "PRS",
                    "Residential heating oil",
                    "$/GAL",
                    "U.S. dollars per gallon",
                ),
                EnergyMetric(
                    "EPLLPA",
                    "EPLLPA",
                    "PRS",
                    "Residential propane",
                    "$/GAL",
                    "U.S. dollars per gallon",
                ),
            ),
            history_start=date(1990, 10, 1),
            refresh_periods=8,
            # The State Heating Oil and Propane Program surveys October to March.
            survey_months=(10, 11, 12, 1, 2, 3),
            citation=(
                "Source: U.S. Energy Information Administration, State Heating "
                "Oil and Propane Program (EIA-877), weekly residential prices, "
                "October to March"
            ),
            reference_url="https://www.eia.gov/petroleum/heatingoilpropane/",
            dataset="petroleum_pri_wfr",
        ),
    )
}

#: The routes ``fact_energy_price`` holds: every one but gasoline.
ENERGY_ROUTES: tuple[str, ...] = tuple(key for key in ROUTES if key != GASOLINE)


def route_for(key: str) -> EiaRoute:
    try:
        return ROUTES[key]
    except KeyError:
        raise ValueError(f"no EIA route is registered as {key!r}") from None


def request_parameters(
    route: EiaRoute, start: str, end: str | None, *, offset: int, length: int
) -> list[tuple[str, str]]:
    """The query for one page of a window, without the key.

    Gasoline's is exactly the request the adapter made before the registry:
    the grades, then the process, sorted by week and series.
    """
    parameters = [
        ("frequency", route.frequency),
        ("data[0]", route.data_column),
        *(
            (f"facets[{facet}][]", value)
            for facet, values in route.facets
            for value in values
        ),
        ("start", start),
    ]
    if end is not None:
        parameters.append(("end", end))
    second = route.series_column or route.area_column
    parameters += [
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
        ("sort[1][column]", second),
        ("sort[1][direction]", "asc"),
    ]
    if route.series_column is None:
        # No series id: the product breaks a tie between two rows of an area.
        parameters += [
            ("sort[2][column]", route.product_column),
            ("sort[2][direction]", "asc"),
        ]
    parameters += [("offset", str(offset)), ("length", str(length))]
    return parameters


def recorded_parameters(
    route: EiaRoute, start: str, end: str | None, *, offset: int, length: int
) -> dict:
    """What a request asked, as recorded with its capture; never the key.

    Gasoline's record is the one it always had, so its fingerprints hold.
    """
    if route.key == GASOLINE:
        return {
            "start": start,
            "end": end,
            "offset": offset,
            "length": length,
            "products": sorted(GASOLINE_PRODUCTS),
            "process": GASOLINE_PROCESS,
        }
    return {
        "route": route.key,
        "start": start,
        "end": end,
        "offset": offset,
        "length": length,
        "facets": {facet: list(values) for facet, values in route.facets},
    }
