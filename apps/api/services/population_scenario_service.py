"""API-owned, assumption-based population scenarios over stable published inputs."""

from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from apps.api.schemas.observations import NeutralObservation
from apps.api.schemas.population_scenario import (
    PopulationScenarioPoint,
    PopulationScenarioResponse,
)
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    list_neutral_observations,
)

POPULATION_METRICS = frozenset(
    {
        "CENSUS_PEP:POPESTIMATE",
        "CENSUS_ACS:acs5:B01003_001",
        "CENSUS_ACS:acs1:B01003_001",
    }
)


def population_scenario(
    db: Session,
    metric_code: str,
    geo_id: str,
    horizon_years: int,
    annual_change_percent: float,
) -> PopulationScenarioResponse | None:
    if metric_code not in POPULATION_METRICS:
        raise NeutralQueryError(
            "A scenario requires a published total-population metric, not another count or rate."
        )
    rate = Decimal(str(annual_change_percent))
    if not rate.is_finite() or not -10 <= rate <= 10 or not 1 <= horizon_years <= 30:
        raise NeutralQueryError(
            "Use a finite annual change from -10 to 10 percent and a horizon from 1 to 30 years."
        )
    response = list_neutral_observations(
        db,
        metric_code=metric_code,
        scope="latest",
        release=None,
        filters={"geo_id": geo_id},
        limit=1,
        offset=0,
        newest_per_geography=True,
    )
    if response is None:
        return None
    if response.total != 1 or len(response.items) != 1:
        raise NeutralQueryError(
            "No single published population baseline exists for this place."
        )
    base = NeutralObservation.model_validate(response.items[0], from_attributes=True)
    if (
        base.metric_code != metric_code
        or base.geo_id != geo_id
        or base.source_code != metric_code.split(":", 1)[0]
    ):
        raise NeutralQueryError(
            "The population baseline does not match the requested source, metric, and geography."
        )
    try:
        population = Decimal(base.value) if base.value is not None else Decimal("NaN")
        year = int((base.period_end or base.period_start or "")[:4])
        # A period_end alone cannot hide an absent baseline period.
        if (
            not base.period_start
            or not 1900 <= year <= 2100
            or not population.is_finite()
            or population < 0
            or base.value_status not in {None, "valid"}
        ):
            raise ValueError
    except (InvalidOperation, ValueError):
        raise NeutralQueryError(
            "The baseline has no usable population value and year; withheld values cannot become projections."
        ) from None
    return PopulationScenarioResponse(
        base=base,
        annual_change_percent=float(rate),
        horizon_years=horizon_years,
        caveats=[
            "An assumption-based planning scenario, not an official forecast or provider-published projection.",
            "The annual change is a user assumption; no migration, births, deaths, or capacity model is fitted.",
            "Baseline uncertainty and revisions remain attached to the input; no prediction interval is calculated.",
            "Years after the baseline are modeled values, including any years that have already elapsed; they are not observations.",
        ],
        items=[
            PopulationScenarioPoint(
                year=year + step,
                value=float(
                    (population * (Decimal(1) + rate / 100) ** step).quantize(
                        Decimal("0.01")
                    )
                ),
            )
            for step in range(1, horizon_years + 1)
        ],
    )
