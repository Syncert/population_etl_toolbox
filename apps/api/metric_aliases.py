from typing import Optional

from fastapi import HTTPException


METRIC_ALIASES = {
    "population": "ACS:acs5:B01003_001",
}


def resolve_metric_code(
    metric_code: Optional[str],
    metric_id: Optional[str],
    detail: str = "metric_code or metric_id is required",
) -> str:
    requested = metric_code or metric_id
    if not requested:
        raise HTTPException(status_code=422, detail=detail)
    return METRIC_ALIASES.get(requested.casefold(), requested)
