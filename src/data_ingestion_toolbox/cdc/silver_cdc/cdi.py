"""Pure parser for the registered CDC CDI Socrata product."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation

from ..registry import CDI_ASSET, CdcAsset
from .models import CdcObservation, QuarantinedObservation, ReplayResult


def _decimal(value: object) -> tuple[Decimal | None, str | None]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None, None
    source = str(value)
    try:
        return Decimal(source), source
    except InvalidOperation:
        return None, source


def _record_id(asset: CdcAsset, row: dict[str, object]) -> str:
    source = json.dumps(
        [row.get(field) for field in asset.source_key],
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(source).hexdigest()


def _adjustment(label: str) -> str:
    normalized = label.lower()
    if "age-adjusted" in normalized or "age adjusted" in normalized:
        return "age_adjusted"
    if "crude" in normalized:
        return "crude"
    return "source_specific"


def _geo(row: dict[str, object]) -> tuple[str, str | None]:
    code = str(row.get("locationid", ""))
    if code == "59":
        return "nation", "us:1"
    if len(code) == 2 and code.isdigit():
        return "state", f"state:{code}"
    return "unsupported", None


def _strata(row: dict[str, object]) -> tuple[tuple[str | None, ...], ...]:
    values: list[tuple[str | None, ...]] = []
    for index in range(1, 4):
        category_label = row.get(f"stratificationcategory{index}")
        stratum_label = row.get(f"stratification{index}")
        category_id = row.get(f"stratificationcategoryid{index}")
        stratum_id = row.get(f"stratificationid{index}")
        if any(
            value is not None
            for value in (category_label, stratum_label, category_id, stratum_id)
        ):
            values.append(
                tuple(
                    str(value) if value is not None else None
                    for value in (
                        category_id,
                        category_label,
                        stratum_id,
                        stratum_label,
                    )
                )
            )
    return tuple(values)


def parse_cdi_rows(
    rows: list[object],
    *,
    release_watermark: str,
    asset: CdcAsset = CDI_ASSET,
) -> ReplayResult:
    """Normalize CDI rows while reconciling every input to one outcome."""
    observations: list[CdcObservation] = []
    quarantined: list[QuarantinedObservation] = []
    required = (
        "yearstart",
        "yearend",
        "locationid",
        "questionid",
        "datavaluetypeid",
        "datasource",
    )
    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            quarantined.append(
                QuarantinedObservation(
                    index, "invalid_row_shape", "CDC CDI row must be an object"
                )
            )
            continue
        missing = [field for field in required if item.get(field) in (None, "")]
        if missing:
            quarantined.append(
                QuarantinedObservation(
                    index, "missing_required_field", "missing: " + ", ".join(missing)
                )
            )
            continue
        value, value_source = _decimal(item.get("datavalue"))
        low, low_source = _decimal(item.get("lowconfidencelimit"))
        high, high_source = _decimal(item.get("highconfidencelimit"))
        if value_source is not None and value is None:
            quarantined.append(
                QuarantinedObservation(
                    index, "invalid_numeric_value", "CDI value is not numeric"
                )
            )
            continue
        if (low_source is not None and low is None) or (
            high_source is not None and high is None
        ):
            quarantined.append(
                QuarantinedObservation(
                    index,
                    "invalid_confidence_interval",
                    "CDI confidence bound is not numeric",
                )
            )
            continue
        if (
            value is not None
            and low is not None
            and high is not None
            and not (low <= value <= high)
        ):
            quarantined.append(
                QuarantinedObservation(
                    index,
                    "invalid_confidence_interval",
                    "CDC confidence bounds do not bracket the value",
                )
            )
            continue
        unit = (
            str(item.get("datavalueunit"))
            if item.get("datavalueunit") is not None
            else None
        )
        if (
            value is not None
            and unit == "%"
            and not (Decimal(0) <= value <= Decimal(100))
        ):
            quarantined.append(
                QuarantinedObservation(
                    index, "value_out_of_range", "CDC percentage is outside 0..100"
                )
            )
            continue
        geo_type, geo_id = _geo(item)
        value_type = str(item["datavaluetypeid"])
        observations.append(
            CdcObservation(
                dataset=asset.asset_id,
                release_watermark=release_watermark,
                source_record_id=_record_id(asset, item),
                source_row=item,
                measure_id=str(item["questionid"]),
                measure_label=str(item.get("question") or item["questionid"]),
                topic=str(item.get("topic") or ""),
                period_start=int(str(item["yearstart"])),
                period_end=int(str(item["yearend"])),
                geo_source_code=str(item["locationid"]),
                geo_source_label=str(item.get("locationdesc"))
                if item.get("locationdesc")
                else None,
                geo_type=geo_type,
                geo_id=geo_id,
                value_source=value_source,
                value=value,
                value_status="valid" if value is not None else "missing",
                unit=unit,
                value_type_id=value_type,
                value_type_label=str(item.get("datavaluetype") or value_type),
                adjustment_status=_adjustment(str(item.get("datavaluetype") or "")),
                confidence_lower=low,
                confidence_upper=high,
                footnote_code=str(item.get("datavaluefootnotesymbol"))
                if item.get("datavaluefootnotesymbol")
                else None,
                footnote_text=str(item.get("datavaluefootnote"))
                if item.get("datavaluefootnote")
                else None,
                strata=_strata(item),
                estimate_method=asset.estimate_method,
                population_basis=asset.population_basis,
                source_row_index=index,
            )
        )
    return ReplayResult(len(rows), tuple(observations), tuple(quarantined))
