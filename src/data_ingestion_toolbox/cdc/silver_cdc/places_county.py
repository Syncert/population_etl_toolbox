"""Pure parser for the registered CDC PLACES county product."""

from __future__ import annotations

from decimal import Decimal

from ..registry import PLACES_COUNTY_ASSET, CdcAsset
from .cdi import _adjustment, _decimal, _record_id
from .models import CdcObservation, QuarantinedObservation, ReplayResult


def _geo(code: str) -> tuple[str, str | None]:
    if code == "59":
        return "nation", "us:1"
    if len(code) == 5 and code.isdigit():
        return "county", f"state:{code[:2]}|county:{code[2:]}"
    return "unsupported", None


def parse_places_county_rows(
    rows: list[object],
    *,
    release_watermark: str,
    asset: CdcAsset = PLACES_COUNTY_ASSET,
) -> ReplayResult:
    """Normalize only the verified PLACES county distribution shape."""
    observations: list[CdcObservation] = []
    quarantined: list[QuarantinedObservation] = []
    required = ("year", "locationid", "measureid", "datavaluetypeid")
    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            quarantined.append(
                QuarantinedObservation(
                    index, "invalid_row_shape", "CDC PLACES row must be an object"
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
        value, value_source = _decimal(item.get("data_value"))
        low, low_source = _decimal(item.get("low_confidence_limit"))
        high, high_source = _decimal(item.get("high_confidence_limit"))
        population, population_source = _decimal(item.get("totalpopulation"))
        adult_population, adult_source = _decimal(item.get("totalpop18plus"))
        if any(
            source is not None and parsed is None
            for parsed, source in (
                (value, value_source),
                (low, low_source),
                (high, high_source),
                (population, population_source),
                (adult_population, adult_source),
            )
        ):
            quarantined.append(
                QuarantinedObservation(
                    index, "invalid_numeric_value", "PLACES numeric field is invalid"
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
            str(item.get("data_value_unit"))
            if item.get("data_value_unit") is not None
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
        code = str(item["locationid"])
        geo_type, geo_id = _geo(code)
        footnote_text = (
            str(item.get("data_value_footnote"))
            if item.get("data_value_footnote")
            else None
        )
        if value is not None:
            status = "valid"
        elif footnote_text and "suppress" in footnote_text.lower():
            status = "suppressed"
        else:
            status = "missing"
        value_type = str(item["datavaluetypeid"])
        observations.append(
            CdcObservation(
                dataset=asset.asset_id,
                release_watermark=release_watermark,
                source_record_id=_record_id(asset, item),
                source_row=item,
                measure_id=str(item["measureid"]),
                measure_label=str(item.get("measure") or item["measureid"]),
                topic=str(item.get("category") or ""),
                period_start=int(str(item["year"])),
                period_end=int(str(item["year"])),
                geo_source_code=code,
                geo_source_label=str(item.get("locationname") or item.get("statedesc"))
                if (item.get("locationname") or item.get("statedesc"))
                else None,
                geo_type=geo_type,
                geo_id=geo_id,
                value_source=value_source,
                value=value,
                value_status=status,
                unit=unit,
                value_type_id=value_type,
                value_type_label=str(item.get("data_value_type") or value_type),
                adjustment_status=_adjustment(str(item.get("data_value_type") or "")),
                confidence_lower=low,
                confidence_upper=high,
                footnote_code=str(item.get("data_value_footnote_symbol"))
                if item.get("data_value_footnote_symbol")
                else None,
                footnote_text=footnote_text,
                strata=(("OVERALL", "Overall", "OVR", "Overall"),),
                estimate_method=asset.estimate_method,
                population_basis=asset.population_basis,
                total_population=population,
                population_18_plus=adult_population,
                source_row_index=index,
            )
        )
    return ReplayResult(len(rows), tuple(observations), tuple(quarantined))
