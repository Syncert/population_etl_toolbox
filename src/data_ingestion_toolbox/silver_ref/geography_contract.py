"""Provider-neutral canonical geography identities and exact-code resolution."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass


_DIGITS = re.compile(r"^[0-9]+$")


def _code(value: object, *, width: int, label: str) -> str:
    text = str(value).strip()
    if not _DIGITS.fullmatch(text) or len(text) > width:
        raise ValueError(f"{label} must contain at most {width} digits")
    return text.zfill(width)


def canonical_geo_id(
    geo_type: str,
    *,
    state_fips: object | None = None,
    county_fips: object | None = None,
    place_fips: object | None = None,
    agency_code: object | None = None,
) -> str:
    """Build a canonical identity only from exact provider codes, never names."""
    kind = geo_type.strip().lower()
    if kind in {"us", "nation", "national"}:
        if any(
            v is not None for v in (state_fips, county_fips, place_fips, agency_code)
        ):
            raise ValueError("nation geography cannot include component codes")
        return "us:1"

    if kind == "agency":
        code = str(agency_code or "").strip()
        if not code or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", code):
            raise ValueError("agency_code must be an exact non-empty provider code")
        return f"agency:{code}"

    state = _code(state_fips, width=2, label="state_fips")
    if kind == "state":
        if county_fips is not None or place_fips is not None:
            raise ValueError("state geography cannot include county/place codes")
        return f"state:{state}"
    if kind == "county":
        if place_fips is not None:
            raise ValueError("county geography cannot include place_fips")
        county = _code(county_fips, width=3, label="county_fips")
        return f"state:{state}|county:{county}"
    if kind in {"place", "city"}:
        if county_fips is not None:
            raise ValueError("place is a state sibling of county, not its child")
        place = _code(place_fips, width=5, label="place_fips")
        return f"state:{state}|place:{place}"
    raise ValueError(f"unsupported geography type: {geo_type}")


@dataclass(frozen=True)
class GeographyResolution:
    provider: str
    source_geo_type: str
    source_code: str
    geo_id: str | None
    method: str | None
    status: str
    reason_code: str | None = None


def resolve_provider_geography(
    provider: str,
    source_geo_type: str,
    *,
    state_fips: object | None = None,
    county_fips: object | None = None,
    place_fips: object | None = None,
    agency_code: object | None = None,
) -> GeographyResolution:
    """Resolve supported provider codes without fuzzy or name-based matching."""
    source = provider.strip().upper()
    kind = source_geo_type.strip().lower()
    parts = [state_fips, county_fips, place_fips, agency_code]
    source_code = ":".join("" if value is None else str(value) for value in parts)
    supported = {
        "CENSUS_ACS": {"nation", "us", "state", "county"},
        "BLS": {"nation", "us", "state", "county"},
        "CENSUS_PEP": {"nation", "us", "state", "county", "place"},
        "CDC": {"nation", "us", "state", "county"},
        "USDA_NASS": {"nation", "us", "state", "county"},
        "FBI": {"nation", "us", "state", "county", "place", "agency"},
    }
    if source not in supported or kind not in supported[source]:
        return GeographyResolution(
            source, kind, source_code, None, None, "unsupported", "unsupported_type"
        )
    try:
        geo_id = canonical_geo_id(
            kind,
            state_fips=state_fips,
            county_fips=county_fips,
            place_fips=place_fips,
            agency_code=agency_code,
        )
    except ValueError:
        return GeographyResolution(
            source, kind, source_code, None, None, "unmapped", "invalid_exact_code"
        )
    return GeographyResolution(
        source, kind, source_code, geo_id, "exact_code", "resolved"
    )


def persist_exact_resolution_outcomes(
    hook: object,
    *,
    provider_source: str,
    provider_dataset: str,
    rows: Iterable[Mapping[str, object]],
) -> None:
    """Persist resolved and unmapped exact-code outcomes without dropping evidence."""
    connection = hook.get_conn()  # type: ignore[attr-defined]
    try:
        with connection.cursor() as cursor:
            for row in rows:
                geo_id = str(row["geo_id"])
                cursor.execute(
                    """
                    INSERT INTO silver_ref.geography_resolution (
                        provider_source, provider_dataset, source_geo_type,
                        source_code, source_vintage, geo_sk, resolution_method,
                        status, reason_code
                    )
                    SELECT %s, %s, %s, %s, %s, entity.geo_sk,
                           CASE WHEN entity.geo_sk IS NULL THEN NULL ELSE 'exact_code' END,
                           CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                           CASE WHEN entity.geo_sk IS NULL THEN 'canonical_id_not_loaded' ELSE NULL END
                    FROM (SELECT 1) AS input
                    LEFT JOIN silver_ref.dim_geo_entity entity ON entity.geo_id = %s
                    ON CONFLICT (
                        provider_source, provider_dataset, source_geo_type,
                        source_code, source_vintage
                    ) DO UPDATE SET
                        geo_sk = EXCLUDED.geo_sk,
                        resolution_method = EXCLUDED.resolution_method,
                        status = EXCLUDED.status,
                        reason_code = EXCLUDED.reason_code,
                        resolved_at = NOW()
                    """,
                    (
                        provider_source.strip().upper(),
                        provider_dataset,
                        str(row["geo_level"]),
                        geo_id,
                        row.get("source_vintage"),
                        geo_id,
                    ),
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
