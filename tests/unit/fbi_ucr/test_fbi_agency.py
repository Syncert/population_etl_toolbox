"""Agency identity, jurisdiction class, and county-evidence contracts."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.fbi_ucr.reference import (
    REVIEWED_PLACE_MAPPINGS,
    reviewed_place_mapping,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.agency import parse_agency_directory

pytestmark = pytest.mark.unit

SLICE = "agency_directory:WI"


def _parse(payload: object):
    return parse_agency_directory(payload, state_code="WI", slice_key=SLICE)


def _by_ori(result) -> dict:
    return {record.ori: record for record in result.agencies}


def test_every_reviewed_jurisdiction_class_keeps_its_own_identity(
    fbi_payload,
) -> None:
    """Covers: ETL-004 — the directory yields one record per ORI."""
    result = _parse(fbi_payload("agency_directory_WI"))
    agencies = _by_ori(result)

    assert result.input_count == len(result.agencies) + len(result.quarantined)
    assert not result.quarantined
    assert {record.agency_type for record in result.agencies} == {
        "City",
        "County",
        "State Police",
        "Tribal",
        "University or College",
    }
    assert agencies["WI0130000"].agency_name == "Dane County Sheriff's Office"
    assert agencies["WI0130000"].agency_type == "County"


def test_multi_county_agency_keeps_one_relationship_per_county(
    fbi_payload,
) -> None:
    """Covers: ETL-024 — a comma-joined key becomes separate county labels."""
    agencies = _by_ori(_parse(fbi_payload("agency_directory_WI")))

    assert agencies["WI0540300"].county_labels == ("DANE", "ROCK")


def test_not_specified_county_label_stays_unresolved(fbi_payload) -> None:
    """Covers: ETL-003 — NOT SPECIFIED is not a county association."""
    agencies = _by_ori(_parse(fbi_payload("agency_directory_WI")))

    assert agencies["WI0400100"].county_labels == ()
    assert agencies["WIWSP0000"].county_labels == ()


def test_countywide_and_campus_agencies_are_not_place_mapped(
    fbi_payload,
) -> None:
    """Covers: ETL-003 — non-municipal jurisdictions get no place bridge."""
    from datetime import date

    agencies = _by_ori(_parse(fbi_payload("agency_directory_WI")))
    window = {"period_start": date(2023, 1, 1), "period_end": date(2023, 6, 30)}

    for ori in ("WI0130000", "WI0050700", "WI0400100", "WIWSP0000"):
        assert ori in agencies
        assert reviewed_place_mapping(ori, **window) is None
    assert reviewed_place_mapping("WI0137000", **window).place_geo_id == (
        "state:55|place:25950"
    )


def test_reviewed_place_mapping_requires_full_period_coverage() -> None:
    """Covers: ETL-007 — a partially covered period is never bridged."""
    from datetime import date

    mapping = REVIEWED_PLACE_MAPPINGS[0]

    assert mapping.covers(mapping.effective_start, date(2023, 6, 30))
    assert not mapping.covers(date(2022, 1, 1), date(2023, 6, 30))
    assert (
        reviewed_place_mapping(
            mapping.ori,
            period_start=date(2022, 1, 1),
            period_end=date(2022, 12, 31),
        )
        is None
    )


def test_reviewed_crosswalk_never_bridges_an_unreviewed_agency() -> None:
    """Covers: ETL-003 — an agency outside the crosswalk has no place."""
    from datetime import date

    assert (
        reviewed_place_mapping(
            "WI9999999",
            period_start=date(2023, 1, 1),
            period_end=date(2023, 6, 30),
        )
        is None
    )


@pytest.mark.parametrize("ori", ["WI013000", "wi0130000", "", None, 12345])
def test_malformed_ori_is_quarantined_not_published(ori: object) -> None:
    """Covers: ETL-005 — an unusable ORI never becomes an agency identity."""
    result = _parse(
        {
            "DANE": [
                {
                    "ori": ori,
                    "counties": "DANE",
                    "state_abbr": "WI",
                    "state_name": "Wisconsin",
                    "agency_name": "Test Police Department",
                    "agency_type_name": "City",
                }
            ]
        }
    )

    assert not result.agencies
    assert [item.error_code for item in result.quarantined] == ["invalid_ori"]
    assert result.input_count == len(result.quarantined)


def test_conflicting_attributes_for_one_ori_are_quarantined() -> None:
    """Covers: ETL-023 — one ORI cannot publish two conflicting identities."""
    entry = {
        "ori": "WI0130000",
        "counties": "DANE",
        "state_abbr": "WI",
        "state_name": "Wisconsin",
        "agency_name": "Dane County Sheriff's Office",
        "agency_type_name": "County",
    }
    result = _parse(
        {
            "DANE": [entry],
            "ROCK": [{**entry, "agency_type_name": "City", "counties": "ROCK"}],
        }
    )

    assert not result.agencies
    assert [item.error_code for item in result.quarantined] == [
        "conflicting_agency_attributes"
    ]


def test_agency_outside_the_requested_state_is_quarantined() -> None:
    """Covers: ETL-024 — a directory answer is scoped to its own request."""
    result = _parse(
        {
            "COOK": [
                {
                    "ori": "IL0160000",
                    "counties": "COOK",
                    "state_abbr": "IL",
                    "state_name": "Illinois",
                    "agency_name": "Cook County Sheriff",
                    "agency_type_name": "County",
                }
            ]
        }
    )

    assert [item.error_code for item in result.quarantined] == [
        "state_scope_mismatch"
    ]


def test_missing_identity_fields_are_quarantined() -> None:
    """Covers: ETL-005 — an agency without name or type is unusable."""
    result = _parse(
        {"DANE": [{"ori": "WI0130000", "counties": "DANE", "state_abbr": "WI"}]}
    )

    assert [item.error_code for item in result.quarantined] == [
        "missing_required_field"
    ]
    assert "agency_name" in result.quarantined[0].error_summary


def test_out_of_range_coordinates_do_not_block_the_agency() -> None:
    """Covers: ETL-022 — coordinates are evidence, never a jurisdiction."""
    result = _parse(
        {
            "DANE": [
                {
                    "ori": "WI0130000",
                    "counties": "DANE",
                    "state_abbr": "WI",
                    "state_name": "Wisconsin",
                    "agency_name": "Dane County Sheriff's Office",
                    "agency_type_name": "County",
                    "latitude": 999.0,
                    "longitude": -1000.0,
                }
            ]
        }
    )

    assert len(result.agencies) == 1
    assert result.agencies[0].latitude is None
    assert result.agencies[0].longitude is None
    assert result.agencies[0].source_row["latitude"] == 999.0


def test_non_object_directory_is_quarantined() -> None:
    """Covers: RES-002 — a list-shaped directory never parses silently."""
    result = _parse([{"ori": "WI0130000"}])

    assert result.input_count == 1
    assert [item.error_code for item in result.quarantined] == [
        "invalid_directory_shape"
    ]


def test_malformed_rows_reconcile_to_one_outcome_each() -> None:
    """Covers: ETL-025 — every input becomes exactly one recorded outcome."""
    result = _parse({"DANE": ["not an object", 12], "ROCK": "not a list"})

    assert result.input_count == 3
    assert len(result.quarantined) == 3
    assert not result.agencies
