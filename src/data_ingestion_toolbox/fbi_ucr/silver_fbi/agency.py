"""Pure parser for the registered FBI CDE Agency resource.

The Agency response groups agencies under county-name labels while keeping the
ORI as the identity. One ORI can appear under several labels, and the label
``NOT SPECIFIED`` means the provider published no county association at all.
Both facts are preserved: county labels are retained as evidence, never turned
into a canonical county code here, and never used to aggregate.

Latitude and longitude describe a source reference point rather than a
jurisdiction boundary. They are retained when they are inside valid ranges and
recorded as absent otherwise; they never gate publication and are never used to
resolve geography.
"""

from __future__ import annotations

from typing import Any

from ..registry import (
    ORI_PATTERN,
    STATE_CODE_CONTRACT,
    UNSPECIFIED_COUNTY_LABEL,
    UNSUPPORTED_STATE_CODES,
)
from .models import FbiAgencyRecord, QuarantinedRecord, SliceResult

_IDENTITY_FIELDS = ("agency_name", "agency_type_name", "state_abbr")


def normalize_county_label(value: object) -> str:
    """Return the provider county label in a stable comparison form."""
    return " ".join(str(value).strip().upper().split())


def _split_county_labels(value: object) -> set[str]:
    """Split one provider county field or grouping key into single labels.

    The Agency resource groups a multi-county agency under a single
    comma-joined key such as ``"DANE, ROCK"``, and repeats the same joined
    value in the row's ``counties`` field. Both are split so one agency keeps
    one relationship per county rather than one relationship to a made-up
    combined area.
    """
    if not isinstance(value, str):
        return set()
    return {normalize_county_label(part) for part in value.split(",") if part.strip()}


def _county_labels(entries: list[tuple[str, dict[str, Any]]]) -> tuple[str, ...]:
    labels: set[str] = set()
    for group_label, row in entries:
        labels |= _split_county_labels(group_label)
        labels |= _split_county_labels(row.get("counties"))
    labels.discard("")
    labels.discard(UNSPECIFIED_COUNTY_LABEL)
    return tuple(sorted(labels))


def _coordinate(value: object, *, limit: float) -> float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    numeric = float(value)
    return numeric if -limit <= numeric <= limit else None


def _boolean(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def parse_agency_directory(
    payload: object,
    *,
    state_code: str,
    slice_key: str,
) -> SliceResult:
    """Normalize one Agency response, reconciling every ORI to one outcome."""
    if not isinstance(payload, dict):
        return SliceResult(
            input_count=1,
            quarantined=(
                QuarantinedRecord(
                    slice_key,
                    0,
                    "invalid_directory_shape",
                    "FBI agency directory must be a JSON object",
                ),
            ),
        )

    grouped: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    malformed = 0
    unidentified = 0
    for group_label in sorted(payload):
        entries = payload[group_label]
        if not isinstance(entries, list):
            malformed += 1
            continue
        for row in entries:
            if not isinstance(row, dict):
                malformed += 1
                continue
            ori = row.get("ori")
            if isinstance(ori, str) and ori.strip():
                key = ori
            else:
                unidentified += 1
                key = f"~~unidentified-{unidentified:04d}"
            grouped.setdefault(key, []).append((group_label, row))

    agencies: list[FbiAgencyRecord] = []
    quarantined: list[QuarantinedRecord] = [
        QuarantinedRecord(
            slice_key, index, "invalid_agency_row", "FBI agency entry must be an object"
        )
        for index in range(malformed)
    ]
    for index, key in enumerate(sorted(grouped), start=malformed):
        entries = grouped[key]
        first = entries[0][1]
        ori = first.get("ori")
        if not isinstance(ori, str) or not ORI_PATTERN.fullmatch(ori):
            quarantined.append(
                QuarantinedRecord(
                    slice_key,
                    index,
                    "invalid_ori",
                    "ORI does not match the published two-letter, nine-character form",
                )
            )
            continue
        conflicting = sorted(
            field
            for field in _IDENTITY_FIELDS
            if len({str(row.get(field)) for _label, row in entries}) > 1
        )
        if conflicting:
            quarantined.append(
                QuarantinedRecord(
                    slice_key,
                    index,
                    "conflicting_agency_attributes",
                    "one ORI published conflicting " + ", ".join(conflicting),
                )
            )
            continue
        name = first.get("agency_name")
        agency_type = first.get("agency_type_name")
        published_state = first.get("state_abbr")
        missing = [
            field
            for field, value in (
                ("agency_name", name),
                ("agency_type_name", agency_type),
                ("state_abbr", published_state),
            )
            if not isinstance(value, str) or not value.strip()
        ]
        if missing:
            quarantined.append(
                QuarantinedRecord(
                    slice_key,
                    index,
                    "missing_required_field",
                    "missing: " + ", ".join(missing),
                )
            )
            continue
        if published_state != state_code:
            quarantined.append(
                QuarantinedRecord(
                    slice_key,
                    index,
                    "state_scope_mismatch",
                    "agency state does not match the requested directory state",
                )
            )
            continue
        if (
            published_state not in STATE_CODE_CONTRACT
            and published_state not in UNSUPPORTED_STATE_CODES
        ):
            quarantined.append(
                QuarantinedRecord(
                    slice_key,
                    index,
                    "undocumented_state_code",
                    "agency state is outside the documented state enumeration",
                )
            )
            continue
        nibrs_start = first.get("nibrs_start_date")
        agencies.append(
            FbiAgencyRecord(
                ori=ori,
                agency_name=str(name).strip(),
                agency_type=str(agency_type).strip(),
                state_code=str(published_state),
                state_name=(
                    str(first["state_name"]).strip()
                    if isinstance(first.get("state_name"), str)
                    else None
                ),
                county_labels=_county_labels(entries),
                is_nibrs=_boolean(first.get("is_nibrs")),
                nibrs_start_date=(
                    str(nibrs_start) if isinstance(nibrs_start, str) else None
                ),
                latitude=_coordinate(first.get("latitude"), limit=90.0),
                longitude=_coordinate(first.get("longitude"), limit=180.0),
                source_row=first,
                source_row_index=index,
            )
        )
    return SliceResult(
        input_count=malformed + len(grouped),
        agencies=tuple(agencies),
        quarantined=tuple(quarantined),
    )
