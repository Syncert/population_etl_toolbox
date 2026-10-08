"""HUD User Data API reads: registry, transport and parsing.

HUD User answers automated workbook downloads with an empty ``202``
challenge, so the scheduled pipeline reads HUD's sanctioned automated
channel instead: the HUD User Data API at
``https://www.huduser.gov/hudapi/public/`` (checked 2026-10-07). Every call
sends ``Authorization: Bearer <HUD_USER_API_TOKEN>``; the limit is 60 calls a
minute.

- ``fmr/statedata/<ST>?year=<FY>`` answers one state's FMRs: every county or,
  in New England, every town (``fips_code``, ten digits), with its area's
  ``metro_name``, plus the state's metro areas with their HUD area ``code``.
- ``fmr/listCounties/<ST>`` lists a state's entities; ``il/data/<fips>?year=<FY>``
  answers one county's income limits. Income limits are read for whole
  counties only (``fips`` ending ``99999``); New England towns are not
  served as counties, so their limits are not requested.

The API serves one edition per fiscal year: the one in force. On 2026-10-07
its FY 2026 FMRs were the May 2026 reissue (Napa County, CA two-bedroom
3,315, the revised workbook's value, not the original's 2,773), so each
registered read names the edition it was checked to serve. A later reissue
changes the answer; the read is then a new revision under the same label
until the registry is updated, and the live contract check compares the
answer with the registered edition's known values.

The API does not publish a HUD area code for nonmetropolitan areas, and the
code is not derivable from the county (Virginia's independent cities share
their county's area), so it is recorded only where the answer names it.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import HudConfig
from .registry import FMR, INCOME_LIMITS, ORIGINAL, REVISED, WHOLE_COUNTY
from .silver_hud_fmr_il.parse import (
    _STATE_CODES,
    COUNTY,
    COUNTY_SUBDIVISION,
    HudObservation,
    HudQuarantine,
    ParsedFile,
)

HUD_API_BASE_URL = "https://www.huduser.gov/hudapi/public"

#: The FMR answer's bedroom fields, as the API names them.
FMR_FIELDS: tuple[tuple[str, str], ...] = (
    ("fmr_0br", "Efficiency"),
    ("fmr_1br", "One-Bedroom"),
    ("fmr_2br", "Two-Bedroom"),
    ("fmr_3br", "Three-Bedroom"),
    ("fmr_4br", "Four-Bedroom"),
)
#: The income-limit answer's groups: (measure prefix, group, field prefix).
IL_GROUPS: tuple[tuple[str, str, str], ...] = (
    ("income_limit_50", "very_low", "il50_p"),
    ("income_limit_30", "extremely_low", "il30_p"),
    ("income_limit_80", "low", "il80_p"),
)


@dataclass(frozen=True)
class ApiRead:
    dataset: str
    fiscal_year: int
    #: The workbook edition the API was checked to serve for this year.
    edition: str
    effective_date: date

    @property
    def key(self) -> str:
        return f"api:{self.dataset}:fy{self.fiscal_year}"


REGISTERED_READS: tuple[ApiRead, ...] = (
    # Checked 2026-10-07: the API's FY 2026 FMRs are the May 21, 2026 reissue.
    ApiRead(FMR, 2026, REVISED, date(2026, 5, 21)),
    ApiRead(FMR, 2027, ORIGINAL, date(2026, 10, 1)),
    ApiRead(INCOME_LIMITS, 2026, ORIGINAL, date(2026, 5, 1)),
)


def registered_reads() -> tuple[ApiRead, ...]:
    return REGISTERED_READS


def get_read(key: str) -> ApiRead:
    for read in REGISTERED_READS:
        if read.key == key:
            return read
    raise KeyError(f"{key} is not a registered HUD API read")


class HudApiError(RuntimeError):
    """A sanitized API failure: the path and status only, never the token."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(
            f"HUD User API request failed ({code}{status_text}) at {endpoint}"
        )


class HudApiPayloadError(HudApiError):
    """A successful response that is not the registered answer's JSON."""


@dataclass(frozen=True)
class ApiResponse:
    path: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


class HudApiClient:
    """Sequential, spaced calls to the HUD User API with one token."""

    def __init__(
        self,
        config: HudConfig,
        *,
        client: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        token = config.hud_user_api_token
        if not token.strip() or token != token.strip():
            raise HudApiError("HUD_USER_API_TOKEN", code="missing_api_token")
        self._token = token
        self._config = config
        self._own = client is None
        self._client = client or httpx.Client(timeout=config.timeout_seconds)
        self._sleep = sleep
        self._clock = clock
        self._last: float | None = None

    def close(self) -> None:
        if self._own:
            self._client.close()

    def _space(self) -> None:
        if self._last is not None:
            wait = self._config.api_min_spacing_seconds - (self._clock() - self._last)
            if wait > 0:
                self._sleep(wait)
        self._last = self._clock()

    def get(
        self, path: str, *, on_retry: Callable[[BaseException], None] | None = None
    ) -> ApiResponse:
        """One answer, checked to be JSON with a ``data`` member."""
        final_status: int | None = None
        final_error: BaseException | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            self._space()
            try:
                response = self._client.get(
                    f"{HUD_API_BASE_URL}/{path}",
                    headers={
                        "Authorization": f"Bearer {self._token}",
                        "User-Agent": self._config.user_agent,
                    },
                )
            except httpx.HTTPError as exc:
                final_error = HudApiError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code == 200 and raw_bytes:
                    check_answer(raw_bytes, path)
                    return ApiResponse(path, raw_bytes, headers, 200)
                if response.status_code in (401, 403):
                    raise HudApiError(
                        path, code="token_refused", status=response.status_code
                    )
                if (
                    response.status_code not in (202, 429)
                    and response.status_code < 500
                ):
                    raise HudApiError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = HudApiError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < self._config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                self._sleep(
                    min(
                        self._config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0,
                        120.0,
                    )
                )
        raise HudApiError(
            path, code="retry_exhausted", status=final_status
        ) from final_error


def check_answer(raw_bytes: bytes, path: str) -> Any:
    try:
        document = json.loads(raw_bytes)
    except ValueError as exc:
        raise HudApiPayloadError(path, code="not_json") from exc
    if not isinstance(document, (dict, list)) or (
        isinstance(document, dict) and "data" not in document
    ):
        raise HudApiPayloadError(path, code="unexpected_answer")
    return document


def state_codes(raw_bytes: bytes) -> tuple[str, ...]:
    """The two-letter codes ``fmr/listStates`` answers, in its order."""
    document = check_answer(raw_bytes, "fmr/listStates")
    if not isinstance(document, list):
        raise HudApiPayloadError("fmr/listStates", code="unexpected_answer")
    codes = [str(entry.get("state_code") or "").strip() for entry in document]
    if not codes or any(len(code) != 2 for code in codes):
        raise HudApiPayloadError("fmr/listStates", code="unexpected_answer")
    return tuple(codes)


def whole_counties(raw_bytes: bytes, state: str) -> tuple[str, ...]:
    """The whole-county ``fips_code`` values ``fmr/listCounties/<ST>`` answers."""
    path = f"fmr/listCounties/{state}"
    entries = check_answer(raw_bytes, path)
    if not isinstance(entries, list):
        raise HudApiPayloadError(path, code="unexpected_answer")
    return tuple(
        str(entry.get("fips_code"))
        for entry in entries
        if str(entry.get("fips_code") or "").endswith(WHOLE_COUNTY)
    )


def _number(value: Any, field: str) -> tuple[str, Decimal | None]:
    if value is None or value == "":
        return "", None
    stored = str(value).strip()
    try:
        number = Decimal(stored)
    except InvalidOperation as exc:
        raise ValueError(f"{field} {stored!r} is not a number") from exc
    if number < 0:
        raise ValueError(f"{field} {stored!r} is negative")
    return stored, number


def _fips(value: Any) -> str:
    fips = str(value or "").strip()
    if len(fips) != 10 or not fips.isdigit() or fips[:2] not in _STATE_CODES:
        raise ValueError(f"fips {fips!r} is not a ten-digit county subdivision code")
    return fips


def _record_id(read: ApiRead, fips: str, measure: str) -> str:
    return hashlib.sha256(
        f"hud_fmr_il|{read.key}|{fips}|{measure}".encode()
    ).hexdigest()


def _observation(
    read: ApiRead,
    index: int,
    fips: str,
    measure: str,
    stored: str,
    value: Decimal | None,
    *,
    area_code: str | None,
    area_name: str,
    metro: bool,
) -> HudObservation:
    whole = fips.endswith(WHOLE_COUNTY)
    geo_id = f"state:{fips[:2]}|county:{fips[2:5]}"
    if not whole:
        geo_id = f"{geo_id}|cousub:{fips[5:]}"
    return HudObservation(
        source_row_index=index,
        measure=measure,
        fips_code=fips,
        geo_type=COUNTY if whole else COUNTY_SUBDIVISION,
        geo_id=geo_id,
        hud_area_code=area_code,
        hud_area_name=area_name,
        metro=metro,
        value_source=stored,
        value=value,
        value_status="valid" if value is not None else "missing",
        missing_reason=None if value is not None else "provider_missing",
        source_record_id=_record_id(read, fips, measure),
    )


def parse_fmr_state(raw_bytes: bytes, *, read: ApiRead) -> ParsedFile:
    """One ``fmr/statedata`` answer: every county or town in the state."""
    try:
        data = check_answer(raw_bytes, "fmr/statedata")["data"]
        if str(data.get("year")) != str(read.fiscal_year):
            return ParsedFile(
                (),
                (
                    HudQuarantine(
                        0, "wrong_year", f"answer is for {data.get('year')!r}"
                    ),
                ),
                0,
                0,
            )
        areas = {
            str(area.get("metro_name")): str(area.get("code"))
            for area in data.get("metroareas") or []
        }
        entries = data.get("counties")
        if not isinstance(entries, list):
            raise HudApiPayloadError("fmr/statedata", code="unexpected_answer")
    except HudApiPayloadError as error:
        return ParsedFile(
            (), (HudQuarantine(0, error.code, f"answer refused: {error.code}"),), 0, 0
        )
    observations: list[HudObservation] = []
    quarantined: list[HudQuarantine] = []
    seen: set[str] = set()
    counties = 0
    for index, entry in enumerate(entries, start=1):
        try:
            fips = _fips(entry.get("fips_code"))
            values = {
                measure: _number(entry.get(field), field)
                for measure, field in FMR_FIELDS
            }
            area_name = str(entry.get("metro_name") or "").strip()
            if not area_name:
                raise ValueError("no area name")
        except ValueError as exc:
            quarantined.append(HudQuarantine(index, "unreadable_row", str(exc)[:200]))
            continue
        if fips in seen:
            quarantined.append(
                HudQuarantine(index, "duplicate_row", f"fips {fips} appears twice")
            )
            continue
        seen.add(fips)
        counties += fips.endswith(WHOLE_COUNTY)
        code = areas.get(area_name)
        for measure, _field in FMR_FIELDS:
            stored, value = values[measure]
            observations.append(
                _observation(
                    read,
                    index,
                    fips,
                    measure,
                    stored,
                    value,
                    area_code=code,
                    area_name=area_name,
                    metro=code is not None and code.startswith("METRO"),
                )
            )
    return ParsedFile(tuple(observations), tuple(quarantined), len(entries), counties)


def parse_il_county(raw_bytes: bytes, *, read: ApiRead, fips: str) -> ParsedFile:
    """One ``il/data/<fips>`` answer: one county's limits."""
    try:
        data = check_answer(raw_bytes, f"il/data/{fips}")["data"]
        if str(data.get("year")) != str(read.fiscal_year):
            return ParsedFile(
                (),
                (
                    HudQuarantine(
                        0, "wrong_year", f"answer is for {data.get('year')!r}"
                    ),
                ),
                0,
                0,
            )
    except HudApiPayloadError as error:
        return ParsedFile(
            (), (HudQuarantine(0, error.code, f"answer refused: {error.code}"),), 0, 0
        )
    try:
        code = _fips(fips)
        area_name = str(data.get("area_name") or "").strip()
        metro_text = str(data.get("metro_status") or "").strip()
        if not area_name or metro_text not in ("0", "1", "0.0", "1.0"):
            raise ValueError(f"area {area_name!r} metro {metro_text!r}")
        values: list[tuple[str, str, Decimal | None]] = []
        stored, value = _number(data.get("median_income"), "median_income")
        values.append(("median_family_income", stored, value))
        for prefix, group, field in IL_GROUPS:
            limits = data.get(group)
            if not isinstance(limits, dict):
                raise ValueError(f"{group} is missing")
            for size in range(1, 9):
                stored, value = _number(limits.get(f"{field}{size}"), f"{field}{size}")
                values.append((f"{prefix}_{size}p", stored, value))
    except ValueError as exc:
        return ParsedFile(
            (), (HudQuarantine(1, "unreadable_row", str(exc)[:200]),), 1, 0
        )
    observations = tuple(
        _observation(
            read,
            1,
            code,
            measure,
            stored,
            value,
            area_code=None,
            area_name=area_name,
            metro=metro_text.startswith("1"),
        )
        for measure, stored, value in values
    )
    return ParsedFile(observations, (), 1, 1)
