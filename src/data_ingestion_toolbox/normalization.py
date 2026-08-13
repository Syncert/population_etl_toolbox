"""Pure normalization primitives shared by deterministic ETL jobs."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation


class NumericParseError(ValueError):
    """A numeric source value is malformed or outside the warehouse contract."""


DEFAULT_NULL_TOKENS = frozenset({"", ".", "NA", "N/A", "NULL", "-"})
_CONNECTION_URL = re.compile(r"\b(?:postgres(?:ql)?|redis)://[^\s]+", re.IGNORECASE)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|registrationkey|password|token|secret)"
    r"(\s*[=:]\s*|\s+)([^\s,;&}]+)"
)


def stable_records_hash(records: Iterable[object]) -> str:
    """Return an order-independent SHA-256 fingerprint for JSON-like records."""
    canonical_records = [
        json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)
        for record in records
    ]
    payload = "\n".join(sorted(canonical_records)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def parse_decimal(
    value: object,
    *,
    null_tokens: Iterable[str] = DEFAULT_NULL_TOKENS,
    max_integral_digits: int = 30,
    max_fractional_digits: int = 12,
) -> Decimal | None:
    """Parse a finite decimal without float precision loss."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise NumericParseError("Boolean values are not numeric observations")

    text = str(value).strip()
    if text.upper() in {token.upper() for token in null_tokens}:
        return None
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise NumericParseError(f"Invalid numeric value: {text!r}") from exc
    if not parsed.is_finite():
        raise NumericParseError(f"Non-finite numeric value: {text!r}")

    _, digits, exponent = parsed.as_tuple()
    fractional_digits = max(0, -exponent)
    integral_digits = max(1, len(digits) + exponent)
    if integral_digits > max_integral_digits:
        raise NumericParseError(
            f"Numeric value exceeds {max_integral_digits} integral digits"
        )
    if fractional_digits > max_fractional_digits:
        raise NumericParseError(
            f"Numeric value exceeds {max_fractional_digits} fractional digits"
        )
    return parsed


def sanitize_error_message(error: BaseException | str, *, limit: int = 4000) -> str:
    """Return bounded operational context without credentials or connection URLs."""
    if limit < 1:
        raise ValueError("Error-message limit must be positive")
    text = str(error)
    text = _CONNECTION_URL.sub("[REDACTED_CONNECTION_URL]", text)
    text = _SECRET_ASSIGNMENT.sub(lambda match: f"{match.group(1)}=***", text)
    return text[:limit]
