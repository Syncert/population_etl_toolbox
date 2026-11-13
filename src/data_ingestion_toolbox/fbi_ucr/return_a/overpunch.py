"""Signed numeric fields in the Return A master file.

The file keeps the punched-card convention: the last character of a signed
field carries its sign. ``{`` and ``A``-``I`` are +0..+9, ``}`` and ``J``-``R``
are -0..-9. Adjustments (an offense unfounded after a month closed) are why a
monthly count can be negative.
"""

from __future__ import annotations

_POSITIVE = {"{": 0, **{chr(ord("A") + index): index + 1 for index in range(9)}}
_NEGATIVE = {"}": 0, **{chr(ord("J") + index): index + 1 for index in range(9)}}


class OverpunchError(ValueError):
    """A field that is not a number in the file's convention."""


def decode_signed(field: str) -> int | None:
    """Decode one signed field; ``None`` only for an all-blank field.

    A blank field is absent, never zero. Anything else that is not digits with
    an optional overpunched last character fails, so a shifted column is
    caught rather than read as a value.
    """
    text = field.strip()
    if not text:
        return None
    head, last = text[:-1], text[-1]
    if head and not head.isdigit():
        raise OverpunchError(f"not a signed field: {field!r}")
    if last.isdigit():
        return int(text)
    if last in _POSITIVE:
        return int((head or "0") + str(_POSITIVE[last]))
    if last in _NEGATIVE:
        return -int((head or "0") + str(_NEGATIVE[last]))
    raise OverpunchError(f"not a signed field: {field!r}")


def decode_unsigned(field: str) -> int | None:
    """Decode one unsigned field; ``None`` only for an all-blank field."""
    text = field.strip()
    if not text:
        return None
    if not text.isdigit():
        raise OverpunchError(f"not an unsigned field: {field!r}")
    return int(text)
