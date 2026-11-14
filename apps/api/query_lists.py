"""Comma-separated lists in a query string, for routes that read several
things in one request (a page of a dozen measures is one read, not a dozen)."""

from __future__ import annotations

from typing import Optional

from fastapi import HTTPException


def code_list(raw: Optional[str], parameter: str, most: int) -> Optional[list[str]]:
    """The distinct non-empty codes in ``raw``, in order; ``None`` when empty.

    More than ``most`` codes is a ``422``: the caller splits the request.
    """
    if not raw:
        return None
    codes = list(dict.fromkeys(part.strip() for part in raw.split(",") if part.strip()))
    if len(codes) > most:
        raise HTTPException(
            status_code=422,
            detail=f"{parameter} names {len(codes)} codes; at most {most} in one request",
        )
    return codes or None
