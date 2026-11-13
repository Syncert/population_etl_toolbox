"""The standard-library reader of R ``.RData`` data frames.

The real Algara & Amlani files are R 4.0.2 ``save()`` output: gzip, then
``RDX3``, serialization version 3 with a native-encoding name, and in the
presidential file a ``.Random.seed`` integer vector saved before the data
frame (read on OFFICE_LAB 2026-11-07). The bytes below are built by hand
in that format, including the ALTREP forms R 3.5 and later write.

Covers: ETL-111
"""

from __future__ import annotations

import gzip
import struct
from collections.abc import Callable
from datetime import date

import pytest

from data_ingestion_toolbox.utility.rdata import (
    RDataError,
    read_data_frames,
    read_rdata,
)
from tests.support.elections import fixture_bytes

pytestmark = pytest.mark.unit

ATTR, TAG = 1 << 9, 1 << 10
NA_INT = -(2**31)
NA_REAL = struct.pack(">II", 0x7FF00000, 1954)


def i(value: int) -> bytes:
    return struct.pack(">i", value)


def charsxp(text: str | None) -> bytes:
    if text is None:
        return i(9) + i(-1)
    raw = text.encode()
    return i(9 | (1 << 3 << 12)) + i(len(raw)) + raw


class Writer:
    """Just enough of R's serializer to write symbols once and refer back."""

    def __init__(self) -> None:
        self.symbols: list[str] = []

    def symbol(self, name: str) -> bytes:
        if name in self.symbols:
            return i(255 | ((self.symbols.index(name) + 1) << 8))
        self.symbols.append(name)
        return i(1) + charsxp(name)

    def pairlist(self, items: list[tuple[str, Callable[[], bytes]]]) -> bytes:
        """Tagged pairlist; each value is built after its tag, in stream order."""
        out = b""
        for name, build in items:
            out += i(2 | TAG) + self.symbol(name)
            out += build()
        return out + i(254)

    def altrep_info(self, class_name: str) -> bytes:
        """ALTREP class info: LIST(class symbol, package symbol, type)."""
        return (
            i(2)
            + self.symbol(class_name)
            + i(2)
            + self.symbol("base")
            + i(2)
            + self.vector(13, intsxp([14]))
            + i(254)
        )

    def vector(self, sexptype: int, body: bytes, attributes: bytes = b"") -> bytes:
        return i(sexptype | (ATTR if attributes else 0)) + body + attributes


def strsxp(values: list[str | None]) -> bytes:
    return i(len(values)) + b"".join(charsxp(v) for v in values)


def realsxp(values: list[float | None]) -> bytes:
    return i(len(values)) + b"".join(
        NA_REAL if v is None else struct.pack(">d", v) for v in values
    )


def intsxp(values: list[int | None]) -> bytes:
    return i(len(values)) + b"".join(i(NA_INT if v is None else v) for v in values)


def _frame(w: Writer) -> bytes:
    columns = [
        ("election_year", lambda: w.vector(14, realsxp([1992.0, 1996.0, None]))),
        ("fips", lambda: w.vector(16, strsxp(["55025", "12025", None]))),
        (
            "office",
            lambda: w.vector(
                13,
                intsxp([1, 1, 2]),
                w.pairlist(
                    [
                        ("levels", lambda: w.vector(16, strsxp(["PRES", "SEN"]))),
                        ("class", lambda: w.vector(16, strsxp(["factor"]))),
                    ]
                ),
            ),
        ),
        (
            "county_first_date",
            lambda: w.vector(
                14,
                realsxp([0.0, 365.0, None]),
                w.pairlist([("class", lambda: w.vector(16, strsxp(["Date"])))]),
            ),
        ),
        # A wrapped vector (ALTREP wrap_real), as R writes after some copies.
        (
            "democratic_raw_votes",
            lambda: (
                i(238)
                + w.altrep_info("wrap_real")
                # The state is CONS(x, meta).
                + i(2)
                + w.vector(14, realsxp([120000.0, None, 5.0]))
                + i(2)
                + w.vector(13, intsxp([0, 0]))
                + i(254)
                + i(254)
            ),
        ),
    ]
    body = i(19 | ATTR) + i(len(columns)) + b"".join(build() for _, build in columns)
    return body + w.pairlist(
        [
            ("names", lambda: w.vector(16, strsxp([name for name, _ in columns]))),
            ("class", lambda: w.vector(16, strsxp(["data.frame"]))),
            # row.names as a compact integer sequence (ALTREP compact_intseq).
            (
                "row.names",
                lambda: (
                    i(238)
                    + w.altrep_info("compact_intseq")
                    + w.vector(14, realsxp([3.0, 1.0, 1.0]))
                    + i(254)
                ),
            ),
        ]
    )


def rdx3_file() -> bytes:
    w = Writer()
    body = (
        b"X\n"
        + i(3)
        + i(0x00040002)
        + i(0x00030500)
        + i(5)
        + b"UTF-8"
        + w.pairlist(
            [
                (".Random.seed", lambda: w.vector(13, intsxp([10403, 624, 1]))),
                ("pres_elections_release", lambda: _frame(w)),
            ]
        )
    )
    return gzip.compress(b"RDX3\n" + body)


def test_an_r4_save_file_reads_every_object_by_name() -> None:
    """Covers: ETL-111 — .Random.seed first, then the frame; selected by name."""
    objects = read_rdata(rdx3_file())
    assert list(objects) == [".Random.seed", "pres_elections_release"]
    (frame,) = read_data_frames(rdx3_file())
    assert frame.name == "pres_elections_release" and frame.row_count == 3
    assert frame.columns == {
        "election_year": [1992.0, 1996.0, None],
        "fips": ["55025", "12025", None],
        "office": ["PRES", "PRES", "SEN"],
        "county_first_date": [date(1970, 1, 1), date(1971, 1, 1), None],
        "democratic_raw_votes": [120000.0, None, 5.0],
    }


def test_the_version_two_fixture_written_by_another_tool_reads_the_same() -> None:
    """Covers: ETL-111 — pyreadr's RDX2 output (the checked-in fixture) decodes."""
    (frame,) = read_data_frames(fixture_bytes("county_president_history.Rdata"))
    assert frame.name == "pres_elections_release"
    assert frame.row_count == 14
    assert frame.columns["fips"][:2] == ["55025", "55025"]
    assert frame.columns["democratic_raw_votes"][6] is None


@pytest.mark.parametrize(
    "raw",
    [b"<html>maintenance</html>", gzip.compress(b"RDA3\nA\n"), b"RDX3\nB\n"],
)
def test_anything_else_is_refused_by_name(raw: bytes) -> None:
    """Covers: ETL-111 — not an R file, or R's ASCII format, is refused, not guessed."""
    with pytest.raises(RDataError):
        read_rdata(raw)
