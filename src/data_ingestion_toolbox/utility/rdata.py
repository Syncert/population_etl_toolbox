"""Read the data frames in an R ``.RData`` file, using only the standard library.

R's ``save()`` writes a compressed (gzip, bzip2 or xz) stream: the magic
``RDX2\\n`` or ``RDX3\\n``, then one serialized pairlist naming each saved
object. The serialization is R's own XDR format ("R Internals", section 1.8).
This reader decodes the subset a published table needs: logical, integer,
double and character vectors, lists, pairlists, symbols, attributes, factors,
``Date`` columns, and the ALTREP forms R 3.5 and later write for compact
sequences, wrapped vectors and deferred strings. Anything else (closures,
environments, bytecode) is refused by name rather than guessed at.

Missing values come back as ``None``. The pyreadr package reads the same
files but is AGPL-3.0 and needs pandas, which is why this module exists.
"""

from __future__ import annotations

import bz2
import gzip
import lzma
import math
import struct
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

NILSXP, SYMSXP, LISTSXP, CLOSXP, ENVSXP, PROMSXP, LANGSXP = 0, 1, 2, 3, 4, 5, 6
CHARSXP, LGLSXP, INTSXP, REALSXP, CPLXSXP, STRSXP = 9, 10, 13, 14, 15, 16
DOTSXP, VECSXP, EXPRSXP, RAWSXP, S4SXP = 17, 19, 20, 24, 25
ALTREP_SXP, ATTRLISTSXP, ATTRLANGSXP = 238, 239, 240
BASEENV_SXP, EMPTYENV_SXP = 241, 242
GENERICREFSXP, PACKAGESXP, PERSISTSXP, NAMESPACESXP, BASENAMESPACE_SXP = (
    245,
    248,
    247,
    249,
    250,
)
MISSINGARG_SXP, UNBOUNDVALUE_SXP, GLOBALENV_SXP, NILVALUE_SXP, REFSXP = (
    251,
    252,
    253,
    254,
    255,
)

_NA_INTEGER = -(2**31)
_EPOCH = date(1970, 1, 1)
_SINGLETONS = {
    NILVALUE_SXP,
    EMPTYENV_SXP,
    BASEENV_SXP,
    GLOBALENV_SXP,
    UNBOUNDVALUE_SXP,
    MISSINGARG_SXP,
    BASENAMESPACE_SXP,
}


class RDataError(ValueError):
    """The bytes are not an R data file this reader can decode."""


@dataclass(frozen=True)
class Symbol:
    name: str


@dataclass
class RObject:
    """A decoded R value: its type, its payload and its attributes."""

    sexptype: int
    value: Any
    attributes: dict[str, Any]

    def attribute(self, name: str) -> Any:
        return self.attributes.get(name)


@dataclass(frozen=True)
class DataFrame:
    """One data frame: its column names, in order, and each column's values."""

    name: str
    columns: dict[str, list[Any]]
    row_count: int


def _decompress(raw: bytes) -> bytes:
    if raw[:2] == b"\x1f\x8b":
        return gzip.decompress(raw)
    if raw[:3] == b"BZh":
        return bz2.decompress(raw)
    if raw[:6] == b"\xfd7zXZ\x00":
        return lzma.decompress(raw)
    return raw


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.position = 0
        self.references: list[Any] = []

    def _take(self, size: int) -> bytes:
        end = self.position + size
        if end > len(self.data):
            raise RDataError("the R stream ends early")
        chunk = self.data[self.position : end]
        self.position = end
        return chunk

    def integer(self) -> int:
        return struct.unpack(">i", self._take(4))[0]

    def length(self) -> int:
        size = self.integer()
        if size == -1:
            upper, lower = struct.unpack(">II", self._take(8))
            return (upper << 32) + lower
        if size < 0:
            raise RDataError(f"vector length {size} is negative")
        return size

    def integers(self, count: int) -> list[int | None]:
        values = struct.unpack(f">{count}i", self._take(4 * count))
        return [None if value == _NA_INTEGER else value for value in values]

    def doubles(self, count: int) -> list[float | None]:
        values = struct.unpack(f">{count}d", self._take(8 * count))
        return [None if math.isnan(value) else value for value in values]

    def header(self) -> None:
        if self._take(2) != b"X\n":
            raise RDataError("only R's XDR (binary) serialization is supported")
        version = self.integer()
        self.integer()  # the R version that wrote the file
        self.integer()  # the oldest R version that can read it
        if version == 3:
            self._take(self.integer())  # the writer's native encoding name
        elif version != 2:
            raise RDataError(f"serialization version {version} is not supported")

    def attributes(self) -> dict[str, Any]:
        pairs = self.item()
        if isinstance(pairs, RObject) and pairs.sexptype == LISTSXP:
            return dict(pairs.value)
        return {}

    def item(self) -> Any:  # noqa: C901, PLR0911, PLR0912 - one branch per R type
        flags = self.integer()
        sexptype = flags & 0xFF
        levels = flags >> 12
        has_attributes = bool(flags & (1 << 9))
        has_tag = bool(flags & (1 << 10))

        if sexptype in _SINGLETONS:
            return None
        if sexptype == REFSXP:
            index = flags >> 8
            if index == 0:
                index = self.integer()
            try:
                return self.references[index - 1]
            except IndexError as exc:
                raise RDataError(f"reference {index} names nothing read") from exc
        if sexptype in (PERSISTSXP, PACKAGESXP, NAMESPACESXP):
            if self.integer() != 0:
                raise RDataError("a persistent name with flags is not supported")
            names = [self.item() for _ in range(self.integer())]
            self.references.append(names)
            return names
        if sexptype == SYMSXP:
            symbol = Symbol(str(self.item()))
            self.references.append(symbol)
            return symbol
        if sexptype == CHARSXP:
            size = self.integer()
            if size == -1:
                return None
            raw = self._take(size)
            encoding = "latin-1" if levels & (1 << 2) else "utf-8"
            return raw.decode(encoding, errors="replace")
        if sexptype in (
            LISTSXP,
            LANGSXP,
            CLOSXP,
            PROMSXP,
            DOTSXP,
            ATTRLISTSXP,
            ATTRLANGSXP,
        ):
            pairs: list[tuple[str | None, Any]] = []
            attributes: dict[str, Any] = {}
            while True:
                if has_attributes:
                    attributes.update(self.attributes())
                tag = self.item() if has_tag else None
                pairs.append(
                    (tag.name if isinstance(tag, Symbol) else None, self.item())
                )
                cdr_flags = self.integer()
                if cdr_flags & 0xFF != sexptype:
                    self.position -= 4
                    tail = self.item()
                    if tail is not None:
                        pairs.append((None, tail))
                    break
                has_attributes = bool(cdr_flags & (1 << 9))
                has_tag = bool(cdr_flags & (1 << 10))
            return RObject(LISTSXP, pairs, attributes)
        if sexptype == ENVSXP:
            raise RDataError("an R environment is not a table")
        if sexptype == ALTREP_SXP:
            return self._altrep()

        value: Any
        if sexptype in (LGLSXP, INTSXP):
            count = self.length()
            numbers = self.integers(count)
            value = (
                [None if v is None else bool(v) for v in numbers]
                if sexptype == LGLSXP
                else numbers
            )
        elif sexptype == REALSXP:
            value = self.doubles(self.length())
        elif sexptype == CPLXSXP:
            pairs_of = self.doubles(2 * self.length())
            value = [
                None if re is None or im is None else complex(re, im)
                for re, im in zip(pairs_of[::2], pairs_of[1::2], strict=True)
            ]
        elif sexptype == STRSXP:
            value = [self.item() for _ in range(self.length())]
        elif sexptype in (VECSXP, EXPRSXP):
            value = [self.item() for _ in range(self.length())]
        elif sexptype == RAWSXP:
            value = self._take(self.length())
        elif sexptype == S4SXP:
            value = None
        else:
            raise RDataError(f"R type {sexptype} is not supported")
        attributes = self.attributes() if has_attributes else {}
        return RObject(sexptype, value, attributes)

    def _altrep(self) -> RObject:
        info = self.item()
        state = self.item()
        attributes_object = self.item()
        attributes = (
            dict(attributes_object.value)
            if isinstance(attributes_object, RObject)
            and attributes_object.sexptype == LISTSXP
            else {}
        )
        class_name = ""
        if isinstance(info, RObject) and info.value:
            head = info.value[0][1]
            if isinstance(head, Symbol):
                class_name = head.name
        if class_name in ("compact_intseq", "compact_realseq"):
            size, start, step = (state.value if isinstance(state, RObject) else state)[
                :3
            ]
            numbers = [start + step * index for index in range(int(size))]
            if class_name == "compact_intseq":
                return RObject(INTSXP, [int(n) for n in numbers], attributes)
            return RObject(REALSXP, numbers, attributes)
        if class_name.startswith("wrap_"):
            # The state is CONS(x, meta): the wrapped vector, then its metadata.
            wrapped = (
                _first(state.value)
                if isinstance(state, RObject) and state.value
                else None
            )
            if isinstance(wrapped, RObject):
                return RObject(
                    wrapped.sexptype,
                    wrapped.value,
                    {**wrapped.attributes, **attributes},
                )
        if class_name == "deferred_string":
            source = (
                _first(state.value)
                if isinstance(state, RObject) and state.value
                else None
            )
            if isinstance(source, RObject):
                strings = [
                    None if v is None else _format_number(v, source.sexptype)
                    for v in source.value
                ]
                return RObject(STRSXP, strings, attributes)
        raise RDataError(f"ALTREP class {class_name or '?'} is not supported")


def _first(values: list[Any]) -> Any:
    """The first element of a list or pairlist payload."""
    head = values[0]
    return head[1] if isinstance(head, tuple) else head


def _format_number(value: Any, sexptype: int) -> str:
    if sexptype == INTSXP:
        return str(value)
    if float(value).is_integer():
        return str(int(value))
    return repr(float(value))


def _column(value: Any) -> list[Any]:
    """A data-frame column as Python values: factors as labels, Dates as days."""
    if not isinstance(value, RObject):
        raise RDataError("a data-frame column is not a vector")
    classes = value.attribute("class")
    class_names = set(classes.value) if isinstance(classes, RObject) else set()
    if "factor" in class_names:
        levels = value.attribute("levels").value
        return [None if code is None else levels[code - 1] for code in value.value]
    if "Date" in class_names:
        return [
            None if days is None else _EPOCH + timedelta(days=int(days))
            for days in value.value
        ]
    if value.sexptype in (LGLSXP, INTSXP, REALSXP, STRSXP, CPLXSXP):
        return list(value.value)
    raise RDataError(f"a data-frame column of R type {value.sexptype} is not supported")


def read_rdata(raw: bytes) -> dict[str, Any]:
    """Every object an ``.RData`` file saves, by name, decoded."""
    data = _decompress(raw)
    if data[:5] not in (b"RDX2\n", b"RDX3\n"):
        raise RDataError("not an R save() file (no RDX2/RDX3 magic)")
    reader = _Reader(data[5:])
    reader.header()
    top = reader.item()
    if not isinstance(top, RObject) or top.sexptype != LISTSXP:
        raise RDataError("an R save() file holds a pairlist of named objects")
    return {name: value for name, value in top.value if name is not None}


def read_data_frames(raw: bytes) -> list[DataFrame]:
    """The data frames an ``.RData`` file saves, in the order saved."""
    frames: list[DataFrame] = []
    for name, value in read_rdata(raw).items():
        if not isinstance(value, RObject) or value.sexptype != VECSXP:
            continue
        classes = value.attribute("class")
        if not isinstance(classes, RObject) or "data.frame" not in classes.value:
            continue
        names = value.attribute("names")
        if not isinstance(names, RObject) or len(names.value) != len(value.value):
            raise RDataError(f"data frame {name} has no column names")
        columns = {
            str(column): _column(values)
            for column, values in zip(names.value, value.value, strict=True)
        }
        lengths = {len(values) for values in columns.values()}
        if len(lengths) > 1:
            raise RDataError(f"data frame {name} has columns of different lengths")
        frames.append(DataFrame(name, columns, lengths.pop() if lengths else 0))
    return frames
