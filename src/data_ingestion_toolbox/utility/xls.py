"""Read the cells of a legacy Excel ``.xls`` workbook with the standard library.

SOI published its county migration files from 1995-1996 to 2010-2011 as
Excel 97-2003 (BIFF8) and Excel 5/95 (BIFF5) workbooks. The Airflow image
carries no spreadsheet library (see ``utility/workbook.py`` for the
``.xlsx`` reader), so this module reads the two layers itself:

* the OLE2 compound file, to find the ``Workbook`` (BIFF8) or ``Book``
  (BIFF5) stream;
* the BIFF records of that stream's first worksheet: numbers (``NUMBER``,
  ``RK``, ``MULRK``, a ``FORMULA``'s cached result) and text (``LABELSST``
  through the shared string table, ``LABEL``, ``RSTRING``).

Only cell values are read: no formats, dates, merged cells or formulas.
A cell keeps what is stored: text as text, a number as a ``float``. A
workbook this reader cannot follow raises :class:`XlsFormatError`, never
a partial sheet.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator

_SIGNATURE = bytes.fromhex("D0CF11E0A1B11AE1")
_END_OF_CHAIN = 0xFFFFFFFE
_FREE_SECTOR = 0xFFFFFFFF
_MAX_SECTORS = 1 << 22

# BIFF record types read here.
_BOF = 0x0809
_BOF_BIFF5 = 0x0809
_EOF = 0x000A
_SST = 0x00FC
_CONTINUE = 0x003C
_CODEPAGE = 0x0042
_NUMBER = 0x0203
_RK = 0x027E
_MULRK = 0x00BD
_LABELSST = 0x00FD
_LABEL = 0x0204
_RSTRING = 0x00D6
_FORMULA = 0x0006
_STRING = 0x0207

Cell = str | float


class XlsFormatError(ValueError):
    """The bytes are not a workbook this reader can follow."""


def _u16(data: bytes, offset: int) -> int:
    return struct.unpack_from("<H", data, offset)[0]


def _u32(data: bytes, offset: int) -> int:
    return struct.unpack_from("<I", data, offset)[0]


def _stream(payload: bytes, names: tuple[str, ...]) -> bytes:
    """The bytes of the first named stream in an OLE2 compound file."""
    if len(payload) < 512 or payload[:8] != _SIGNATURE:
        raise XlsFormatError("not an OLE2 compound file")
    sector_size = 1 << _u16(payload, 0x1E)
    if sector_size not in (512, 4096):
        raise XlsFormatError("unsupported sector size")
    fat_sector_count = _u32(payload, 0x2C)
    first_directory = _u32(payload, 0x30)
    mini_cutoff = _u32(payload, 0x38)
    difat_next = _u32(payload, 0x44)
    difat_count = _u32(payload, 0x48)

    def sector(index: int) -> bytes:
        start = (index + 1) * sector_size
        if index >= _MAX_SECTORS or start + sector_size > len(payload) + sector_size:
            raise XlsFormatError("sector outside the file")
        return payload[start : start + sector_size]

    fat_sectors = [
        _u32(payload, 0x4C + 4 * i)
        for i in range(109)
        if _u32(payload, 0x4C + 4 * i) not in (_FREE_SECTOR, _END_OF_CHAIN)
    ]
    per_difat = sector_size // 4 - 1
    for _ in range(difat_count):
        if difat_next in (_FREE_SECTOR, _END_OF_CHAIN):
            break
        block = sector(difat_next)
        for i in range(per_difat):
            entry = _u32(block, 4 * i)
            if entry not in (_FREE_SECTOR, _END_OF_CHAIN):
                fat_sectors.append(entry)
        difat_next = _u32(block, 4 * per_difat)
    fat_sectors = fat_sectors[:fat_sector_count]
    fat_bytes = b"".join(sector(index) for index in fat_sectors)
    fat = struct.unpack(f"<{len(fat_bytes) // 4}I", fat_bytes)

    def chain(start: int) -> bytes:
        parts: list[bytes] = []
        seen: set[int] = set()
        current = start
        while current != _END_OF_CHAIN:
            if current in seen or current >= len(fat):
                raise XlsFormatError("broken sector chain")
            seen.add(current)
            parts.append(sector(current))
            current = fat[current]
        return b"".join(parts)

    directory = chain(first_directory)
    for offset in range(0, len(directory), 128):
        entry = directory[offset : offset + 128]
        name_length = _u16(entry, 64)
        if not 2 <= name_length <= 64 or entry[66] != 2:  # 2 = stream
            continue
        name = entry[: name_length - 2].decode("utf-16-le", errors="replace")
        if name not in names:
            continue
        size = _u32(entry, 120)
        if size < mini_cutoff:
            raise XlsFormatError("workbook stream is in the mini stream")
        return chain(_u32(entry, 116))[:size]
    raise XlsFormatError("no Workbook or Book stream")


def _records(stream: bytes) -> Iterator[tuple[int, bytes]]:
    offset = 0
    while offset + 4 <= len(stream):
        kind, length = struct.unpack_from("<HH", stream, offset)
        yield kind, stream[offset + 4 : offset + 4 + length]
        offset += 4 + length


def _rk(value: int) -> float:
    if value & 0x02:
        number = float(value >> 2 if value < 0x80000000 else (value >> 2) - (1 << 30))
    else:
        number = struct.unpack("<d", struct.pack("<Q", (value & 0xFFFFFFFC) << 32))[0]
    return number / 100 if value & 0x01 else number


class _Chunks:
    """The SST record and its CONTINUE records, read across their boundaries."""

    def __init__(self, parts: list[bytes]) -> None:
        self.parts = parts
        self.part = 0
        self.offset = 0

    def _ensure(self) -> None:
        while self.part < len(self.parts) and self.offset >= len(self.parts[self.part]):
            self.part += 1
            self.offset = 0
        if self.part >= len(self.parts):
            raise XlsFormatError("shared string table ends early")

    def take(self, count: int) -> bytes:
        out = bytearray()
        while count:
            self._ensure()
            data = self.parts[self.part]
            piece = data[self.offset : self.offset + count]
            out += piece
            self.offset += len(piece)
            count -= len(piece)
        return bytes(out)

    def characters(self, count: int, wide: bool) -> str:
        """``count`` characters; a CONTINUE restates the width in one byte."""
        text: list[str] = []
        while count:
            self._ensure()
            data = self.parts[self.part]
            if self.offset == 0 and self.part > 0 and text:
                wide = bool(data[0] & 0x01)
                self.offset = 1
            width = 2 if wide else 1
            available = (len(data) - self.offset) // width
            if available == 0:
                self.part += 1
                self.offset = 0
                if self.part >= len(self.parts):
                    raise XlsFormatError("shared string table ends early")
                data = self.parts[self.part]
                wide = bool(data[0] & 0x01)
                self.offset = 1
                continue
            take = min(count, available)
            raw = data[self.offset : self.offset + take * width]
            text.append(raw.decode("utf-16-le" if wide else "latin-1"))
            self.offset += take * width
            count -= take
            if count and self.offset >= len(data):
                self.part += 1
                self.offset = 0
                if self.part >= len(self.parts):
                    raise XlsFormatError("shared string table ends early")
                wide = bool(self.parts[self.part][0] & 0x01)
                self.offset = 1
        return "".join(text)


def _shared_strings(parts: list[bytes]) -> list[str]:
    if not parts:
        return []
    unique = _u32(parts[0], 4)
    chunks = _Chunks([parts[0][8:], *parts[1:]])
    strings: list[str] = []
    for _ in range(unique):
        count = _u16(chunks.take(2), 0)
        flags = chunks.take(1)[0]
        runs = _u16(chunks.take(2), 0) if flags & 0x08 else 0
        extension = _u32(chunks.take(4), 0) if flags & 0x04 else 0
        strings.append(chunks.characters(count, bool(flags & 0x01)))
        if runs or extension:
            chunks.take(4 * runs + extension)
    return strings


def _biff8_string(data: bytes, offset: int) -> str:
    count = _u16(data, offset)
    flags = data[offset + 2]
    start = offset + 3
    if flags & 0x08:
        start += 2
    if flags & 0x04:
        start += 4
    if flags & 0x01:
        return data[start : start + 2 * count].decode("utf-16-le")
    return data[start : start + count].decode("latin-1")


def _biff5_string(data: bytes, offset: int, encoding: str) -> str:
    count = _u16(data, offset)
    return data[offset + 2 : offset + 2 + count].decode(encoding, errors="replace")


def read_first_sheet(payload: bytes) -> list[list[Cell]]:
    """Every row of the first worksheet, as stored; an empty cell is ``""``."""
    stream = _stream(payload, ("Workbook", "Book"))
    records = list(_records(stream))
    if not records or records[0][0] not in (_BOF, _BOF_BIFF5, 0x0209, 0x0409):
        raise XlsFormatError("the stream does not open with a BOF record")
    biff8 = _u16(records[0][1], 0) == 0x0600
    encoding = "cp1252"
    sst_parts: list[bytes] = []
    cells: dict[tuple[int, int], Cell] = {}
    substream = 0
    in_sst = False
    pending_string: tuple[int, int] | None = None
    for kind, data in records:
        if kind in (_BOF, 0x0209, 0x0409):
            substream += 1
            in_sst = False
            if substream > 2:
                break
            continue
        if substream == 1:
            if kind == _SST:
                sst_parts = [data]
                in_sst = True
            elif kind == _CONTINUE and in_sst:
                sst_parts.append(data)
            else:
                in_sst = False
                if kind == _CODEPAGE and len(data) >= 2 and not biff8:
                    page = _u16(data, 0)
                    encoding = (
                        "cp1252" if page in (1200, 0x8000, 0x8001) else f"cp{page}"
                    )
            continue
        if kind == _EOF:
            break
        if kind == _NUMBER:
            row, col = _u16(data, 0), _u16(data, 2)
            cells[(row, col)] = struct.unpack_from("<d", data, 6)[0]
        elif kind == _RK:
            row, col = _u16(data, 0), _u16(data, 2)
            cells[(row, col)] = _rk(_u32(data, 6))
        elif kind == _MULRK:
            row, first = _u16(data, 0), _u16(data, 2)
            last = _u16(data, len(data) - 2)
            for index, col in enumerate(range(first, last + 1)):
                cells[(row, col)] = _rk(_u32(data, 4 + 6 * index + 2))
        elif kind == _LABELSST:
            row, col = _u16(data, 0), _u16(data, 2)
            cells[(row, col)] = (row, col, _u32(data, 6))  # type: ignore[assignment]
        elif kind in (_LABEL, _RSTRING):
            row, col = _u16(data, 0), _u16(data, 2)
            cells[(row, col)] = (
                _biff8_string(data, 6) if biff8 else _biff5_string(data, 6, encoding)
            )
        elif kind == _FORMULA:
            row, col = _u16(data, 0), _u16(data, 2)
            result = data[6:14]
            if result[6:8] != b"\xff\xff":
                cells[(row, col)] = struct.unpack("<d", result)[0]
            elif result[0] == 0:
                pending_string = (row, col)
            else:
                cells[(row, col)] = ""
        elif kind == _STRING and pending_string is not None:
            cells[pending_string] = (
                _biff8_string(data, 0) if biff8 else _biff5_string(data, 0, encoding)
            )
            pending_string = None
    if substream < 2:
        raise XlsFormatError("the workbook has no worksheet")
    strings = _shared_strings(sst_parts) if biff8 else []
    for key, value in list(cells.items()):
        if isinstance(value, tuple):
            index = value[2]
            if index >= len(strings):
                raise XlsFormatError("a cell names a shared string that is absent")
            cells[key] = strings[index]
    if not cells:
        return []
    rows = max(row for row, _col in cells) + 1
    cols = max(col for _row, col in cells) + 1
    return [[cells.get((row, col), "") for col in range(cols)] for row in range(rows)]
