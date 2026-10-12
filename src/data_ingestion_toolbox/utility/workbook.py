"""A minimal, read-only reader for one sheet of an Office Open XML workbook.

Public-data workbooks (FHFA's annual HPI files, HUD's FMR and income-limit
files) are read without a spreadsheet library, which also refuses some of
them over malformed document properties. Only what a sheet needs is read:
the shared string table, inline strings and numbers. A cell keeps the text the file
stores, so a number is the file's own decimal representation and a code
stored as text keeps its leading zeros. Nothing is evaluated: a formula's
cached value is read as written.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from xml.etree.ElementTree import ParseError, fromstring, iterparse

_MAIN = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
_PACKAGE_REL = "{http://schemas.openxmlformats.org/package/2006/relationships}"


class WorkbookError(ValueError):
    """The bytes are not a workbook holding the named sheet."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class Cell:
    """``kind`` is ``text`` or ``number``; ``value`` is as the file stores it."""

    kind: str
    value: str


def _column(reference: str) -> int:
    """``C7`` -> 3."""
    number = 0
    for character in reference:
        if not character.isalpha():
            break
        number = number * 26 + (ord(character.upper()) - 64)
    return number


def _text(element) -> str:  # noqa: ANN001
    return "".join(node.text or "" for node in element.iter(f"{_MAIN}t"))


def _sheet_member(archive: zipfile.ZipFile, sheet: str) -> str:
    try:
        workbook = fromstring(archive.read("xl/workbook.xml"))
        relations = fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    except (KeyError, ParseError) as exc:
        raise WorkbookError("not_a_workbook") from exc
    targets = {
        rel.get("Id"): rel.get("Target", "")
        for rel in relations.iter(f"{_PACKAGE_REL}Relationship")
    }
    for element in workbook.iter(f"{_MAIN}sheet"):
        if element.get("name") == sheet:
            target = targets.get(element.get(f"{_REL}id"), "")
            target = target.lstrip("/")
            return target if target.startswith("xl/") else f"xl/{target}"
    raise WorkbookError("sheet_missing")


def read_sheet(raw_bytes: bytes, sheet: str) -> Iterator[tuple[int, dict[int, Cell]]]:
    """Yield (row number, {column number: cell}) for every stored row."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw_bytes))
    except zipfile.BadZipFile as exc:
        raise WorkbookError("not_a_workbook") from exc
    member = _sheet_member(archive, sheet)
    strings: list[str] = []
    if "xl/sharedStrings.xml" in archive.namelist():
        try:
            table = fromstring(archive.read("xl/sharedStrings.xml"))
        except ParseError as exc:
            raise WorkbookError("not_a_workbook") from exc
        strings = [_text(item) for item in table.findall(f"{_MAIN}si")]
    try:
        stream = archive.open(member)
    except KeyError as exc:
        raise WorkbookError("sheet_missing") from exc
    try:
        for _event, element in iterparse(stream):
            if element.tag != f"{_MAIN}row":
                continue
            cells: dict[int, Cell] = {}
            for cell in element.findall(f"{_MAIN}c"):
                kind = cell.get("t", "n")
                if kind == "inlineStr":
                    inline = cell.find(f"{_MAIN}is")
                    if inline is not None:
                        cells[_column(cell.get("r", ""))] = Cell("text", _text(inline))
                    continue
                stored = cell.find(f"{_MAIN}v")
                if stored is None or stored.text is None:
                    continue
                if kind == "s":
                    cells[_column(cell.get("r", ""))] = Cell(
                        "text", strings[int(stored.text)]
                    )
                elif kind in ("str", "e"):
                    cells[_column(cell.get("r", ""))] = Cell("text", stored.text)
                else:
                    cells[_column(cell.get("r", ""))] = Cell("number", stored.text)
            yield int(element.get("r", "0")), cells
            element.clear()
    except (ParseError, IndexError, ValueError) as exc:
        raise WorkbookError("unreadable_sheet") from exc
