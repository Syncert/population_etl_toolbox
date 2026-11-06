"""Stand-in SOI zips (1990-1991 to 2010-2011) built from reviewed members.

SOI publishes these pairs as zips of per-state members, each 2.5 to 15 MB.
The checked-in members under ``tests/fixtures/irs_migration/state_members``
are Delaware's, copied verbatim from four of those zips; a test zip holds
one reviewed pair's members laid out as SOI lays them out. No network.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

MEMBER_DIR = (
    Path(__file__).resolve().parents[2] / "tests/fixtures/irs_migration/state_members"
)

#: Delaware's own members of SOI's zips (copied verbatim, see the folder's
#: README), by the pair whose zip they came from.
STATE_MEMBERS: dict[str, tuple[str, ...]] = {
    "1990-1991": ("C9091dei.txt", "C9091deo.txt"),
    "1993-1994": ("co934dei.xls", "co934deo.xls"),
    "2003-2004": ("co0304DEi.xls", "co0304DEo.xls"),
    "2010-2011": ("co1011ide.xls", "co1011ode.xls"),
}


def members_for(year1: int) -> str:
    """The reviewed pair whose members stand in for a pair's zip."""
    if year1 <= 1991:
        return "1990-1991"
    if year1 <= 1994:
        return "1993-1994"
    return "2003-2004" if year1 <= 2009 else "2010-2011"


def state_member_zip(source_pair: str) -> bytes:
    """A zip holding one reviewed pair's Delaware members, laid out as SOI's.

    Text members sit in ``...Inflow/`` and ``...Outflow/`` folders, as in
    the 1990-1991 zip; workbooks at the root, as in the 2004 and later zips.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in STATE_MEMBERS[source_pair]:
            data = (MEMBER_DIR / name).read_bytes()
            if name.endswith(".txt"):
                folder = "Inflow" if name[-5] == "i" else "Outflow"
                name = f"CountyMigration{folder}/{name}"
            # A fixed timestamp: `writestr(name, ...)` stamps each member
            # with the current time, so two builds in different two-second
            # windows were different bytes, and a test asserting that a
            # re-capture stores no new payload failed whenever the suite ran
            # slowly enough to cross one.
            member = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            member.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(member, data)
    return buffer.getvalue()


def zip_payload_for(path: str) -> bytes | None:
    """Stand-in bytes for a registered zip path, or ``None`` for a CSV."""
    name = path.rsplit("/", 1)[-1]
    if not name.endswith(".zip"):
        return None
    if name.startswith("county"):
        year1 = 2000 + int(name[6:8])
    else:
        year1 = int(name[:4])
    return state_member_zip(members_for(year1))
