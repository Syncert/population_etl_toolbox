"""No committed merge-conflict markers (ENV-024).

A conflict marker that reaches a branch is not a merge conflict any more: Git
has stopped tracking it, so nothing reminds anyone it is there. The reader of
the rendered file is the first to see it. This module makes it a test failure.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

#: A marker line as Git writes it: seven characters at the start of a line,
#: alone or followed by a space and a label. Setext headings and Markdown
#: table rules use longer runs, so the exact width matters.
CONFLICT_MARKER = re.compile(r"^(<{7}|={7}|>{7}|\|{7})(?: .*)?$")

#: Files whose content is data, where a seven-character run is legitimate.
EXEMPT_SUFFIXES = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".ico",
    ".pdf",
    ".zip",
    ".gz",
    ".parquet",
    ".woff",
    ".woff2",
    ".pbf",
    ".mbtiles",
}


def _tracked_files() -> list[Path]:
    output = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8")
    return [
        REPOSITORY_ROOT / name
        for name in output.split("\0")
        if name and Path(name).suffix.lower() not in EXEMPT_SUFFIXES
    ]


def _marker_lines(text: str) -> list[int]:
    return [
        number
        for number, line in enumerate(text.splitlines(), start=1)
        if CONFLICT_MARKER.match(line)
    ]


def _conflicted_files() -> list[str]:
    # `git grep` narrows the corpus to files with an opening or closing
    # marker (one stray `=======` can be prose; a conflict always leaves at
    # least one of the other two), then the exact pattern is applied here.
    candidates = subprocess.run(
        ["git", "grep", "-I", "-l", "-E", "^(<{7}|>{7})([ \r]|$)"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
    ).stdout.decode("utf-8")
    exempt = set(EXEMPT_SUFFIXES)
    found = []
    for name in candidates.splitlines():
        path = REPOSITORY_ROOT / name
        if path.suffix.lower() in exempt or not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        lines = _marker_lines(text.replace("\r\n", "\n"))
        if lines:
            found.append(f"{name}: lines {lines}")
    return found


def test_marker_pattern_matches_what_git_writes() -> None:
    """Covers: ENV-024 -- the pattern catches Git's markers, and only those."""
    conflicted = "a\n<<<<<<< HEAD\nb\n=======\nc\n>>>>>>> branch\n"
    assert _marker_lines(conflicted) == [2, 4, 6]
    assert _marker_lines("<<<<<<<\n=======\n>>>>>>>\n") == [1, 2, 3]
    # A Setext heading underline and a longer rule are not markers.
    assert _marker_lines("Title\n========\n--------\n") == []
    assert _marker_lines("x <<<<<<< y\n") == []


def test_tracked_files_are_found() -> None:
    """Covers: ENV-024 -- the check reads the repository, not an empty list."""
    names = {path.relative_to(REPOSITORY_ROOT).as_posix() for path in _tracked_files()}
    assert "AGENTS.md" in names
    assert "docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md" in names


def test_no_tracked_file_carries_a_conflict_marker() -> None:
    """Covers: ENV-024 -- no tracked text file holds a merge-conflict marker."""
    assert _conflicted_files() == []
