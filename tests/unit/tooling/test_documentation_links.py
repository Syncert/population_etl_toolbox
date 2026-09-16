"""Documentation cross-reference integrity (ENV-021).

A stale link is not noticed by the person who wrote it. It is noticed by the
next reader, who follows it, finds nothing, and has no way to tell whether the
document moved, was renamed, or never existed. This module makes that a test
failure instead.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

#: `[text](href)`, with href stopping at the first closing parenthesis.
MARKDOWN_LINK = re.compile(r"\[([^\]]*)\]\(([^)]+)\)")

#: Links this check cannot resolve against the filesystem.
EXTERNAL = re.compile(r"^(https?:|mailto:|ftp:|#)")


def _documentation_files() -> list[Path]:
    """Every Markdown file a reader reaches from the repository root."""
    files = sorted(REPOSITORY_ROOT.glob("*.md"))
    files.extend(sorted(REPOSITORY_ROOT.glob("docs/**/*.md")))
    return files


def _links(path: Path) -> list[tuple[str, str]]:
    return MARKDOWN_LINK.findall(path.read_text(encoding="utf-8"))


def _relative_links(path: Path) -> list[tuple[str, str, str]]:
    """Yield (text, href, target) for links this check can resolve."""
    found = []
    for text, href in _links(path):
        href = href.strip()
        if EXTERNAL.match(href):
            continue
        # A link may carry an anchor; the file is what exists or does not.
        target = href.split("#", 1)[0].strip()
        if not target:
            continue
        found.append((text, href, target))
    return found


def test_documentation_files_are_found() -> None:
    """Covers: ENV-021 -- the check reads a corpus, not an empty glob.

    Without this, a glob that stopped matching would turn every assertion
    below into a vacuous pass, which is the failure mode a link check can
    least afford.
    """
    files = _documentation_files()
    assert len(files) > 100, (
        f"expected the documentation corpus, found {len(files)} files"
    )
    names = {path.relative_to(REPOSITORY_ROOT).as_posix() for path in files}
    assert "README.md" in names
    assert "docs/plans/README.md" in names
    assert any(name.startswith("docs/reference/") for name in names)


def test_every_relative_link_resolves() -> None:
    """Covers: ENV-021 -- no documentation link points at a missing file."""
    broken = []
    for path in _documentation_files():
        for _text, href, target in _relative_links(path):
            if not (path.parent / target).resolve().exists():
                broken.append(
                    f"{path.relative_to(REPOSITORY_ROOT).as_posix()} -> {href}"
                )

    assert not broken, "documentation links resolve to nothing:\n  " + "\n  ".join(
        broken
    )


def test_a_link_labelled_with_a_path_names_the_file_it_opens() -> None:
    """Covers: ENV-021 -- link text and href describe the same file.

    Comparing the label to the href as strings would fail on almost every
    link here, because the two are written in different spellings on purpose:
    the href is always relative to the containing file, while the label is
    written from wherever is clearest to a reader. Both spellings are honest.
    `docs/plans/README.md` is labelled from the repository root and linked as
    `../plans/README.md` from `docs/reference/`; `completed/GATE.md` is
    labelled relative to its own directory. So a label is accepted when it
    names the linked file under *either* reading.

    What that still catches is the case that matters: a label that resolves to
    no file at all under either reading, because it names something other than
    what the link opens. `README.md` advertised
    `docs/plans/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md` while opening the
    copy in `completed/` -- and in a repository where the folder *is* the
    plan's workflow state, that label is a claim about status, not a typo.
    """
    mislabelled = []
    for path in _documentation_files():
        for text, href, target in _relative_links(path):
            label = text.strip().strip("`")
            if not label.endswith(".md") or "/" not in label:
                continue
            resolved = (path.parent / target).resolve()
            if not resolved.exists():
                continue  # the previous test owns this failure
            readings = {
                (REPOSITORY_ROOT / label).resolve(),
                (path.parent / label).resolve(),
            }
            if resolved not in readings:
                actual = resolved.relative_to(REPOSITORY_ROOT).as_posix()
                mislabelled.append(
                    f"{path.relative_to(REPOSITORY_ROOT).as_posix()}: "
                    f"labelled {label!r} but opens {actual!r}"
                )

    assert not mislabelled, "documentation links are mislabelled:\n  " + "\n  ".join(
        mislabelled
    )
