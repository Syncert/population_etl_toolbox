"""The API lock covers the dependencies the project declares (ENV-022).

Everything about the API image was pinned by digest except the Python it
runs: the base image, Redis and PostGIS carry sha256 digests in every Compose
file, while ``pip install -e .[api]`` re-resolved eight ranges at build time.
Two builds of one commit a week apart could differ in every one of them, so a
green CI run did not describe the image an operator built the next day.

``requirements/api.lock.txt`` is that resolution, written down. What this file
guards is the relationship between the lock and the declaration it must
satisfy -- a lock nobody checks is a lock that quietly stops covering
something.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit]

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
LOCK = REPOSITORY_ROOT / "requirements/api.lock.txt"
PYPROJECT = REPOSITORY_ROOT / "pyproject.toml"

#: `name==version \` at the start of a line: one locked distribution.
_PINNED = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s\\]+)", re.MULTILINE)
#: The distribution name at the head of a requirement, before any extras,
#: version specifier or environment marker.
_REQUIREMENT_NAME = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)")


def _normalise(name: str) -> str:
    """PEP 503 normalisation: `psycopg2_binary` and `psycopg2-binary` are one."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _locked() -> dict[str, str]:
    return {
        _normalise(name): version
        for name, version in _PINNED.findall(LOCK.read_text(encoding="utf-8"))
    }


def _declared() -> list[str]:
    """Every requirement the API image installs: the core group and `api`."""
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    project = data["project"]
    return [*project["dependencies"], *project["optional-dependencies"]["api"]]


def _satisfies(version: str, specifier: str) -> bool:
    """Whether ``version`` falls inside a comma-separated PEP 440 range.

    Deliberately small: the ranges here are `>=`/`<`/`==` over release
    segments, and a full PEP 440 implementation would be a dependency this
    test does not need. An operator that writes something cleverer will find
    this raising rather than quietly passing.
    """

    def parts(text: str) -> tuple[int, ...]:
        cleaned = re.match(r"\d+(?:\.\d+)*", text)
        if cleaned is None:
            raise AssertionError(f"cannot read a release number from {text!r}")
        return tuple(int(piece) for piece in cleaned.group(0).split("."))

    def padded(left: tuple[int, ...], right: tuple[int, ...]) -> tuple[tuple, tuple]:
        width = max(len(left), len(right))
        return left + (0,) * (width - len(left)), right + (0,) * (width - len(right))

    for clause in (piece.strip() for piece in specifier.split(",")):
        if not clause:
            continue
        operator = re.match(r"(>=|<=|==|!=|<|>)", clause)
        assert operator is not None, f"unreadable version clause {clause!r}"
        bound = clause[operator.end() :].strip()
        have, want = padded(parts(version), parts(bound))
        symbol = operator.group(1)
        if symbol == ">=" and not have >= want:
            return False
        if symbol == "<=" and not have <= want:
            return False
        if symbol == ">" and not have > want:
            return False
        if symbol == "<" and not have < want:
            return False
        if symbol == "==" and have != want:
            return False
        if symbol == "!=" and have == want:
            return False
    return True


def test_every_declared_api_dependency_is_locked() -> None:
    """Covers: ENV-022 — the lock covers what the image is told to install."""
    locked = _locked()
    assert locked, "the lock names no distributions; the read is broken"

    missing = []
    for requirement in _declared():
        # A requirement that only applies elsewhere is not the image's.
        if ";" in requirement and "platform_system == 'Windows'" in requirement:
            continue
        name = _REQUIREMENT_NAME.match(requirement.strip())
        assert name is not None, requirement
        if _normalise(name.group(1)) not in locked:
            missing.append(requirement)

    assert not missing, (
        "these are declared in pyproject.toml and absent from "
        f"requirements/api.lock.txt: {missing}. Regenerate the lock."
    )


def test_every_locked_version_satisfies_its_declared_range() -> None:
    """Covers: ENV-022 — the lock is a resolution of the contract, not a rival."""
    locked = _locked()
    outside = []
    for requirement in _declared():
        head, _, marker = requirement.partition(";")
        if marker and "Windows" in marker:
            continue
        name = _REQUIREMENT_NAME.match(head.strip())
        assert name is not None, requirement
        specifier = head.strip()[name.end() :].strip()
        # `uvicorn[standard]` and friends: the extras are not the range.
        specifier = re.sub(r"^\[[^\]]*\]", "", specifier).strip()
        version = locked.get(_normalise(name.group(1)))
        if version is None or specifier == "":
            continue
        if not _satisfies(version, specifier):
            outside.append(f"{name.group(1)}: locked {version}, declared {specifier}")

    assert not outside, (
        f"the lock pins versions outside the ranges pyproject.toml declares: {outside}"
    )


def test_the_lock_carries_a_hash_for_every_distribution() -> None:
    """Covers: ENV-022 — `--require-hashes` is only as good as the hashes."""
    text = LOCK.read_text(encoding="utf-8")
    unhashed = []
    for match in _PINNED.finditer(text):
        # The hashes for a pin are the indented lines that follow it, up to
        # the next pin or the end.
        following = text[match.end() :]
        block = following.split("\n==", 1)[0]
        next_pin = _PINNED.search(following)
        if next_pin is not None:
            block = following[: next_pin.start()]
        if "--hash=sha256:" not in block:
            unhashed.append(match.group(1))

    assert not unhashed, f"locked without a hash: {unhashed}"


def test_the_airflow_extra_is_not_in_this_lock() -> None:
    """Covers: ENV-022 — one resolution cannot hold both SQLAlchemy majors."""
    locked = _locked()
    assert "apache-airflow" not in locked
    # The API's own major, not Airflow's 1.4. If this ever reads 1.x, the two
    # environments have been merged and the API is running on the ORM the
    # contract says it is not.
    assert locked["sqlalchemy"].startswith("2."), locked["sqlalchemy"]


def test_the_resolution_is_pinned_to_the_image_platform() -> None:
    """Covers: ENV-022 — the lock is a property of this file, not of a machine.

    `--python-version` was pinned and the platform was not, so the resolution
    followed whoever ran `make lock-api`. That is not a style question:
    `uvicorn[standard]` brings `uvloop` only where `sys_platform != "win32"`,
    and this project declares `tzdata` only on Windows, so a refresh from a
    Windows checkout produced a lock with `tzdata` added and `uvloop` silently
    gone.

    The lock carries no environment markers, and `Dockerfile.api` installs it
    with `--require-hashes` on `python:3.11-slim`. So that image would have
    dropped to uvicorn's asyncio loop, with a green CI run and a diff nobody
    would read as a behaviour change. It happened while ADR-0005 was adding
    `PyJWT[crypto]`, which is the only reason anybody looked.
    """
    from tools.lock.refresh_api_lock import COMPILE

    assert "--python-platform" in COMPILE, (
        "the resolution follows the machine that ran it; pin the image's platform"
    )
    platform = COMPILE[COMPILE.index("--python-platform") + 1]
    assert platform == "linux", f"the API image is Linux, not {platform}"


def test_the_lock_carries_the_platform_dependent_packages_the_image_needs() -> None:
    """Covers: ENV-022 — and the pin above is checked by its consequence.

    `uvloop` is the package that proves the resolution was Linux's: it is what
    `uvicorn[standard]` adds everywhere except Windows, and its absence is the
    visible half of a lock resolved on the wrong platform. `tzdata` is the
    other half -- Windows-only here, so its presence would mean the same
    mistake in the other direction.
    """
    locked = _locked()

    assert "uvloop" in locked, (
        "uvloop is absent; the lock was resolved on Windows, and the Linux "
        "image will fall back to uvicorn's asyncio loop"
    )
    assert "tzdata" not in locked, (
        "tzdata is Windows-only in pyproject.toml; its presence means this "
        "lock was resolved on Windows"
    )
