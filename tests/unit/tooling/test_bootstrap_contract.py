"""The one-command bootstrap contract (ENV-020).

A fresh clone's usefulness depends on `make bootstrap` staying the single
definition of "ready". These tests guard the three surfaces that definition
lives on -- the Makefile target, the SessionStart hook that calls it, and the
documentation a reader reaches first -- against the drift that would put an
install step back into rediscovery.
"""

from __future__ import annotations

import json
import os
import re
import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MAKEFILE = REPOSITORY_ROOT / "Makefile"
SETTINGS = REPOSITORY_ROOT / ".claude" / "settings.json"
README = REPOSITORY_ROOT / "README.md"
RUNNING_TESTS = REPOSITORY_ROOT / "docs" / "user-guides" / "RUNNING_TESTS.md"

# The dispatcher's own state path. `.claude/` is shared, and neither file
# belongs in the other's schema (docs/reference/PLAN_DISPATCHER.md).
DISPATCHER_STATE = REPOSITORY_ROOT / ".claude" / "plan-runner-state.json"


def _recipe(target: str) -> str:
    """Return the recipe body of a Makefile target, tabs and all."""
    text = MAKEFILE.read_text(encoding="utf-8")
    match = re.search(
        rf"^{re.escape(target)}:[^\n]*\n((?:\t[^\n]*\n|\n(?=\t))*)",
        text,
        re.MULTILINE,
    )
    assert match is not None, f"Makefile declares no {target} target"
    return match.group(1)


def _prerequisites(target: str) -> list[str]:
    text = MAKEFILE.read_text(encoding="utf-8")
    match = re.search(rf"^{re.escape(target)}:([^\n]*)$", text, re.MULTILINE)
    assert match is not None, f"Makefile declares no {target} target"
    return match.group(1).split()


def _phony_targets() -> set[str]:
    text = MAKEFILE.read_text(encoding="utf-8")
    match = re.search(r"^\.PHONY:([^\n]*)$", text, re.MULTILINE)
    assert match is not None, "Makefile declares no .PHONY line"
    return set(match.group(1).split())


def _hook_path() -> Path:
    """Resolve the SessionStart hook command registered in settings.json."""
    settings = json.loads(SETTINGS.read_text(encoding="utf-8"))
    matchers = settings["hooks"]["SessionStart"]
    commands = [
        hook["command"]
        for matcher in matchers
        for hook in matcher["hooks"]
        if hook.get("type") == "command"
    ]
    assert len(commands) == 1, (
        f"expected exactly one SessionStart command, got {commands}"
    )
    # `$CLAUDE_PROJECT_DIR` is the repository root at session start.
    relative = commands[0].replace("$CLAUDE_PROJECT_DIR", "").lstrip("/")
    return REPOSITORY_ROOT / relative


def test_bootstrap_is_a_phony_target_over_both_halves() -> None:
    """Covers: ENV-020 -- one command installs both environments."""
    phony = _phony_targets()
    prerequisites = _prerequisites("bootstrap")

    assert prerequisites == ["bootstrap-python", "bootstrap-web"], (
        "bootstrap must delegate to both halves so either can be run alone"
    )
    for target in ("bootstrap", *prerequisites):
        assert target in phony, (
            f"{target} produces no file of its own and must be .PHONY"
        )


def test_bootstrap_installs_the_declared_local_extra() -> None:
    """Covers: ENV-020 -- bootstrap installs pyproject's set, not its own."""
    extras = tomllib.loads(
        (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]["optional-dependencies"]
    assert "local" in extras, "the local extra is what bootstrap promises to install"

    recipe = _recipe("bootstrap-python")
    assert '-e ".[local]"' in recipe, (
        "bootstrap must install the local extra rather than restate a package set"
    )
    # A pin repeated here is a pin that drifts from pyproject.toml.
    assert "==" not in recipe, f"bootstrap-python must name no version pin:\n{recipe}"


def test_bootstrap_python_refuses_a_bare_system_interpreter() -> None:
    """Covers: ENV-020 -- the Python half installs into an isolated prefix."""
    recipe = _recipe("bootstrap-python")

    assert "sys.base_prefix" in recipe, (
        "bootstrap-python must detect whether a virtual environment is active"
    )
    assert "python -m venv" in recipe, (
        "with no virtual environment active bootstrap-python must create one: "
        "Ubuntu 24.04's /usr/lib/python3/dist-packages is on the 3.11 "
        "interpreter's sys.path with extensions built for 3.12"
    )


def test_bootstrap_web_installs_from_the_committed_lockfile() -> None:
    """Covers: ENV-020 -- the web half resolves nothing CI would not."""
    recipe = _recipe("bootstrap-web")

    assert "npm ci --prefix apps/web" in recipe, (
        "bootstrap-web must install from package-lock.json, as CI does"
    )
    assert not re.search(r"npm install\b", recipe), (
        "npm install resolves a tree CI never graded; a lockfile that no "
        "longer matches package.json is a defect to fix, not to work around"
    )


def test_session_start_hook_runs_the_same_one_command() -> None:
    """Covers: ENV-020 -- the hook holds no second definition of ready."""
    hook = _hook_path()
    assert hook.is_file(), f"settings.json registers {hook}, which does not exist"
    assert os.access(hook, os.X_OK), (
        f"{hook.name} is registered as a command but is not executable"
    )

    body = hook.read_text(encoding="utf-8")
    assert "make bootstrap" in body, "the hook must call the Makefile target"
    # Any other installer in the hook is a second answer to "what is ready".
    for installer in ("pip install", "npm ci", "npm install", "python -m venv"):
        assert installer not in body, (
            f"the hook runs {installer!r} itself; bootstrap is the only install definition"
        )


def test_session_start_hook_cannot_end_the_session() -> None:
    """Covers: ENV-020 -- a failed install is reported, not fatal."""
    body = _hook_path().read_text(encoding="utf-8")

    for line in body.splitlines():
        stripped = line.strip()
        if (
            stripped.startswith("set -")
            and "pipefail" not in stripped.split("pipefail")[0]
        ):
            flags = re.findall(r"-(\w+)", stripped)
            assert not any("e" in flag for flag in flags), (
                f"{stripped!r} enables errexit: a slow network would abort the hook "
                "before it can report why the install failed"
            )

    meaningful = [
        line.strip()
        for line in body.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    assert meaningful[-1] == "exit 0", (
        "the hook must exit zero: a session that cannot install is still a "
        "session that can read the repository"
    )


def test_claude_settings_do_not_claim_the_dispatcher_state_path() -> None:
    """Covers: ENV-020 -- two files share .claude/ and neither owns the other."""
    settings_text = SETTINGS.read_text(encoding="utf-8")
    assert DISPATCHER_STATE.name not in settings_text, (
        "the hook settings must not reference the dispatcher's run state"
    )
    assert "plan-runner-state" not in _hook_path().read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "document",
    (
        pytest.param(README, id="README.md"),
        pytest.param(RUNNING_TESTS, id="RUNNING_TESTS.md"),
    ),
)
def test_the_install_is_named_before_any_tier_command(document: Path) -> None:
    """Covers: ENV-020 -- a reader meets the one command first."""
    text = document.read_text(encoding="utf-8")

    bootstrap_at = text.find("make bootstrap")
    assert bootstrap_at != -1, f"{document.name} never names `make bootstrap`"

    first_tier = re.search(
        r"(?:make test-|\./tests/run\.ps1|\.\\tests\\run\.ps1)", text
    )
    if first_tier is not None:
        assert bootstrap_at < first_tier.start(), (
            f"{document.name} gives a tier command at offset {first_tier.start()} "
            f"before naming the install at {bootstrap_at}"
        )
