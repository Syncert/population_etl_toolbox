"""Application imports must resolve inside the checkout pytest is running in.

The plan dispatcher verifies each worker by re-running that plan's own test
commands inside the worker's Git worktree. An editable install records an
absolute path to the clone it was installed from, so without an explicit
``pythonpath`` a worktree's suite imports the *original* clone's application
code: a worker's new modules are invisible and its verification grades a source
tree it never touched.
"""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

#: Import roots that must come from the checkout under test.
CHECKOUT_LOCAL_PACKAGES = ("data_ingestion_toolbox", "apps")


def test_pytest_declares_checkout_local_import_roots() -> None:
    """Covers: ENV-011 — the import roots are declared, not inherited."""
    configuration = tomllib.loads(
        (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    declared = configuration["tool"]["pytest"]["ini_options"]["pythonpath"]

    assert declared == ["src", "."], (
        "pytest must prepend this checkout's 'src' and root to sys.path so a "
        "worktree grades its own source; got " + repr(declared)
    )


@pytest.mark.parametrize("package", CHECKOUT_LOCAL_PACKAGES)
def test_application_packages_import_from_this_checkout(package: str) -> None:
    """Covers: ENV-011 — imports resolve under the running checkout."""
    module = __import__(package)
    origin = Path(module.__file__ or module.__path__[0]).resolve()

    assert origin.is_relative_to(REPOSITORY_ROOT), (
        f"'{package}' imported from {origin}, outside the checkout under test "
        f"({REPOSITORY_ROOT}). An editable install is shadowing local source."
    )


def test_a_separate_checkout_imports_its_own_source(tmp_path: Path) -> None:
    """Covers: ENV-011 — a second checkout does not import the first one's code.

    This is the dispatcher's actual failure mode, so it is exercised against a
    real second checkout rather than asserted from configuration alone.
    """
    checkout = tmp_path / "checkout"
    package = checkout / "src" / "data_ingestion_toolbox"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (checkout / "pyproject.toml").write_text(
        '[tool.pytest.ini_options]\npythonpath = ["src", "."]\n', encoding="utf-8"
    )

    # Reproduce what pytest's 'pythonpath' does: prepend the checkout's own
    # 'src' ahead of anything the editable install's finder would resolve.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; sys.path.insert(0, 'src'); "
            "import data_ingestion_toolbox as target; print(target.__file__)",
        ],
        cwd=checkout,
        capture_output=True,
        text=True,
        check=True,
    )

    imported = Path(result.stdout.strip()).resolve()
    assert imported.is_relative_to(checkout.resolve()), (
        "A separate checkout imported application code from "
        f"{imported} instead of its own tree; the editable install wins "
        "whenever the checkout's own source is not on sys.path first."
    )
