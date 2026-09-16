"""Which execution environment each plan's verification needs.

A plan is only finishable where its own ``verify`` commands can run. Two of
this repository's tiers need services an agent container does not have:

- the **database** tiers (``tests/integration/database`` and the API
  integration tier that shares its fixtures) need the pinned disposable
  PostGIS 16 container, and
- the **compose** tiers (the live-stack frontend smoke and the Compose smoke)
  need a Docker daemon to bring the stack up.

Everything else -- the Python and web unit tiers, Ruff, the Next build and its
bundle and CSP checks, the Playwright browser tier, and the Airflow DAG tier --
runs in a container with no Docker and no PostgreSQL. So the queue splits, and
the split is derived from each plan's declared ``verify`` block rather than
remembered: a plan that adds a database command to its own verification moves
itself into the machine column without anyone editing a list.

What cannot be derived is a criterion whose *text* names an environment the
verify block does not. Those are declared once in ``CRITERION_BLOCKERS``
below, each with the criterion that needs it, because a sentence in prose is
not something a parser should be guessing at.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from tools.plan_dispatcher.metadata import parse_plan  # noqa: E402

PLANS_ROOT = REPOSITORY_ROOT / "docs/plans"

#: Environments a plan's verification can need beyond the base toolchain.
POSTGRES = "postgres"
COMPOSE = "compose"
AIRFLOW = "airflow"
BROWSER = "browser"

#: Plans whose *acceptance criteria* name an environment their ``verify``
#: block does not. Each entry is the criterion, quoted closely enough that a
#: reader can find it, and the environment it needs. These are declared rather
#: than derived because they are prose.
CRITERION_BLOCKERS: dict[str, tuple[str, str]] = {
    "deployment-smoke-target": (
        COMPOSE,
        "`DEPLOYMENT_SMOKE_BASE_URL` is set to a reachable deployment origin -- "
        "which is a running deployment, not merely a Docker daemon.",
    ),
    "api-dependency-lock": (
        COMPOSE,
        "The API image builds from the lock in `deployment-smoke`.",
    ),
    "client-error-and-vitals-reporting": (
        COMPOSE,
        "The Compose smoke job sees the vitals line in the web container log.",
    ),
    "api-request-log-is-emitted": (
        COMPOSE,
        "The deployment smoke job fails if the completion line is absent from "
        "the container log, proved failing-first.",
    ),
}


@dataclass(frozen=True)
class PlanEnvironment:
    """One plan, and what finishing it needs."""

    plan_id: str
    filename: str
    workflow_state: str
    complexity: str
    depends_on: tuple[str, ...]
    #: Environments named by the plan's own ``verify`` commands.
    verify_needs: tuple[str, ...]
    #: Environment named only by an acceptance criterion, with its reason.
    criterion_blocker: tuple[str, str] | None

    @property
    def needs_machine(self) -> bool:
        """Whether this plan can be *finished* only on a developer machine."""
        return bool({POSTGRES, COMPOSE} & set(self.verify_needs)) or (
            self.criterion_blocker is not None
        )

    @property
    def buildable_in_cloud(self) -> bool:
        """Whether the implementation can be written and largely verified here.

        True for a plan whose only machine-bound requirement is a criterion,
        not a verify command: the code and every other tier can be done in a
        cloud session, and one check finishes on the machine.
        """
        return (
            not ({POSTGRES, COMPOSE} & set(self.verify_needs))
            and self.criterion_blocker is not None
        )


def _verify_needs(command: str) -> set[str]:
    needs: set[str] = set()
    database_paths = ("tests/integration/database", "tests/integration/api")
    if "integration and database" in command or any(
        path in command for path in database_paths
    ):
        needs.add(POSTGRES)
    if "RUN_COMPOSE_TESTS" in command or "test:smoke" in command:
        needs.add(COMPOSE)
    if "RUN_DAG_TESTS" in command:
        needs.add(AIRFLOW)
    if "test:browser" in command:
        needs.add(BROWSER)
    return needs


def classify_plans(
    states: tuple[str, ...] = ("to_do", "in_progress"),
) -> list[PlanEnvironment]:
    """Classify every dispatchable plan in the given workflow states."""
    classified: list[PlanEnvironment] = []
    for state in states:
        for path in sorted((PLANS_ROOT / state).glob("*.md")):
            meta = parse_plan(path, PLANS_ROOT)
            if meta is None or meta.is_gate:
                continue
            needs: set[str] = set()
            for command in meta.verify:
                needs |= _verify_needs(command)
            classified.append(
                PlanEnvironment(
                    plan_id=meta.plan_id,
                    filename=path.name,
                    workflow_state=state,
                    complexity=meta.complexity,
                    depends_on=meta.depends_on,
                    verify_needs=tuple(sorted(needs)),
                    criterion_blocker=CRITERION_BLOCKERS.get(meta.plan_id),
                )
            )
    return classified


#: The document this renders, and the test that holds it to the plans.
DOCUMENT_PATH = PLANS_ROOT / "EXECUTION_ENVIRONMENTS.md"

_HEADER = """<!-- Generated by `python -m tests.support.plan_environments`. Do not hand-edit
     the tables: they are derived from each plan's own `verify` block, and
     `tests/unit/tooling/test_plan_environments.py` fails when they drift. The
     prose around them is written by hand. -->

# Where each plan can be finished

This repository's queue does not run in one place. Two tiers need services a
cloud agent container does not have, and a plan is only finishable where its
own `verify` commands can run.

| Tier | Needs | Runs in a cloud session |
| --- | --- | --- |
| Python unit, Ruff | nothing beyond the venv | yes |
| Web unit, lint, typecheck, build, `check:bundle`, `check:csp` | Node | yes |
| Playwright browser (`test:browser`) | a Chromium binary | yes -- see the note below |
| Airflow DAG (`RUN_DAG_TESTS=1`) | the `airflow-dev` extra | yes, after installing it -- see the note below |
| Database integration (`integration and database`, `tests/integration/api`) | the pinned disposable PostGIS 16 container | **no** |
| Compose and live-stack smoke (`RUN_COMPOSE_TESTS=1`, `test:smoke`) | a Docker daemon | **no** |

So the backlog splits three ways. The tables below are **derived from each
plan's declared `verify` block**, not maintained by hand: a plan that adds a
database command to its own verification moves itself into the machine column
with no edit here.

## Regenerating

```bash
python -m tests.support.plan_environments --write
```

`tests/unit/tooling/test_plan_environments.py` fails if this file and the
plans disagree, so a stale table cannot merge.
"""

_CLOUD_PROSE = """
## 1. Finishable in a cloud session

Every acceptance criterion these declare can be met and verified in an agent
container. This is the "blast through it" column.
"""

_HYBRID_PROSE = """
## 2. Buildable in a cloud session, finished on a machine

The implementation and every tier in the `verify` block can be done in a cloud
session. One acceptance criterion names an environment the container does not
have, so the plan stays in `in_progress/` until that check runs -- per the
completion gate in [`README.md`](README.md), an unavailable environment is not
passing evidence.

A cloud session should still take these: it leaves the machine session with one
check to run rather than a plan to write.
"""

_MACHINE_PROSE = """
## 3. Needs a machine with the warehouse up

These declare a database or Compose command in `verify`. Bring the disposable
PostGIS container up first:

```bash
make test-integration      # database tiers
make test-compose-smoke    # the Compose stack
make test-web-smoke        # the live-stack frontend smoke
```
"""

_NOTES = """
## Two notes on the cloud container

**Playwright.** The browser tier runs, but the pinned `@playwright/test` looks
for a Chromium revision the image does not carry. Point it at the one that is
installed:

```bash
PLAYWRIGHT_CHROMIUM_EXECUTABLE=/opt/pw-browsers/chromium-1194/chrome-linux/chrome \\
  npm --prefix apps/web run test:browser
```

`playwright.config.mjs` documents that variable for this case. CI installs its
own browsers and needs nothing.

**Airflow.** `make bootstrap` does not install the DAG tier's dependency, so
`RUN_DAG_TESTS=1 pytest -m dag tests/dags` reports "Airflow is required for the
DAG tier". `pip install -e '.[airflow-dev]'` fixes it and the tier then runs.
Do not install Airflow with CI's constraints file into the project venv: it
downgrades `protobuf`, `PyJWT`, `greenlet` and `requests` under the other
tiers, and the venv has to be rebuilt.
"""


def _table(rows: list[PlanEnvironment]) -> str:
    lines = [
        "| Plan | Complexity | Verification beyond the base tiers | Depends on |",
        "| --- | --- | --- | --- |",
    ]
    for row in sorted(rows, key=lambda item: item.plan_id):
        extra = ", ".join(f"`{need}`" for need in row.verify_needs) or "none"
        depends = ", ".join(f"`{dep}`" for dep in row.depends_on) or "--"
        lines.append(
            f"| [`{row.plan_id}`]({row.workflow_state}/{row.filename}) "
            f"| {row.complexity} | {extra} | {depends} |"
        )
    return "\n".join(lines)


def _blocker_table(rows: list[PlanEnvironment]) -> str:
    lines = [
        "| Plan | Complexity | The criterion that needs a machine |",
        "| --- | --- | --- |",
    ]
    for row in sorted(rows, key=lambda item: item.plan_id):
        assert row.criterion_blocker is not None
        lines.append(
            f"| [`{row.plan_id}`]({row.workflow_state}/{row.filename}) "
            f"| {row.complexity} | {row.criterion_blocker[1]} |"
        )
    return "\n".join(lines)


def render_document() -> str:
    """Render the classification document from the plans themselves."""
    rows = classify_plans()
    cloud = [row for row in rows if not row.needs_machine]
    hybrid = [row for row in rows if row.buildable_in_cloud]
    machine = [row for row in rows if row.needs_machine and not row.buildable_in_cloud]

    parts = [
        _HEADER,
        _CLOUD_PROSE,
        f"**{len(cloud)} plans.**\n",
        _table(cloud),
        "\n",
        _HYBRID_PROSE,
        f"**{len(hybrid)} plans.**\n",
        _blocker_table(hybrid),
        "\n",
        _MACHINE_PROSE,
        f"**{len(machine)} plans.**\n",
        _table(machine),
        "\n",
        _NOTES,
    ]
    return "\n".join(parts).replace("\n\n\n", "\n\n").strip() + "\n"


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    arguments = parser.parse_args()
    document = render_document()
    if arguments.write:
        DOCUMENT_PATH.write_text(document, encoding="utf-8")
        print(f"wrote {DOCUMENT_PATH.relative_to(REPOSITORY_ROOT)}")
    else:
        print(document, end="")


if __name__ == "__main__":
    main()
