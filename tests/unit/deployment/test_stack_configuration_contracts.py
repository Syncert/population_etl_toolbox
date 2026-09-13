"""A compose stack and the env example an operator copies say the same thing.

Two deployments ship in this repository: `docker-compose.yml` composes its own
Postgres, and `docker-compose.external.yml` points at a warehouse that already
exists. Each is paired with the `*.env.example` an operator copies to `.env`,
and nothing read either example until this module, so a compose file and its
example could drift in both directions -- a variable the stack needs and the
example never mentions, or a password the example asks for that no stack reads.

The rule distinguishes the three shapes an interpolation can take, because
they fail differently:

* `${VAR}` and `${VAR:?...}` have no default -- what follows `:?` is the
  message Compose prints when the value is missing, not a value. The operator
  must supply them or the stack does not come up, so the example must declare
  them.
* `${VAR:-}` defaults to the empty string. Compose resolves it silently and the
  container fails later, at request time, on a credential nobody was told to
  set -- so the example must declare these too.
* `${VAR:-something}` is a documented override with a working value behind it.
  The example may declare it, but need not: the Postgres tuning block alone
  holds fifteen knobs that would bury the five settings an operator needs.

The reverse direction has no exemption. A key the example declares that its
compose file never interpolates asks the operator for something that cannot
take effect.

Compose's own reading of `${...}` -- including a reference nested inside
another's default -- is in ``tests/support/compose_expressions``, because the
warehouse-target guard beside this one needs the same answers.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tests.support.compose_expressions import interpolations

pytestmark = [pytest.mark.unit, pytest.mark.deployment]

ROOT = Path(__file__).resolve().parents[3]
COMPOSE_DIRECTORY = ROOT / "infra/docker"

# Each deployment stack and the env example it tells the operator to copy.
STACK_PAIRS = {
    "docker-compose.yml": "stack.env.example",
    "docker-compose.external.yml": "stack.external.env.example",
}

APPLICATION_STORAGE_VARIABLE = "APP_API_DATABASE_URL"
API_COMMAND = "uvicorn apps.api.main:app"


def _declared_keys(example: str) -> list[str]:
    keys = []
    for line in example.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        keys.append(stripped.split("=", 1)[0].strip())
    return keys


def _read(name: str) -> str:
    return (COMPOSE_DIRECTORY / name).read_text(encoding="utf-8")


@pytest.mark.parametrize(("compose_name", "example_name"), sorted(STACK_PAIRS.items()))
def test_every_value_a_stack_needs_is_in_the_example_it_ships_with(
    compose_name: str, example_name: str
) -> None:
    """Covers: DEPLOY-006 — an operator's env example names what the stack needs."""
    references = interpolations(_read(compose_name))
    declared = set(_declared_keys(_read(example_name)))
    undeclared = sorted(
        {
            reference.name
            for reference in references
            if reference.must_be_supplied and reference.name not in declared
        }
    )
    assert not undeclared, (
        f"{compose_name} interpolates these with no usable default, so the "
        f"stack starts without them and fails later; {example_name} never "
        f"mentions them: {undeclared}"
    )


@pytest.mark.parametrize(("compose_name", "example_name"), sorted(STACK_PAIRS.items()))
def test_an_example_asks_only_for_values_its_stack_reads(
    compose_name: str, example_name: str
) -> None:
    """Covers: DEPLOY-006 — an env example asks for nothing the stack ignores."""
    referenced = {reference.name for reference in interpolations(_read(compose_name))}
    unread = [
        key for key in _declared_keys(_read(example_name)) if key not in referenced
    ]
    assert not unread, (
        f"{example_name} asks the operator to configure these, and "
        f"{compose_name} never interpolates them, so setting them changes "
        f"nothing: {unread}"
    )


def test_an_example_declares_each_value_once() -> None:
    """Covers: DEPLOY-006 — a repeated key silently overrides the earlier one."""
    for example_name in sorted(STACK_PAIRS.values()):
        keys = _declared_keys(_read(example_name))
        duplicated = sorted({key for key in keys if keys.count(key) > 1})
        assert not duplicated, f"{example_name} declares these twice: {duplicated}"


def test_every_stack_that_serves_the_api_configures_storage_or_says_it_does_not() -> (
    None
):
    """Covers: DEPLOY-006 — unconfigured application storage is stated, not implied.

    `apps/api/appdb.py` answers 503 from every account, saved-analysis, and
    evidence-packet route when `APP_API_DATABASE_URL` is unset. That is the
    honest answer for a deployment that chose not to configure storage, and
    `docker-compose.smoke.yml` chooses exactly that and says so in a comment.
    An omission stated is a choice; an omission unstated is a stack that ships
    with its account routes dead and nothing that tells the operator. So the
    convention this reads is the comment the smoke stack already carries --
    the variable named, followed by `is unset` -- and never a list of files
    exempted here.
    """
    serving_composes = sorted(
        path.name
        for path in COMPOSE_DIRECTORY.glob("docker-compose*.yml")
        if API_COMMAND in path.read_text(encoding="utf-8")
    )
    assert serving_composes, "no compose file runs the API; the rule read nothing"

    unaccounted = []
    for compose_name in serving_composes:
        source = _read(compose_name)
        configures = f"{APPLICATION_STORAGE_VARIABLE}:" in source
        declines = f"{APPLICATION_STORAGE_VARIABLE} is unset" in source
        if not configures and not declines:
            unaccounted.append(compose_name)
    assert not unaccounted, (
        f"these stacks run {API_COMMAND} without setting "
        f"{APPLICATION_STORAGE_VARIABLE} and without a comment saying it "
        f"'is unset', so their account and saved-analysis routes answer 503 "
        f"and nothing says that was the intent: {unaccounted}"
    )


def test_both_deployment_stacks_pass_the_same_environment_to_airflow() -> None:
    """Covers: DEPLOY-006 — one stack's DAGs are not better credentialled.

    Both stacks build the same Airflow image from the same `dags/` directory,
    so the environment each passes it is a statement about the same code. A
    variable one stack passes and the other does not is drift by definition,
    and the shape it took here was silent: `docker-compose.external.yml`
    never passed `CENSUS_API_KEY`, `BLS_API_KEY`, or `FRED_API_KEY`, and
    `infra/airflow/airflow.env.example` does not carry them either, so on
    that stack the Census, BLS, and FRED DAGs authenticated with nothing.
    """
    environments = {}
    files = {}
    for compose_name in sorted(STACK_PAIRS):
        service = yaml.safe_load(_read(compose_name))["services"]["airflow-common"]
        environments[compose_name] = set(service["environment"])
        files[compose_name] = list(service.get("env_file", []))

    internal, external = sorted(STACK_PAIRS)
    only_internal = sorted(environments[internal] - environments[external])
    only_external = sorted(environments[external] - environments[internal])
    assert not only_internal and not only_external, (
        f"the two stacks run the same DAGs with different environments: "
        f"{internal} alone passes {only_internal}, {external} alone passes "
        f"{only_external}"
    )
    assert files[internal] == files[external], (
        f"the two stacks read different Airflow env files, so a default in "
        f"one is absent from the other: {files}"
    )
