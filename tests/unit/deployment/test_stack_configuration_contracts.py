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

import re

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


def test_the_database_container_has_shared_memory_for_parallel_work() -> None:
    """Covers: DEPLOY-010 — the parallel workers have somewhere to exchange.

    `BETA_RESET_REINGESTION.md` §7 recorded that a parallel `VACUUM` "fails
    inside Compose". That is not a PostgreSQL limit: Docker gives a container
    64 MB of `/dev/shm`, and parallel workers pass their tuples through it.
    The stack asks for up to four parallel maintenance workers and eight
    parallel workers in the same service definition, so the two settings have
    to agree or the ones asking for parallelism are a request the container
    cannot honour (DB-048).
    """
    compose = yaml.safe_load(
        (COMPOSE_DIRECTORY / "docker-compose.yml").read_text(encoding="utf-8")
    )
    database = compose["services"]["analytics_postgres"]

    assert "shm_size" in database, (
        "the composed warehouse sets no shm_size, so it keeps Docker's 64 MB "
        "default and a parallel VACUUM fails the way §7 records"
    )
    # A documented override with a working default, like the tuning knobs
    # beside it: an operator raises it on a warehouse host without editing the
    # compose file.
    assert str(database["shm_size"]).startswith("${ANALYTICS_PG_SHM_SIZE:-")

    # And the setting is only meaningful beside the ones asking for parallel
    # work, so this fails if those are ever removed and this is left behind.
    command = " ".join(str(database["command"]).split())
    assert "max_parallel_maintenance_workers" in command
    assert "max_parallel_workers" in command


def test_the_external_stack_composes_no_database_to_size() -> None:
    """Covers: DEPLOY-010 — the external stack points at someone else's.

    The plan that added `shm_size` asked for it in both compose files "where
    the database is local". It is not local in the external stack: that stack
    has no Postgres service at all, and sizing a container it does not run
    would be a setting with nothing to apply to.
    """
    compose = yaml.safe_load(
        (COMPOSE_DIRECTORY / "docker-compose.external.yml").read_text(encoding="utf-8")
    )
    services = compose.get("services", {})
    assert "analytics_postgres" not in services
    for name, service in services.items():
        image = str(service.get("image", ""))
        assert "postgis" not in image and "postgres:" not in image, name


def test_the_smoke_stack_runs_the_web_container_and_reads_its_reports() -> None:
    """Covers: DEPLOY-011 — the reporting path is proved where it is deployed.

    The client-report sink is a Next route handler: a report is a POST to
    `/client-report` in the web process and a line on that container's stdout.
    Until this stack composed `web`, no CI job ran that container, so the only
    tier that drove a real browser drove it against `next dev` on the runner --
    which has no container and therefore no log to read.

    Three things have to hold together, and each is useless alone: the stack
    composes the container, the job starts it, and the job greps its log. The
    navigation between them is `report-a-vital.mjs`, which fails on its own if
    the browser sent nothing, so a green grep cannot come from a silent
    browser.
    """
    smoke = yaml.safe_load(
        (COMPOSE_DIRECTORY / "docker-compose.smoke.yml").read_text(encoding="utf-8")
    )
    web = smoke["services"].get("web")
    assert web is not None, (
        "the smoke stack composes no web service, so nothing in CI runs the "
        "container the client-report sink lives in"
    )
    # Built here rather than pulled: the sink is this repository's code.
    assert web["build"]["dockerfile"] == "infra/docker/Dockerfile.web"

    workflow = (ROOT / ".github/workflows/frontend-smoke.yml").read_text(
        encoding="utf-8"
    )
    assert "up --detach --wait postgres martin api proxy web" in workflow, (
        "the job does not start the web container, so its log is empty"
    )
    assert "npm run report:vital" in workflow, (
        "nothing drives a browser at the container, and curl produces no vital"
    )
    assert 'grep -F "client_report kind=vital"' in workflow, (
        "the job never reads the line, so the container could log nothing and "
        "the job would still be green"
    )
    assert (ROOT / "apps/web/scripts/report-a-vital.mjs").exists()


def test_the_external_stack_refuses_to_run_the_api_as_the_warehouse_owner() -> None:
    """Covers: DEPLOY-012 — the serving role is required, not defaulted.

    `docker-compose.external.yml` used to resolve the API's and Martin's
    credentials as `${ANALYTICS_API_DB_USER:-${ANALYTICS_DB_USER}}`. A
    deployment that set every other variable and forgot that one ran its
    public API and tile server as the ETL owner, with write access to every
    schema -- and came up cleanly, which is what makes it worth refusing
    rather than documenting.

    The `:?` form is what refuses it. Compose fails the render and names the
    variable, so the failure arrives before anything is listening.
    """
    source = _read("docker-compose.external.yml")

    assert "${ANALYTICS_API_DB_USER:-" not in source, (
        "the API credentials fall back to another variable, so an unset "
        "serving role silently becomes the warehouse owner"
    )
    assert "${ANALYTICS_API_DB_PASSWORD:-" not in source

    # Required for both services that read the warehouse, not just one: they
    # are configured in different places in this file and only one of them
    # used to carry any guard at all.
    required = source.count("${ANALYTICS_API_DB_USER:?")
    assert required >= 3, (
        "the serving role is not required everywhere the external stack "
        f"connects with it; found {required} guarded references"
    )
    assert "${ANALYTICS_API_DB_PASSWORD:?" in source

    compose = yaml.safe_load(source)
    api = compose["services"]["api"]["environment"]
    martin = compose["services"]["martin"]["environment"]
    assert "ANALYTICS_API_DB_USER:?" in str(api)
    assert "ANALYTICS_API_DB_USER:?" in str(martin), (
        "Martin reads the warehouse too, and an unguarded tile server is the "
        "same exposure as an unguarded API"
    )


#: Settings the application reads that no deployment passes, and why.
#:
#: The list is the point of the gate below, not an escape from it: each of
#: these is a decision that somebody made and can be argued with, rather than
#: an omission nobody noticed. A new setting is absent from here by default,
#: so it fails until it is either passed through or added with a reason.
NOT_DEPLOYMENT_CONFIGURED: dict[str, str] = {
    # Presentation only, and the registry's own description is the default
    # (API-144). A deployment that overrode these would be publishing a
    # different contract under the same name.
    "API_TITLE": "the served document's title; not a deployment concern",
    "API_VERSION": "the served contract version; decided by the code, not a host",
    "API_DESCRIPTION": (
        "an operator override for a description the registry already composes"
    ),
    # Bounds whose defaults have never needed tuning per deployment. They are
    # readable from the environment because the test tiers set them, not
    # because a host is expected to.
    "API_DB_POOL_RECYCLE_SECONDS": "pool hygiene; no deployment has needed a different value",
    "API_MAX_REQUEST_BODY_BYTES": (
        "the evidence-packet cap from ADR-0004, which is a contract rather than a knob"
    ),
}


def _api_service_environment(compose_name: str) -> set[str]:
    """The variable names the `api` service actually passes to its container."""
    document = _read(compose_name)
    service = document.split("\n  api:", 1)[1]
    service = service.split("\n    depends_on:", 1)[0]
    return set(re.findall(r"^\s{6}([A-Z0-9_]+):", service, re.MULTILINE))


def _settings_read_from_environment() -> set[str]:
    """Every variable `Settings` reads, from the module that reads them."""
    source = (ROOT / "src/data_ingestion_toolbox/config.py").read_text(encoding="utf-8")
    return set(re.findall(r'os\.environ\.get\(\s*"([A-Z0-9_]+)"', source))


@pytest.mark.parametrize("compose_name", sorted(STACK_PAIRS))
def test_a_stack_that_serves_the_api_passes_every_setting_the_api_reads(
    compose_name: str,
) -> None:
    """Covers: DEPLOY-013 — a setting the application reads reaches the container.

    The two gates above check a compose file against its own env example, in
    both directions. Neither checks either of them against the *application*,
    so a setting the code reads and no stack passes is invisible to both --
    and the symptom is the worst kind: an operator sets the value, the
    container never sees it, and the feature stays switched off with nothing
    anywhere saying why.

    ADR-0005 is what found it. Eleven identity settings were added to
    `Settings` and none of them was passed by either stack, including the
    client id and secret without which the sign-in routes cannot be switched
    on at all. Every existing test passed throughout.

    The `api` service enumerates its environment rather than inheriting it,
    which is the right way round -- a container should receive what it was
    given deliberately. This is the check that makes the enumeration
    maintainable.
    """
    passed = _api_service_environment(compose_name)
    read = _settings_read_from_environment()
    # Only the API's own namespace: `Settings` also reads warehouse and ETL
    # variables that no API container is given.
    owned = {
        name
        for name in read
        if name.startswith(("API_", "APP_API_", "OIDC_", "BACKUP_"))
    }
    missing = sorted(owned - passed - set(NOT_DEPLOYMENT_CONFIGURED))
    assert not missing, (
        f"{compose_name} never passes these to the api service, so setting "
        f"them changes nothing: {missing}. Pass them through, or record in "
        "NOT_DEPLOYMENT_CONFIGURED why a deployment is not expected to."
    )


def test_the_exemption_list_names_only_settings_that_exist() -> None:
    """Covers: DEPLOY-013 — a stale exemption hides the next real one.

    An entry for a setting the application no longer reads is a name the gate
    will never test again, and the next setting that needs the same reasoning
    gets waved through beside it.
    """
    read = _settings_read_from_environment()
    stale = sorted(name for name in NOT_DEPLOYMENT_CONFIGURED if name not in read)
    assert not stale, f"NOT_DEPLOYMENT_CONFIGURED names settings nothing reads: {stale}"
