"""The deployment entrypoint's decisions, made once and graded here (DEPLOY-008).

`deploy_stack` refuses to start when Airflow's metadata database and the
warehouse turn out to be the same database, because `airflow-init` runs
`airflow db migrate` and would write Airflow's schema into production data.
That refusal lived only in 355 lines of PowerShell, which meant it could only
run on an operator's Windows workstation and nothing graded it anywhere.

These tests grade the decisions themselves: what resolves to what, and when
the refusal fires. Nothing here runs Docker.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.compose_expressions import interpolations
from tools.deployment import (
    EXTERNAL_SERVICE_SET,
    POWERSHELL_FLAGS,
    ComposeContext,
    DatabaseTarget,
    DeploymentError,
    FlagNames,
    airflow_metadata_isolation,
    compose_defaults,
    compose_file,
    compose_steps,
    default_env_file,
    example_env_file,
    guard_applies,
    read_env_file,
    resolve_compose_context,
    resolve_env_value,
    same_database,
)

pytestmark = [pytest.mark.unit, pytest.mark.deployment]

ROOT = Path(__file__).resolve().parents[3]
COMPOSE_DIRECTORY = ROOT / "infra/docker"

#: The keys the guard resolves, and the only ones its defaults map may carry.
GUARDED_KEYS = frozenset(
    {
        "ANALYTICS_DB_HOST",
        "ANALYTICS_DB_PORT",
        "ANALYTICS_DB_NAME",
        "AIRFLOW_METADATA_DB_HOST",
        "AIRFLOW_METADATA_DB_PORT",
        "AIRFLOW_METADATA_DB_NAME",
    }
)


def _context(
    env_file: str = "stack.env", *, use_host_env: bool = False
) -> ComposeContext:
    return ComposeContext(
        compose_file="infra/docker/docker-compose.yml",
        env_file=env_file,
        use_host_env=use_host_env,
    )


def _write_env(root: Path, name: str, body: str) -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


# --------------------------------------------------------------------------
# File and service selection
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "expected_compose", "expected_env"),
    (
        ("internal", "infra/docker/docker-compose.yml", "infra/docker/stack.env"),
        (
            "external",
            "infra/docker/docker-compose.external.yml",
            "infra/docker/stack.external.env",
        ),
    ),
)
def test_each_mode_selects_its_own_files(
    mode: str, expected_compose: str, expected_env: str
) -> None:
    """Covers: DEPLOY-008 -- a mode names its compose file and its env file."""
    assert compose_file(mode) == expected_compose
    assert default_env_file(mode) == expected_env
    assert example_env_file(mode) == f"{expected_env}.example"
    assert (ROOT / expected_compose).is_file()
    assert (ROOT / example_env_file(mode)).is_file()


def test_a_missing_env_file_is_refused_and_names_its_example(tmp_path: Path) -> None:
    """Covers: DEPLOY-008 -- the refusal tells the operator what to copy."""
    with pytest.raises(DeploymentError) as refusal:
        resolve_compose_context("internal", root=tmp_path)

    message = str(refusal.value)
    assert "infra/docker/stack.env" in message
    assert "infra/docker/stack.env.example" in message
    assert "--use-host-env" in message


def test_host_environment_needs_no_env_file(tmp_path: Path) -> None:
    """Covers: DEPLOY-008 -- --use-host-env does not require a file on disk."""
    context = resolve_compose_context("internal", use_host_env=True, root=tmp_path)
    assert context.use_host_env is True
    assert context.compose_arguments("up", "-d") == [
        "-f",
        "infra/docker/docker-compose.yml",
        "up",
        "-d",
    ]


def test_an_env_file_is_passed_before_the_compose_file(tmp_path: Path) -> None:
    """Covers: DEPLOY-008 -- `--env-file` precedes `-f`, as Compose requires."""
    _write_env(tmp_path, "infra/docker/stack.env", "ANALYTICS_DB_NAME=population_etl\n")
    context = resolve_compose_context("internal", root=tmp_path)
    assert context.compose_arguments("up") == [
        "--env-file",
        "infra/docker/stack.env",
        "-f",
        "infra/docker/docker-compose.yml",
        "up",
    ]


def test_the_base_env_file_is_passed_under_the_mode_file(tmp_path: Path) -> None:
    """Covers: DEPLOY-008 -- `.env` is a layer, not something `--env-file` erases.

    Passing `--env-file` stops Compose reading `.env` at all, and that cost
    this warehouse its tuning silently: `infra/docker/.env` asked for
    `ANALYTICS_PG_SHARED_BUFFERS=48GB`, `stack.env` carries credentials and no
    tuning, and the stack ran on the compose default of 4 GB on a 101 GB host
    until someone ran `SHOW shared_buffers`. Nothing failed, because nothing
    was wrong -- the file was simply never read.

    Compose merges repeated `--env-file` flags left to right, so the base is
    named first and the mode's file wins any key both set.
    """
    _write_env(tmp_path, "infra/docker/.env", "ANALYTICS_PG_SHARED_BUFFERS=48GB\n")
    _write_env(tmp_path, "infra/docker/stack.env", "ANALYTICS_DB_NAME=population_etl\n")

    context = resolve_compose_context("internal", root=tmp_path)

    assert context.compose_arguments("up") == [
        "--env-file",
        "infra/docker/.env",
        "--env-file",
        "infra/docker/stack.env",
        "-f",
        "infra/docker/docker-compose.yml",
        "up",
    ]


def test_a_deployment_without_a_base_env_file_passes_only_its_own(
    tmp_path: Path,
) -> None:
    """Covers: DEPLOY-008 -- a missing `.env` is normal, not a refusal.

    It is gitignored, and a deployment may set everything in its mode file.
    Naming a file that is not there would make `docker compose` refuse to run
    at all, which is a worse failure than the one this fixes.
    """
    _write_env(tmp_path, "infra/docker/stack.env", "ANALYTICS_DB_NAME=population_etl\n")
    context = resolve_compose_context("internal", root=tmp_path)
    assert context.base_env_file == ""
    assert context.compose_arguments("up").count("--env-file") == 1


def test_a_compose_file_override_replaces_only_the_file(tmp_path: Path) -> None:
    """Covers: DEPLOY-008 -- the override changes the file and nothing else.

    CI drives this entrypoint against `docker-compose.test.yml` so the
    execution loop is exercised for real. The override must not become a
    second mode: the env file, and the defaults the guard resolves against,
    stay the ones the mode declares.
    """
    context = resolve_compose_context(
        "internal",
        use_host_env=True,
        root=tmp_path,
        compose_file_override="infra/docker/docker-compose.test.yml",
    )

    assert context.compose_file == "infra/docker/docker-compose.test.yml"
    assert context.env_file == default_env_file("internal")
    assert (ROOT / context.compose_file).is_file()


def test_an_empty_override_keeps_the_modes_own_compose_file(tmp_path: Path) -> None:
    """Covers: DEPLOY-008 -- the default path is untouched by the affordance."""
    for mode in ("internal", "external"):
        context = resolve_compose_context(
            mode, use_host_env=True, root=tmp_path, compose_file_override="   "
        )
        assert context.compose_file == compose_file(mode)


def test_an_override_does_not_move_the_guard_onto_another_modes_defaults() -> None:
    """Covers: DEPLOY-008 -- defaults follow the mode, never the file.

    External declares no `${VAR:-default}` for the guarded keys, so an
    external run with nothing set has nothing to compare and defers to
    Compose. Pointing it at the internal compose file must not import that
    file's defaults and manufacture a verdict.
    """
    verdict = airflow_metadata_isolation(
        "external",
        resolve_compose_context(
            "external",
            use_host_env=True,
            compose_file_override="infra/docker/docker-compose.yml",
        ),
        environ={},
    )
    assert verdict.status == "skipped"


# --------------------------------------------------------------------------
# Reading an env file, and resolving a value the way Compose will
# --------------------------------------------------------------------------


def test_env_file_parsing_matches_compose(tmp_path: Path) -> None:
    """Covers: DEPLOY-008 -- comments, blanks, quotes, and malformed lines."""
    path = tmp_path / "stack.env"
    path.write_text(
        "\n".join(
            [
                "# a comment",
                "",
                "PLAIN=value",
                'DOUBLE="quoted"',
                "SINGLE='quoted'",
                "  SPACED  =  padded  ",
                "WITH_EQUALS=a=b",
                "=leading-equals-is-not-a-key",
                "NOEQUALS",
                "EMPTY=",
            ]
        ),
        encoding="utf-8",
    )

    values = read_env_file(path)
    assert values == {
        "PLAIN": "value",
        "DOUBLE": "quoted",
        "SINGLE": "quoted",
        "SPACED": "padded",
        "WITH_EQUALS": "a=b",
        "EMPTY": "",
    }


def test_a_missing_env_file_reads_as_empty(tmp_path: Path) -> None:
    """Covers: DEPLOY-008 -- absence is not an error at the parsing layer."""
    assert read_env_file(tmp_path / "absent.env") == {}


def test_the_host_environment_wins_then_the_file_then_the_default() -> None:
    """Covers: DEPLOY-008 -- resolution order matches Compose's own."""
    common = {
        "file_values": {"KEY": "from-file"},
        "defaults": {"KEY": "from-default"},
    }
    assert resolve_env_value(["KEY"], environ={"KEY": "from-host"}, **common) == (
        "from-host"
    )
    assert resolve_env_value(["KEY"], environ={}, **common) == "from-file"
    assert (
        resolve_env_value(["KEY"], environ={}, file_values={}, defaults={"KEY": "d"})
        == "d"
    )
    assert resolve_env_value(["KEY"], environ={}, file_values={}, defaults={}) == ""


def test_a_blank_value_falls_through_rather_than_winning() -> None:
    """Covers: DEPLOY-008 -- an empty override is not a value.

    Compose treats `KEY=` as unset for `${KEY:-default}`, so a guard that let a
    blank win would grade the empty string and compare two targets that the
    stack will never actually use.
    """
    assert (
        resolve_env_value(
            ["KEY"],
            environ={"KEY": "   "},
            file_values={"KEY": ""},
            defaults={"KEY": "default"},
        )
        == "default"
    )


def test_a_fallback_chain_mirrors_a_nested_compose_expression() -> None:
    """Covers: DEPLOY-008 -- `${PUBLIC_DATA_DB_NAME:-${ANALYTICS_DB_NAME}}`."""
    chain = ["PUBLIC_DATA_DB_NAME", "ANALYTICS_DB_NAME"]
    assert (
        resolve_env_value(
            chain,
            environ={},
            file_values={"ANALYTICS_DB_NAME": "warehouse"},
            defaults={},
        )
        == "warehouse"
    )
    assert (
        resolve_env_value(
            chain,
            environ={},
            file_values={
                "PUBLIC_DATA_DB_NAME": "own",
                "ANALYTICS_DB_NAME": "warehouse",
            },
            defaults={},
        )
        == "own"
    )


# --------------------------------------------------------------------------
# Comparing two targets
# --------------------------------------------------------------------------


def test_a_blank_port_compares_as_the_postgres_default() -> None:
    """Covers: DEPLOY-008 -- `host/db` and `host:5432/db` are one database."""
    blank = DatabaseTarget("left", "db", "", "warehouse")
    explicit = DatabaseTarget("right", "db", "5432", "warehouse")
    assert same_database(blank, explicit)
    assert str(blank) == "db:5432/warehouse"


def test_host_and_name_compare_case_insensitively() -> None:
    """Covers: DEPLOY-008 -- a capitalised host is the same host."""
    assert same_database(
        DatabaseTarget("left", "Analytics_Postgres", "5432", "Population_ETL"),
        DatabaseTarget("right", "analytics_postgres", "5432", "population_etl"),
    )


def test_a_different_port_is_a_different_database() -> None:
    """Covers: DEPLOY-008 -- the guard does not over-refuse."""
    assert not same_database(
        DatabaseTarget("left", "db", "5432", "warehouse"),
        DatabaseTarget("right", "db", "5433", "warehouse"),
    )


# --------------------------------------------------------------------------
# The isolation guard
# --------------------------------------------------------------------------


def test_the_guard_refuses_when_metadata_is_the_analytics_warehouse() -> None:
    """Covers: DEPLOY-008 -- the documented collision is refused."""
    verdict = airflow_metadata_isolation(
        "external",
        _context(use_host_env=True),
        environ={
            "AIRFLOW_METADATA_DB_HOST": "warehouse.example",
            "AIRFLOW_METADATA_DB_NAME": "public_data",
            "ANALYTICS_DB_HOST": "warehouse.example",
            "ANALYTICS_DB_NAME": "public_data",
        },
    )

    assert verdict.refuses
    assert "Refusing to run airflow-init" in verdict.message
    assert "warehouse.example:5432/public_data" in verdict.message
    assert "ANALYTICS_DB_* (API and Martin warehouse)" in verdict.message


def test_the_guard_refuses_through_the_public_data_fallback_chain() -> None:
    """Covers: DEPLOY-008 -- the collision via `${PUBLIC_DATA_DB_*}` is caught.

    `PUBLIC_DATA_DB_NAME` is what the `public_data` Airflow connection is
    created with, and it falls back to `ANALYTICS_DB_NAME`. A deployment that
    sets it explicitly at the metadata database collides just as surely.
    """
    verdict = airflow_metadata_isolation(
        "external",
        _context(use_host_env=True),
        environ={
            "AIRFLOW_METADATA_DB_HOST": "db.example",
            "AIRFLOW_METADATA_DB_NAME": "airflow",
            "ANALYTICS_DB_HOST": "db.example",
            "ANALYTICS_DB_NAME": "population_etl",
            "PUBLIC_DATA_DB_NAME": "airflow",
        },
    )

    assert verdict.refuses
    assert "PUBLIC_DATA_DB_* (the public_data Airflow connection)" in verdict.message


def test_the_guard_passes_when_the_two_are_separate() -> None:
    """Covers: DEPLOY-008 -- an isolated deployment is not refused."""
    verdict = airflow_metadata_isolation(
        "external",
        _context(use_host_env=True),
        environ={
            "AIRFLOW_METADATA_DB_HOST": "service.example",
            "AIRFLOW_METADATA_DB_NAME": "airflow",
            "ANALYTICS_DB_HOST": "warehouse.example",
            "ANALYTICS_DB_NAME": "population_etl",
        },
    )
    assert verdict.status == "ok"
    assert not verdict.refuses


def test_the_escape_hatch_bypasses_the_guard_and_says_so() -> None:
    """Covers: DEPLOY-008 -- the bypass is announced, never silent."""
    verdict = airflow_metadata_isolation(
        "external",
        _context(use_host_env=True),
        allow_metadata_in_warehouse=True,
        environ={
            "AIRFLOW_METADATA_DB_HOST": "same.example",
            "AIRFLOW_METADATA_DB_NAME": "public_data",
            "ANALYTICS_DB_HOST": "same.example",
            "ANALYTICS_DB_NAME": "public_data",
        },
    )
    assert verdict.status == "bypassed"
    assert "--allow-airflow-metadata-in-warehouse" in verdict.message


def test_an_unresolved_metadata_target_defers_to_compose() -> None:
    """Covers: DEPLOY-008 -- a half-informed guard does not guess.

    Compose reports an unresolved required variable better than this can, and
    external mode declares no defaults, so there is nothing to compare.
    """
    verdict = airflow_metadata_isolation(
        "external", _context(use_host_env=True), environ={}
    )
    assert verdict.status == "skipped"
    assert "not fully resolved" in verdict.message


def test_the_refusal_names_the_env_file_it_read(tmp_path: Path) -> None:
    """Covers: DEPLOY-008 -- the remedy points at the file the operator edits."""
    _write_env(
        tmp_path,
        "infra/docker/stack.external.env",
        "\n".join(
            [
                "AIRFLOW_METADATA_DB_HOST=db.example",
                "AIRFLOW_METADATA_DB_NAME=public_data",
                "ANALYTICS_DB_HOST=db.example",
                "ANALYTICS_DB_NAME=public_data",
            ]
        ),
    )
    context = resolve_compose_context("external", root=tmp_path)
    verdict = airflow_metadata_isolation("external", context, environ={}, root=tmp_path)

    assert verdict.refuses
    assert "infra/docker/stack.external.env" in verdict.message


def test_the_refusal_suggests_flags_the_caller_can_actually_pass() -> None:
    """Covers: DEPLOY-008 -- each entrypoint's own flag spelling.

    Telling a PowerShell operator to pass `--allow-...`, or a POSIX one to pass
    `-AllowAirflowMetadataInWarehouse`, is advice they cannot follow.
    """
    environ = {
        "AIRFLOW_METADATA_DB_HOST": "db.example",
        "AIRFLOW_METADATA_DB_NAME": "public_data",
        "ANALYTICS_DB_HOST": "db.example",
        "ANALYTICS_DB_NAME": "public_data",
    }
    posix = airflow_metadata_isolation(
        "external", _context(use_host_env=True), environ=environ, flags=FlagNames()
    )
    powershell = airflow_metadata_isolation(
        "external",
        _context(use_host_env=True),
        environ=environ,
        flags=POWERSHELL_FLAGS,
    )

    assert "--with-local-airflow" in posix.message
    assert "--allow-airflow-metadata-in-warehouse" in posix.message
    assert "-WithLocalAirflow" in powershell.message
    assert "-AllowAirflowMetadataInWarehouse" in powershell.message
    # Everything except the two suggested flags is the same refusal.
    assert powershell.message.splitlines()[:8] == posix.message.splitlines()[:8]


# --------------------------------------------------------------------------
# Which Compose commands an action runs, and when the guard applies
# --------------------------------------------------------------------------


def test_internal_init_and_up_run_airflow_init_and_the_whole_stack() -> None:
    """Covers: DEPLOY-008 -- internal mode starts everything."""
    context = _context(use_host_env=True)
    init = compose_steps("init", context, mode="internal")
    up = compose_steps("up", context, mode="internal")

    assert init[0].arguments[-2:] == ["up", "airflow-init"]
    assert up[0].arguments[-2:] == ["up", "-d"]
    assert [
        step.arguments for step in compose_steps("all", context, mode="internal")
    ] == [
        init[0].arguments,
        up[0].arguments,
    ]


def test_external_without_local_airflow_starts_only_this_repositorys_services() -> None:
    """Covers: DEPLOY-008 -- external mode does not start someone else's Airflow."""
    context = _context(use_host_env=True)
    steps = compose_steps("up", context, mode="external", with_local_airflow=False)

    assert len(steps) == 1
    assert steps[0].arguments[-len(EXTERNAL_SERVICE_SET) :] == list(
        EXTERNAL_SERVICE_SET
    )
    assert "airflow-init" not in steps[0].arguments


def test_external_with_local_airflow_runs_airflow_init() -> None:
    """Covers: DEPLOY-008 -- opting in to local Airflow opts in to its init."""
    steps = compose_steps(
        "init", _context(use_host_env=True), mode="external", with_local_airflow=True
    )
    assert steps[0].arguments[-2:] == ["up", "airflow-init"]


def test_down_stops_the_stack_in_either_mode() -> None:
    """Covers: DEPLOY-008 -- `down` is one command and starts nothing."""
    for mode in ("internal", "external"):
        steps = compose_steps("down", _context(use_host_env=True), mode=mode)
        assert len(steps) == 1
        assert steps[0].arguments[-1] == "down"


@pytest.mark.parametrize(
    ("action", "mode", "with_local_airflow", "expected"),
    (
        ("init", "internal", False, True),
        ("up", "internal", False, True),
        ("all", "internal", False, True),
        ("down", "internal", False, False),
        ("up", "external", False, False),
        ("up", "external", True, True),
        ("down", "external", True, False),
    ),
)
def test_the_guard_runs_exactly_where_airflow_init_can_run(
    action: str, mode: str, with_local_airflow: bool, expected: bool
) -> None:
    """Covers: DEPLOY-008 -- the guard grades the dangerous commands only.

    `down` stops containers and external-without-local-Airflow never starts
    `airflow-init`, so neither can migrate anything. Refusing them would refuse
    a command that is not the one the guard exists for.
    """
    assert (
        guard_applies(action, mode, with_local_airflow=with_local_airflow) is expected
    )


# --------------------------------------------------------------------------
# The defaults map against the compose files it mirrors
# --------------------------------------------------------------------------


def _declared_defaults(path: Path) -> dict[str, set[str]]:
    """Every `${KEY:-default}` a compose file declares for the guarded keys."""
    declared: dict[str, set[str]] = {}
    for interpolation in interpolations(path.read_text(encoding="utf-8")):
        if interpolation.name not in GUARDED_KEYS or interpolation.separator != ":-":
            continue
        declared.setdefault(interpolation.name, set()).add(interpolation.default)
    return declared


def test_internal_defaults_are_the_ones_the_compose_file_declares() -> None:
    """Covers: DEPLOY-008 -- the guard grades the value the stack will use.

    The defaults map is a second spelling of what `docker-compose.yml` writes
    as `${VAR:-default}`. If the compose file's default moves and this map does
    not, the guard compares a value no service will ever see -- and passes a
    run that then migrates production.
    """
    declared = _declared_defaults(COMPOSE_DIRECTORY / "docker-compose.yml")
    defaults = compose_defaults("internal")

    assert set(defaults) == set(declared), (
        "the guard's defaults and the compose file's name different keys"
    )
    for key, values in declared.items():
        assert values == {defaults[key]}, (
            f"{key}: compose declares {sorted(values)}, the guard assumes "
            f"{defaults[key]!r}"
        )


def test_external_declares_no_defaults_and_the_guard_assumes_none() -> None:
    """Covers: DEPLOY-008 -- no invented default for someone else's deployment.

    External mode targets infrastructure this repository does not own. Its
    compose file states the guarded keys as bare `${VAR}` or `${VAR:?...}`,
    both of which demand a value rather than supplying one.
    """
    declared = _declared_defaults(COMPOSE_DIRECTORY / "docker-compose.external.yml")
    assert declared == {}
    assert compose_defaults("external") == {}
