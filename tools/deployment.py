"""The decisions `deploy_stack` makes before it invokes Compose (DEPLOY-008).

`scripts/deploy_stack.ps1` is not `docker compose up`. It resolves the mode's
env file, selects the compose file and service set, derives which database each
half of the stack will actually talk to, and refuses to start when Airflow's
metadata database and the warehouse turn out to be the same database. That
refusal is the reason this module exists rather than a second shell script: a
second entrypoint that re-implements the guard is a second place for it to
drift, and a second entrypoint without it is a way to point `airflow db
migrate` at production.

So the decisions live here once, as pure functions over an environment
mapping, and both entrypoints -- `deploy_stack.py` on POSIX and
`deploy_stack.ps1` on Windows -- ask this module what to do. Nothing in this
module runs Docker or touches a network; it reads one env file and returns
what an entrypoint should then execute.

It deliberately does not live under `src/`. The wheel `package-api` builds and
installs carries the API and the toolbox; deployment orchestration has no
business being importable by anyone who installs that wheel.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

MODES = ("internal", "external")
ACTIONS = ("init", "up", "down", "all")

#: Started by name in external mode: this repository owns these four services
#: and not the Airflow that drives them.
EXTERNAL_SERVICE_SET = ("redis", "api", "martin", "web")

#: The port Compose and PostgreSQL both fall back to, used on either side of a
#: comparison so a blank port and an explicit 5432 are recognised as the same.
DEFAULT_POSTGRES_PORT = "5432"

#: Only the `${VAR:-default}` fallbacks the compose file for each mode actually
#: declares. External mode declares none for these keys on purpose -- it
#: targets infrastructure this repository does not own, so an invented default
#: would be a guess about someone else's deployment. `tests/unit/deployment`
#: asserts this map against the compose files, so a default that moves there
#: cannot silently leave the guard grading a value the stack will not use.
_INTERNAL_COMPOSE_DEFAULTS = {
    "ANALYTICS_DB_HOST": "analytics_postgres",
    "ANALYTICS_DB_PORT": "5432",
    "ANALYTICS_DB_NAME": "population_etl",
    "AIRFLOW_METADATA_DB_HOST": "service_postgres",
    "AIRFLOW_METADATA_DB_PORT": "5432",
    "AIRFLOW_METADATA_DB_NAME": "airflow",
}


class DeploymentError(Exception):
    """A refusal an entrypoint should report and exit non-zero on."""


@dataclass(frozen=True)
class DatabaseTarget:
    """Where one half of the stack will actually connect."""

    label: str
    host: str
    port: str
    name: str

    @property
    def effective_port(self) -> str:
        return self.port.strip() or DEFAULT_POSTGRES_PORT

    @property
    def resolved(self) -> bool:
        """Whether enough is known to compare this target with another."""
        return bool(self.host.strip()) and bool(self.name.strip())

    def __str__(self) -> str:
        return f"{self.host}:{self.effective_port}/{self.name}"


@dataclass(frozen=True)
class ComposeContext:
    """Which compose file to use, and whether to pass an --env-file."""

    compose_file: str
    env_file: str
    use_host_env: bool
    #: `infra/docker/.env` when the deployment has one, empty when it does not.
    #: Resolved against the caller's root by `resolve_compose_context`, so a
    #: test driving a temporary tree gets that tree's answer rather than this
    #: checkout's.
    base_env_file: str = ""

    def compose_arguments(self, *arguments: str) -> list[str]:
        """The `docker compose` argument vector, env-file first as it must be.

        `--env-file` *replaces* the automatic `.env` rather than adding to it,
        and that cost a warehouse its tuning without anything reporting it:
        `infra/docker/.env` asked for `ANALYTICS_PG_SHARED_BUFFERS=48GB`,
        `stack.env` carries credentials and no tuning, and the stack ran on
        the compose defaults -- 4 GB of shared buffers on a 101 GB host -- for
        as long as nobody ran `SHOW shared_buffers`. Section 7 of
        `BETA_RESET_REINGESTION.md` measures that difference at six to eight
        times the re-serve throughput.

        So both are passed, base first. A missing `.env` is normal -- it is
        gitignored and a deployment may set everything in its mode file -- so
        it is included only when it exists rather than turned into a refusal.
        """
        prefix: list[str] = []
        if not self.use_host_env:
            if self.base_env_file:
                prefix = ["--env-file", self.base_env_file]
            prefix = [*prefix, "--env-file", self.env_file]
        return [*prefix, "-f", self.compose_file, *arguments]


@dataclass(frozen=True)
class GuardVerdict:
    """What the isolation guard decided, and why."""

    #: "ok", "bypassed", "skipped", or "refused".
    status: str
    message: str = ""

    @property
    def refuses(self) -> bool:
        return self.status == "refused"


@dataclass(frozen=True)
class FlagNames:
    """How the invoking entrypoint spells the two flags the refusal suggests.

    The refusal text is identical between entrypoints except here: telling a
    PowerShell operator to pass `--allow-airflow-metadata-in-warehouse`, or a
    POSIX one to pass `-WithLocalAirflow`, is advice they cannot follow.
    """

    with_local_airflow: str = "--with-local-airflow"
    allow_metadata_in_warehouse: str = "--allow-airflow-metadata-in-warehouse"


POWERSHELL_FLAGS = FlagNames(
    with_local_airflow="-WithLocalAirflow",
    allow_metadata_in_warehouse="-AllowAirflowMetadataInWarehouse",
)


#: The file `docker compose` reads on its own, and stops reading the moment
#: `--env-file` is passed.
BASE_ENV_FILE = "infra/docker/.env"


def default_env_file(mode: str) -> str:
    return (
        "infra/docker/stack.external.env"
        if mode == "external"
        else "infra/docker/stack.env"
    )


def example_env_file(mode: str) -> str:
    return f"{default_env_file(mode)}.example"


def compose_file(mode: str) -> str:
    return (
        "infra/docker/docker-compose.external.yml"
        if mode == "external"
        else "infra/docker/docker-compose.yml"
    )


def compose_defaults(mode: str) -> dict[str, str]:
    if mode == "external":
        return {}
    return dict(_INTERNAL_COMPOSE_DEFAULTS)


def resolve_compose_context(
    mode: str,
    env_file: str | None = None,
    *,
    use_host_env: bool = False,
    root: Path | None = None,
    compose_file_override: str = "",
) -> ComposeContext:
    """Pick the compose and env files, refusing an env file that is not there.

    ``compose_file_override`` points the entrypoint at a compose file other
    than the mode's own. It exists so CI can drive the real entrypoint against
    the disposable stack in `docker-compose.test.yml` -- the execution loop is
    otherwise the one part of this path no test reaches, because it is the
    only part that needs a Docker daemon.

    It deliberately does **not** change which defaults the guard resolves
    against: those are the mode's, because the mode is what says whether a
    `${VAR:-default}` exists to fall back to. An override that quietly
    switched defaults would let a test pass under rules the deployment does
    not use.
    """
    root = root or REPOSITORY_ROOT
    effective = (env_file or "").strip() or default_env_file(mode)

    if not use_host_env and not (root / effective).exists():
        raise DeploymentError(
            f"Missing env file '{effective}'. Copy '{example_env_file(mode)}' to "
            f"'{effective}' and fill required secrets, or rerun with --use-host-env."
        )

    return ComposeContext(
        compose_file=(compose_file_override or "").strip() or compose_file(mode),
        env_file=effective,
        base_env_file=(BASE_ENV_FILE if (root / BASE_ENV_FILE).is_file() else ""),
        use_host_env=use_host_env,
    )


def read_env_file(path: Path) -> dict[str, str]:
    """The `KEY=value` pairs in an env file, read the way Compose reads them."""
    values: dict[str, str] = {}
    if not path or not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        separator = line.find("=")
        if separator < 1:
            continue
        key = line[:separator].strip()
        value = line[separator + 1 :].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key] = value
    return values


def resolve_env_value(
    names: Sequence[str],
    *,
    file_values: Mapping[str, str],
    defaults: Mapping[str, str],
    environ: Mapping[str, str],
) -> str:
    """Resolve a value the way Compose will, following a fallback chain.

    The host environment wins over `--env-file`, and an unset key falls back to
    the `${VAR:-default}` written into the compose file for this mode. Grading
    a value the stack will not actually use is worse than not grading at all --
    it would pass a run that then migrates production.

    ``names`` is the fallback chain, mirroring a nested compose expression such
    as `${PUBLIC_DATA_DB_HOST:-${ANALYTICS_DB_HOST:-analytics_postgres}}`.
    """
    for name in names:
        for source in (environ, file_values, defaults):
            value = (source.get(name) or "").strip()
            if value:
                return value
    return ""


def database_target(
    label: str,
    *,
    host_names: Sequence[str],
    port_names: Sequence[str],
    name_names: Sequence[str],
    file_values: Mapping[str, str],
    defaults: Mapping[str, str],
    environ: Mapping[str, str],
) -> DatabaseTarget:
    def resolve(names: Sequence[str]) -> str:
        return resolve_env_value(
            names, file_values=file_values, defaults=defaults, environ=environ
        )

    return DatabaseTarget(
        label=label,
        host=resolve(host_names),
        port=resolve(port_names),
        name=resolve(name_names),
    )


def same_database(left: DatabaseTarget, right: DatabaseTarget) -> bool:
    """Whether two targets name one database.

    Host is compared as written: this cannot resolve DNS, so it will not catch
    localhost spelled two ways. It is a guard against the documented collision,
    not proof of isolation.
    """
    return (
        left.host.casefold() == right.host.casefold()
        and left.effective_port == right.effective_port
        and left.name.casefold() == right.name.casefold()
    )


def _warehouse_targets(
    *,
    file_values: Mapping[str, str],
    defaults: Mapping[str, str],
    environ: Mapping[str, str],
) -> list[DatabaseTarget]:
    common = {"file_values": file_values, "defaults": defaults, "environ": environ}
    return [
        database_target(
            "ANALYTICS_DB_* (API and Martin warehouse)",
            host_names=["ANALYTICS_DB_HOST"],
            port_names=["ANALYTICS_DB_PORT"],
            name_names=["ANALYTICS_DB_NAME"],
            **common,
        ),
        database_target(
            "PUBLIC_DATA_DB_* (the public_data Airflow connection)",
            host_names=["PUBLIC_DATA_DB_HOST", "ANALYTICS_DB_HOST"],
            port_names=["PUBLIC_DATA_DB_PORT", "ANALYTICS_DB_PORT"],
            name_names=["PUBLIC_DATA_DB_NAME", "ANALYTICS_DB_NAME"],
            **common,
        ),
    ]


def _refusal(
    metadata: DatabaseTarget,
    warehouse: DatabaseTarget,
    env_label: str,
    flags: FlagNames,
) -> str:
    return "\n".join(
        [
            "Refusing to run airflow-init: the Airflow metadata database and "
            "the warehouse are the same database.",
            "",
            f"  metadata  ({metadata.label}): {metadata}",
            f"  warehouse ({warehouse.label}): {warehouse}",
            "",
            "airflow-init would run 'airflow db migrate' against that database, "
            "creating Airflow's",
            "metadata schema inside the warehouse, then delete and recreate the "
            "public_data",
            "connection and reset every API pool to this repository's defaults.",
            "",
            "Fix one of:",
            f"  - point AIRFLOW_METADATA_DB_NAME at a database of its own in "
            f"'{env_label}'",
            f"  - drop {flags.with_local_airflow} to start only "
            f"redis/api/martin/web against existing Airflow",
            f"  - pass {flags.allow_metadata_in_warehouse} if this really is intended",
        ]
    )


def airflow_metadata_isolation(
    mode: str,
    context: ComposeContext,
    *,
    allow_metadata_in_warehouse: bool = False,
    environ: Mapping[str, str] | None = None,
    root: Path | None = None,
    flags: FlagNames | None = None,
) -> GuardVerdict:
    """Decide whether the Airflow metadata database is the warehouse.

    `airflow-init` runs `airflow db migrate`, which creates Airflow's metadata
    schema in whatever database `AIRFLOW_METADATA_DB_*` names, and then resets
    the `public_data` connection and every API pool. Aimed at the warehouse,
    that is a one-way schema write into production data by an admin-capable
    role.
    """
    environ = os.environ if environ is None else environ
    root = root or REPOSITORY_ROOT
    flags = flags or FlagNames()

    if allow_metadata_in_warehouse:
        return GuardVerdict(
            "bypassed",
            f"Metadata isolation guard bypassed by {flags.allow_metadata_in_warehouse}",
        )

    file_values: dict[str, str] = {}
    env_label = "the host environment"
    if not context.use_host_env:
        env_label = context.env_file
        file_values = read_env_file(root / context.env_file)

    defaults = compose_defaults(mode)
    resolution = {
        "file_values": file_values,
        "defaults": defaults,
        "environ": environ,
    }

    metadata = database_target(
        "AIRFLOW_METADATA_DB_*",
        host_names=["AIRFLOW_METADATA_DB_HOST"],
        port_names=["AIRFLOW_METADATA_DB_PORT"],
        name_names=["AIRFLOW_METADATA_DB_NAME"],
        **resolution,
    )

    if not metadata.resolved:
        # Compose reports an unresolved required variable far better than a
        # half-informed guard can; let it.
        return GuardVerdict(
            "skipped",
            "Metadata isolation guard skipped: AIRFLOW_METADATA_DB_* is not "
            "fully resolved",
        )

    for warehouse in _warehouse_targets(**resolution):
        if not warehouse.resolved or not same_database(metadata, warehouse):
            continue
        return GuardVerdict("refused", _refusal(metadata, warehouse, env_label, flags))

    return GuardVerdict("ok")


@dataclass(frozen=True)
class ComposeStep:
    """One `docker compose` invocation an entrypoint should run."""

    description: str
    arguments: list[str] = field(default_factory=list)


def service_only(mode: str, *, with_local_airflow: bool) -> bool:
    """External mode starts this repository's four services and no Airflow."""
    return mode == "external" and not with_local_airflow


def compose_steps(
    action: str,
    context: ComposeContext,
    *,
    mode: str,
    with_local_airflow: bool = False,
) -> list[ComposeStep]:
    """The Compose invocations for an action, in order."""
    if action == "all":
        return [
            *compose_steps(
                "init", context, mode=mode, with_local_airflow=with_local_airflow
            ),
            *compose_steps(
                "up", context, mode=mode, with_local_airflow=with_local_airflow
            ),
        ]

    if action == "down":
        return [ComposeStep("Stopping stack", context.compose_arguments("down"))]

    if service_only(mode, with_local_airflow=with_local_airflow):
        return [
            ComposeStep(
                "External service-only stack: starting redis/api/martin/web",
                context.compose_arguments("up", "-d", *EXTERNAL_SERVICE_SET),
            )
        ]

    if action == "init":
        return [
            ComposeStep(
                "Running airflow-init", context.compose_arguments("up", "airflow-init")
            )
        ]

    # Internal compose leaves airflow-init in the default profile, so a bare
    # `up -d` runs it too; the guard belongs on `up` as much as on `init`.
    return [
        ComposeStep(
            "Starting stack in detached mode", context.compose_arguments("up", "-d")
        )
    ]


def guard_applies(action: str, mode: str, *, with_local_airflow: bool) -> bool:
    """Whether this action can reach `airflow db migrate` at all.

    `down` stops containers and `external` without local Airflow never starts
    `airflow-init`, so neither can migrate anything; running the guard there
    would refuse a command that is not the dangerous one.
    """
    if action == "down":
        return False
    return not service_only(mode, with_local_airflow=with_local_airflow)
