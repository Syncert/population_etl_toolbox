#!/usr/bin/env python3
"""Bring the Compose stack up or down, on any host with Docker and Python.

The POSIX half of the deployment path. `deploy_stack.ps1` is the other half,
and both ask `tools/deployment.py` what to do -- which compose and env file to
use, which services to start, and whether the Airflow metadata database and
the warehouse have turned out to be the same database. Neither entrypoint
carries a rule of its own, so neither can drift from the other.

    python scripts/deploy_stack.py --mode internal --action all
    python scripts/deploy_stack.py --action down
    python scripts/deploy_stack.py --mode external --action up

`--emit-plan` prints what would run, as JSON, and executes nothing. That is
how `deploy_stack.ps1` asks this module for its decisions, and it is also the
honest way to see what a mode would do before doing it.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from tools.deployment import (  # noqa: E402  (path set above)
    ACTIONS,
    MODES,
    POWERSHELL_FLAGS,
    ComposeContext,
    DeploymentError,
    FlagNames,
    airflow_metadata_isolation,
    compose_steps,
    guard_applies,
    resolve_compose_context,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="deploy_stack.py", description=__doc__.splitlines()[0]
    )
    parser.add_argument("--mode", choices=MODES, default="internal")
    parser.add_argument("--action", choices=ACTIONS, default="all")
    parser.add_argument("--env-file", default="")
    parser.add_argument(
        "--use-host-env",
        action="store_true",
        help="read configuration from the host environment instead of an env file",
    )
    parser.add_argument(
        "--with-local-airflow",
        action="store_true",
        help="in external mode, also start this repository's Airflow services",
    )
    parser.add_argument(
        # Named for what it permits rather than --force, so it cannot be
        # reached for casually to get past an error whose whole point is that
        # the target is production.
        "--allow-airflow-metadata-in-warehouse",
        action="store_true",
        help="permit Airflow's metadata database to be the warehouse",
    )
    parser.add_argument(
        "--emit-plan",
        action="store_true",
        help="print the resolved plan as JSON and execute nothing",
    )
    parser.add_argument(
        "--powershell-flags",
        action="store_true",
        help="spell the refusal's suggested flags as deploy_stack.ps1 does",
    )
    return parser


def _default_commit_sha(environ: dict[str, str]) -> None:
    """Stamp evidence with the deployed commit when the host has not.

    The warehouse data-quality assessment records which code produced it, and
    an unstamped run is evidence nobody can trace back to a revision.
    """
    if (environ.get("DATA_QUALITY_COMMIT_SHA") or "").strip():
        return
    try:
        resolved = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return
    if resolved.returncode == 0 and resolved.stdout.strip():
        environ["DATA_QUALITY_COMMIT_SHA"] = resolved.stdout.strip()


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    flags: FlagNames = POWERSHELL_FLAGS if arguments.powershell_flags else FlagNames()
    log_prefix = f"[deploy:{arguments.mode}/{arguments.action}]"

    def log(message: str) -> None:
        print(f"{log_prefix} {message}", flush=True)

    try:
        context: ComposeContext = resolve_compose_context(
            arguments.mode,
            arguments.env_file,
            use_host_env=arguments.use_host_env,
            root=REPOSITORY_ROOT,
        )
    except DeploymentError as error:
        # `--emit-plan` always answers JSON, including when it answers a
        # refusal: its caller is `deploy_stack.ps1`, and a contract that is
        # JSON on success and prose on failure is one the caller has to parse
        # twice and will eventually parse wrongly.
        if arguments.emit_plan:
            print(json.dumps({"error": str(error)}, indent=2))
        else:
            print(f"{log_prefix} {error}", file=sys.stderr)
        return 1

    verdict = None
    if guard_applies(
        arguments.action,
        arguments.mode,
        with_local_airflow=arguments.with_local_airflow,
    ):
        verdict = airflow_metadata_isolation(
            arguments.mode,
            context,
            allow_metadata_in_warehouse=arguments.allow_airflow_metadata_in_warehouse,
            root=REPOSITORY_ROOT,
            flags=flags,
        )

    steps = compose_steps(
        arguments.action,
        context,
        mode=arguments.mode,
        with_local_airflow=arguments.with_local_airflow,
    )

    if arguments.emit_plan:
        print(
            json.dumps(
                {
                    "error": "",
                    "compose_file": context.compose_file,
                    "env_file": context.env_file,
                    "use_host_env": context.use_host_env,
                    "guard": {
                        "status": verdict.status if verdict else "not_applicable",
                        "message": verdict.message if verdict else "",
                    },
                    "steps": [
                        {"description": step.description, "arguments": step.arguments}
                        for step in steps
                    ],
                },
                indent=2,
            )
        )
        return 1 if verdict is not None and verdict.refuses else 0

    if verdict is not None:
        if verdict.refuses:
            print(verdict.message, file=sys.stderr)
            return 1
        if verdict.message:
            log(verdict.message)

    _default_commit_sha(os.environ)

    for step in steps:
        log(step.description)
        log("docker compose " + " ".join(step.arguments))
        completed = subprocess.run(["docker", "compose", *step.arguments], check=False)
        if completed.returncode != 0:
            print(
                f"{log_prefix} docker compose failed with exit code "
                f"{completed.returncode}",
                file=sys.stderr,
            )
            return completed.returncode

    log("Completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
