"""The scheduled live-deployment smoke job is configured or it is red.

Covers: ENV-019 — a scheduled job exists that runs the live-stack smoke tier
        against a deployed origin, it refuses to pass by skipping, and its
        target is validated before the tier starts rather than failing as a
        connection error inside vitest.

Every other tier in this repository grades a stack CI built seconds earlier
from seeded fixtures. None of them can see the deployment: an API pointed at
an empty or half-loaded warehouse passes every one of them and draws a blank
chart on every screen. This job is the one that looks at the thing users
open, so the ways it could quietly stop looking are what this file guards.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tests.support.deployment_smoke import (
    BASE_URL_VARIABLE,
    validate_deployment_target,
)

pytestmark = [pytest.mark.unit, pytest.mark.deployment]

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_PATH = ROOT / ".github/workflows/live-deployment-smoke.yml"


def _workflow() -> dict:
    payload = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    # PyYAML reads the YAML 1.1 word `on` as boolean True.
    if True in payload:
        payload["on"] = payload.pop(True)
    return payload


def _job() -> dict:
    jobs = _workflow()["jobs"]
    assert len(jobs) == 1, f"expected one job, found {sorted(jobs)}"
    return next(iter(jobs.values()))


def _run_commands() -> list[str]:
    return [str(step.get("run") or "") for step in _job()["steps"]]


def test_an_unset_target_is_an_error_and_not_a_skip() -> None:
    """Covers: ENV-019 — the scheduled run cannot pass by looking at nothing.

    The tier itself skips without ``SMOKE_BASE_URL``, which is right for a
    developer and fatal for a scheduled observer: a skipped suite is a
    passing suite, so an unset variable would report the deployment healthy
    without having contacted it.
    """
    with pytest.raises(RuntimeError) as unset:
        validate_deployment_target({})
    assert BASE_URL_VARIABLE in str(unset.value)
    # The message has to be actionable: this failure is read by whoever has
    # not finished setting the job up.
    assert "DEPLOYMENT_SMOKE_BASE_URL" in str(unset.value)

    # Whitespace is not a target either: a variable set to an empty string in
    # a workflow expression arrives as spaces, not as an absent key.
    with pytest.raises(RuntimeError, match=BASE_URL_VARIABLE):
        validate_deployment_target({BASE_URL_VARIABLE: "   "})


def test_a_target_that_is_not_a_reachable_origin_is_refused() -> None:
    """Covers: ENV-019 — a misconfigured origin fails saying so.

    Each of these produces a connection error or a 404 deep inside the tier,
    which reads exactly like the outage the job exists to report. Naming the
    misconfiguration here keeps the two apart.
    """
    for target in (
        "example.org",  # no scheme
        "ftp://example.org",  # not a scheme anything is served over
        "https://",  # no host
        # A path is silently concatenated with the application's own absolute
        # `/api/v1/...` paths, producing a URL nothing serves.
        "https://example.org/api/v1",
    ):
        with pytest.raises(RuntimeError, match=BASE_URL_VARIABLE):
            validate_deployment_target({BASE_URL_VARIABLE: target})


def test_a_reachable_origin_is_accepted_as_given() -> None:
    """Covers: ENV-019 — a valid origin passes through unchanged."""
    for target in (
        "https://example.org",
        "http://127.0.0.1:33001",
        "https://example.org/",
    ):
        assert validate_deployment_target({BASE_URL_VARIABLE: target}) == target


def test_the_job_is_scheduled_and_can_be_run_on_demand() -> None:
    """Covers: ENV-019 — it watches on its own and answers a human asking."""
    trigger = _workflow()["on"]
    assert "schedule" in trigger, "nothing runs this job on its own"
    assert trigger["schedule"], "the schedule declares no cron expression"
    assert "workflow_dispatch" in trigger, "an operator cannot run it on demand"

    # Deliberately not a pull-request or push gate. It observes a deployment
    # rather than grading a change, so a red run means the deployment needs
    # attention, not that a branch is broken -- and a job no branch can fix
    # must never be able to block a merge.
    assert "pull_request" not in trigger
    assert "push" not in trigger


def test_the_job_validates_its_target_before_running_the_tier() -> None:
    """Covers: ENV-019 — the guard runs first, or it is not a guard."""
    commands = _run_commands()
    validation = [
        index
        for index, command in enumerate(commands)
        if "tests.support.deployment_smoke" in command
    ]
    tier = [index for index, command in enumerate(commands) if "test:smoke" in command]
    assert validation, "the job never validates its deployment target"
    assert tier, "the job never runs the smoke tier"
    assert min(validation) < min(tier), (
        "the target validation must run before the tier, or a misconfigured "
        "origin fails as a connection error instead of as a configuration one"
    )


def test_the_tier_runs_under_the_settings_that_refuse_a_silent_pass() -> None:
    """Covers: ENV-019 — the two environment settings that make it evidence.

    ``SMOKE_REQUIRED`` makes the tier assert its own configuration rather than
    skipping. ``SMOKE_REQUIRE_ALL_SOURCES`` grades the deployment against
    every registered source, the way the end-to-end tier grades against its
    product inventory -- without it a deployment that quietly lost six of
    seven sources still passes (WEB-102).
    """
    job = _job()
    environment = {**(_workflow().get("env") or {}), **(job.get("env") or {})}
    for step in job["steps"]:
        environment.update(step.get("env") or {})

    assert str(environment.get("SMOKE_REQUIRED")) == "1"
    assert "SMOKE_REQUIRE_ALL_SOURCES" in environment, (
        "the job does not state whether every registered source must publish; "
        "leaving it unset silently accepts a deployment missing most of them"
    )


def test_the_runner_can_be_pointed_at_a_deployment_behind_a_network() -> None:
    """Covers: ENV-019 — a private deployment is not an unwatchable one.

    A GitHub-hosted runner cannot reach a stack on a private network, and
    this repository's deployment path (`scripts/deploy_stack.ps1` over
    `infra/docker/docker-compose.yml`) produces exactly that. A job hard-wired
    to `ubuntu-latest` would be permanently red for the one deployment shape
    the repository actually ships.
    """
    runs_on = str(_job()["runs-on"])
    assert "DEPLOYMENT_SMOKE_RUNNER" in runs_on, (
        "the job's runner is not configurable, so a deployment that is not "
        f"publicly reachable can never be watched: runs-on is {runs_on!r}"
    )
