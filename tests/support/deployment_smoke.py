"""Configuration guard for the scheduled live-deployment smoke run.

The live-stack tier reads whatever origin ``SMOKE_BASE_URL`` names, and skips
when it names nothing. That is right for a developer running it by hand and
wrong for a scheduled job whose entire purpose is to observe the deployment:
a skipped suite is a passing suite, so an unset variable would turn the one
check that watches production green and silent -- the same failure
``SMOKE_REQUIRED`` was added to the tier to end (WEB-027).

So the scheduled run validates its target before it starts, the way
``tests/support/external.py`` validates its credentials, and says exactly
what to set rather than failing somewhere inside vitest with a connection
error.

Run as a module: ``python -m tests.support.deployment_smoke``.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from urllib.parse import urlparse

#: The origin the scheduled run points the smoke tier at. A repository
#: variable rather than a secret: an origin is not a credential, and a masked
#: value would turn every failure message into ``***``.
BASE_URL_VARIABLE = "SMOKE_BASE_URL"

#: Schemes a deployment can actually be reached over.
SUPPORTED_SCHEMES = frozenset({"http", "https"})

CONFIGURATION_HELP = (
    "Set the repository variable DEPLOYMENT_SMOKE_BASE_URL to the deployment's "
    "origin -- the one that serves /api/v1 and /tiles through the same proxy a "
    "browser uses, e.g. https://example.org. If the deployment is not reachable "
    "from a GitHub-hosted runner, also set DEPLOYMENT_SMOKE_RUNNER to a "
    "self-hosted runner label that can reach it. If this repository has no "
    "deployment to watch yet, disable the live-deployment-smoke workflow rather "
    "than leaving it unconfigured: a scheduled job that cannot reach its target "
    "is not evidence about the deployment either way."
)


def validate_deployment_target(environment: Mapping[str, str]) -> str:
    """Return the configured origin, or explain precisely what is missing.

    Raises rather than skipping. The whole value of this job is that it fails
    when the deployment is not answering, and a job that quietly passes
    because nobody told it where to look reports the same green as one that
    checked a healthy deployment.
    """
    raw = environment.get(BASE_URL_VARIABLE, "").strip()
    if not raw:
        raise RuntimeError(
            f"{BASE_URL_VARIABLE} is not set, so the scheduled live-deployment "
            f"smoke run has no deployment to check. {CONFIGURATION_HELP}"
        )

    parsed = urlparse(raw)
    if parsed.scheme not in SUPPORTED_SCHEMES:
        raise RuntimeError(
            f"{BASE_URL_VARIABLE} is {raw!r}, whose scheme "
            f"{parsed.scheme or '(none)'!r} is not one of "
            f"{sorted(SUPPORTED_SCHEMES)}. {CONFIGURATION_HELP}"
        )
    if not parsed.netloc:
        raise RuntimeError(
            f"{BASE_URL_VARIABLE} is {raw!r}, which names no host. {CONFIGURATION_HELP}"
        )
    # A path would be silently concatenated with the app's own absolute paths
    # (`/api/v1/...`), producing a URL nothing serves and a failure that reads
    # like an outage. The tier strips one trailing slash itself; anything more
    # is a misconfiguration worth naming here.
    if parsed.path.strip("/"):
        raise RuntimeError(
            f"{BASE_URL_VARIABLE} is {raw!r}; the tier resolves the "
            "application's own absolute paths against this origin, so it must "
            f"be an origin and carry no path. {CONFIGURATION_HELP}"
        )
    return raw


def main() -> int:
    try:
        target = validate_deployment_target(os.environ)
    except RuntimeError as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    print(f"live-deployment smoke target: {target}")
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
