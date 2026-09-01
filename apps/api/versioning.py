"""The API's version and deprecation policy, expressed as code.

Every public resource is served twice: once under ``/api/v1``, which is the
contract a client should depend on, and once under the original unversioned
``/api`` path it has always had. The two are the same operation -- same
parameters, same response model, same service call -- so an existing consumer
keeps working untouched while new work targets a path whose stability is
promised.

The legacy alias is not a permanent second surface. It answers with the
``Deprecation``, ``Sunset``, and ``Link`` headers RFC 8594 defines, so a consumer
learns from an ordinary response that it is on a retiring path and where the
successor lives. API-008 removes the alias, and only after evidence shows no
required consumer still calls it.

``/health`` without the ``/api`` prefix is deliberately outside this policy: it
is the container and load-balancer probe named in the deployment files, it
carries no data contract, and marking it deprecated would put a retirement
signal on infrastructure that is not retiring.

See ``docs/decisions/0002-api-versioning-and-deprecation.md``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

#: The version new consumers should target. Adding ``v2`` means appending to
#: ``SUPPORTED_VERSIONS`` and moving this constant, never editing ``v1`` in place.
CURRENT_VERSION = "v1"

#: Every version the application currently serves, newest last.
SUPPORTED_VERSIONS: tuple[str, ...] = ("v1",)

API_ROOT = "/api"
VERSIONED_ROOT = f"{API_ROOT}/{CURRENT_VERSION}"

#: Every prefix a public resource is mounted under, versioned first. Code that
#: reasons about whole groups of routes -- cache eligibility, rate-limit
#: classes -- builds its paths from this rather than repeating literals that a
#: new version would leave behind.
API_PREFIXES: tuple[str, ...] = (
    *(f"{API_ROOT}/{version}" for version in SUPPORTED_VERSIONS),
    API_ROOT,
)

#: Paths served outside the version policy: infrastructure probes with no data
#: contract. They are neither versioned nor deprecated.
UNVERSIONED_PATHS: frozenset[str] = frozenset({"/health"})

LEGACY_DEPRECATION_HEADER = "deprecation"
LEGACY_SUNSET_HEADER = "sunset"
LEGACY_LINK_HEADER = "link"

#: The date the unversioned aliases stop being served, in the IMF-fixdate form
#: RFC 8594 requires. API-008 owns the removal; publishing the date up front is
#: what makes the compatibility window bounded rather than open-ended.
LEGACY_SUNSET_DATE = "Mon, 01 Mar 2027 00:00:00 GMT"


def is_versioned_path(path: str) -> bool:
    """True when ``path`` already names an explicit API version."""
    return any(
        path == f"{API_ROOT}/{version}" or path.startswith(f"{API_ROOT}/{version}/")
        for version in SUPPORTED_VERSIONS
    )


def is_legacy_path(path: str) -> bool:
    """True when ``path`` is an unversioned alias inside the compatibility window."""
    if path in UNVERSIONED_PATHS:
        return False
    if not path.startswith(f"{API_ROOT}/"):
        return False
    return not is_versioned_path(path)


def versioned_path_for(legacy_path: str) -> str:
    """Map ``/api/catalog/metrics`` to ``/api/v1/catalog/metrics``."""
    if is_versioned_path(legacy_path):
        return legacy_path
    if not legacy_path.startswith(f"{API_ROOT}/"):
        return legacy_path
    return f"{VERSIONED_ROOT}/{legacy_path[len(API_ROOT) + 1 :]}"


def legacy_path_for(versioned_path: str) -> str:
    """Map ``/api/v1/catalog/metrics`` back to ``/api/catalog/metrics``."""
    for version in SUPPORTED_VERSIONS:
        prefix = f"{API_ROOT}/{version}/"
        if versioned_path.startswith(prefix):
            return f"{API_ROOT}/{versioned_path[len(prefix) :]}"
    return versioned_path


Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]


class LegacyDeprecationMiddleware:
    """Announce the retirement of unversioned aliases on their own responses.

    This runs outside the response cache so a cached legacy body still carries
    the signal; the headers describe the route, not the payload, and must not
    depend on whether Redis happened to answer.
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(
        self, scope: dict[str, Any], receive: Receive, send: Send
    ) -> None:
        if scope.get("type") != "http" or not is_legacy_path(
            str(scope.get("path", ""))
        ):
            await self.app(scope, receive, send)
            return

        successor = versioned_path_for(str(scope.get("path", "")))

        async def send_with_deprecation(message: Message) -> None:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend(
                    [
                        (LEGACY_DEPRECATION_HEADER.encode(), b"true"),
                        (LEGACY_SUNSET_HEADER.encode(), LEGACY_SUNSET_DATE.encode()),
                        (
                            LEGACY_LINK_HEADER.encode(),
                            f'<{successor}>; rel="successor-version"'.encode(),
                        ),
                    ]
                )
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_deprecation)
