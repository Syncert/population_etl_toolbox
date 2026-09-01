"""The API's version policy, expressed as code.

Every public resource is served under ``/api/v1``. That is the only public
surface: the unversioned ``/api`` aliases that carried the MVP's original
paths were retired in API-008, once the repository's only consumer
(``apps/web``) had been migrated to the versioned prefix and before any
downstream plan began depending on the API. Retiring them while the
dependency set was still empty is why the removal cost nothing; the same
change after a consumer existed would have been a breaking one.

``/health`` and ``/health/ready`` without the ``/api`` prefix remain outside
the version policy: they are the container and load-balancer probes named in
the deployment files, they carry no data contract, and versioning them would
put a data-contract promise on infrastructure.

A new version is additive: append to ``SUPPORTED_VERSIONS`` and move
``CURRENT_VERSION``. ``v1`` is never edited in place once a consumer depends
on it -- a change that would break a ``v1`` client belongs in ``v2``, and a
change that would not (a new optional field, a new operation, a widened
bound) belongs in ``v1``.

See ``docs/decisions/0002-api-versioning-and-deprecation.md``.
"""

from __future__ import annotations

#: The version new consumers should target. Adding ``v2`` means appending to
#: ``SUPPORTED_VERSIONS`` and moving this constant, never editing ``v1`` in place.
CURRENT_VERSION = "v1"

#: Every version the application currently serves, newest last.
SUPPORTED_VERSIONS: tuple[str, ...] = ("v1",)

API_ROOT = "/api"
VERSIONED_ROOT = f"{API_ROOT}/{CURRENT_VERSION}"

#: Every prefix a public resource is mounted under. Code that reasons about
#: whole groups of routes -- cache eligibility, rate-limit classes -- builds
#: its paths from this rather than repeating literals that a new version would
#: leave behind.
API_PREFIXES: tuple[str, ...] = tuple(
    f"{API_ROOT}/{version}" for version in SUPPORTED_VERSIONS
)

#: Paths served outside the version policy: infrastructure probes with no data
#: contract. They are neither versioned nor deprecated.
UNVERSIONED_PATHS: frozenset[str] = frozenset({"/health", "/health/ready"})


def is_versioned_path(path: str) -> bool:
    """True when ``path`` names an explicit API version this app serves."""
    return any(
        path == f"{API_ROOT}/{version}" or path.startswith(f"{API_ROOT}/{version}/")
        for version in SUPPORTED_VERSIONS
    )
