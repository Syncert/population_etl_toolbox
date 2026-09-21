"""Application settings loaded from environment variables.

Uses ``functools.lru_cache`` so ``get_settings()`` returns the same object
throughout the process lifetime (mirroring the common FastAPI pattern).
"""

from __future__ import annotations

import os
from functools import lru_cache


#: The levels ``API_LOG_LEVEL`` accepts. ``NOTSET`` is deliberately absent: on
#: a logger it means "inherit", which for ``apps.api`` would mean the root's
#: ``WARNING`` -- the exact silence this setting exists to end.
LOG_LEVEL_NAMES = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")


def _validated_log_level(value: str) -> str:
    level = value.strip().upper()
    if level not in LOG_LEVEL_NAMES:
        raise ValueError(
            f"API_LOG_LEVEL must be one of {', '.join(LOG_LEVEL_NAMES)}; got {value!r}"
        )
    return level


class Settings:
    """Minimal application settings read from environment variables."""

    def __init__(self) -> None:
        self.api_title: str = os.environ.get("API_TITLE", "Population ETL Toolbox API")
        self.api_version: str = os.environ.get("API_VERSION", "0.1.0")
        #: An operator's override for the served ``info.description``. Empty
        #: by default, and empty is the normal case: the application builds
        #: the description from its own source registry, so the front door
        #: cannot fall behind the platform the way a typed default did
        #: (API-144). Set this only to say something the registry cannot.
        self.api_description: str = os.environ.get("API_DESCRIPTION", "")
        self.redis_url: str = os.environ.get("REDIS_URL", "")
        self.api_cache_ttl_seconds: int = int(
            os.environ.get("API_CACHE_TTL_SECONDS", "300")
        )
        #: How long a cached publication epoch may be reused before the API
        #: re-reads gold_glossary.publisher_harvest_state. This bounds cache
        #: staleness after a warehouse publication: a republication is served
        #: within this window regardless of the response TTL. 0 re-reads on
        #: every cacheable request (deterministic tests).
        self.api_cache_freshness_seconds: int = int(
            os.environ.get("API_CACHE_FRESHNESS_SECONDS", "15")
        )
        self.database_url: str = os.environ.get("DATABASE_URL", "")
        #: API-owned application storage (ADR-0003). Separate URL and role from
        #: the read-only warehouse connection; empty disables the saved-analysis
        #: routes, which then answer an explicit 503.
        self.app_api_database_url: str = os.environ.get("APP_API_DATABASE_URL", "")
        # -- API-owned database limits (API-006). These configure the API's
        # engine only; ETL connections are owned elsewhere and keep their own
        # budgets.
        self.db_pool_size: int = int(os.environ.get("API_DB_POOL_SIZE", "5"))
        self.db_max_overflow: int = int(os.environ.get("API_DB_MAX_OVERFLOW", "10"))
        #: Seconds a request waits for a pooled connection before failing fast
        #: to the sanitized 503 instead of queueing behind an exhausted pool.
        self.db_pool_timeout_seconds: int = int(
            os.environ.get("API_DB_POOL_TIMEOUT_SECONDS", "5")
        )
        self.db_connect_timeout_seconds: int = int(
            os.environ.get("API_DB_CONNECT_TIMEOUT_SECONDS", "5")
        )
        #: Server-side statement timeout: the cancellation contract for a
        #: runaway query. 0 disables.
        self.db_statement_timeout_ms: int = int(
            os.environ.get("API_DB_STATEMENT_TIMEOUT_MS", "15000")
        )
        self.db_pool_recycle_seconds: int = int(
            os.environ.get("API_DB_POOL_RECYCLE_SECONDS", "1800")
        )
        # -- Rate limiting (API-006). Requests per minute per client, split by
        # cost class; 0 disables a bucket. Off by default so local iteration
        # and deterministic suites are unthrottled; deployment configuration
        # enables it.
        self.api_rate_limit_catalog_per_minute: int = int(
            os.environ.get("API_RATE_LIMIT_CATALOG_PER_MINUTE", "0")
        )
        self.api_rate_limit_analysis_per_minute: int = int(
            os.environ.get("API_RATE_LIMIT_ANALYSIS_PER_MINUTE", "0")
        )
        #: Reverse proxies whose ``X-Forwarded-For`` the rate limiter may
        #: believe (API-075), as addresses or CIDR blocks. Empty -- the
        #: default -- means the limiter keys on the TCP peer and ignores the
        #: header, which is the only safe reading when the hop in front is
        #: unknown. Every deployed topology here fronts the API with a proxy,
        #: so a deployment that leaves this empty gives the whole site one
        #: budget; `infra/docker/stack.env.example` declares the compose
        #: stack's own networks.
        self.api_trusted_proxy_ips: tuple[str, ...] = tuple(
            entry.strip()
            for entry in os.environ.get("API_TRUSTED_PROXY_IPS", "").split(",")
            if entry.strip()
        )
        #: How much of the API's own logging reaches the process log
        #: (API-143). ``INFO`` is the default because the request completion
        #: line -- the whole of this API's request observability -- is written
        #: at ``INFO``; ``WARNING`` silences it and keeps the unhandled-failure
        #: traceback. Validated here rather than at the first log call, so a
        #: typo fails the process at startup instead of quietly logging
        #: nothing.
        self.api_log_level: str = _validated_log_level(
            os.environ.get("API_LOG_LEVEL", "INFO")
        )
        # The largest request body any route accepts (ADR-0004). Bounds the
        # authenticated write resources' JSONB documents; public reads carry
        # no body. 256 KB is the evidence packet cap.
        self.api_max_request_body_bytes: int = int(
            os.environ.get("API_MAX_REQUEST_BODY_BYTES", "262144")
        )
        # -- Self-service identity (ADR-0005). Every value below is optional,
        # and with the client id and secret unset the sign-in routes answer a
        # 503 naming an unconfigured feature -- the same shape unconfigured
        # application storage already answers. A deployment that has not
        # registered an OIDC client is a deployment fact, not a caller error,
        # and operator-issued tokens keep working there with nothing changed.
        #
        # The issuer defaults to Google because ADR-0005 s1 names Google, and
        # it stays configurable because the ADR records the substitution to
        # make if the third-party dependency is judged worse than the mail
        # one. The value is the issuer's *identifier*, which is also where its
        # discovery document lives; it is compared against the ID token's
        # `iss` claim exactly, never by prefix.
        self.oidc_issuer: str = os.environ.get(
            "API_OIDC_ISSUER", "https://accounts.google.com"
        )
        self.oidc_client_id: str = os.environ.get("API_OIDC_CLIENT_ID", "")
        self.oidc_client_secret: str = os.environ.get("API_OIDC_CLIENT_SECRET", "")
        #: The exact-match redirect allowlist (ADR-0005 s1). Exact match, not
        #: prefix match and not origin match: an open redirect on a registered
        #: origin turns a prefix rule into a way to deliver somebody else's
        #: authorization code to an attacker. Comma-separated, and a
        #: deployment declares every origin it is reachable at -- including
        #: `http://localhost:3000` in development, which Google permits.
        self.oidc_redirect_uris: tuple[str, ...] = tuple(
            entry.strip()
            for entry in os.environ.get("API_OIDC_REDIRECT_URIS", "").split(",")
            if entry.strip()
        )
        #: Seconds of clock skew tolerated when checking `exp`, `iat`, and
        #: `nbf`. Bounded rather than generous: this is the window in which an
        #: expired token is still accepted.
        self.oidc_clock_skew_seconds: int = int(
            os.environ.get("API_OIDC_CLOCK_SKEW_SECONDS", "60")
        )
        #: How long a started sign-in may take to come back. Ten minutes is
        #: long enough for a person to work through a provider's consent
        #: screens and short enough that an abandoned transaction is not a
        #: state value sitting in a table for a day.
        self.oidc_transaction_ttl_seconds: int = int(
            os.environ.get("API_OIDC_TRANSACTION_TTL_SECONDS", "600")
        )
        #: ADR-0005 s2's three session bounds, in the units the ADR states
        #: them in. The access token is short because it is the half that
        #: lives in script's reach; the refresh token is long because it does
        #: not.
        self.api_access_token_ttl_seconds: int = int(
            os.environ.get("API_ACCESS_TOKEN_TTL_SECONDS", "900")
        )
        self.api_session_idle_days: int = int(
            os.environ.get("API_SESSION_IDLE_DAYS", "30")
        )
        self.api_session_absolute_days: int = int(
            os.environ.get("API_SESSION_ABSOLUTE_DAYS", "90")
        )
        #: The window in which a rotated refresh token's immediate predecessor
        #: is still tolerated (ADR-0005 s2). Two tabs refreshing at once is not
        #: an attack, and treating it as one signs people out for using a
        #: browser normally. Kept small: it is also the window in which a
        #: genuinely stolen token can be spent once.
        self.api_refresh_grace_seconds: int = int(
            os.environ.get("API_REFRESH_GRACE_SECONDS", "10")
        )
        #: The third rate-limit bucket (ADR-0005 s4), far tighter than either
        #: existing one because a human signs in rarely and a script does not.
        #: 0 disables it, like the others, so the deterministic suites are
        #: unthrottled and the deployment turns it on.
        self.api_rate_limit_identity_per_minute: int = int(
            os.environ.get("API_RATE_LIMIT_IDENTITY_PER_MINUTE", "0")
        )
        #: A ceiling on new accounts per hour across the whole deployment, so
        #: an attacker holding many provider accounts degrades into a queue
        #: rather than an unbounded row count. 0 disables it.
        self.api_account_creation_per_hour: int = int(
            os.environ.get("API_ACCOUNT_CREATION_PER_HOUR", "0")
        )
        #: How recently an account must have signed in to destroy itself
        #: (ADR-0005 s5). A 30-day session is a convenience for saving charts;
        #: it is not authority to delete everything from an unattended laptop.
        self.api_deletion_freshness_seconds: int = int(
            os.environ.get("API_DELETION_FRESHNESS_SECONDS", "600")
        )
        #: Per-account storage quotas (ADR-0005 s4). ADR-0004 already bounds
        #: one packet's size; these bound how many an account may hold. 0
        #: disables a quota.
        self.api_saved_analysis_quota: int = int(
            os.environ.get("API_SAVED_ANALYSIS_QUOTA", "200")
        )
        self.api_evidence_packet_quota: int = int(
            os.environ.get("API_EVIDENCE_PACKET_QUOTA", "100")
        )
        #: The backup retention window ADR-0005 s5 requires a deployment to
        #: declare, in days. It is reported by the account-deletion response
        #: and by the consumer guide, so "deleted data is gone from every
        #: retained backup once that window has passed" is a published number
        #: rather than an accident of configuration. 0 means the deployment
        #: has not declared one, and the API says so rather than inventing it.
        self.backup_retention_days: int = int(
            os.environ.get("BACKUP_RETENTION_DAYS", "0")
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached application settings singleton."""
    return Settings()
