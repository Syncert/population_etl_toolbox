"""The OpenID Connect authorization-code flow, and its refusals (ADR-0005 §1).

ADR-0005 names what must be got right and why each is named:

    "a ``state`` parameter bound to the caller's session, a ``nonce`` echoed in
    the ID token, an exact-match redirect-URI allowlist, ID-token signature
    verification against the provider's JWKS with issuer, audience and expiry
    all checked, and a bounded clock skew. A library does this; the
    implementing plan uses one and tests the refusals rather than writing the
    protocol by hand."

The library is PyJWT, which owns signature verification and the `iss`/`aud`/
`exp`/`iat`/`nbf` checks. This module owns the three things a JWT library
cannot know about: that the algorithm must be asymmetric, that the `nonce`
must match the one this server generated, and that an email claim is only
storable when the provider marked it verified.

**Why the algorithm list is explicit.** ``jwt.decode`` will honour whatever
``alg`` the token's own header asks for unless it is told not to. A token
presenting ``alg: none`` verifies against no key at all, and one presenting
``HS256`` verifies against the *public* key read as an HMAC secret -- which an
attacker has, because it is public. Passing ``algorithms=`` is not a
preference; it is the difference between checking a signature and being told
there was one.

**What reaches a log.** The *classification* of a refusal and nothing else:
never the token, the authorization code, the subject, the email, the state, or
the nonce. An operator needs to know that sign-ins are failing on signature
verification rather than on clock skew; nobody needs the credential to learn
that, and ``apps/api/telemetry.py`` already forbids request content in a log
line. The caller is told none of it -- every refusal answers one stable
string, because "your nonce did not match" tells an attacker which check to
work on next.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Protocol
from urllib.parse import urlencode

import httpx
import jwt

from data_ingestion_toolbox.config import Settings, get_settings

logger = logging.getLogger(__name__)

#: The one refusal a caller ever sees. Sign-in either completed or it did not.
SIGN_IN_REFUSED_DETAIL = "sign-in could not be completed"

#: The one answer when the deployment has registered no OIDC client. Shaped
#: like the unconfigured-storage 503 beside it: a deployment fact, not a
#: caller error, and not something to page an operator over.
IDENTITY_UNCONFIGURED_DETAIL = (
    "self-service sign-in is not configured for this deployment"
)

#: Google's issuer identifier, and the second form its ID tokens may carry.
#: See ``OidcSettings.accepted_issuers`` for why both are named.
_GOOGLE_ISSUER = "https://accounts.google.com"
_GOOGLE_BARE_ISSUER = "accounts.google.com"

#: Asymmetric signatures only. See the module docstring: this list is what
#: stops `alg: none` and the HMAC-with-the-public-key confusion, and it is
#: passed to every ``jwt.decode`` call rather than defaulted anywhere.
ID_TOKEN_ALGORITHMS = ("RS256", "RS384", "RS512", "ES256", "ES384", "ES512")

#: How long a fetched discovery document is reused. The provider's endpoints
#: change rarely; refetching per sign-in makes the provider's availability a
#: dependency of every single request rather than of one an hour.
_DISCOVERY_TTL_SECONDS = 3600

#: Bound on the provider requests, so a hung provider is a refused sign-in
#: rather than a held worker.
_PROVIDER_TIMEOUT_SECONDS = 10.0


class IdentityRefused(Exception):
    """A sign-in that will not be completed.

    ``reason`` is a fixed classification, not a message: it is compared in
    tests and logged as a category, and it is never rendered to a caller.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class IdentityUnconfigured(Exception):
    """No OIDC client is registered for this deployment."""


@dataclass(frozen=True)
class OidcSettings:
    """The deployment's identity configuration, validated once."""

    issuer: str
    client_id: str
    client_secret: str
    redirect_uris: tuple[str, ...]
    clock_skew_seconds: int
    transaction_ttl_seconds: int

    @classmethod
    def from_settings(cls, settings: Optional[Settings] = None) -> "OidcSettings":
        configured = settings or get_settings()
        return cls(
            issuer=configured.oidc_issuer,
            client_id=configured.oidc_client_id,
            client_secret=configured.oidc_client_secret,
            redirect_uris=tuple(configured.oidc_redirect_uris),
            clock_skew_seconds=configured.oidc_clock_skew_seconds,
            transaction_ttl_seconds=configured.oidc_transaction_ttl_seconds,
        )

    @property
    def configured(self) -> bool:
        """Every part must be present, **including at least one redirect URI**.

        A client id with no registered redirect URI is not a half-configured
        deployment: it is one where the exact-match allowlist is empty, so
        every sign-in is refused no matter what a caller sends. Answering that
        as "this deployment does not offer accounts" is both true and useful;
        answering it as a refused request per attempt tells an operator their
        callers are doing something wrong, which they are not.
        """
        return bool(
            self.issuer and self.client_id and self.client_secret and self.redirect_uris
        )

    @property
    def accepted_issuers(self) -> tuple[str, ...]:
        """Every `iss` value a token from this issuer may legitimately carry.

        Normally one: the issuer identifier, compared exactly. Google is the
        documented exception, and it is this deployment's provider, so getting
        it wrong would break the first real sign-in rather than an edge case.
        Its OpenID Connect documentation says of the `iss` claim:

            "Always ``https://accounts.google.com`` or ``accounts.google.com``
            for Google ID tokens."

        and of validating it:

            "Verify that the value of the ``iss`` claim in the ID token is
            equal to ``https://accounts.google.com`` or
            ``accounts.google.com``."

        while the discovery document's own ``issuer`` field is the first form.
        So an exact-match check against the discovered issuer refuses a token
        Google says is valid, and it refuses it *intermittently*, which is the
        worst way to find out.

        Widened for that provider by name rather than by a rule like "also
        accept the host without its scheme". Such a rule would silently accept
        ``accounts.google.com`` from an issuer that never sends it, which is a
        weakening nobody asked for; this is one documented fact about one
        provider, written where the fact is.
        """
        if self.issuer == _GOOGLE_ISSUER:
            return (_GOOGLE_ISSUER, _GOOGLE_BARE_ISSUER)
        return (self.issuer,)

    def allows_redirect(self, redirect_uri: str) -> bool:
        """Exact string match against the allowlist, deliberately.

        Not a prefix match, not a parsed-origin match, and not a match that
        ignores a trailing slash. Each of those turns an open redirect
        anywhere on a registered origin into a way to have the provider
        deliver somebody else's authorization code to an attacker.
        """
        return redirect_uri in self.redirect_uris


@dataclass(frozen=True)
class IdentityClaims:
    """What a verified ID token is allowed to tell us.

    Three fields, and the ADR is explicit that there is no fourth: no display
    name, no avatar, no locale, nothing else harvested from the provider.
    """

    issuer: str
    subject: str
    email: Optional[str]


@dataclass(frozen=True)
class AuthorizationRequest:
    """A started sign-in: what the browser is sent to, and what to remember."""

    authorization_url: str
    state: str
    nonce: str
    code_verifier: str
    redirect_uri: str


class KeyResolver(Protocol):
    """Resolves the provider's signing key for a specific token."""

    def signing_key(self, id_token: str) -> Any: ...  # pragma: no cover


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def code_challenge_for(code_verifier: str) -> str:
    """The S256 PKCE challenge. ``plain`` is never offered: it is the challenge
    equal to the verifier, which protects against nothing."""
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return _b64url(digest)


class OidcProvider:
    """The provider's endpoints and keys, discovered rather than hardcoded.

    Discovery is why ADR-0005 §1 requires OpenID Connect rather than bare
    OAuth 2.0: the authorization endpoint, the token endpoint and the JWKS URI
    are published by the issuer, so none of them is a constant in this
    repository that could drift from what the provider actually serves.

    Every network call this class makes is injectable, because a test that
    needs the internet to check that a tampered signature is refused will
    eventually be deleted for being flaky, and the refusal will stop being
    tested.
    """

    def __init__(
        self,
        settings: OidcSettings,
        *,
        client_factory=None,
        key_resolver: Optional[KeyResolver] = None,
        clock=time.time,
    ) -> None:
        self.settings = settings
        self._client_factory = client_factory or (
            lambda: httpx.Client(timeout=_PROVIDER_TIMEOUT_SECONDS)
        )
        self._key_resolver = key_resolver
        self._clock = clock
        self._metadata: Optional[Mapping[str, Any]] = None
        self._metadata_fetched_at = 0.0

    # -- discovery -------------------------------------------------------
    @property
    def discovery_url(self) -> str:
        return f"{self.settings.issuer.rstrip('/')}/.well-known/openid-configuration"

    def metadata(self) -> Mapping[str, Any]:
        now = self._clock()
        if (
            self._metadata is not None
            and now - self._metadata_fetched_at < _DISCOVERY_TTL_SECONDS
        ):
            return self._metadata
        try:
            with self._client_factory() as client:
                response = client.get(self.discovery_url)
                response.raise_for_status()
                document = response.json()
        except Exception as exc:  # noqa: BLE001 - every failure is one refusal
            raise IdentityRefused("provider_discovery_unavailable") from exc
        # The document says who issued it. A discovery document served from
        # our configured issuer that names a different one is either a
        # misconfiguration or an attack, and in both cases continuing would
        # mean accepting tokens from an issuer nobody chose.
        if document.get("issuer") != self.settings.issuer:
            raise IdentityRefused("provider_issuer_mismatch")
        for required in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
            if not document.get(required):
                raise IdentityRefused("provider_discovery_incomplete")
        self._metadata = document
        self._metadata_fetched_at = now
        return document

    # -- step one: send the browser to the provider ----------------------
    def start(self, redirect_uri: str) -> AuthorizationRequest:
        if not self.settings.configured:
            raise IdentityUnconfigured()
        if not self.settings.allows_redirect(redirect_uri):
            raise IdentityRefused("redirect_uri_not_allowed")

        state = secrets.token_urlsafe(32)
        nonce = secrets.token_urlsafe(32)
        code_verifier = secrets.token_urlsafe(64)
        query = {
            "response_type": "code",
            "client_id": self.settings.client_id,
            "redirect_uri": redirect_uri,
            # `openid` for the ID token, `email` because §1 stores a verified
            # address. `profile` is deliberately absent: the ADR harvests no
            # profile field, so asking for one would be requesting consent to
            # read something this platform has decided not to keep.
            "scope": "openid email",
            "state": state,
            "nonce": nonce,
            "code_challenge": code_challenge_for(code_verifier),
            "code_challenge_method": "S256",
        }
        endpoint = str(self.metadata()["authorization_endpoint"])
        separator = "&" if "?" in endpoint else "?"
        return AuthorizationRequest(
            authorization_url=f"{endpoint}{separator}{urlencode(query)}",
            state=state,
            nonce=nonce,
            code_verifier=code_verifier,
            redirect_uri=redirect_uri,
        )

    # -- step two: trade the code for an ID token ------------------------
    def exchange_code(self, *, code: str, redirect_uri: str, code_verifier: str) -> str:
        """Return the raw ID token, or refuse.

        The provider's own access token is deliberately discarded. This
        platform calls no provider API on a reader's behalf, so keeping a
        credential that would let it is holding something it has no use for.
        """
        if not self.settings.configured:
            raise IdentityUnconfigured()
        if not self.settings.allows_redirect(redirect_uri):
            raise IdentityRefused("redirect_uri_not_allowed")
        token_endpoint = str(self.metadata()["token_endpoint"])
        form = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": self.settings.client_id,
            "client_secret": self.settings.client_secret,
            "code_verifier": code_verifier,
        }
        try:
            with self._client_factory() as client:
                response = client.post(token_endpoint, data=form)
        except Exception as exc:  # noqa: BLE001
            raise IdentityRefused("token_endpoint_unavailable") from exc
        if response.status_code != 200:
            # The provider's error body is not propagated. It can contain the
            # code and the client id, and a caller who reached this point has
            # already been told everything they are owed.
            raise IdentityRefused("token_exchange_refused")
        try:
            payload = response.json()
        except Exception as exc:  # noqa: BLE001
            raise IdentityRefused("token_response_unparseable") from exc
        id_token = payload.get("id_token")
        if not id_token or not isinstance(id_token, str):
            raise IdentityRefused("token_response_carried_no_id_token")
        return id_token

    # -- step three: believe the ID token, or do not ---------------------
    def resolve_key(self, id_token: str) -> Any:
        if self._key_resolver is not None:
            return self._key_resolver.signing_key(id_token)
        jwks_uri = str(self.metadata()["jwks_uri"])
        try:
            client = jwt.PyJWKClient(jwks_uri, cache_keys=True)
            return client.get_signing_key_from_jwt(id_token).key
        except Exception as exc:  # noqa: BLE001
            raise IdentityRefused("signing_key_unavailable") from exc

    def verify_id_token(self, id_token: str, *, nonce: str) -> IdentityClaims:
        _refuse_if_this_process_cannot_check_signatures()
        key = self.resolve_key(id_token)
        try:
            claims = jwt.decode(
                id_token,
                key=key,
                algorithms=list(ID_TOKEN_ALGORITHMS),
                audience=self.settings.client_id,
                issuer=list(self.settings.accepted_issuers),
                leeway=self.settings.clock_skew_seconds,
                options={
                    "require": ["iss", "aud", "exp", "iat", "sub"],
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_iat": True,
                    "verify_aud": True,
                    "verify_iss": True,
                },
            )
        except jwt.ExpiredSignatureError as exc:
            raise IdentityRefused("id_token_expired") from exc
        except jwt.ImmatureSignatureError as exc:
            raise IdentityRefused("id_token_not_yet_valid") from exc
        except jwt.InvalidAudienceError as exc:
            raise IdentityRefused("id_token_audience") from exc
        except jwt.InvalidIssuerError as exc:
            raise IdentityRefused("id_token_issuer") from exc
        except jwt.MissingRequiredClaimError as exc:
            raise IdentityRefused("id_token_missing_claim") from exc
        except jwt.InvalidSignatureError as exc:
            raise IdentityRefused("id_token_signature") from exc
        except jwt.InvalidAlgorithmError as exc:
            raise IdentityRefused("id_token_algorithm") from exc
        except jwt.DecodeError as exc:
            # `alg: none` and a token PyJWT cannot even read land here. The
            # classification is deliberately the same: both mean "this is not
            # a token signed by the provider", which is the only distinction
            # that matters.
            raise IdentityRefused("id_token_undecodable") from exc
        except jwt.InvalidTokenError as exc:
            raise IdentityRefused("id_token_invalid") from exc

        # The nonce: PyJWT has no opinion about it, because it is not a JWT
        # concept. Without this check a token replayed from a different
        # sign-in -- same issuer, same audience, unexpired -- verifies
        # perfectly, which is exactly the attack the nonce exists for.
        presented = claims.get("nonce")
        if not isinstance(presented, str) or not secrets.compare_digest(
            presented, nonce
        ):
            raise IdentityRefused("id_token_nonce")

        subject = claims.get("sub")
        if not isinstance(subject, str) or not subject:
            raise IdentityRefused("id_token_missing_claim")

        # The *configured* issuer, not the claim. This is the half of the
        # Google quirk that would do real damage: `(issuer, subject)` is the
        # account's identity, so storing whichever spelling the token happened
        # to carry would give one person two accounts -- and the second one
        # would be empty, with their saved work apparently gone. One canonical
        # value per configured provider, decided here, once.
        return IdentityClaims(
            issuer=self.settings.issuer,
            subject=subject,
            email=storable_email(claims),
        )


def unavailable_algorithms() -> tuple[str, ...]:
    """Declared algorithms the installed PyJWT cannot actually perform.

    PyJWT implements the asymmetric algorithms only when ``cryptography`` is
    installed; without it they are simply absent from its registry. That is a
    packaging fault, not a token fault, and it is invisible at import: the
    process starts, serves everything else, and refuses every sign-in.
    """
    from jwt.algorithms import get_default_algorithms

    available = set(get_default_algorithms())
    return tuple(name for name in ID_TOKEN_ALGORITHMS if name not in available)


def _refuse_if_this_process_cannot_check_signatures() -> None:
    """Fail as a deployment fault rather than as a bad token.

    Without this the refusal is indistinguishable from a provider sending an
    algorithm we do not accept -- ``jwt.decode`` raises
    ``InvalidAlgorithmError`` either way -- so the log line would read
    ``id_token_algorithm`` and point an operator at the provider. The provider
    would be blameless, the token would be fine, and the real answer is that
    this process cannot verify an RS256 signature at all.

    Checked here rather than at import so a deployment that never turns
    identity on is not refused a startup over a dependency it does not use.
    """
    missing = unavailable_algorithms()
    if missing:
        raise IdentityRefused("verifier_cannot_check_signatures")


def storable_email(claims: Mapping[str, Any]) -> Optional[str]:
    """The email claim, but only when the provider marked it verified.

    ADR-0005 §1: "An unverified claim is discarded rather than stored, because
    an unverified address is an assertion about someone else's mailbox."

    Discarded, not refused: a visitor whose provider gives no verified address
    still gets an account. They simply have no contact address on file, which
    is the honest record of what the platform knows.

    ``email_verified`` is compared to ``True`` rather than read as truthy: the
    string ``"false"`` is truthy in Python, and some providers have sent that
    claim as a string.
    """
    email = claims.get("email")
    if not isinstance(email, str) or not email:
        return None
    verified = claims.get("email_verified")
    if verified is True or (isinstance(verified, str) and verified.lower() == "true"):
        return email
    return None


def log_refusal(reason: str) -> None:
    """One line, one word, no request content. See the module docstring."""
    logger.warning("sign-in refused: %s", reason)
