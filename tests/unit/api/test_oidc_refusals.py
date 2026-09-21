"""Every way a sign-in must not complete (ADR-0005 §1, API-146).

ADR-0005 lists the denial paths and says what weight they carry:

    "Denial-path tests are the point of the plan, not a garnish: a tampered
    ``state``, a replayed ``nonce``, an unregistered redirect URI, an ID token
    with a bad signature or a wrong audience or an expired ``exp`` [...]"

So this module is organised by refusal rather than by function. Every test
builds a token that is valid in every respect but one, because a token that is
wrong in three ways proves only that *something* was checked.

The keys are generated here, in this process. A fixture holding a real
provider's key would make these tests depend on a key rotation nobody
controls, and one holding a checked-in private key would be a checked-in
private key.
"""

from __future__ import annotations

import base64
import json
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from apps.api.oidc import (
    ID_TOKEN_ALGORITHMS,
    IdentityRefused,
    IdentityUnconfigured,
    OidcProvider,
    OidcSettings,
    code_challenge_for,
    storable_email,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]

ISSUER = "https://accounts.google.com"
CLIENT_ID = "client-id.apps.googleusercontent.com"
REDIRECT = "https://example.test/auth/callback"
NONCE = "the-nonce-this-server-generated"
SUBJECT = "provider-subject-1234567890"

_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_PUBLIC_KEY = _PRIVATE_KEY.public_key()
_OTHER_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)

_DISCOVERY = {
    "issuer": ISSUER,
    "authorization_endpoint": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_endpoint": "https://oauth2.googleapis.com/token",
    "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
}


def _b64(raw: bytes) -> str:
    """URL-safe base64 with the padding stripped, as a JWT segment is."""
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


class _FixedKeyResolver:
    """Always the public key this module's private key signs with.

    Resolving by `kid` is PyJWKClient's job and is exercised against a real
    provider, not here. What these tests need is a key that is *correct*, so
    that a refusal proves the claim under test rather than a lookup failure.
    """

    def __init__(self, key=_PUBLIC_KEY):
        self.key = key

    def signing_key(self, id_token: str):
        return self.key


class _Response:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"status {self.status_code}")

    def json(self):
        return self._payload


class _FakeClient:
    """Answers discovery and the token endpoint, and records what it was sent."""

    def __init__(self, discovery=None, token_response=None):
        self.discovery = discovery if discovery is not None else dict(_DISCOVERY)
        self.token_response = token_response or _Response(200, {"id_token": "unused"})
        self.posted: list[tuple[str, dict]] = []
        self.get_calls: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, url):
        self.get_calls.append(url)
        return _Response(200, self.discovery)

    def post(self, url, data=None):
        self.posted.append((url, dict(data or {})))
        return self.token_response


def _settings(**overrides) -> OidcSettings:
    base = dict(
        issuer=ISSUER,
        client_id=CLIENT_ID,
        client_secret="client-secret-value",
        redirect_uris=(REDIRECT,),
        clock_skew_seconds=60,
        transaction_ttl_seconds=600,
    )
    base.update(overrides)
    return OidcSettings(**base)


def _provider(settings=None, client=None, key=_PUBLIC_KEY) -> OidcProvider:
    fake = client or _FakeClient()
    provider = OidcProvider(
        settings or _settings(),
        client_factory=lambda: fake,
        key_resolver=_FixedKeyResolver(key),
    )
    provider.fake_client = fake  # type: ignore[attr-defined]
    return provider


def _claims(**overrides) -> dict:
    now = int(time.time())
    claims = {
        "iss": ISSUER,
        "aud": CLIENT_ID,
        "sub": SUBJECT,
        "iat": now,
        "exp": now + 300,
        "nonce": NONCE,
        "email": "reader@example.test",
        "email_verified": True,
    }
    claims.update(overrides)
    return {key: value for key, value in claims.items() if value is not _ABSENT}


_ABSENT = object()


def _token(private_key=_PRIVATE_KEY, algorithm="RS256", **overrides) -> str:
    return jwt.encode(_claims(**overrides), private_key, algorithm=algorithm)


# -- the path that must work ------------------------------------------------


def test_a_well_formed_id_token_yields_exactly_three_claims() -> None:
    """Covers: API-150 -- The happy path, stated so the refusals below mean something.

    It also holds the ADR's "nothing else" rule: the token carries a name and
    a picture, as a real Google token does, and neither survives into the
    identity this platform stores.
    """
    provider = _provider()
    identity = provider.verify_id_token(
        _token(name="Ada Lovelace", picture="https://example.test/a.png"),
        nonce=NONCE,
    )

    assert identity.issuer == ISSUER
    assert identity.subject == SUBJECT
    assert identity.email == "reader@example.test"
    assert set(vars(identity)) == {"issuer", "subject", "email"}


# -- signature and algorithm ------------------------------------------------


def test_a_token_signed_by_another_key_is_refused() -> None:
    """Covers: API-150."""
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(_token(private_key=_OTHER_PRIVATE_KEY), nonce=NONCE)
    assert refusal.value.reason == "id_token_signature"


def test_a_tampered_payload_is_refused() -> None:
    """Covers: API-150 -- The signature covers the payload; editing one claim breaks it."""
    header, payload, signature = _token().split(".")
    decoded = json.loads(base64.urlsafe_b64decode(payload + "=="))
    decoded["sub"] = "somebody-else"
    forged = _b64(json.dumps(decoded).encode())
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(f"{header}.{forged}.{signature}", nonce=NONCE)
    assert refusal.value.reason == "id_token_signature"


def test_an_unsigned_token_is_refused() -> None:
    """Covers: API-150 -- ``alg: none`` is the oldest JWT attack and is still worth a test."""
    header = _b64(json.dumps({"alg": "none", "typ": "JWT"}).encode())
    payload = _b64(json.dumps(_claims()).encode())
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(f"{header}.{payload}.", nonce=NONCE)
    assert refusal.value.reason in {"id_token_algorithm", "id_token_undecodable"}


def test_a_token_signed_with_the_public_key_as_an_hmac_secret_is_refused() -> None:
    """Covers: API-150 -- Algorithm confusion: the attacker has the public key, because it is
    public. If the verifier honours the token's own ``alg``, an ``HS256``
    token signed with that key verifies."""
    import hmac

    from cryptography.hazmat.primitives import serialization

    public_pem = _PUBLIC_KEY.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    # Assembled by hand, because PyJWT refuses to *encode* this: it guards the
    # signing side against the same confusion. An attacker is not using PyJWT.
    header = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64(json.dumps(_claims()).encode())
    signing_input = f"{header}.{payload}".encode("ascii")
    signature = _b64(hmac.new(public_pem, signing_input, "sha256").digest())
    forged = f"{header}.{payload}.{signature}"

    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(forged, nonce=NONCE)
    assert refusal.value.reason in {"id_token_algorithm", "id_token_undecodable"}


def test_the_declared_algorithms_are_all_asymmetric() -> None:
    """Covers: API-150 -- A future edit adding ``HS256`` here would silently undo the test above."""
    assert all(name.startswith(("RS", "ES", "PS")) for name in ID_TOKEN_ALGORITHMS), (
        ID_TOKEN_ALGORITHMS
    )


# -- audience, issuer, expiry -----------------------------------------------


def test_a_token_minted_for_another_client_is_refused() -> None:
    """Covers: API-150 -- A token for a different `aud` is a real token -- for somebody else's
    application. Accepting it lets any site the reader signs into hand us a
    token that authenticates them here."""
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(_token(aud="another-client-id"), nonce=NONCE)
    assert refusal.value.reason == "id_token_audience"


def test_a_token_from_another_issuer_is_refused() -> None:
    """Covers: API-150."""
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(_token(iss="https://evil.test"), nonce=NONCE)
    assert refusal.value.reason == "id_token_issuer"


def test_an_expired_token_is_refused() -> None:
    """Covers: API-150."""
    now = int(time.time())
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(_token(iat=now - 3600, exp=now - 3000), nonce=NONCE)
    assert refusal.value.reason == "id_token_expired"


def test_the_clock_skew_window_is_bounded_and_is_honoured() -> None:
    """Covers: API-150 -- Both halves. A token that expired ten seconds ago is accepted under a
    sixty-second skew, because clocks differ; one that expired ten minutes ago
    is not, because that is not a clock difference."""
    now = int(time.time())
    provider = _provider(_settings(clock_skew_seconds=60))

    identity = provider.verify_id_token(
        _token(iat=now - 100, exp=now - 10), nonce=NONCE
    )
    assert identity.subject == SUBJECT

    with pytest.raises(IdentityRefused):
        provider.verify_id_token(_token(iat=now - 900, exp=now - 600), nonce=NONCE)


def test_a_token_missing_a_required_claim_is_refused() -> None:
    """Covers: API-150."""
    provider = _provider()
    for missing in ("sub", "exp", "iat"):
        with pytest.raises(IdentityRefused) as refusal:
            provider.verify_id_token(_token(**{missing: _ABSENT}), nonce=NONCE)
        assert refusal.value.reason in {
            "id_token_missing_claim",
            "id_token_invalid",
        }, missing


def test_an_empty_subject_is_refused_rather_than_becoming_an_account() -> None:
    """Covers: API-150 -- `(issuer, subject)` is the account's identity. An empty subject would
    make one account for everyone the provider failed to identify."""
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(_token(sub=""), nonce=NONCE)
    assert refusal.value.reason == "id_token_missing_claim"


# -- the nonce --------------------------------------------------------------


def test_a_token_carrying_another_sign_ins_nonce_is_refused() -> None:
    """Covers: API-150 -- Replay. The token is valid in every JWT sense -- right issuer, right
    audience, unexpired, correctly signed -- and belongs to a different
    sign-in."""
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(
            _token(nonce="a-nonce-from-another-sign-in"), nonce=NONCE
        )
    assert refusal.value.reason == "id_token_nonce"


def test_a_token_carrying_no_nonce_at_all_is_refused() -> None:
    """Covers: API-150."""
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(_token(nonce=_ABSENT), nonce=NONCE)
    assert refusal.value.reason == "id_token_nonce"


# -- the email claim --------------------------------------------------------


@pytest.mark.parametrize(
    "verified",
    [False, "false", "False", None, 0, "", _ABSENT],
    ids=[
        "boolean-false",
        "string-false",
        "string-False",
        "null",
        "zero",
        "empty-string",
        "claim-absent",
    ],
)
def test_an_unverified_address_is_discarded_and_the_sign_in_still_completes(
    verified,
) -> None:
    """Covers: API-150 -- ADR-0005 §1. Discarded, not refused: a visitor whose provider gives no
    verified address still gets an account, with no contact address on file.

    ``"false"`` is in this list because it is truthy in Python and some
    providers have sent the claim as a string.
    """
    provider = _provider()
    identity = provider.verify_id_token(_token(email_verified=verified), nonce=NONCE)
    assert identity.subject == SUBJECT
    assert identity.email is None


def test_a_verified_address_is_stored() -> None:
    """Covers: API-150."""
    assert storable_email({"email": "a@b.test", "email_verified": True}) == "a@b.test"
    assert storable_email({"email": "a@b.test", "email_verified": "true"}) == "a@b.test"
    assert storable_email({"email_verified": True}) is None


# -- the redirect allowlist -------------------------------------------------


def test_an_unregistered_redirect_uri_cannot_start_a_sign_in() -> None:
    """Covers: API-150."""
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.start("https://attacker.test/callback")
    assert refusal.value.reason == "redirect_uri_not_allowed"


@pytest.mark.parametrize(
    "candidate",
    [
        "https://example.test/auth/callback/",
        "https://example.test/auth/callback?x=1",
        "https://example.test/auth/callback#x",
        "http://example.test/auth/callback",
        "https://example.test/auth/callback/../../evil",
        "https://example.test.attacker.test/auth/callback",
    ],
    ids=[
        "trailing-slash",
        "extra-query",
        "fragment",
        "downgraded-scheme",
        "traversal",
        "suffix-domain",
    ],
)
def test_the_allowlist_is_exact_and_not_nearly_exact(candidate: str) -> None:
    """Covers: API-150 -- Each of these is a near-match that a prefix, origin, or normalising
    comparison would accept, and each is a way to have the provider deliver
    somebody else's authorization code somewhere else."""
    provider = _provider()
    with pytest.raises(IdentityRefused):
        provider.start(candidate)


def test_the_code_exchange_re_checks_the_redirect_uri() -> None:
    """Covers: API-150 -- The check at `start` is not sufficient on its own: the exchange is a
    separate request and the value it sends is the one the provider matched."""
    provider = _provider()
    with pytest.raises(IdentityRefused) as refusal:
        provider.exchange_code(
            code="c", redirect_uri="https://attacker.test/cb", code_verifier="v"
        )
    assert refusal.value.reason == "redirect_uri_not_allowed"


# -- what `start` asks the provider for -------------------------------------


def test_start_uses_s256_pkce_and_asks_for_no_profile() -> None:
    """Covers: API-150."""
    from urllib.parse import parse_qs, urlparse

    provider = _provider()
    request = provider.start(REDIRECT)
    query = parse_qs(urlparse(request.authorization_url).query)

    assert query["response_type"] == ["code"]
    assert query["code_challenge_method"] == ["S256"]
    assert query["code_challenge"] == [code_challenge_for(request.code_verifier)]
    assert query["state"] == [request.state]
    assert query["nonce"] == [request.nonce]
    # `profile` would be consent to read a display name and a picture this
    # platform has decided not to keep.
    assert query["scope"] == ["openid email"]
    assert "profile" not in request.authorization_url


def test_state_nonce_and_verifier_are_independent_and_unguessable() -> None:
    """Covers: API-150."""
    provider = _provider()
    first = provider.start(REDIRECT)
    second = provider.start(REDIRECT)

    values = {
        first.state,
        first.nonce,
        first.code_verifier,
        second.state,
        second.nonce,
        second.code_verifier,
    }
    assert len(values) == 6, "a repeated value means one is derived from another"
    for value in values:
        assert len(value) >= 32


# -- discovery --------------------------------------------------------------


def test_a_discovery_document_naming_another_issuer_is_refused() -> None:
    """Covers: API-150."""
    client = _FakeClient(discovery={**_DISCOVERY, "issuer": "https://evil.test"})
    provider = _provider(client=client)
    with pytest.raises(IdentityRefused) as refusal:
        provider.start(REDIRECT)
    assert refusal.value.reason == "provider_issuer_mismatch"


def test_an_incomplete_discovery_document_is_refused() -> None:
    """Covers: API-150."""
    incomplete = {key: value for key, value in _DISCOVERY.items() if key != "jwks_uri"}
    provider = _provider(client=_FakeClient(discovery=incomplete))
    with pytest.raises(IdentityRefused) as refusal:
        provider.start(REDIRECT)
    assert refusal.value.reason == "provider_discovery_incomplete"


def test_discovery_is_fetched_once_and_reused() -> None:
    """Covers: API-150 -- A provider outage must not be a dependency of every single sign-in."""
    provider = _provider()
    provider.start(REDIRECT)
    provider.start(REDIRECT)
    provider.start(REDIRECT)
    assert len(provider.fake_client.get_calls) == 1


# -- the code exchange ------------------------------------------------------


def test_the_exchange_sends_the_verifier_and_keeps_no_provider_access_token() -> None:
    """Covers: API-150."""
    client = _FakeClient(
        token_response=_Response(
            200,
            {
                "id_token": "the-id-token",
                "access_token": "a-google-access-token",
                "refresh_token": "a-google-refresh-token",
            },
        )
    )
    provider = _provider(client=client)
    returned = provider.exchange_code(
        code="the-code", redirect_uri=REDIRECT, code_verifier="the-verifier"
    )

    assert returned == "the-id-token"
    _, form = client.posted[0]
    assert form["grant_type"] == "authorization_code"
    assert form["code_verifier"] == "the-verifier"
    assert form["redirect_uri"] == REDIRECT
    # This platform calls no provider API on a reader's behalf. Keeping a
    # credential that would let it is holding something it has no use for.
    assert "a-google-access-token" not in returned


def test_a_refused_exchange_does_not_propagate_the_providers_error_body() -> None:
    """Covers: API-150 -- The provider's error body can carry the code and the client id."""
    client = _FakeClient(
        token_response=_Response(
            400,
            {
                "error": "invalid_grant",
                "error_description": "code 4/0AX4 was already redeemed",
            },
        )
    )
    provider = _provider(client=client)
    with pytest.raises(IdentityRefused) as refusal:
        provider.exchange_code(code="4/0AX4", redirect_uri=REDIRECT, code_verifier="v")
    assert refusal.value.reason == "token_exchange_refused"
    assert "4/0AX4" not in str(refusal.value)
    assert "invalid_grant" not in str(refusal.value)


def test_a_token_response_without_an_id_token_is_refused() -> None:
    """Covers: API-150 -- A bare OAuth 2.0 response. ADR-0005 rules GitHub out for exactly this:
    it issues no ID token, so there would be nothing to verify."""
    provider = _provider(
        client=_FakeClient(
            token_response=_Response(200, {"access_token": "only-an-access-token"})
        )
    )
    with pytest.raises(IdentityRefused) as refusal:
        provider.exchange_code(code="c", redirect_uri=REDIRECT, code_verifier="v")
    assert refusal.value.reason == "token_response_carried_no_id_token"


# -- the unconfigured deployment --------------------------------------------


@pytest.mark.parametrize(
    "missing",
    ["client_id", "client_secret", "issuer"],
)
def test_a_deployment_with_no_registered_client_refuses_before_any_network_call(
    missing: str,
) -> None:
    """Covers: API-150 -- Unconfigured is not the same as refused, and answers differently.

    It also must not reach the provider: a deployment with no client id has
    nothing to ask, and a discovery fetch on every attempt would be an
    outbound request an operator never configured.
    """
    provider = _provider(_settings(**{missing: ""}))
    with pytest.raises(IdentityUnconfigured):
        provider.start(REDIRECT)
    assert provider.fake_client.get_calls == []
