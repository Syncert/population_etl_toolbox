"""The provider-facing HTTP a deployment actually performs (ADR-0005 §1, API-150).

Every other test of this flow injects a `key_resolver`, which is right for
them: they are about what happens once the key is known, and a test of a
tampered signature should not also depend on a key fetch. The consequence is
that `OidcProvider.resolve_key`'s *other* branch — the one that runs in
production, builds a `PyJWKClient`, fetches the provider's JWKS and picks a key
by `kid` — was exercised by nothing at all.

That is the branch a first real sign-in runs, and `exchange_code`'s outbound
`POST` is in the same position: every other test replaces the client, so the
form encoding, the content type and the response handling that a real provider
meets are exercised nowhere.

So this module serves a real discovery document, a real JWKS and a real token
endpoint over a real socket, and lets the real client talk to them, end to end.
That includes the `kid` selection that decides which of a provider's several
keys signed a token -- providers publish more than one and rotate them, and
picking the wrong one is a refused sign-in that looks exactly like a bad
signature.

It binds loopback on an ephemeral port and reaches nothing beyond it. A test
of key resolution that needed `accounts.google.com` would fail on Google's
rotation schedule rather than on this repository's defects.

**Why it is in the integration tier rather than the unit one**, despite needing
no database: `tests/conftest.py` blocks every real socket for a test marked
`unit`, including loopback, and that guard is correct. A unit test that stands
up a server is not a unit test, and the tier's whole value is that nothing in
it can quietly start depending on something being up. This needs a real HTTP
round trip precisely because the code under test is the one that makes one.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from apps.api.oidc import IdentityRefused, OidcProvider, OidcSettings

#: No `database` marker: this needs a socket, not a warehouse.
pytestmark = [pytest.mark.integration, pytest.mark.api]

CLIENT_ID = "jwks-client-id"
REDIRECT = "https://example.test/auth/callback"
NONCE = "the-nonce-this-server-generated"
SUBJECT = "jwks-subject"

#: Two keys, because one proves nothing about `kid` selection. A provider
#: publishes several and rotates them; a client that ignored `kid` and took the
#: first would work until the day it did not.
_SIGNING_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)

_SIGNING_KID = "the-key-that-signed-it"
_OTHER_KID = "a-key-that-did-not"


def _jwk(private_key, kid: str) -> dict:
    document = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
    document.update({"kid": kid, "use": "sig", "alg": "RS256"})
    return document


class _ProviderServer:
    """A provider's public documents, on loopback."""

    def __init__(self) -> None:
        self.jwks_requests = 0
        self.exchanges: list[dict] = []
        self.exchange_content_types: list[str] = []
        self.token_response_status = 200
        self.token_response_body: dict = {"id_token": "replaced-per-test"}
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):  # noqa: D102 - quiet in test output
                return

            def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler's contract
                if self.path != "/token":
                    self.send_response(404)
                    self.end_headers()
                    return
                length = int(self.headers.get("content-length") or 0)
                raw = self.rfile.read(length).decode("utf-8")
                outer.exchanges.append(
                    {
                        key: values[0]
                        for key, values in parse_qs(raw, keep_blank_values=True).items()
                    }
                )
                outer.exchange_content_types.append(
                    self.headers.get("content-type", "")
                )
                if outer.token_response_status != 200:
                    body = {"error": "invalid_grant", "error_description": raw}
                else:
                    body = dict(outer.token_response_body)
                payload = json.dumps(body).encode("utf-8")
                self.send_response(outer.token_response_status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler's contract
                if self.path == "/.well-known/openid-configuration":
                    body = {
                        "issuer": outer.issuer,
                        "authorization_endpoint": f"{outer.issuer}/authorize",
                        "token_endpoint": f"{outer.issuer}/token",
                        "jwks_uri": f"{outer.issuer}/certs",
                    }
                elif self.path == "/certs":
                    outer.jwks_requests += 1
                    # The signing key second, so a client that took the first
                    # key rather than reading `kid` fails here.
                    body = {
                        "keys": [
                            _jwk(_OTHER_KEY, _OTHER_KID),
                            _jwk(_SIGNING_KEY, _SIGNING_KID),
                        ]
                    }
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                payload = json.dumps(body).encode("utf-8")
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self.issuer = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> "_ProviderServer":
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> bool:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
        return False


@pytest.fixture
def provider_server():
    with _ProviderServer() as server:
        yield server


def _provider(server: _ProviderServer) -> OidcProvider:
    """A provider with **no** injected key resolver. That is the whole point."""
    return OidcProvider(
        OidcSettings(
            issuer=server.issuer,
            client_id=CLIENT_ID,
            client_secret="a-secret",
            redirect_uris=(REDIRECT,),
            clock_skew_seconds=60,
            transaction_ttl_seconds=600,
        )
    )


def _token(*, private_key=_SIGNING_KEY, kid: str = _SIGNING_KID, issuer=None, **claims):
    now = int(time.time())
    payload = {
        "iss": issuer,
        "aud": CLIENT_ID,
        "sub": SUBJECT,
        "iat": now,
        "exp": now + 300,
        "nonce": NONCE,
        "email": "reader@example.test",
        "email_verified": True,
    }
    payload.update(claims)
    return jwt.encode(payload, private_key, algorithm="RS256", headers={"kid": kid})


def test_the_signing_key_is_fetched_from_the_published_jwks_and_chosen_by_kid(
    provider_server: _ProviderServer,
) -> None:
    """Covers: API-150 — the branch a deployment runs, end to end.

    Discovery, the JWKS fetch, and the `kid` selection, with no resolver
    injected anywhere. The signing key is published second, so a client that
    took the first key rather than reading `kid` fails.
    """
    provider = _provider(provider_server)
    identity = provider.verify_id_token(
        _token(issuer=provider_server.issuer), nonce=NONCE
    )

    assert identity.subject == SUBJECT
    assert identity.issuer == provider_server.issuer
    assert provider_server.jwks_requests >= 1


def test_a_token_signed_by_a_key_the_provider_does_not_publish_is_refused(
    provider_server: _ProviderServer,
) -> None:
    """Covers: API-150 — the JWKS is the authority, not the token's own header.

    The token names a `kid` the provider really publishes and is signed with a
    key it does not. Resolution succeeds and verification must not.
    """
    provider = _provider(provider_server)
    forged = _token(
        private_key=_OTHER_KEY, kid=_SIGNING_KID, issuer=provider_server.issuer
    )
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(forged, nonce=NONCE)
    assert refusal.value.reason == "id_token_signature"


def test_a_token_naming_an_unpublished_kid_is_refused(
    provider_server: _ProviderServer,
) -> None:
    """Covers: API-150 — an unknown `kid` is a refusal, not a crash.

    `PyJWKClient` raises its own exception type here, which is not a
    `jwt.InvalidTokenError`, so without the wrapping in `resolve_key` this
    would escape as a 500 rather than as a refused sign-in.
    """
    provider = _provider(provider_server)
    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(
            _token(kid="a-kid-nobody-published", issuer=provider_server.issuer),
            nonce=NONCE,
        )
    assert refusal.value.reason == "signing_key_unavailable"


def test_a_provider_whose_jwks_cannot_be_reached_is_a_refusal_not_a_crash() -> None:
    """Covers: API-150 — an outage at the provider refuses a sign-in.

    The server is started and stopped, so the endpoints are gone while the
    issuer and the discovery document that named them are exactly what a
    reachable provider would have served.
    """
    with _ProviderServer() as server:
        provider = _provider(server)
        # Warm the discovery cache while it is reachable, so what fails below
        # is the key fetch specifically rather than discovery.
        provider.metadata()
        token = _token(issuer=server.issuer)

    with pytest.raises(IdentityRefused) as refusal:
        provider.verify_id_token(token, nonce=NONCE)
    assert refusal.value.reason == "signing_key_unavailable"


def test_a_provider_that_is_down_at_discovery_is_a_refusal_not_a_crash() -> None:
    """Covers: API-150 — the same, one step earlier."""
    with _ProviderServer() as server:
        issuer = server.issuer
        provider = _provider(server)

    with pytest.raises(IdentityRefused) as refusal:
        provider.start(REDIRECT)
    assert refusal.value.reason == "provider_discovery_unavailable"
    assert issuer.startswith("http://127.0.0.1:")


# ---------------------------------------------------------------------------
# The code exchange, over the same real socket
# ---------------------------------------------------------------------------


def test_the_exchange_posts_a_form_the_way_a_provider_expects_it(
    provider_server: _ProviderServer,
) -> None:
    """Covers: API-150 — the outbound `POST`, unfaked.

    Google's discovery document lists `client_secret_post` among its
    `token_endpoint_auth_methods_supported`, which is the method this sends:
    credentials in the form body rather than in a `Basic` header. That is a
    claim about a wire format, and every other test of `exchange_code`
    substitutes the client, so nothing checked that what goes out is a
    URL-encoded form at all.
    """
    provider_server.token_response_body = {
        "id_token": _token(issuer=provider_server.issuer),
        "access_token": "the-provider's-own-token",
        "token_type": "Bearer",
    }
    provider = _provider(provider_server)

    returned = provider.exchange_code(
        code="4/a-real-code", redirect_uri=REDIRECT, code_verifier="the-verifier"
    )

    assert returned == provider_server.token_response_body["id_token"]
    assert provider_server.exchange_content_types[0].startswith(
        "application/x-www-form-urlencoded"
    )
    sent = provider_server.exchanges[0]
    assert sent == {
        "grant_type": "authorization_code",
        "code": "4/a-real-code",
        "redirect_uri": REDIRECT,
        "client_id": CLIENT_ID,
        "client_secret": "a-secret",
        "code_verifier": "the-verifier",
    }


def test_the_exchanged_token_verifies_against_the_published_keys(
    provider_server: _ProviderServer,
) -> None:
    """Covers: API-150 — exchange and verification, one continuous path.

    The two halves are tested separately everywhere else. This is the only
    place the token that comes back over the wire is the token that gets
    verified, which is what a sign-in actually does.
    """
    provider_server.token_response_body = {
        "id_token": _token(issuer=provider_server.issuer)
    }
    provider = _provider(provider_server)

    id_token = provider.exchange_code(
        code="c", redirect_uri=REDIRECT, code_verifier="v"
    )
    identity = provider.verify_id_token(id_token, nonce=NONCE)

    assert identity.subject == SUBJECT
    assert identity.email == "reader@example.test"


def test_a_provider_refusing_the_exchange_leaks_none_of_its_reply(
    provider_server: _ProviderServer,
) -> None:
    """Covers: API-150 — over the wire, not against a stand-in.

    The stub echoes the whole form back inside `error_description`, which is
    the worst case a real provider could produce: the authorization code and
    the client secret, in a body this code must not propagate.
    """
    provider_server.token_response_status = 400
    provider = _provider(provider_server)

    with pytest.raises(IdentityRefused) as refusal:
        provider.exchange_code(
            code="4/the-code", redirect_uri=REDIRECT, code_verifier="v"
        )

    assert refusal.value.reason == "token_exchange_refused"
    rendered = repr(refusal.value) + str(refusal.value)
    assert "4/the-code" not in rendered
    assert "a-secret" not in rendered


def test_a_token_endpoint_that_answers_no_id_token_is_refused(
    provider_server: _ProviderServer,
) -> None:
    """Covers: API-150 — a bare OAuth 2.0 answer, over the wire.

    ADR-0005 rules GitHub out for exactly this shape: an access token and no
    ID token, so there would be nothing to verify.
    """
    provider_server.token_response_body = {"access_token": "only-this"}
    provider = _provider(provider_server)

    with pytest.raises(IdentityRefused) as refusal:
        provider.exchange_code(code="c", redirect_uri=REDIRECT, code_verifier="v")
    assert refusal.value.reason == "token_response_carried_no_id_token"
