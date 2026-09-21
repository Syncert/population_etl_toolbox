"""A signed-in visitor, against the real schema and a faked provider.

Shared by every integration module that needs an authenticated self-service
account, because two of them do and a copied fixture is two fixtures that
drift. What it fakes is the provider's *network* and nothing else: the ID
tokens are really signed, really verified against a real key, and really
refused when they should be.

What none of it does is reach ``accounts.google.com``. A denial-path suite
that needs the internet is a denial-path suite that gets deleted.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.auth import get_app_session_dep
from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.oidc import OidcProvider, OidcSettings
from apps.api.routers.identity import (
    get_oidc_provider,
)
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_SQL = REPOSITORY_ROOT / "sql/bootstrap/002_app_api.sql"

ISSUER = "https://accounts.google.test"
CLIENT_ID = "integration-client-id"
REDIRECT = "https://app.example.test/auth/callback"

_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_PUBLIC_KEY = _PRIVATE_KEY.public_key()

_DISCOVERY = {
    "issuer": ISSUER,
    "authorization_endpoint": f"{ISSUER}/o/oauth2/v2/auth",
    "token_endpoint": f"{ISSUER}/token",
    "jwks_uri": f"{ISSUER}/certs",
}


class _Response:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(str(self.status_code))

    def json(self):
        return self._payload


class _ProviderNetwork:
    """The provider's HTTP surface, in process.

    ``id_token_for`` is set by each test to whatever the provider should hand
    back for the next exchange, so a test can make the provider return a token
    for a different person, an expired one, or one carrying somebody else's
    nonce, without any of the rest of the flow changing.
    """

    def __init__(self):
        self.next_id_token: str | None = None
        self.exchanges: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, url):
        return _Response(200, dict(_DISCOVERY))

    def post(self, url, data=None):
        self.exchanges.append(dict(data or {}))
        if self.next_id_token is None:
            return _Response(400, {"error": "invalid_grant"})
        return _Response(200, {"id_token": self.next_id_token})


class _Keys:
    def signing_key(self, id_token: str):
        return _PUBLIC_KEY


def id_token(
    *,
    nonce: str,
    subject: str = "subject-one",
    email: str | None = "reader@example.test",
    email_verified: bool = True,
    issuer: str = ISSUER,
    audience: str = CLIENT_ID,
    lifetime: int = 300,
) -> str:
    now = int(time.time())
    claims = {
        "iss": issuer,
        "aud": audience,
        "sub": subject,
        "iat": now,
        "exp": now + lifetime,
        "nonce": nonce,
    }
    if email is not None:
        claims["email"] = email
        claims["email_verified"] = email_verified
    return jwt.encode(claims, _PRIVATE_KEY, algorithm="RS256")


class SignInHarness:
    """A client, the provider's network, and the account labels to clean up."""

    def __init__(
        self,
        client: TestClient,
        network: _ProviderNetwork,
        connect,
        metric_code: str,
    ):
        self.client = client
        self.network = network
        self._connect = connect
        #: A metric the saved-analysis document contract can resolve, so the
        #: isolation test exercises a real stored document rather than a
        #: refusal that happens to also be a 404.
        self.metric_code = metric_code

    # -- the flow, in the shape a browser walks it -----------------------
    def start(self, redirect_uri: str = REDIRECT):
        return self.client.post(
            "/api/v1/auth/sign-in", json={"redirect_uri": redirect_uri}
        )

    def authorization_query(self, response) -> dict[str, list[str]]:
        url = response.json()["authorization_url"]
        return parse_qs(urlsplit(url).query)

    def sign_in(self, **token_kwargs):
        """Walk a whole sign-in and return the callback response."""
        started = self.start()
        assert started.status_code == 200, started.text
        query = self.authorization_query(started)
        self.network.next_id_token = id_token(nonce=query["nonce"][0], **token_kwargs)
        return self.client.post(
            "/api/v1/auth/callback",
            json={"code": "an-authorization-code", "state": query["state"][0]},
        )

    # -- looking at what the database actually holds ---------------------
    def query(self, sql: str, params: tuple = ()):
        database = self._connect()
        try:
            with database.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
        finally:
            database.close()

    def execute(self, sql: str, params: tuple = ()) -> int:
        database = self._connect()
        database.autocommit = True
        try:
            with database.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.rowcount
        finally:
            database.close()


@pytest.fixture
def sign_in(
    postgres_connection_factory: Callable[[], connection],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[SignInHarness]:
    metric_code = f"FRED:SESSION_{uuid4().hex[:10].upper()}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
            cursor.execute(
                """
                INSERT INTO gold_glossary.dim_metric_catalog (
                    metric_code, metric_display_name, source_code,
                    source_object_type, source_object_key,
                    valid_geo_grains, valid_time_grains
                ) VALUES (%s, 'Self-service session fixture', 'FRED', 'FRED_SERIES',
                          %s, ARRAY['NATIONAL'], ARRAY['MONTHLY'])
                """,
                (metric_code, metric_code.split(":", 1)[1]),
            )
        writer.commit()
    finally:
        writer.close()

    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    monkeypatch.setenv(
        "APP_API_DATABASE_URL",
        "postgresql+psycopg2://"
        f"{settings.user}:{settings.password}"
        f"@{settings.host}:{settings.port}/{settings.database}",
    )
    # `Secure` cookies are not stored by a client speaking plain HTTP, and the
    # test transport speaks plain HTTP. The flag itself is asserted on
    # elsewhere; here it would only hide every cookie under test.
    monkeypatch.setenv("API_COOKIE_SECURE", "0")
    monkeypatch.setenv("API_OIDC_REDIRECT_URIS", REDIRECT)
    from data_ingestion_toolbox.config import get_settings

    get_settings.cache_clear()

    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": settings.host,
            "port": settings.port,
            "user": settings.user,
            "password": settings.password,
            "dbname": settings.database,
        },
        pool_pre_ping=True,
    )

    def session() -> Iterator[Session]:
        with Session(engine) as active:
            yield active

    network = _ProviderNetwork()
    provider = OidcProvider(
        OidcSettings(
            issuer=ISSUER,
            client_id=CLIENT_ID,
            client_secret="integration-client-secret",
            redirect_uris=(REDIRECT,),
            clock_skew_seconds=60,
            transaction_ttl_seconds=600,
        ),
        client_factory=lambda: network,
        key_resolver=_Keys(),
    )

    app.dependency_overrides[get_app_session_dep] = session
    app.dependency_overrides[get_db_session_dep] = session
    app.dependency_overrides[get_oidc_provider] = lambda: provider
    try:
        yield SignInHarness(
            TestClient(app), network, postgres_connection_factory, metric_code
        )
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        cleanup.autocommit = True
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM app_api.user_account WHERE issuer = %s", (ISSUER,)
                )
                cursor.execute("DELETE FROM app_api.sign_in_transaction")
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE metric_code = %s",
                    (metric_code,),
                )
        finally:
            cleanup.close()
        get_settings.cache_clear()
