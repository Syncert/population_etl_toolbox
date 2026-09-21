"""Sign-in, sessions, rotation, and reuse detection (ADR-0005 §1-§4, API-151).

These run against the real ``app_api`` schema rather than a stand-in, and the
reason is specific to what is being tested. Rotation and reuse detection are
statements about *concurrent* rows, timestamps, and a uniqueness constraint;
an in-memory fake would be a second implementation of exactly the behaviour
under test, and it would agree with the first one by construction.

The harness -- the client, the faked provider network, and the signed ID
tokens -- is in ``tests/support/sign_in_harness.py``, and the ``sign_in``
fixture is registered in this package's ``conftest.py``.
"""

from __future__ import annotations

import pytest

from apps.api.routers.identity import REFRESH_COOKIE, REFRESH_PATH, TRANSACTION_COOKIE
from tests.support.sign_in_harness import SignInHarness, id_token

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]


# ---------------------------------------------------------------------------
# The path that must work
# ---------------------------------------------------------------------------


def test_a_visitor_signs_in_and_receives_a_session(sign_in: SignInHarness) -> None:
    """Covers: API-151."""
    response = sign_in.sign_in()
    assert response.status_code == 200, response.text

    body = response.json()
    assert body["token_type"] == "Bearer"
    assert body["access_token"]
    assert 0 < body["expires_in"] <= 900

    # The refresh token is a cookie and is not in the body. This is the whole
    # of ADR-0005 §2's change: script holds the short half, not the long one.
    assert "refresh" not in response.text.lower() or REFRESH_COOKIE not in body
    assert REFRESH_COOKIE in response.cookies
    assert body["access_token"] != response.cookies[REFRESH_COOKIE]

    # And the response was never cacheable.
    assert response.headers["cache-control"] == "private, no-store"


def test_the_session_authenticates_the_owner_scoped_routes(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- The point of the whole plan: a stranger's credential reaches the write
    paths that previously needed an operator to mint a token by hand."""
    token = sign_in.sign_in().json()["access_token"]
    listed = sign_in.client.get(
        "/api/v1/analysis-configurations",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert listed.status_code == 200
    assert listed.json()["items"] == []
    assert listed.headers["cache-control"] == "private, no-store"


def test_signing_in_twice_finds_the_same_account(sign_in: SignInHarness) -> None:
    """Covers: API-151 -- `(issuer, subject)` is the identity, so the second sign-in is a return
    rather than a registration."""
    first = sign_in.sign_in(subject="returning-visitor")
    second = sign_in.sign_in(subject="returning-visitor")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["access_token"] != second.json()["access_token"]

    rows = sign_in.query(
        "SELECT COUNT(*) FROM app_api.user_account WHERE subject = %s",
        ("returning-visitor",),
    )
    assert rows[0][0] == 1


def test_the_callback_does_not_say_whether_the_account_already_existed(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- ADR-0005 §2: "A response that distinguishes 'welcome back' from
    'welcome' at the API layer would be an oracle for whether a given person
    uses this site." """
    first = sign_in.sign_in(subject="oracle-probe")
    second = sign_in.sign_in(subject="oracle-probe")

    assert first.status_code == second.status_code
    assert sorted(first.json()) == sorted(second.json())
    # The only fields that differ are the credential and the moment it expires.
    differing = {key for key in first.json() if first.json()[key] != second.json()[key]}
    assert differing <= {"access_token", "expires_at", "expires_in"}


def test_two_different_subjects_are_two_different_accounts(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151."""
    one = sign_in.sign_in(subject="person-a", email="shared@example.test")
    two = sign_in.sign_in(subject="person-b", email="shared@example.test")

    assert one.status_code == 200
    assert two.status_code == 200
    # Deliberately the *same* verified address on both. ADR-0005 §1: the email
    # is contact information, never a key, and merging on it is the standard
    # shape of an account-takeover bug.
    rows = sign_in.query(
        "SELECT COUNT(*) FROM app_api.user_account WHERE email = %s",
        ("shared@example.test",),
    )
    assert rows[0][0] == 2


def test_an_unverified_address_is_not_stored(sign_in: SignInHarness) -> None:
    """Covers: API-151."""
    response = sign_in.sign_in(
        subject="unverified-person",
        email="unverified@example.test",
        email_verified=False,
    )
    assert response.status_code == 200

    rows = sign_in.query(
        "SELECT email FROM app_api.user_account WHERE subject = %s",
        ("unverified-person",),
    )
    assert rows[0][0] is None


def test_a_self_service_account_gets_no_public_name_and_an_opaque_label(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- ADR-0005 §3: a public name is absent until the account publishes, and
    `display_label` stays an operator label rather than becoming one."""
    sign_in.sign_in(subject="unnamed-person")
    rows = sign_in.query(
        "SELECT display_label, public_display_name FROM app_api.user_account "
        "WHERE subject = %s",
        ("unnamed-person",),
    )
    label, public_name = rows[0]
    assert public_name is None
    assert label.startswith("self-service:")
    assert "reader@example.test" not in label


# ---------------------------------------------------------------------------
# Refusals at the callback
# ---------------------------------------------------------------------------


def test_a_tampered_state_is_refused(sign_in: SignInHarness) -> None:
    """Covers: API-151 -- Without this the attacker completes a sign-in *as themselves* in the
    victim's browser, and the victim saves their work into it."""
    started = sign_in.start()
    query = sign_in.authorization_query(started)
    sign_in.network.next_id_token = id_token(nonce=query["nonce"][0])

    response = sign_in.client.post(
        "/api/v1/auth/callback",
        json={"code": "a-code", "state": query["state"][0] + "x"},
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "sign-in could not be completed"


def test_a_callback_with_no_transaction_cookie_is_refused(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151."""
    started = sign_in.start()
    query = sign_in.authorization_query(started)
    sign_in.network.next_id_token = id_token(nonce=query["nonce"][0])
    sign_in.client.cookies.delete(TRANSACTION_COOKIE)

    response = sign_in.client.post(
        "/api/v1/auth/callback",
        json={"code": "a-code", "state": query["state"][0]},
    )
    assert response.status_code == 401


def test_a_transaction_is_spent_once(sign_in: SignInHarness) -> None:
    """Covers: API-151 -- The claim is a ``DELETE ... RETURNING``, so a replayed callback finds
    nothing. Two concurrent callbacks cannot both win."""
    started = sign_in.start()
    query = sign_in.authorization_query(started)
    handle = sign_in.client.cookies[TRANSACTION_COOKIE]
    sign_in.network.next_id_token = id_token(nonce=query["nonce"][0])

    first = sign_in.client.post(
        "/api/v1/auth/callback",
        json={"code": "a-code", "state": query["state"][0]},
    )
    assert first.status_code == 200

    sign_in.network.next_id_token = id_token(nonce=query["nonce"][0])
    sign_in.client.cookies.set(TRANSACTION_COOKIE, handle)
    replayed = sign_in.client.post(
        "/api/v1/auth/callback",
        json={"code": "a-code", "state": query["state"][0]},
    )
    assert replayed.status_code == 401

    assert sign_in.query("SELECT COUNT(*) FROM app_api.sign_in_transaction")[0][0] == 0


def test_a_token_carrying_another_sign_ins_nonce_is_refused(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- The replay the nonce exists for, walked through the real routes."""
    other = sign_in.start()
    other_nonce = sign_in.authorization_query(other)["nonce"][0]

    started = sign_in.start()
    query = sign_in.authorization_query(started)
    sign_in.network.next_id_token = id_token(nonce=other_nonce)

    response = sign_in.client.post(
        "/api/v1/auth/callback",
        json={"code": "a-code", "state": query["state"][0]},
    )
    assert response.status_code == 401
    assert sign_in.query("SELECT COUNT(*) FROM app_api.user_account")[0][0] == 0


def test_an_unregistered_redirect_uri_never_starts_a_transaction(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151."""
    response = sign_in.start("https://attacker.test/callback")
    assert response.status_code == 401
    assert sign_in.query("SELECT COUNT(*) FROM app_api.sign_in_transaction")[0][0] == 0
    assert TRANSACTION_COOKIE not in response.cookies


def test_a_blocked_account_cannot_sign_in_again(sign_in: SignInHarness) -> None:
    """Covers: API-151 -- ADR-0005 §4's operator action, at the door rather than only inside."""
    assert sign_in.sign_in(subject="blocked-person").status_code == 200
    sign_in.execute(
        "UPDATE app_api.user_account SET blocked_at = NOW() WHERE subject = %s",
        ("blocked-person",),
    )
    assert sign_in.sign_in(subject="blocked-person").status_code == 401


def test_a_refused_exchange_creates_no_account(sign_in: SignInHarness) -> None:
    """Covers: API-151."""
    started = sign_in.start()
    query = sign_in.authorization_query(started)
    sign_in.network.next_id_token = None  # the provider refuses the exchange

    response = sign_in.client.post(
        "/api/v1/auth/callback",
        json={"code": "a-stale-code", "state": query["state"][0]},
    )
    assert response.status_code == 401
    assert sign_in.query("SELECT COUNT(*) FROM app_api.user_account")[0][0] == 0


# ---------------------------------------------------------------------------
# Rotation, reuse detection, and the two-tab race
# ---------------------------------------------------------------------------


def test_a_refresh_rotates_both_halves(sign_in: SignInHarness) -> None:
    """Covers: API-151."""
    first = sign_in.sign_in()
    first_access = first.json()["access_token"]
    first_refresh = sign_in.client.cookies[REFRESH_COOKIE]

    rotated = sign_in.client.post("/api/v1/auth/refresh")
    assert rotated.status_code == 200, rotated.text

    assert rotated.json()["access_token"] != first_access
    assert sign_in.client.cookies[REFRESH_COOKIE] != first_refresh

    # The old refresh token is spent, not merely superseded.
    from apps.api.services.identity_service import digest

    rows = sign_in.query(
        "SELECT revoked_at FROM app_api.account_credential WHERE token_sha256 = %s",
        (digest(first_refresh),),
    )
    assert rows[0][0] is not None


def test_the_previous_access_token_keeps_working_until_it_expires(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- Rotation replaces the refresh token. Cutting the access token at the
    same moment would break every request already in flight."""
    first = sign_in.sign_in().json()["access_token"]
    sign_in.client.post("/api/v1/auth/refresh")

    still_valid = sign_in.client.get(
        "/api/v1/analysis-configurations",
        headers={"Authorization": f"Bearer {first}"},
    )
    assert still_valid.status_code == 200


def test_a_reused_refresh_token_revokes_the_whole_family(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- ADR-0005 §2: "A second use of an already-spent token means it was
    captured, so the whole session family is revoked immediately."

    The grace window is set to zero for this test, because the alternative is
    a test that sleeps.
    """
    import apps.api.routers.identity as identity_router

    sign_in.sign_in()
    captured = sign_in.client.cookies[REFRESH_COOKIE]
    rotated = sign_in.client.post("/api/v1/auth/refresh")
    assert rotated.status_code == 200
    live_access = rotated.json()["access_token"]

    from apps.api.services.identity_service import SessionPolicy

    original = identity_router.session_policy
    identity_router.session_policy = lambda: SessionPolicy(grace_seconds=0)
    try:
        sign_in.client.cookies.set(REFRESH_COOKIE, captured)
        replayed = sign_in.client.post("/api/v1/auth/refresh")
    finally:
        identity_router.session_policy = original

    assert replayed.status_code == 401

    # The victim is signed out too. That is the intended outcome: one of the
    # two holders has a stolen credential and the server cannot tell which.
    after = sign_in.client.get(
        "/api/v1/analysis-configurations",
        headers={"Authorization": f"Bearer {live_access}"},
    )
    assert after.status_code == 401


def test_two_tabs_refreshing_at_once_does_not_revoke_anything(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- The race ADR-0005 §2 names, and the reason the grace window exists.

    Both tabs hold the same refresh token and both spend it within a second of
    each other. Treating the second as an attack signs a reader out for having
    two tabs open, which is a browser working normally.
    """
    sign_in.sign_in()
    shared = sign_in.client.cookies[REFRESH_COOKIE]

    first = sign_in.client.post("/api/v1/auth/refresh")
    assert first.status_code == 200

    sign_in.client.cookies.set(REFRESH_COOKIE, shared)
    second = sign_in.client.post("/api/v1/auth/refresh")
    assert second.status_code == 200, "the second tab was treated as an attacker"

    # Both tabs now hold working access tokens.
    for token in (first.json()["access_token"], second.json()["access_token"]):
        checked = sign_in.client.get(
            "/api/v1/analysis-configurations",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert checked.status_code == 200


def test_an_unknown_refresh_token_is_refused_and_the_cookie_is_cleared(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- A browser holding a token the server has revoked -- especially one whose
    family was just cut for reuse -- should stop presenting it rather than
    retry on every page load.

    Asserted on the ``Set-Cookie`` the server sent rather than on the client's
    jar: the jar's copy here was placed by the test with no path, so a
    correctly path-scoped expiry would not match it and the assertion would be
    about the test's own fixture.
    """
    sign_in.client.cookies.set(REFRESH_COOKIE, "a-token-nobody-ever-issued")
    response = sign_in.client.post("/api/v1/auth/refresh")
    assert response.status_code == 401

    instruction = response.headers.get("set-cookie", "")
    assert REFRESH_COOKIE in instruction
    assert "Max-Age=0" in instruction or "expires=Thu, 01 Jan 1970" in instruction
    assert REFRESH_PATH in instruction


def test_an_expired_refresh_token_is_refused(sign_in: SignInHarness) -> None:
    """Covers: API-151."""
    from apps.api.services.identity_service import digest

    sign_in.sign_in()
    held = sign_in.client.cookies[REFRESH_COOKIE]
    sign_in.execute(
        "UPDATE app_api.account_credential SET expires_at = NOW() - INTERVAL '1 second'"
        " WHERE token_sha256 = %s",
        (digest(held),),
    )
    assert sign_in.client.post("/api/v1/auth/refresh").status_code == 401


def test_a_session_past_its_absolute_ceiling_cannot_be_refreshed(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- Thirty days of inactivity is one bound; ninety days since sign-in is the
    other, and without it a session refreshed weekly never ends."""
    sign_in.sign_in()
    sign_in.execute(
        "UPDATE app_api.account_credential"
        " SET issued_at = NOW() - INTERVAL '100 days',"
        "     expires_at = NOW() + INTERVAL '30 days'"
        " WHERE session_family IS NOT NULL"
    )
    response = sign_in.client.post("/api/v1/auth/refresh")
    assert response.status_code == 401


def test_a_refresh_for_a_revoked_account_mints_nothing(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- An operator cutting an account must not be undone by a rotation that
    happens to arrive a second later."""
    sign_in.sign_in(subject="revoked-person")
    sign_in.execute(
        "UPDATE app_api.user_account SET revoked_at = NOW() WHERE subject = %s",
        ("revoked-person",),
    )
    assert sign_in.client.post("/api/v1/auth/refresh").status_code == 401


# ---------------------------------------------------------------------------
# Ending a session
# ---------------------------------------------------------------------------


def test_signing_out_ends_the_access_token_and_the_refresh_cookie(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- Revoking only the access token would leave the refresh cookie live and
    the next rotation would mint a new one: a sign-out that signs nobody out."""
    token = sign_in.sign_in().json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    assert (
        sign_in.client.post("/api/v1/auth/sign-out", headers=headers).status_code == 204
    )

    assert (
        sign_in.client.get(
            "/api/v1/analysis-configurations", headers=headers
        ).status_code
        == 401
    )
    assert sign_in.client.post("/api/v1/auth/refresh").status_code == 401


def test_signing_out_everywhere_ends_every_session(sign_in: SignInHarness) -> None:
    """Covers: API-151."""
    first = sign_in.sign_in(subject="many-devices").json()["access_token"]
    second = sign_in.sign_in(subject="many-devices").json()["access_token"]

    assert (
        sign_in.client.post(
            "/api/v1/auth/sign-out-everywhere",
            headers={"Authorization": f"Bearer {second}"},
        ).status_code
        == 204
    )

    for token in (first, second):
        assert (
            sign_in.client.get(
                "/api/v1/analysis-configurations",
                headers={"Authorization": f"Bearer {token}"},
            ).status_code
            == 401
        )


def test_signing_out_one_session_leaves_the_other_alone(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- Two devices are two families. "Sign out" is about this one."""
    first = sign_in.sign_in(subject="two-devices").json()["access_token"]
    second = sign_in.sign_in(subject="two-devices").json()["access_token"]

    sign_in.client.post(
        "/api/v1/auth/sign-out", headers={"Authorization": f"Bearer {second}"}
    )

    assert (
        sign_in.client.get(
            "/api/v1/analysis-configurations",
            headers={"Authorization": f"Bearer {first}"},
        ).status_code
        == 200
    )


# ---------------------------------------------------------------------------
# Isolation: the new credential must not widen anything
# ---------------------------------------------------------------------------


def test_one_accounts_work_is_invisible_to_another(sign_in: SignInHarness) -> None:
    """Covers: API-151 -- The denial path that matters most, under the new credential rather than
    under an operator token."""
    owner = sign_in.sign_in(subject="owner").json()["access_token"]
    created = sign_in.client.post(
        "/api/v1/analysis-configurations",
        headers={"Authorization": f"Bearer {owner}"},
        json={
            "name": "mine",
            "document": {
                "kind": "observations",
                "metric_code": sign_in.metric_code,
                "scope": "latest",
                "filters": {"geo_level": "NATIONAL"},
                "visualization": {"chart": "line"},
            },
        },
    )
    assert created.status_code == 201, created.text
    configuration_id = created.json()["configuration_id"]

    stranger = sign_in.sign_in(subject="stranger").json()["access_token"]
    seen = sign_in.client.get(
        f"/api/v1/analysis-configurations/{configuration_id}",
        headers={"Authorization": f"Bearer {stranger}"},
    )
    # 404, not 403: indistinguishable from an id that never existed, so ids
    # cannot be enumerated across accounts.
    assert seen.status_code == 404


def test_a_refresh_token_is_not_a_bearer_token(sign_in: SignInHarness) -> None:
    """Covers: API-151 -- It is an ambient cookie credential scoped to one path. Honouring it as
    a bearer token would undo the containment that makes a cookie acceptable
    at all."""
    sign_in.sign_in()
    refresh = sign_in.client.cookies[REFRESH_COOKIE]

    response = sign_in.client.get(
        "/api/v1/analysis-configurations",
        headers={"Authorization": f"Bearer {refresh}"},
    )
    assert response.status_code == 401


def test_the_transaction_handle_is_not_a_bearer_token(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151."""
    started = sign_in.start()
    assert started.status_code == 200
    handle = sign_in.client.cookies[TRANSACTION_COOKIE]

    response = sign_in.client.get(
        "/api/v1/analysis-configurations",
        headers={"Authorization": f"Bearer {handle}"},
    )
    assert response.status_code == 401


def test_no_credential_value_is_ever_stored_in_readable_form(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 -- Every row in the credential table is a digest. A leak of this database
    yields nothing presentable, which is ADR-0003's rule extended to a
    stranger's session."""
    body = sign_in.sign_in().json()
    access = body["access_token"]
    refresh = sign_in.client.cookies[REFRESH_COOKIE]

    stored = sign_in.query("SELECT token_sha256 FROM app_api.account_credential")
    values = {row[0] for row in stored}
    assert access not in values
    assert refresh not in values
    assert all(len(value) == 64 for value in values)
