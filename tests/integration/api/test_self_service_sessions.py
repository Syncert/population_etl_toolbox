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

from apps.api.session_cookies import REFRESH_COOKIE, REFRESH_PATH, TRANSACTION_COOKIE
from tests.support.sign_in_harness import ISSUER, SignInHarness, id_token

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


# ---------------------------------------------------------------------------
# What a review found, and what now holds it
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fetch_site",
    ["same-site", "cross-site", "none"],
    ids=["a-sibling-subdomain", "another-site", "a-typed-url"],
)
def test_only_a_same_origin_request_may_spend_the_refresh_cookie(
    sign_in: SignInHarness, fetch_site: str
) -> None:
    """Covers: API-151 — ADR-0005 §2 asks for a check "refusing anything not
    same-origin", and the word is load-bearing.

    `same-site` is the one worth a test of its own. `SameSite=Strict` keeps the
    cookie away from other *sites*, not from other origins on the same site: a
    deployment at `app.example.com` shares a site with anything else under
    `example.com`, and the browser attaches the refresh cookie to a request
    from there. Accepting it would make every subdomain a deployment has -- or
    ever loses control of -- able to rotate somebody's session.
    """
    sign_in.sign_in()
    assert sign_in.client.post("/api/v1/auth/refresh").status_code == 200

    refused = sign_in.client.post(
        "/api/v1/auth/refresh", headers={"Sec-Fetch-Site": fetch_site}
    )
    assert refused.status_code == 401


def test_a_same_origin_request_is_still_allowed_with_the_header_present(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 — the tightening must not refuse the real browser.

    A page's own `fetch` to its own origin sends exactly this, so a rule that
    refused it would break every sign-in rather than any attack.
    """
    sign_in.sign_in()
    allowed = sign_in.client.post(
        "/api/v1/auth/refresh", headers={"Sec-Fetch-Site": "same-origin"}
    )
    assert allowed.status_code == 200


def test_a_cross_site_refusal_does_not_clear_the_readers_cookie(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 — otherwise refusing the attack becomes the attack.

    A cross-site request is somebody else's page speaking. Answering it with an
    instruction that expires the reader's live session would turn the CSRF
    defence into a way to sign people out from anywhere.
    """
    sign_in.sign_in()
    refused = sign_in.client.post(
        "/api/v1/auth/refresh", headers={"Sec-Fetch-Site": "cross-site"}
    )
    assert refused.status_code == 401
    assert "set-cookie" not in {key.lower() for key in refused.headers}

    # And the session is untouched.
    assert sign_in.client.post("/api/v1/auth/refresh").status_code == 200


def test_signing_out_does_not_destroy_an_operator_token(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 — ADR-0005 §6 promises operator tokens "continue to work
    unchanged", and a public route that revokes one on a single click would be
    a widening the ADR did not authorise.

    It is unrecoverable, too: only a privileged script can reissue one.
    """
    from tests.support.app_accounts import create_account

    operator_token = "an-operator-token-that-must-survive-sign-out"
    database = sign_in._connect()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            cursor.execute(
                "DELETE FROM app_api.user_account WHERE display_label = %s",
                ("sign-out-operator",),
            )
            create_account(cursor, "sign-out-operator", operator_token)
    finally:
        database.close()

    headers = {"Authorization": f"Bearer {operator_token}"}
    try:
        # Answered the same way a real sign-out is: from the caller's side,
        # "your session ended" and "you had no session" are one outcome.
        assert (
            sign_in.client.post("/api/v1/auth/sign-out", headers=headers).status_code
            == 204
        )
        assert (
            sign_in.client.get(
                "/api/v1/analysis-configurations", headers=headers
            ).status_code
            == 200
        ), "the operator token was revoked by a sign-out"
    finally:
        cleanup = sign_in._connect()
        cleanup.autocommit = True
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM app_api.user_account WHERE display_label = %s",
                    ("sign-out-operator",),
                )
        finally:
            cleanup.close()


def test_two_callbacks_racing_one_identity_produce_one_account(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 — the same defect API-148 fixed for saved analyses, one
    level up.

    Two callbacks for an identity that does not exist yet both look, neither
    finds, and both insert; the partial unique index on `(issuer, subject)`
    decides. Before this, the loser's `IntegrityError` reached the router as a
    `SQLAlchemyError` and answered the sanitized 503 -- telling somebody
    signing in for the very first time that the database was unavailable, when
    what actually happened is that their account was created.

    Two threads at a barrier, the way `test_name_collision_race.py` drives the
    same shape of race. Both are held until each has opened a transaction, so
    neither can win by arriving first: they are both inside the window the
    check-then-insert leaves open. Sequencing them instead would prove nothing
    -- the second would simply find the first's committed row, which is the
    ordinary path this test is not about.
    """
    import threading

    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session as SqlSession

    from apps.api.oidc import IdentityClaims
    from apps.api.services.identity_service import _resolve_account
    from tests.support.postgres import PostgresTestConfig

    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": settings.host,
            "port": settings.port,
            "user": settings.user,
            "password": settings.password,
            "dbname": settings.database,
        },
        pool_size=4,
    )
    claims = IdentityClaims(
        issuer=ISSUER, subject="raced-identity", email="raced@example.test"
    )

    ready = threading.Barrier(2, timeout=20)
    outcomes: list[object] = []
    lock = threading.Lock()

    def resolver() -> None:
        try:
            with SqlSession(engine) as session:
                # Inside a transaction before the barrier, so both are.
                session.execute(text("SELECT 1"))
                ready.wait()
                resolved = _resolve_account(
                    session, claims, ceiling_per_hour=0, now=None
                )
                session.commit()
            with lock:
                outcomes.append(resolved)
        except Exception as failure:  # noqa: BLE001 - recorded, then asserted on
            with lock:
                outcomes.append(failure)

    threads = [threading.Thread(target=resolver) for _ in range(2)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
            assert not thread.is_alive(), "a racing resolver did not finish"
    finally:
        engine.dispose()

    failures = [item for item in outcomes if isinstance(item, Exception)]
    assert not failures, (
        f"a racing sign-in failed rather than finding the account: {failures}"
    )
    assert len(set(outcomes)) == 1, f"the race produced two accounts: {outcomes}"
    assert (
        sign_in.query(
            "SELECT COUNT(*) FROM app_api.user_account WHERE subject = %s",
            ("raced-identity",),
        )[0][0]
        == 1
    )


def test_a_refresh_racing_a_sign_out_does_not_sign_the_reader_back_in(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 — the grace window must not resurrect an ended session.

    A recent `revoked_at` is not on its own evidence of a rotation. Sign-out
    revokes the whole family in one statement, so the token a second tab holds
    gets exactly the stamp a just-rotated token gets, at exactly the same
    moment. Treating that as the two-tab race mints a new pair and signs the
    reader straight back in.

    It is not a contrived race either: `maintainSession`'s timer, or any second
    tab, makes a refresh arriving a second after sign-out an ordinary event.
    """
    token = sign_in.sign_in(subject="signing-out-mid-refresh").json()["access_token"]
    held = sign_in.client.cookies[REFRESH_COOKIE]

    assert (
        sign_in.client.post(
            "/api/v1/auth/sign-out", headers={"Authorization": f"Bearer {token}"}
        ).status_code
        == 204
    )

    # The other tab's refresh, arriving immediately -- well inside the grace
    # window, with a `revoked_at` from a moment ago.
    sign_in.client.cookies.set(REFRESH_COOKIE, held)
    racing = sign_in.client.post("/api/v1/auth/refresh")
    assert racing.status_code == 401, "a sign-out was undone by a racing refresh"

    # And nothing new was minted for the family.
    live = sign_in.query(
        "SELECT COUNT(*) FROM app_api.account_credential AS c"
        " JOIN app_api.user_account AS a ON a.user_account_id = c.user_account_id"
        " WHERE a.subject = %s AND c.revoked_at IS NULL",
        ("signing-out-mid-refresh",),
    )
    assert live[0][0] == 0


def test_a_refresh_after_reuse_detection_cannot_spend_the_grace_window(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 — the same check, against the attacker it matters most for.

    Reuse detection revokes a family in one statement, exactly as sign-out
    does. Without this, whoever triggered the revocation could immediately
    present any token from that family and be inside the grace window they had
    just created.
    """
    import apps.api.routers.identity as identity_router
    from apps.api.services.identity_service import SessionPolicy

    sign_in.sign_in(subject="reuse-then-grace")
    captured = sign_in.client.cookies[REFRESH_COOKIE]
    rotated = sign_in.client.post("/api/v1/auth/refresh")
    assert rotated.status_code == 200
    successor = sign_in.client.cookies[REFRESH_COOKIE]

    original = identity_router.session_policy
    identity_router.session_policy = lambda: SessionPolicy(grace_seconds=0)
    try:
        sign_in.client.cookies.set(REFRESH_COOKIE, captured)
        assert sign_in.client.post("/api/v1/auth/refresh").status_code == 401
    finally:
        identity_router.session_policy = original

    # The family is revoked. The successor is now inside a generous grace
    # window by `revoked_at`, and must still be refused.
    sign_in.client.cookies.set(REFRESH_COOKIE, successor)
    assert sign_in.client.post("/api/v1/auth/refresh").status_code == 401


def test_the_two_tab_race_still_works_after_the_tightening(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-151 — the check must not break what the grace window is for.

    This is the assertion that keeps the fix honest: a genuine rotation leaves
    a live successor, so a second tab presenting the spent token still gets a
    working session.
    """
    sign_in.sign_in(subject="two-tabs-after-fix")
    shared = sign_in.client.cookies[REFRESH_COOKIE]

    first = sign_in.client.post("/api/v1/auth/refresh")
    assert first.status_code == 200

    sign_in.client.cookies.set(REFRESH_COOKIE, shared)
    second = sign_in.client.post("/api/v1/auth/refresh")
    assert second.status_code == 200, "the fix broke the race it must tolerate"


def test_nothing_from_a_sign_in_reaches_a_log_a_body_or_a_header(
    sign_in: SignInHarness, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: API-151 — the plan's fourth acceptance criterion, directly.

    It asks for this "proven by tests in the shape `tests/integration/api`
    already uses for tokens", which is API-059: walk the flow with logging
    captured and assert the credential is in none of it. Everything else here
    proves refusals are *indistinguishable*; nothing proved they were *quiet*,
    and those are different claims.

    Seven secrets, because they leak differently. The authorization code and
    the state travel in a body, the nonce and the tokens are generated here,
    and the email and provider subject are the two things the platform stores
    about a person -- an access log that carried either would be a record of
    who reads this site.
    """
    import logging as logging_module

    responses = []
    with caplog.at_level(logging_module.DEBUG):
        started = sign_in.start()
        query = sign_in.authorization_query(started)
        state = query["state"][0]
        nonce = query["nonce"][0]
        code = "4/a-code-that-must-not-be-logged"
        sign_in.network.next_id_token = id_token(
            nonce=nonce,
            subject="a-subject-that-must-not-be-logged",
            email="logged@example.test",
        )
        completed = sign_in.client.post(
            "/api/v1/auth/callback", json={"code": code, "state": state}
        )
        assert completed.status_code == 200
        access = completed.json()["access_token"]
        refresh = sign_in.client.cookies[REFRESH_COOKIE]

        rotated = sign_in.client.post("/api/v1/auth/refresh")
        # And a refused one, because a failure path logs more than a success.
        sign_in.client.cookies.set(REFRESH_COOKIE, "a-token-nobody-issued")
        refused = sign_in.client.post("/api/v1/auth/refresh")
        responses = [started, completed, rotated, refused]

    secrets_in_play = {
        "the authorization code": code,
        "the state": state,
        "the nonce": nonce,
        "the access token": access,
        "the refresh token": refresh,
        "the email address": "logged@example.test",
        "the provider subject": "a-subject-that-must-not-be-logged",
    }

    logged = "\n".join(record.getMessage() for record in caplog.records)
    for description, secret in secrets_in_play.items():
        assert secret not in logged, f"{description} reached a log line"

    # Bodies and headers are a narrower rule than logs, and writing it as the
    # same rule is how this assertion was wrong the first time: `state` and
    # `nonce` *belong* in the authorization URL. They are parameters the
    # provider requires, the whole point of the start response is to carry
    # them, and a test that called that a leak would be describing a different
    # protocol.
    #
    # What must never come back out is the code the caller sent, the identity
    # the platform stored, and the long-lived half of the session.
    never_returned = {
        "the authorization code": code,
        "the email address": "logged@example.test",
        "the provider subject": "a-subject-that-must-not-be-logged",
    }
    for response in responses:
        headers = " ".join(f"{key}: {value}" for key, value in response.headers.items())
        for description, secret in never_returned.items():
            assert secret not in response.text, f"{description} was in a body"
            assert secret not in headers, f"{description} was in a header"
        # The refresh token is in exactly one place: a `Set-Cookie`. Never a
        # body, which is what keeps it out of script's reach.
        assert refresh not in response.text, "the refresh token was in a body"

    # And the start response carries `state` and `nonce` only inside the
    # authorization URL it exists to hand over -- not as fields of its own,
    # which would put them somewhere a caller might store them.
    assert set(started.json()) == {"authorization_url"}

    # The refusal said one thing, and the log classified it in one word.
    assert refused.json()["detail"] == "sign-in could not be completed"
    assert any("sign-in refused:" in record.getMessage() for record in caplog.records)
