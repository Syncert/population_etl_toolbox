"""Export, rename, and deletion (ADR-0005 §3, §5, API-148).

ADR-0005 §5 ties two of these together and it is worth restating why they are
in one file: "Account-level export [...] is the answer to 'let me leave' that
makes immediate hard deletion defensible rather than punitive." A test suite
that covered deletion and not export would be evidence for half a promise.

These run against the real schema because the whole of deletion is a claim
about ``ON DELETE CASCADE`` -- that is a database behaviour, and asserting it
against a stand-in asserts nothing.
"""

from __future__ import annotations

import pytest

from tests.support.sign_in_harness import SignInHarness

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _save_something(harness: SignInHarness, token: str, name: str):
    return harness.client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(token),
        json={
            "name": name,
            "document": {
                "kind": "observations",
                "metric_code": harness.metric_code,
                "scope": "latest",
                "filters": {"geo_level": "NATIONAL"},
                "visualization": {"chart": "line"},
            },
        },
    )


# ---------------------------------------------------------------------------
# The round trip the plan's first acceptance criterion names
# ---------------------------------------------------------------------------


def test_a_visitor_saves_work_signs_out_signs_in_again_and_finds_it(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-152 -- The plan's first acceptance criterion, walked end to end.

    Everything before this plan made each half of it possible and none of it
    reachable: the write paths worked and nobody could obtain a credential.
    """
    first = sign_in.sign_in(subject="a-returning-reader").json()["access_token"]
    saved = _save_something(sign_in, first, "my-unemployment-view")
    assert saved.status_code == 201, saved.text

    assert (
        sign_in.client.post("/api/v1/auth/sign-out", headers=_auth(first)).status_code
        == 204
    )
    assert (
        sign_in.client.get(
            "/api/v1/analysis-configurations", headers=_auth(first)
        ).status_code
        == 401
    )

    second = sign_in.sign_in(subject="a-returning-reader").json()["access_token"]
    listing = sign_in.client.get(
        "/api/v1/analysis-configurations", headers=_auth(second)
    )
    assert listing.status_code == 200
    assert [item["name"] for item in listing.json()["items"]] == [
        "my-unemployment-view"
    ]


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


def test_the_export_carries_everything_and_no_credential(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-152."""
    token = sign_in.sign_in(subject="exporting-reader").json()["access_token"]
    _save_something(sign_in, token, "exported-view")

    exported = sign_in.client.get("/api/v1/account/export", headers=_auth(token))
    assert exported.status_code == 200
    assert exported.headers["cache-control"] == "private, no-store"

    document = exported.json()
    # ADR-0005 §5's list, exhaustively.
    assert document["issuer"] == "https://accounts.google.test"
    assert document["subject"] == "exporting-reader"
    assert document["email"] == "reader@example.test"
    assert document["public_display_name"] is None
    assert [item["name"] for item in document["saved_analyses"]] == ["exported-view"]
    assert document["evidence_packets"] == []

    # The credentials appear as timestamps and kinds. Not as digests: a
    # credential-shaped string in a file readers are encouraged to download is
    # a bad idea whatever it actually is.
    kinds = sorted(item["kind"] for item in document["credentials"])
    assert kinds == ["access", "refresh"]
    serialised = exported.text
    assert token not in serialised
    for item in document["credentials"]:
        assert set(item) == {
            "kind",
            "issued_at",
            "last_used_at",
            "expires_at",
            "revoked_at",
        }


def test_one_accounts_export_contains_only_its_own_work(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-152."""
    mine = sign_in.sign_in(subject="export-owner").json()["access_token"]
    _save_something(sign_in, mine, "owned")
    theirs = sign_in.sign_in(subject="export-stranger").json()["access_token"]

    stranger_export = sign_in.client.get(
        "/api/v1/account/export", headers=_auth(theirs)
    )
    assert stranger_export.status_code == 200
    assert stranger_export.json()["saved_analyses"] == []
    assert stranger_export.json()["subject"] == "export-stranger"
    assert "export-owner" not in stranger_export.text


# ---------------------------------------------------------------------------
# The public display name
# ---------------------------------------------------------------------------


def test_a_public_name_is_absent_until_it_is_chosen(sign_in: SignInHarness) -> None:
    """Covers: API-152 -- ADR-0005 §3: nothing from the provider is used as a default. The token
    in these fixtures carries no name, but the account also must not fall back
    to the email or to the operator label."""
    token = sign_in.sign_in(subject="unnamed").json()["access_token"]
    account = sign_in.client.get("/api/v1/account", headers=_auth(token))
    assert account.status_code == 200
    assert account.json()["public_display_name"] is None


def test_a_public_name_can_be_chosen_and_changed(sign_in: SignInHarness) -> None:
    """Covers: API-152."""
    token = sign_in.sign_in(subject="namer").json()["access_token"]

    chosen = sign_in.client.put(
        "/api/v1/account/public-display-name",
        headers=_auth(token),
        json={"public_display_name": "Ada"},
    )
    assert chosen.status_code == 200
    assert chosen.json()["public_display_name"] == "Ada"

    changed = sign_in.client.put(
        "/api/v1/account/public-display-name",
        headers=_auth(token),
        json={"public_display_name": "Ada L"},
    )
    assert changed.status_code == 200
    assert changed.json()["public_display_name"] == "Ada L"

    # The previous name is released for reuse, which §3 states explicitly.
    other = sign_in.sign_in(subject="second-namer").json()["access_token"]
    reused = sign_in.client.put(
        "/api/v1/account/public-display-name",
        headers=_auth(other),
        json={"public_display_name": "Ada"},
    )
    assert reused.status_code == 200


def test_a_name_already_held_in_another_case_is_refused(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-152 -- "So one account cannot dress as another." Uniqueness that respects case
    is not uniqueness for a name a reader reads."""
    first = sign_in.sign_in(subject="first-claimer").json()["access_token"]
    sign_in.client.put(
        "/api/v1/account/public-display-name",
        headers=_auth(first),
        json={"public_display_name": "Gauss"},
    )

    second = sign_in.sign_in(subject="second-claimer").json()["access_token"]
    clash = sign_in.client.put(
        "/api/v1/account/public-display-name",
        headers=_auth(second),
        json={"public_display_name": "gAuSs"},
    )
    assert clash.status_code == 409


@pytest.mark.parametrize(
    "candidate",
    [
        "ab",
        "a" * 33,
        "Ada​Lovelace",
        "‮Ada",
        "Ada!",
        "<script>",
        "-Ada",
        "   ",
    ],
    ids=[
        "too-short",
        "too-long",
        "zero-width-space",
        "right-to-left-override",
        "punctuation",
        "markup",
        "leading-hyphen",
        "only-whitespace",
    ],
)
def test_a_name_that_could_impersonate_or_is_not_a_name_is_refused(
    sign_in: SignInHarness, candidate: str
) -> None:
    """Covers: API-152 -- This is the one string the platform renders as a person's chosen
    identity beside content they published. A name carrying a zero-width space
    or a direction override can be made to look like somebody else's."""
    token = sign_in.sign_in(subject="bad-namer").json()["access_token"]
    response = sign_in.client.put(
        "/api/v1/account/public-display-name",
        headers=_auth(token),
        json={"public_display_name": candidate},
    )
    assert response.status_code == 422, candidate


@pytest.mark.parametrize(
    ("submitted", "stored"),
    [
        (" Ada ", "Ada"),
        ("Ada  Lovelace", "Ada Lovelace"),
        ("Ada\tLovelace", "Ada Lovelace"),
    ],
    ids=["surrounding-space", "doubled-space", "tab"],
)
def test_a_name_is_normalised_rather_than_refused_for_its_whitespace(
    sign_in: SignInHarness, submitted: str, stored: str
) -> None:
    """Covers: API-152 -- Normalising beats refusing, and the reason is impersonation rather than
    tidiness: collapsing the spellings into one name is what lets the
    case-insensitive unique index decide, in one place, for all of them."""
    token = sign_in.sign_in(subject=f"normalising-{stored}").json()["access_token"]
    response = sign_in.client.put(
        "/api/v1/account/public-display-name",
        headers=_auth(token),
        json={"public_display_name": submitted},
    )
    assert response.status_code == 200, response.text
    assert response.json()["public_display_name"] == stored


def test_a_doubled_space_cannot_impersonate_a_single_one(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-152 -- The half that is easy to leave out. "Ada  Lovelace" and "Ada Lovelace"
    render almost identically in proportional type."""
    first = sign_in.sign_in(subject="spacing-first").json()["access_token"]
    claimed = sign_in.client.put(
        "/api/v1/account/public-display-name",
        headers=_auth(first),
        json={"public_display_name": "Ada Lovelace"},
    )
    assert claimed.status_code == 200

    second = sign_in.sign_in(subject="spacing-second").json()["access_token"]
    impostor = sign_in.client.put(
        "/api/v1/account/public-display-name",
        headers=_auth(second),
        json={"public_display_name": "Ada  Lovelace"},
    )
    assert impostor.status_code == 409


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------


def test_deletion_destroys_the_account_and_everything_it_owns(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-152."""
    token = sign_in.sign_in(subject="departing").json()["access_token"]
    _save_something(sign_in, token, "about-to-go")

    deleted = sign_in.client.request("DELETE", "/api/v1/account", headers=_auth(token))
    assert deleted.status_code == 200, deleted.text
    assert deleted.json()["deleted"] is True
    assert (
        "cannot recall" in deleted.json()["notice"]
        or "beyond this platform" in (deleted.json()["notice"])
    )

    # Gone from the database, with nothing left behind: no soft-delete state,
    # no orphaned configuration, no credential row.
    assert (
        sign_in.query(
            "SELECT COUNT(*) FROM app_api.user_account WHERE subject = %s",
            ("departing",),
        )[0][0]
        == 0
    )
    assert (
        sign_in.query(
            "SELECT COUNT(*) FROM app_api.saved_analysis_configuration WHERE name = %s",
            ("about-to-go",),
        )[0][0]
        == 0
    )

    # And the credential stops working immediately.
    assert (
        sign_in.client.get(
            "/api/v1/analysis-configurations", headers=_auth(token)
        ).status_code
        == 401
    )


def test_deletion_requires_a_recent_sign_in(sign_in: SignInHarness) -> None:
    """Covers: API-152 -- ADR-0005 §5: "A 30-day session is a convenience for saving charts; it is
    not sufficient authority to destroy everything an account owns from an
    unattended laptop." """
    token = sign_in.sign_in(subject="stale-session").json()["access_token"]
    sign_in.execute(
        "UPDATE app_api.account_credential"
        " SET issued_at = NOW() - INTERVAL '2 hours'"
        " WHERE user_account_id = ("
        "   SELECT user_account_id FROM app_api.user_account WHERE subject = %s)",
        ("stale-session",),
    )

    refused = sign_in.client.request("DELETE", "/api/v1/account", headers=_auth(token))
    assert refused.status_code == 403
    assert "sign in again" in refused.json()["detail"]

    # And nothing was destroyed on the way to refusing.
    assert (
        sign_in.query(
            "SELECT COUNT(*) FROM app_api.user_account WHERE subject = %s",
            ("stale-session",),
        )[0][0]
        == 1
    )


def test_signing_in_again_makes_deletion_possible(sign_in: SignInHarness) -> None:
    """Covers: API-152 -- The remedy the refusal names has to actually work."""
    sign_in.sign_in(subject="re-authenticating")
    sign_in.execute(
        "UPDATE app_api.account_credential SET issued_at = NOW() - INTERVAL '2 hours'"
    )
    fresh = sign_in.sign_in(subject="re-authenticating").json()["access_token"]

    deleted = sign_in.client.request("DELETE", "/api/v1/account", headers=_auth(fresh))
    assert deleted.status_code == 200


def test_deletion_reports_the_backup_window_only_when_one_is_declared(
    sign_in: SignInHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-152 -- ADR-0005 §5 makes the window "a published number rather than an accident
    of configuration". Reporting a default would be inventing it."""
    from data_ingestion_toolbox.config import get_settings

    token = sign_in.sign_in(subject="undeclared-window").json()["access_token"]
    undeclared = sign_in.client.get("/api/v1/account/export", headers=_auth(token))
    assert undeclared.json()["backup_retention_days"] is None

    monkeypatch.setenv("BACKUP_RETENTION_DAYS", "14")
    get_settings.cache_clear()
    declared = sign_in.client.get("/api/v1/account/export", headers=_auth(token))
    assert declared.json()["backup_retention_days"] == 14


def test_an_operator_token_cannot_delete_an_account_through_the_api(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-152 -- It has no sign-in moment and therefore can never be fresh.

    That is the right answer rather than a gap: holding a long-lived
    credential is exactly the authority ADR-0005 §5 says is not sufficient.
    An operator with a real reason uses the reviewed privileged script.
    """
    from tests.support.app_accounts import create_account

    operator_token = "an-operator-token-for-the-deletion-test"
    database = sign_in._connect()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            cursor.execute(
                "DELETE FROM app_api.user_account WHERE display_label = %s",
                ("deletion-operator",),
            )
            create_account(cursor, "deletion-operator", operator_token)
    finally:
        database.close()

    try:
        refused = sign_in.client.request(
            "DELETE", "/api/v1/account", headers=_auth(operator_token)
        )
        assert refused.status_code == 403
    finally:
        cleanup = sign_in._connect()
        cleanup.autocommit = True
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM app_api.user_account WHERE display_label = %s",
                    ("deletion-operator",),
                )
        finally:
            cleanup.close()


def test_every_account_route_refuses_an_anonymous_caller(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-152 -- None of these takes an account identifier, so there is no cross-account
    denial path -- but there is still an anonymous one, and it is the same 401
    every other owner-scoped route answers."""
    assert sign_in.client.get("/api/v1/account").status_code == 401
    assert sign_in.client.get("/api/v1/account/export").status_code == 401
    assert sign_in.client.request("DELETE", "/api/v1/account").status_code == 401
    assert (
        sign_in.client.put(
            "/api/v1/account/public-display-name",
            json={"public_display_name": "Anonymous"},
        ).status_code
        == 401
    )
