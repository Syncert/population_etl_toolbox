"""The deletion promise, across a restore (ADR-0005 §5, API-155).

ADR-0005 §5 commits to three things and a hard ``DELETE`` only does the first:

    "deleted data is gone from production immediately and from every retained
    backup once that window has passed, and a restore performed inside the
    window re-applies the deletion log before the database serves traffic."

The third clause is the one with a mechanism behind it, and the only honest
way to test it is to *perform the restore*: delete an account, put the row
back the way a point-in-time snapshot would, and check that re-applying the
log removes it again along with everything it owned.

Putting the row back is what makes this a real test. A test that deleted an
account and then checked it was absent would pass with no mechanism at all.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from tests.support.sign_in_harness import SignInHarness

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:  # pragma: no cover - import path
    sys.path.insert(0, str(REPOSITORY_ROOT))

from scripts.apply_deletion_log import (  # noqa: E402
    apply_log,
    export_log,
    purge_expired,
    read_export,
)


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


def test_a_deletion_is_recorded_in_the_same_transaction(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-155 — the log entry and the delete commit together.

    A log entry with no delete would have a restore destroy a live account; a
    delete with no log entry is a deletion that a restore inside the backup
    window silently undoes. Neither is a state this should be able to reach.
    """
    token = sign_in.sign_in(subject="logged-departure").json()["access_token"]
    owner = sign_in.query(
        "SELECT user_account_id FROM app_api.user_account WHERE subject = %s",
        ("logged-departure",),
    )[0][0]

    assert (
        sign_in.client.request(
            "DELETE", "/api/v1/account", headers=_auth(token)
        ).status_code
        == 200
    )

    logged = sign_in.query(
        "SELECT COUNT(*) FROM app_api.account_deletion_log WHERE user_account_id = %s",
        (owner,),
    )
    assert logged[0][0] == 1


def test_the_log_holds_an_id_and_a_time_and_nothing_about_the_person(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-155 — the mechanism that keeps the promise must not become
    the thing the promise was about.

    "What it keeps after deletion is nothing" cannot survive a log carrying an
    address or a provider subject, so the table's columns are asserted rather
    than assumed.
    """
    token = sign_in.sign_in(
        subject="private-departure", email="departing@example.test"
    ).json()["access_token"]
    sign_in.client.request("DELETE", "/api/v1/account", headers=_auth(token))

    columns = sign_in.query(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = 'app_api' AND table_name = 'account_deletion_log'"
    )
    assert {row[0] for row in columns} == {"user_account_id", "deleted_at"}


def test_a_restore_inside_the_window_does_not_bring_the_account_back(
    sign_in: SignInHarness, tmp_path: Path
) -> None:
    """Covers: API-155 — the whole of §5's third clause, performed.

    The restore is simulated by re-inserting the rows a point-in-time snapshot
    would have carried: the account, and a saved analysis it owned. That is
    what a restore *is* from this table's point of view, and it is the state
    every other test in this file would pass without noticing.
    """
    token = sign_in.sign_in(subject="restored-departure").json()["access_token"]
    assert (
        _save_something(sign_in, token, "work-that-should-stay-gone").status_code == 201
    )
    owner = sign_in.query(
        "SELECT user_account_id FROM app_api.user_account WHERE subject = %s",
        ("restored-departure",),
    )[0][0]

    sign_in.client.request("DELETE", "/api/v1/account", headers=_auth(token))

    # The operator's pre-restore export. It has to happen before, which is the
    # point of the flag: a restored log does not contain the deletions made
    # after the restore point.
    database = sign_in._connect()
    try:
        exported = tmp_path / "deletion-log.csv"
        assert export_log(database, exported) >= 1
    finally:
        database.close()

    # The restore. An older snapshot carried both rows, and the log entry that
    # says the account should not exist was written after the snapshot, so the
    # restore does not carry it either.
    sign_in.execute(
        "INSERT INTO app_api.user_account"
        " (user_account_id, display_label, issuer, subject)"
        " VALUES (%s, 'restored', 'https://accounts.google.test', 'restored-departure')",
        (owner,),
    )
    sign_in.execute(
        "INSERT INTO app_api.saved_analysis_configuration"
        " (owner_user_id, name, document)"
        " VALUES (%s, 'work-that-should-stay-gone', '{}'::jsonb)",
        (owner,),
    )
    sign_in.execute("DELETE FROM app_api.account_deletion_log")
    assert (
        sign_in.query(
            "SELECT COUNT(*) FROM app_api.user_account WHERE user_account_id = %s",
            (owner,),
        )[0][0]
        == 1
    ), "the restore did not put the row back; this test would prove nothing"

    # Before the database serves traffic.
    database = sign_in._connect()
    try:
        deleted, already_gone = apply_log(database, read_export(exported))
    finally:
        database.close()

    assert deleted >= 1
    assert (
        sign_in.query(
            "SELECT COUNT(*) FROM app_api.user_account WHERE user_account_id = %s",
            (owner,),
        )[0][0]
        == 0
    )
    # And what the account owned went with it, by the same cascade the original
    # deletion used rather than by a second mechanism that could disagree.
    assert (
        sign_in.query(
            "SELECT COUNT(*) FROM app_api.saved_analysis_configuration"
            " WHERE owner_user_id = %s",
            (owner,),
        )[0][0]
        == 0
    )
    assert already_gone >= 0


def test_re_applying_a_log_twice_changes_nothing_the_second_time(
    sign_in: SignInHarness, tmp_path: Path
) -> None:
    """Covers: API-155 — an operator who is unsure whether it ran can run it
    again. An id already absent is the normal case, not an error."""
    token = sign_in.sign_in(subject="twice-applied").json()["access_token"]
    sign_in.client.request("DELETE", "/api/v1/account", headers=_auth(token))

    database = sign_in._connect()
    try:
        exported = tmp_path / "log.csv"
        export_log(database, exported)
        entries = read_export(exported)
        first = apply_log(database, entries)
        second = apply_log(database, entries)
    finally:
        database.close()

    assert second[0] == 0, "the second application deleted something"
    assert second[1] == len(entries)
    assert first[0] + first[1] == len(entries)


def test_a_file_that_is_not_a_deletion_log_is_refused(tmp_path: Path) -> None:
    """Covers: API-155 — a wrong file here deletes accounts.

    So the header is checked rather than skipped: a CSV of something else whose
    first column happens to hold integers would otherwise be read as a list of
    people to destroy.
    """
    wrong = tmp_path / "metrics.csv"
    wrong.write_text("metric_id,label\n41,Unemployment\n", encoding="utf-8")
    with pytest.raises(ValueError, match="does not look like a deletion log"):
        read_export(wrong)

    empty = tmp_path / "empty.csv"
    empty.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="is not a deletion log"):
        read_export(empty)


def test_the_log_is_purged_once_no_backup_could_still_hold_the_account(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-155 — the last trace goes too.

    Past the retention window no retained backup contains the account, so the
    log entry is the only thing left pointing at it, and §5's "what it keeps
    after deletion is nothing" is only true once that is gone as well.
    """
    token = sign_in.sign_in(subject="aged-out").json()["access_token"]
    sign_in.client.request("DELETE", "/api/v1/account", headers=_auth(token))
    sign_in.execute(
        "UPDATE app_api.account_deletion_log SET deleted_at = NOW() - INTERVAL '40 days'"
    )

    database = sign_in._connect()
    try:
        # Inside the window: kept, because a restore could still need it.
        assert purge_expired(database, 90) == 0
        # Past it: the last trace goes.
        assert purge_expired(database, 30) >= 1
    finally:
        database.close()

    assert sign_in.query("SELECT COUNT(*) FROM app_api.account_deletion_log")[0][0] == 0


def test_a_zero_day_window_is_refused_rather_than_purging_everything(
    sign_in: SignInHarness,
) -> None:
    """Covers: API-155 — a deployment that has declared no window has not
    declared zero. Reading an unset value as "purge immediately" would destroy
    the mechanism at exactly the deployment least likely to notice."""
    database = sign_in._connect()
    try:
        with pytest.raises(ValueError, match="opposite of what it is for"):
            purge_expired(database, 0)
    finally:
        database.close()
