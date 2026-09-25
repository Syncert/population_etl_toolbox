"""Request and response bodies for the identity routes (ADR-0005).

What is absent from these models is the contract. No response here carries a
credential digest, a provider subject, an account identifier, an issuer, or a
display label. A session response carries a token the caller is about to use
and the moment it stops working, and nothing else -- an identity surface that
answers "who am I" with a database key has published a key.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class SignInStartRequest(BaseModel):
    """Where the provider should send the browser back to.

    Supplied by the caller and then checked against the deployment's
    exact-match allowlist. It is a request, not an instruction: a value the
    allowlist does not contain is refused rather than corrected.
    """

    model_config = ConfigDict(extra="forbid")

    redirect_uri: str = Field(
        min_length=1,
        max_length=2048,
        description=(
            "One of the deployment's registered redirect URIs, matched exactly."
        ),
    )


class SignInStartResponse(BaseModel):
    """Where to send the browser. The transaction handle is a cookie, not a
    field: putting it in the body would make it readable by script, which is
    the property the cookie exists to deny."""

    authorization_url: str


class SignInCallbackRequest(BaseModel):
    """The authorization code, delivered in a body rather than a URL.

    ``apps/api/telemetry.py`` logs no query-string values, so a code in a query
    would not reach this API's logs -- but it would reach the browser's
    history, and any redirect chain that followed. A body reaches neither, and
    the web application strips the code from its own URL as soon as it has it.
    """

    model_config = ConfigDict(extra="forbid")

    code: str = Field(min_length=1, max_length=4096)
    state: str = Field(min_length=1, max_length=512)


class SessionResponse(BaseModel):
    """A live session. The refresh token is a cookie and is never in here."""

    access_token: str
    token_type: str = "Bearer"
    expires_at: datetime
    expires_in: int


class AccountResponse(BaseModel):
    """What an account may learn about itself.

    ``public_display_name`` is nullable and normally null: ADR-0005 §3 makes it
    absent until the account first publishes something.
    """

    public_display_name: Optional[str] = None
    email: Optional[str] = None
    created_at: datetime


class PublicDisplayNameRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    public_display_name: str = Field(min_length=3, max_length=32)


class AccountExportResponse(BaseModel):
    """Everything the platform holds about a person, in one document.

    ADR-0005 §5 requires this to be exhaustive, and names the list: the
    identity pair, a verified email if there is one, the timestamps on the
    credentials, and the content they created. The provider ``subject`` is
    included **because it is theirs** -- an export that withholds part of the
    record is not the answer to "let me leave" that makes immediate hard
    deletion defensible.
    """

    issuer: Optional[str] = None
    subject: Optional[str] = None
    email: Optional[str] = None
    public_display_name: Optional[str] = None
    created_at: datetime
    credentials: list[dict]
    saved_analyses: list[dict]
    evidence_packets: list[dict]
    #: What deletion does and does not promise, carried with the export rather
    #: than left in documentation the reader has not got open.
    backup_retention_days: Optional[int] = None


class AccountDeletionResponse(BaseModel):
    """The receipt for a hard delete.

    It states the backup window because ADR-0005 §5 makes that a published
    number rather than an accident of configuration, and it states the limit
    of the promise because a platform that cannot say "we cannot recall a copy"
    is claiming something it cannot do.
    """

    deleted: bool
    backup_retention_days: Optional[int] = None
    notice: str
