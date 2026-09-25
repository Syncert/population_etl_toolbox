# One real sign-in, against Google

**What it checks:** that the OIDC flow `self-service-accounts` built works
against the actual provider, not only against a faked provider network.

**Why it was not automated:** it needs an OIDC client registration and secret
from the Google Cloud console — a credential neither an agent container nor CI
holds, and one that should not be checked in or given to a scheduled job.

**What you need:** a Google account, about twenty minutes, the repository, and
a local Postgres the API can use for `app_api`. **No deployment.** Google
permits `http://localhost` redirect URIs, which is what makes this doable
before a stable origin exists.

**What it touches:** one throwaway OIDC client in a Google Cloud project, and
one row in your local `app_api.user_account`. Nothing deployed, nothing shared.

## Read this first

**This does not close the plan's first acceptance criterion.** Per
[`README.md`](README.md), filing something here never substitutes for a
criterion, and `self-service-accounts` stays in `in_progress/` until a real
sign-in has happened. This file exists so the *action* is somewhere you will
find it, not so the criterion can be skipped.

What is already proven without a provider, so you are not re-checking it: the
whole protocol surface, including every refusal (API-150, 47 unit tests), the
session lifecycle and reuse detection against a real database (API-151), and
the browser half including the code leaving the address bar (WEB-116). What is
*not* proven is the one thing only Google can tell us — that the assumptions
this implementation makes about its endpoints, its token and its claims are
right.

One of those assumptions was already found wrong by reading Google's
documentation rather than trusting the fake: its ID tokens carry `iss` as
either `https://accounts.google.com` or `accounts.google.com`, and an
exact-match check refused the second intermittently. That is the class of thing
this check is for, and finding one already is the reason to expect another.

## Steps

### 1. Register a client

In the Google Cloud console, under **APIs & Services → Credentials**, create an
**OAuth client ID** of type **Web application**. Add exactly one authorised
redirect URI:

```text
http://localhost:3100/auth/callback
```

Exactly that, with no trailing slash — both Google and this API match the
redirect URI exactly, and a trailing slash is a different URI to both of them.
Use whatever port you actually serve the web app on; 3100 is what the browser
tier uses.

You do not need to publish the consent screen. Leave it in testing and add your
own Google account as a test user.

### 2. Configure the API

```bash
export API_OIDC_CLIENT_ID='<the client id>'
export API_OIDC_CLIENT_SECRET='<the client secret>'
export API_OIDC_REDIRECT_URIS='http://localhost:3100/auth/callback'
export API_COOKIE_SECURE=0   # a browser will not store a Secure cookie over plain http
export APP_API_DATABASE_URL='postgresql+psycopg2://api_app_writer:...@localhost:5432/...'
```

`API_COOKIE_SECURE=0` is **local only**. Outside localhost it publishes the
refresh token to anything watching the network, and the setting says so.

`API_OIDC_ISSUER` needs no value: it defaults to `https://accounts.google.com`.

Apply the schema if this database has not had it:

```bash
python scripts/provision_app_api.py --apply-schema
```

### 3. Sign in

Start the API and the web app, open the site, and press **Sign in** in the
header. You should reach Google's consent screen, come back, and land on
`/saved` signed in.

## What to look at, beyond it working

The point is not the green path. It is these:

- **The address bar after the redirect.** It must not contain `code=`. If it
  does, the `replaceState` in `SignInCallback` did not run, and an
  authorization code is sitting in your history.
- **What got stored.** One row, and the right shape:

  ```sql
  SELECT user_account_id, display_label, issuer, subject, email,
         public_display_name
  FROM app_api.user_account;
  ```

  Expect `issuer` to be exactly `https://accounts.google.com`, `email` to be
  your verified address, `public_display_name` to be `NULL`, and
  `display_label` to be an opaque `self-service:<uuid>` — **not your name, not
  your email**. If your Google display name appears anywhere in that row,
  something is harvesting a profile field ADR-0005 §1 says it does not.

- **Sign in a second time.** Still one row. Two rows means the identity is not
  matching — most likely the `iss` spelling again, in a form not yet handled.
- **The credential table.** `SELECT kind, expires_at, revoked_at FROM
  app_api.account_credential;` — an `access` and a `refresh`, both with an
  expiry, neither revoked. No 64-character value in there should be anything
  you can present.
- **Reload the page.** You should stay signed in. That is the refresh cookie
  working; nothing in `sessionStorage` or `localStorage` should contain the
  access token, and you can check that in the console.
- **Sign out, then reload.** You should be signed out and stay signed out.
- **The API's log.** Nothing in it should contain the authorization code, the
  access token, your email, or your Google subject. A refused sign-in should
  log one classification word, like `sign-in refused: id_token_nonce`.

## If something fails

That is a finding about the implementation, not about this check. The most
likely candidates, in order, are all things only the real provider can expose:

1. A claim spelled differently than assumed (the `iss` defect was one).
2. The token endpoint refusing the exchange — check that the redirect URI in
   the console, in `API_OIDC_REDIRECT_URIS`, and in the browser's address bar
   are all byte-identical.
3. A discovery field moving.

Refusals are deliberately indistinguishable to a caller, so read the API's log
for the classification word rather than the browser. Then tell the agent what
the word was and what you did.

## When it passes

Record the date and move this file to `completed/`. Then say so on
`docs/plans/in_progress/SELF_SERVICE_ACCOUNTS_PLAN.md` — that is what lets the
first acceptance criterion be marked satisfied and the plan move to
`needs_review/`.
