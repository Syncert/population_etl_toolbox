# Register a HUD User API token

**What it unblocks:** the HUD Fair Market Rent and income-limit pipeline
([`HUD_FAIR_MARKET_RENTS_AND_INCOME_LIMITS_PLAN.md`](../in_progress/HUD_FAIR_MARKET_RENTS_AND_INCOME_LIMITS_PLAN.md)).
HUD User's website answers this pipeline's honest, self-identifying
downloads with an empty `202` challenge, so the scheduled job cannot fetch
the workbooks. On 2026-10-07 you chose HUD's API as the capture path
instead: it is HUD's sanctioned channel for automated access, and it needs a
token.

**Why it was not automated:** the token belongs to a person's HUD User
account and comes with HUD's API terms, which only the account holder can
accept.

**What you need:** an email address, about ten minutes, and write access to
`infra/docker/stack.env` on the machine that runs the stack.

**What it touches:** one HUD User account and one line in your local
`stack.env` (the example files already list the variable, empty).
Nothing is committed: `stack.env` holds secrets and is not checked in.

## Steps

1. Register (or sign in) at HUD User and open the dataset API page:
   <https://www.huduser.gov/portal/dataset/fmr-api.html>.
2. Create an API token for the **Fair Market Rents and Income Limits** API
   (the page's "Create New Token" flow). Give it a name such as
   `population-etl-toolbox`.
3. Read the API terms of service shown with the token
   (<https://www.huduser.gov/portal/dataset/api-terms-of-service.html>). They
   permit search, display, analysis and retrieval, limit use to 60 queries a
   minute, and require services to show "This product uses the HUD User Data
   API but is not endorsed or certified by HUD User." If anything there
   forbids republishing county values, stop and say so in the thread instead
   of adding the token.
4. Add the token to your environment file. The variable is already listed,
   empty, in both example files this repository checks in
   (`infra/docker/stack.env.example` and
   `infra/docker/stack.external.env.example`), and Compose passes it to the
   Airflow containers. In your own `infra/docker/stack.env` (and
   `stack.external.env` if you run the external stack), find or add the line
   and set it:

   ```text
   HUD_USER_API_TOKEN=<the token>
   ```

   The expected value is the whole token string exactly as HUD User shows it
   when you create it: one line, no quotes, no spaces, and no `Bearer `
   prefix (the adapter adds that). Leave the example files empty; only your
   local, uncommitted files get the real value. Do not paste the token into
   the project thread, a commit, or an issue.
5. Restart the stack (`make up`) so the Airflow containers pick up the new
   value. To check it arrived without printing it, run
   `docker exec docker-airflow-scheduler-1 sh -c 'test -n "$HUD_USER_API_TOKEN" && echo set'`;
   it should print `set`.
6. Tell the agent the token is in place. The agent then switches the HUD
   adapter's capture path to the API, records real API answers as fixtures,
   and reruns the live contract check.

## When it is done

Move this file to [`completed/`](completed/) with the date. The HUD plan
stays in `in_progress/` until the API capture path is built and its live
check passes; this item only makes that possible.
