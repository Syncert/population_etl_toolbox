# ADR-0004: Evidence packet persistence

- **Status:** Accepted
- **Date:** 2026-09-12
- **Accepted:** 2026-09-12 (human review; the evidence packet plan's explicit precondition)
- **Decision owners:** API platform maintainers
- **Related work:** implemented by the [evidence packet persistence plan](../plans/completed/EVIDENCE_PACKET_PERSISTENCE_PLAN.md); unblocks WEB-007 in the [web analytics first-wave plan](../plans/needs_review/WEB_ANALYTICS_FIRST_WAVE_PLAN.md); extends [ADR-0003](0003-saved-analysis-authentication-and-persistence.md)

## Context

The evidence packet composer (`/builder`) and the composed-article reader
(`/articles`) both persist to `localStorage` under
`economic-data-studio:builder-draft:v1`. They are the last two screens that do,
and the web plan's definition of done requires user analytical state to live
behind the versioned saved-configuration contract.

They cannot move today. `/api/v1/analysis-configurations` stores an
`AnalysisDocument` whose `kind` is `observations`, `comparison`, or
`distribution`: **one resource with its filters**. An evidence packet is an
ordered composition of heterogeneous blocks — narrative, methodology, caveats,
and analytical blocks that each carry a query plus a reproducibility envelope.
That contract cannot describe one.

The web side has two ways to force it and both are wrong:

- **Invent the resource client-side** — build packet storage out of several
  configuration rows and a naming convention. The web plan's non-goals forbid
  exactly this ("Building new ingestion pipelines, API endpoints, or Martin
  layers as client-side workarounds"), and it would put the composition rules
  in the client, where the API could never enforce them.
- **Smuggle the packet through `visualization`** — that field is opaque user
  content the API stores verbatim and never inspects. A packet hidden there
  would be unvalidatable by the very contract that exists to guarantee a
  stored document cannot encode a request the API would refuse. It would also
  still need a `kind` and a `metric_code`, and a packet has no single answer
  for either.

So the boundary is real and this is the upstream decision it needs.

A note on ordering: `AGENTS.md` requires warehouse contracts before API
contracts before web features. This proposal needs **no warehouse change**.
`app_api` is not warehouse content — no ETL reads or writes it, it is absent
from the warehouse manifest — and packet validation only *reads* the glossary
through the existing read-only serving session. The dependency is API → web
only.

## Decision

### A new resource, not a fourth configuration kind

**`/api/v1/evidence-packets`**, a sibling of `/analysis-configurations`,
reusing its authentication, ownership, concurrency, privacy, and retention
contracts wholesale.

Not a `kind: "packet"` on `AnalysisDocument`. That model is `extra="forbid"`
with flat single-resource fields (`metric_code`, `filters`, `bin_count`);
adding a composition kind would make every one of those fields conditionally
meaningful, give `validate_document` a fourth branch of a different arity, and
make `SavedAnalysisSummary.kind` report "packet" for rows that are not queries
at all — so a client listing configurations in order to reopen an analysis
would have to filter out the things that cannot be reopened. Two resources
with different lifecycles are two resources.

### The central rule: refuse contradictions, report incompleteness

This is the design's load-bearing distinction and every validation rule below
follows from it.

A packet is composed over days. A half-filled analytical block is the normal
state of work in progress, and an API that refused to store one would make
"save and come back to it" impossible — the single most important thing a
composer does. So **incompleteness is stored and reported**, exactly as
`packetIssues` already reports it client-side.

A *contradiction* is different. A block whose envelope names one measure while
its query asks for another will display one measure's name over another
measure's numbers. That is the precise failure the envelope exists to prevent,
it is invisible to the client, and no amount of later editing makes it
legitimate. **Contradictions are refused at write with a 422 naming the
block.**

| Condition | Write | Read |
| --- | --- | --- |
| Analytical block missing its `document` or `envelope` | stored | reported per block |
| Envelope field the composer never captured | stored empty | reported per block |
| Block `document` the live contracts would refuse | **422** | — |
| Envelope naming a measure the block's query does not ask for | **422** | — |
| Envelope scope/release disagreeing with the block's query | **422** | — |
| Non-analytical block carrying a `document` or `envelope` | **422** | — |
| Duplicate `block_id` within one packet | **422** | — |
| A block's measure retired after it was stored | — | reported per block, document unmodified |

The last row is ADR-0003's rule unchanged: reported, never repaired. The
document is the user's content.

### Blocks embed their query; they do not reference a configuration

A block could point at a `configuration_id` instead of carrying an
`AnalysisDocument`. Rejected, and the reason is the difference between the two
resources:

**A configuration is a live question its owner re-asks. A packet is a document
its owner hands to somebody else.** If a block referenced a configuration,
editing that configuration later would silently change what an already-issued
grant proposal argued — and the block's reproducibility envelope, captured when
it was composed, would then describe a query that no longer exists, with
nothing reporting the disagreement.

So each block carries its own `AnalysisDocument`, captured together with its
envelope and consistent with it by construction. This is deliberately *not* the
"intent, not data" rule inverted: the packet still stores no observation
values, and a live block is still replayed against the latest publication.
What is frozen is the **question**, which is what makes the answer
reproducible.

Provenance is still recorded. A block may carry
`source_configuration_id` — informational only, **not a foreign key**, so
deleting a configuration can never delete or break a packet that was composed
from it.

### Document schema

```python
# apps/api/schemas/evidence_packet.py

PacketBlockType = Literal[
    "text", "analysis", "table", "map", "source-note", "methodology", "caveat"
]
ANALYTICAL_BLOCK_TYPES = frozenset({"analysis", "table", "map"})


class ReproducibilityEnvelope(BaseModel):
    """What the composer recorded about a block, as it recorded it."""

    model_config = ConfigDict(extra="forbid")

    metric_codes: list[str] = Field(default_factory=list, max_length=8)
    source_codes: list[str] = Field(default_factory=list, max_length=8)
    geo_id: str = Field("", max_length=100)
    geo_level: str = Field("", max_length=50)
    scope: Literal["latest", "as_released"] = "latest"
    release: str = Field("", max_length=100)
    period: str = Field("", max_length=100)
    units: str = Field("", max_length=100)
    transformation: str = Field("none", max_length=200)
    api_query: str = Field("", max_length=2000)
    caveats: list[str] = Field(default_factory=list, max_length=20)


class PacketBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    block_id: str = Field(..., min_length=1, max_length=100)
    type: PacketBlockType
    title: str = Field("", max_length=200)
    content: str = Field("", max_length=20_000)
    envelope: Optional[ReproducibilityEnvelope] = None
    document: Optional[AnalysisDocument] = None
    #: Where this block came from. Informational; never resolved, never a FK.
    source_configuration_id: Optional[int] = Field(default=None, ge=1)


class EvidencePacketDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    #: The document's own shape version. Deliberately not called `version`:
    #: the row's `version` is the optimistic-concurrency counter and the two
    #: would be confused at every call site.
    schema_version: Literal[1] = 1
    title: str = Field("", max_length=200)
    purpose: str = Field("", max_length=2_000)
    blocks: list[PacketBlock] = Field(default_factory=list, max_length=100)
```

`AnalysisDocument` is reused unchanged, so a block's query is validated by the
same `validate_document` the live routes and the configuration contract
already use. One definition of "a query this API would accept", not two.

The API is snake_case; `lib/evidencePackets.ts` is camelCase. The client maps
at the boundary, as `lib/api/client.ts` already does for every other resource.

### The envelope/query consistency rule, precisely

Only the envelope fields that **duplicate the request** are cross-checked:

- `envelope.metric_codes` ⊆ the block document's metric codes
  (`metric_code`, or `metric_code_a`/`metric_code_b` for a comparison);
- `envelope.scope` equals the document's `scope`;
- `envelope.release` equals the document's `release`.

`period`, `units`, `geo_id`, `geo_level`, `transformation`, and `caveats` are
**not** checked. They are observations about what the source published when
the block was composed, and the API second-guessing them would substitute its
present view for what the composer actually saw — the same reason the client's
rule is "a field the view never captured stays empty".

### Per-block validation on read

```json
{
  "packet_id": 12,
  "name": "Housing needs assessment",
  "version": 3,
  "document": { "...": "as stored" },
  "validation": {
    "valid": false,
    "reason": "2 of 5 blocks cannot be read as evidence",
    "blocks": [
      {"block_id": "population-evidence", "valid": true,  "reason": null, "missing": []},
      {"block_id": "condition-evidence",  "valid": false,
       "reason": "this block presents no analysis yet, so it carries no reproducibility envelope",
       "missing": ["metric_codes", "source_codes", "geo_id", "period", "api_query"]},
      {"block_id": "rent-burden", "valid": false,
       "reason": "metric_code 'CENSUS_ACS:acs5:B25070_001' is not a published metric",
       "missing": []}
    ]
  },
  "created_at": "...", "updated_at": "..."
}
```

Per block rather than one boolean, because a twelve-block needs assessment
with one retired measure should name the block to fix rather than send its
author hunting. The two failure kinds stay distinct: *incomplete* (the
composer never filled it) and *stale* (the warehouse moved under it) are
different problems with different fixes.

### Routes

Mirroring `/analysis-configurations` exactly — same auth dependency, same
`private, no-store`, same 404-for-another-owner, same `expected_version`
conflict:

| Route | Answers |
| --- | --- |
| `GET /api/v1/evidence-packets?limit&offset` | The caller's packets, ordered by `name, packet_id` |
| `POST /api/v1/evidence-packets` | `201` with the stored packet; `422` contradiction, `409` duplicate name |
| `GET /api/v1/evidence-packets/{packet_id}` | The document plus per-block validation |
| `PUT /api/v1/evidence-packets/{packet_id}` | Replace; `409` on a stale `expected_version` |
| `DELETE /api/v1/evidence-packets/{packet_id}` | `204`, immediate and permanent |

The list summary carries `packet_id`, `name`, `version`, `block_count`,
`analytical_block_count`, `created_at`, `updated_at` — enough to choose a
packet without fetching each one.

**The list deliberately does not validate.** Validating every row would mean
warehouse lookups multiplied by page size on a route whose job is to let
someone pick a packet. This has to be stated in the consumer guide, or an
absent `validation` on a summary will be read as "valid" — which is the exact
"unknown presented as healthy" failure the rest of the contract works to
avoid.

### Storage

```sql
-- appended to sql/bootstrap/002_app_api.sql, above its GRANT block
CREATE TABLE IF NOT EXISTS app_api.evidence_packet (
    packet_id      BIGSERIAL PRIMARY KEY,
    owner_user_id  BIGINT NOT NULL
        REFERENCES app_api.user_account (user_account_id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    version        INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    document       JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (owner_user_id, name)
);

CREATE INDEX IF NOT EXISTS evidence_packet_owner_idx
    ON app_api.evidence_packet (owner_user_id, packet_id);
```

One row per packet with the composition as JSONB, not a `packet_block` child
table. Block order is part of the argument; a single row makes a packet update
atomic under the same optimistic-version check the configuration contract
already uses. A child table would need an ordinal column, multi-statement
writes, and its own concurrency story, and would buy nothing — nothing queries
*into* a packet.

**Deployment mechanic.** `002_app_api.sql` ends with
`GRANT ... ON ALL TABLES IN SCHEMA app_api`, which is positional: it grants on
the tables that exist when it runs. Placing the `CREATE TABLE` above that block
means a fresh bootstrap is correct, and re-running the whole file against a
deployed database is the migration — every statement in it is already
idempotent. `BETA_RESET_REINGESTION.md` gains that one step; there is no new
role, schema, or engine.

### Bounds, and why each number exists

A packet is a much larger authenticated write than a configuration, and
validating one resolves metrics against the glossary per analytical block.

- **≤ 100 blocks, ≤ 50 analytical blocks per packet.** The composer's own
  templates are five to twelve blocks; this is headroom, not a target.
- **≤ 256 KB serialized document.** The per-field `max_length` values in the
  schema above are shape sanity, not the real bound — a hundred blocks each
  holding 20,000 characters of prose would be two megabytes. The document cap
  is the binding constraint and is checked first; the field bounds only stop
  any single field from being absurd on its own.
- **Metric codes are deduplicated across blocks and resolved once per
  request.** A needs assessment reuses three or four measures across a dozen
  blocks; resolving per block would multiply warehouse work by the repetition.
- Writes land in the existing `analysis` rate-limit class (the path carries no
  `/catalog/` fragment), which is right — they reach warehouse SQL.
- The path sits outside `CACHEABLE_PREFIXES` automatically, and is
  additionally `private, no-store`.

**This needs a mechanism the API does not have yet, and the gap is already
live.** There is no request-body size limit anywhere in `apps/api` — no
middleware reads `content-length`, and FastAPI imposes none. Worse,
`AnalysisDocument.filters` and `AnalysisDocument.visualization` are
`dict[str, Any]` with no bound at all, so **an authenticated user can already
store an arbitrarily large JSONB document through `/analysis-configurations`
today.** Packets make the exposure larger and more obvious, but they do not
create it.

So the bound should be a small shared body-size middleware applied to the
authenticated write paths, and `/analysis-configurations` should adopt it in
the same change. Fixing it only for the new resource would leave the older
one as the easier target.

### Consequences for the web

Blocked until the above ships; then:

- `lib/evidencePackets.ts` gains encode/decode at the snake/camel boundary.
  `readComposedPacket` stays — it is still how the signed-out local draft is
  read, and its three-outcome discipline is unchanged.
- Saving reuses `lib/savedAnalysis.saveDestination` rather than growing a
  second destination decision: the account whenever a token is held, the
  browser otherwise, the destination stated on the control before the save and
  on the outcome after it, and a refused account save reported rather than
  rewritten to the browser store.
- `planLocalMigration` extends to bridge `builder-draft:v1` into a packet
  document. A block it cannot describe is listed as skipped with the reason,
  never coerced; the local store is not cleared by importing.
- `/articles` lists the account's packets and renders the selected one.
  **The selection does not go in the URL.** ADR-0003's privacy boundary keeps
  a configuration's name, id, version, and owner out of the address bar, and a
  packet id is the same class of fact — so `/articles` keeps writing nothing
  to the address bar, as `/saved` does. Packet links are therefore not
  shareable, which is correct: sharing a packet is publishing, and publishing
  is the deferred follow-on plan's to define.

## Rejected alternatives

- **`kind: "packet"` on `AnalysisDocument`** — conflates a query with a
  composition in one `extra="forbid"` model, and makes the configuration list
  return rows that cannot be reopened.
- **Blocks referencing `configuration_id`** — editing a configuration would
  silently rewrite what an issued proposal argued, and desynchronize the
  block's envelope from its query with nothing reporting it.
- **Smuggling the packet through `visualization`** — stored verbatim and never
  validated, so the packet would be unvalidatable by the contract meant to
  guarantee it.
- **A `packet_block` child table** — ordinal column, multi-statement writes,
  and a second concurrency story, for a query capability nothing needs.
- **Refusing incomplete analytical blocks at write** — would make "save and
  come back to it" impossible, which is the composer's primary use.
- **Server-side packet rendering, export, or PDF** — the packet is the user's
  document; print and CSV already work client-side, and rendering user content
  server-side is a new attack surface for no gain.
- **Sharing, public packets, approval workflow** — the follow-on publishing
  plan's scope, explicitly out of the web plan's.

## Consequences

One new table in an existing schema, one new router, one new service, no new
role, no new engine, no warehouse change, and nothing about the public
analytical surface changes. Additive under [ADR-0002](0002-api-versioning-and-deprecation.md):
a new operation lands in `v1`.

Implementation obligations, should this be accepted:

- new `API-0xx` catalog rows in `TESTING_CONTRACT.md` for the resource, its
  ownership and concurrency denials, and the contradiction-versus-incompleteness
  split, with `AUDITED_COUNTS` and the register bumped;
- `DB-0xx` rows for the owner-scoped storage and cascade behaviour;
- denial-path tests matching API-007's: cross-user access, id enumeration,
  revoked tokens, cache and telemetry isolation;
- the shared request-body bound above, applied to both authenticated write
  resources, with its own catalog row — this one is worth doing whether or not
  packets ship, because the exposure exists now;
- a `Saved evidence packets` section in `API_CONSUMER_GUIDE.md`, including the
  explicit statement that a list summary carries no validation verdict;
- the one re-run step in `BETA_RESET_REINGESTION.md`;
- then the web migration above, which closes the last item blocking
  `WEB_ANALYTICS_FIRST_WAVE_PLAN.md`.

Accepted 2026-09-12; the implementation plan was claimed the same day.
