# Web first-wave handoff

What the first-wave web plan built, what a later publishing or social plan
may build on, and what it must not assume. This is the WEB-009 handoff
required by `docs/plans/.../WEB_ANALYTICS_FIRST_WAVE_PLAN.md`.

## Dependency boundary

```text
completed warehouse products
    -> completed and versioned API contracts
        -> reusable web analytics foundation   <- this plan
            -> first-wave product configurations
                -> later publishing and social workflows
```

The web application consumes `/api/v1` and the Martin tile boundary and
nothing else. It holds no database connection, no source-provider
credential, and no copy of warehouse publication or comparison rules. A
later plan inherits that boundary: publishing and social features are
composition and identity over these contracts, not a second data path.

## Stable components a later plan may build on

Contract-boundary modules are TypeScript under `strict` plus
`noUncheckedIndexedAccess`. These are the pieces intended for reuse.

| Module | Owns | Reuse it for |
| --- | --- | --- |
| `lib/api/client.ts` | The single transport boundary: versioned paths, query construction, typed decoding, the classified `ApiError`, bounded paging, bearer-token writes | Any new API call. Do not add a second fetch path. |
| `lib/api/requestState.ts` | The shared request-state vocabulary and the stale-response tracker | Every async surface. New states go in the reserved list, never as ad-hoc strings. |
| `lib/explorerSources.ts` | Capability-derived source membership and access shapes | Anything that needs to know which sources exist or what filters they accept |
| `lib/observationAccess.ts` | Building observation requests bounded by declared filters; scope and release handling; stratification reporting | Any surface that reads observations |
| `lib/explorerViewModel.ts` | Choropleth model, legend, join index, `publishedNumber` | Any map or numeric rendering |
| `lib/viewModes.ts` | Which presentations a selection or comparison can answer, and why not | Any new analytical surface |
| `lib/comparison.ts` | The preflight verdict model and derived-value labelling | Anything combining two measures |
| `lib/catalog.ts` | Catalog paging, published provenance, metric quality state | Any catalog-shaped list |
| `lib/dataQuality.ts` | The published freshness rollup and where each kind of quality evidence lives | Any quality surface |
| `lib/productTemplates.ts` | Product configuration over catalog identities | New first-wave-style products |
| `lib/evidencePackets.ts` | Blocks and the reproducibility envelope | Any composition surface |
| `lib/savedAnalysis.ts` | Configuration documents, validation, conflicts, local migration | Anything persisting a user's analysis |
| `lib/urlState.ts` | Parse/serialize for explorer, comparison, catalog, and profile links | Any shareable state |
| `components/StatusPill.js` | The one visual mapping of request state | Every status surface |
| `components/ChoroplethMap.tsx` | A read-only choropleth over the shared colouring model | A new map that does not need the explorer's interaction |

## Saved-analysis contract

Saved analyses are the API's, not the browser's. `/api/v1/analysis-configurations`
is user-scoped and requires a bearer token; documents are validated on write
against the same capability and compatibility contracts the live routes
enforce, so a stored configuration cannot encode a request the API would
refuse.

Three properties a later plan must preserve:

1. **A configuration is intent, not data.** It names the resource, measures,
   and filters and is replayed against live publications. Storing observation
   values would turn a saved analysis into a snapshot that silently drifts
   from the warehouse.
2. **Stale is reported, never repaired.** A document whose measure was
   retired comes back unmodified with `validation.valid = false` and a
   reason. Rewriting it would substitute a guess for the user's intent.
3. **Conflicts are refused, not merged.** Updates send `expected_version`;
   a `409` names the current version. Overwriting a version the client never
   read discards someone else's change.

The browser-local store (`economic-data-studio:saved-charts:v1`,
`economic-data-studio:builder-draft:v1`) predates the contract and still
backs the explorer's and comparison workspace's save buttons and the packet
draft. `lib/savedAnalysis.planLocalMigration` bridges what it can and names
what it cannot; the local store is never cleared by importing.

## Privacy boundaries

- **Public state travels in URLs by design.** Explorer, comparison, catalog,
  and profile links carry a query — source, measure, scope, release,
  geography, page — so a shared link reproduces an analysis. They never carry
  a value, and never carry an identity.
- **Private content never reaches a URL.** The bearer token is sent only as
  an `Authorization` header. No configuration's name, id, version, or owner
  is written to the address bar, a link, a referrer, or history. The
  saved-analysis screen writes nothing to the address bar at all.
- **Private responses are never publicly cached.** `/analysis-configurations`
  answers `private, no-store` and sits outside the cacheable public prefixes.
- **The token is not stored beside public data.** It lives in memory and, at
  the user's explicit choice, `sessionStorage` for the tab — never
  `localStorage` alongside the public saved-chart store.

A later publishing plan adds a *deliberate* path from private to public. It
must be an explicit approval step with its own record, not a widening of any
boundary above.

## Invariants a later plan must not break

These are the rules the first wave is built on. Every one is gated by a
catalog entry in `docs/reference/TESTING_CONTRACT.md` (WEB-001–WEB-025).

- **The API owns semantics.** Membership, access shapes, declared filters,
  compatibility verdicts, distribution bins, freshness, and validation are
  read from the API and never recomputed. A rule this client has never heard
  of must not be able to flip a decision.
- **Nothing undeclared is requested, and nothing declared is dropped.** A
  filter a source does not declare is not sent; a filter it does declare is
  not silently omitted, because omitting it widens the answer.
- **A value the source did not publish is never a zero.** `null` and empty
  are rejected before any numeric coercion — in colouring, in sizing, in
  formatting, in plotting, and in export.
- **Provider-published values and API-derived values stay visually and
  semantically distinct.** Anything the API names in `derivations` is
  labelled derived wherever it appears.
- **Distinct states stay distinct.** Missing, suppressed, not-reported,
  unknown, stale, retired, incompatible, unauthorized, rate-limited, and
  unavailable are different facts and never collapse into one another or
  into "no data".
- **No client-authored composite.** No score, index, grade, or ranking over
  unlike measures. No client-authored definition that could be mistaken for a
  provider fact.
- **A presentation is offered only where it can answer.** A mode that cannot
  answer is absent with a stated reason, not blank.
- **Analytical context travels with the value.** Source, measure, period,
  unit, uncertainty, scope and release, geography, and caveats accompany
  every displayed value, including into exports and composed packets.

## Explicit non-goals of this plan

Out of scope here, and deliberately not stubbed:

- Public publishing approval, moderation, and takedown.
- Comments, follows, sharing to third parties, and any social graph.
- Account self-registration, password flows, and session management beyond
  presenting an operator-provisioned bearer token.
- Server-side rendering of user content, and any multi-tenant theming.
- A quality score, and any cross-source composite measure.
- Native mobile applications.

## Known follow-ons

Named so they are picked up deliberately rather than rediscovered:

- The explorer and comparison workspaces still save to the browser-local
  store; moving their save buttons onto `/analysis-configurations` is
  mechanical now that the migration path exists.
- Evidence packets persist to the local draft rather than the account.
- The comparison map and the explorer map are separate MapLibre wirings over
  one shared colouring model; unifying the presentations is a consolidation
  task, not a contract gap.
- Script `'unsafe-inline'` in the CSP needs a per-request nonce to remove.
- `/articles` still carries a hand-written example rather than composed
  blocks.
