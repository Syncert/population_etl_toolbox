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
| `lib/evidencePackets.ts` | Blocks, the reproducibility envelope, the local-draft reader, the account boundary translation, and the merge of the API's per-block verdict with the client's own report | Any composition surface, and anything storing or reading a packet |
| `lib/urlState.ts` | Parse/serialize for explorer, comparison, catalog, and profile links | Any shareable state |
| `components/StatusPill.js` | The one visual mapping of request state | Every status surface |
| `components/ChoroplethMap.tsx` | A read-only choropleth over the shared colouring model | A new map that does not need the explorer's interaction |
| `components/useMapLibre.ts` + `lib/mapWiring.ts` | The one way a map is brought up and taken down, and the one way its layers are painted, filtered, shown, and hidden | Any new map; add a source and layers in `onLoad`, never a second construction |
| `components/ChoroplethLegend.tsx` | The one legend | Any coloured presentation; a legend is the textual carrier colour alone must not be |
| `components/EvidenceEnvelope.tsx` | The one presentation of an analytical block's reproducibility envelope and its live/frozen basis | Any surface that shows a composed block — composer, reader, or a later publishing screen |

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

The explorer and the comparison workspace save to the account whenever a
token is held, through the one destination decision in
`lib/savedAnalysis.saveDestination`. Both screens state the destination on
the control before the save and on the outcome after it, because "saved to
your account" and "saved in this browser" have very different consequences
for whether the work exists tomorrow. A refused account save is reported, not
rewritten to the browser store: telling a user their work is safe somewhere
they did not choose and cannot see from their account is worse than telling
them it was not saved.

The browser-local store (`economic-data-studio:saved-charts:v1`,
`economic-data-studio:builder-draft:v1`) predates the contract and remains
the signed-out destination and the evidence packet builder's input.
`lib/savedAnalysis.planLocalMigration` bridges what it can and names what it
cannot; the local store is never cleared by importing.

The bearer token has one home, `lib/apiToken.ts`: `sessionStorage` only, at
the user's explicit choice, guarded on every access because storage throws
outright in a private window rather than returning null. Any new screen that
authenticates reads it from there — a second copy of the key would be a
second place for that discipline to drift out of.

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

- **No inline script runs without this response's nonce.** `middleware.ts`
  owns the Content-Security-Policy; a later plan that needs an inline
  script reads the `x-nonce` request header and stamps it, never widens
  `script-src`.
- **No inline `<style>` element runs in production either.** `style-src-elem`
  admits `'self'` alone there; the development exception for Next's dev
  overlay is compiled away, and `npm run check:csp` reads the built middleware
  to prove it. `style-src-attr 'unsafe-inline'` stays open because MapLibre,
  Next's route announcer, and three data-driven styles of this application's
  own (legend swatch colour, coverage segment width, map tooltip position)
  write `style` attributes that no nonce or hash can cover. A plan that adds
  an inline style adds it as an attribute on a recorded owner, or adds a rule
  to the stylesheet -- never a `<style>` element.
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

- Evidence packets persist to the account through `/api/v1/evidence-packets`
  (ADR-0004) whenever a token is held; `builder-draft:v1` remains the
  signed-out destination and `saved-charts:v1` remains the composer's input
  for filling analytical blocks, which is why both stores stay. A later plan
  inherits the packet contract's rule — contradictions are refused at write,
  incompleteness is stored and reported per block — and the one translation
  in `lib/evidencePackets.packetToDocument`/`documentToPacket`. Nothing about
  a packet reaches the address bar from either screen; sharing a packet is
  publishing, and publishing is that later plan's to define.
- A build-your-own analytics surface — several measures on one time axis,
  cross-sectional scatter, bar and heatmap presentations, and an API-derived
  correlation — is planned as the workbench in
  `docs/plans/to_do/ANALYTICS_WORKBENCH_PLAN.md`. It reuses `lib/comparison.ts`,
  `lib/observationAccess.ts`, `ScatterChart.tsx` and the saved-analysis
  destination rule, adds `/comparison/correlation` and `/comparison/matrix`
  on the API, and states its grain rules once, in the plan, so a later
  reader does not rediscover why nothing is rolled up client-side.
