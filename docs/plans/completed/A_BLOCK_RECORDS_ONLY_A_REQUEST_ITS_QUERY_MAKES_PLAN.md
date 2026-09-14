---
id: a-block-records-only-a-request-its-query-makes
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api/test_evidence_packets.py -q
  - npm --prefix apps/web run test:unit -- --run tests/frontend/unit/evidence-packets.test.js tests/frontend/unit/observation-access.test.js
---

# The request a packet hands its reader is the request its block replays

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Investigated, authored and implemented 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/services/evidence_packet_service.py`,
  `apps/api/schemas/evidence_packet.py`,
  `apps/web/lib/observationAccess.ts`, `apps/web/lib/evidencePackets.ts`,
  `apps/web/components/SourceExplorerPage.tsx`,
  `docs/reference/API_CONSUMER_GUIDE.md`

## Context

`ReproducibilityEnvelope` has fourteen fields, and its own docstring divides
them: the ones that duplicate the block's query are cross-checked against it
(the measures, the scope, the release, both reductions, the geography), and
the rest are observations about what the source published, which "the API
second-guessing would substitute its present view for what the composer
actually saw."

`api_query` was in neither list. It is not an observation about a
publication — it is the request itself, written out — and it is the one field
a reader *uses*: `EvidenceEnvelope` renders it under "Reproducible request",
and `packetExport` writes it into the file the packet is handed over as. So
it was the only duplicated request parameter the API never crossed, and it is
the one that spells all the others out.

The failure is on the record. WEB-048, in this repository's own words: the
packet builder hand-built each attached block's document, "it recorded no
reduction, so a map block replayed the source's whole latest publication
while the envelope beside it recorded `newest_per_geography=true` in its
`api_query` — the block did not reproduce the request its own envelope names,
in the one resource whose purpose is that a reader can re-derive the evidence
without this application." That was fixed by building both from one place,
which holds for one client. Storage stayed a back door for the same claim —
the reason API-117 gave for checking a stored filter's *name* and API-123 for
checking its *value*.

And a second instance of it was live. The explorer saves a view two ways. The
account path passes `dimensions: dimensionSelections` into
`explorerDocument`, so a configuration replays with its dimension filters.
The browser path — `saveChart`, which is the store the packet builder
attaches from — recorded no dimensions at all. Only three sources declare a
dimension filter (`CDC`: `adjustment_status`, `stratum_id`; `USDA_NASS`:
`domain_desc`, `domaincat_desc`; `FBI_UCR`: `subject_type`, `subject_code`),
and CDC and USDA NASS both read through the neutral resource, where
`dimensionParams` sends them. So a CDC measure read for one stratum, attached
to a packet, became a block whose query asked for **every stratum the source
publishes** — a different population — under an envelope whose `api_query`
still named the one stratum the argument was composed from. The reader was
handed a file whose recorded request and whose replay answer different
questions, and `validation.valid` was `true`.

## What was changed

Two halves, in this order, because the second would otherwise refuse a packet
the first still produces.

**The client records what its request carried** (WEB-081).
`dimensionsCarriedBy(request, dimensionFilters)` reads the dimension
narrowing back from the built request rather than from the selection it came
from — the idiom already used for the reduction (`viewedNewestPerGeography`)
and the state (`viewedStateFips`), and the accurate one: `dimensionParams`
sends only the names the capability declares under this scope, so a stratum
chosen under `scope=as_released` and still selected after the reader returns
to the latest publication is not part of what the map shows. Both save paths
record that, and `documentFromSavedChart` carries it into the block's query
under the source's own declared filter names. A view that recorded none asks
for none: an absent field is not "every stratum" filled in by the builder.

**The API crosses the recorded request** (API-129).
`_recorded_request_contradiction` parses `api_query`'s query string and
crosses every parameter the document determines: the three measure fields,
the scope, the release, both reductions, and every filter
`OBSERVATION_FILTER_BOUNDS` declares — read from there, so a filter added to
the resource is crossed here without being named again — with `geo_level`
compared through `normalize_geo_level`, as the envelope's own grain already
is.

The comparison is asymmetric, and that is the substance of the rule:

- A parameter the recorded request names and the query does not ask for is
  refused. The query would answer a **wider** set than the reader's own
  request returns, so the block does not reproduce its own numbers.
- A filter the query adds is not. `api_query` records the request the view
  issued, and a block narrating one geography of a map carries `geo_id` in
  its query where the map's request carried none — which is exactly what the
  envelope's `geo_id` field records, and why the existing geography check is
  written as "only when both sides name one".
- The resource path is never compared. One document is legitimately served
  by more than one path: the source-scoped `/{source}/observations/latest`
  and the neutral `/observations` answer the same question, and the
  comparison workspace records `/comparison/preflight` until the pair is
  comparable. A path rule would refuse requests that do reproduce the block.
- A string with no query is not read as a request. An unfilled `api_query` is
  incompleteness, which `_validation_state` already reports, and a rule that
  guessed at prose could refuse a block for something the composer never
  said. The boolean tokens are the same set the request layer accepts; a
  token outside it is not read rather than guessed.

`ReproducibilityEnvelope`'s docstring was wrong in a second way and is
corrected: it listed `source_codes` among the fields *not* cross-checked,
though `_stray_sources` has crossed it since API-113.

## Validation

`tests/unit/api/test_evidence_packets.py`:

- `test_a_block_cannot_record_a_request_its_query_does_not_make` — six
  parameters, each refused with the block named: another measure, a
  reduction, another publication, a release the query does not pin, another
  grain, and a stratum the query would replay past.
- `test_a_recorded_request_that_reproduces_its_block_is_stored` — six the
  refusal must not touch: paging bounds, a grain alias beside its vocabulary
  word, a query narrower than the recorded request, a source-scoped path for
  a neutral document, prose, and an empty value.
- `test_a_request_a_stored_block_cannot_reproduce_is_reported_on_read` — a
  divergence stored before the rule reads as invalid with the reason, and the
  composer's document comes back unmodified.

`tests/frontend/unit/observation-access.test.js` and
`evidence-packets.test.js`:

- `a saved view records the stratum its request carried, not its selection` —
  the CDC request carries it and is recorded; the same selection against a
  source declaring no such filter is dropped from the request and therefore
  recorded as no narrowing.
- `a stratified view's document carries the stratum its request named` and
  `a view that recorded no stratum asks for none`.

`tests/frontend/browser/explorer.spec.js` —
`a saved stratified view records the stratum it was read for` proves the
component wiring the unit tier cannot reach: narrow CDC to one stratum,
then save each way. Signed in, the configuration's document carries
`stratum_id`; signed out, the browser store's chart carries
`dimensions: {stratum_id: "overall"}` *and* an `apiQuery` naming the same
stratum -- which is the pair API-129 crosses.

Each was proved by breaking it back:

- With `_contradiction` returning `None` instead of the new check, all six
  refusals and the read-back test fail.
- Without `dimensions: savedDimensions(chart)` in
  `documentFromSavedChart`: `expected { geo_level: 'STATE' } to deeply equal
  { geo_level: 'STATE', stratum_id: 'female-45-54' }`.
- Without `dimensions: viewedDimensions` on the browser-saved chart, the
  browser spec fails on the store itself: `Expected: {"stratum_id":
  "overall"} Received: undefined` -- which is the defect exactly as it
  stood, since the account save in the same test passed throughout.

One existing catalog row needed a correction rather than a change of
behaviour: the `query-the-live-contracts-refuse` case built a block whose
document asked for `NO:SUCH:METRIC` while its envelope's default `api_query`
still named `FRED:UNRATE`. The new check caught that first, which is correct
— the fixture was internally inconsistent — so the fixture now records the
same measure in both and the case still reaches the live-contract refusal it
exists for.

## Deliberately not done

- **A chart saved before this change is not repaired.** Its `dimensions` are
  absent, so its block's query carries no stratum and the API now refuses to
  store it, naming the filter the recorded request names. Recovering the
  filter by parsing the old `api_query` was considered and declined: it would
  put this client's guess about an old record where the composer's own
  selection belongs, and the refusal names exactly what to do — re-save the
  view. Silently storing a block that replays a different population is the
  defect, not the remedy.
- **`period`, `units`, `transformation` and `caveats` stay uncrossed.** They
  are observations about what the source published when the block was
  composed, and the envelope's docstring already says why the API must not
  substitute its present view for them.
- **The comparison workspace's `api_query` is unchanged.** Its parameters and
  its document are built from one selection by two functions that drop
  `state_fips` at the national grain identically, so the two already agree;
  the new check reads it without needing anything from it.
