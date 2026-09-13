import { describe, expect, test } from "vitest";

// Covers: WEB-031 — evidence packets on the account. The one translation
// between the composer's packet and the API's document carries every
// recorded envelope field verbatim in both directions, drops a query or
// envelope from prose (which the API refuses), and merges the API's
// per-block verdicts with the client's own issue report so that stale (a
// measure retired since the block was composed) and incomplete (a field the
// composer never filled) stay distinct.

import {
  documentToPacket,
  mergeBlockStates,
  packetExport,
  packetToDocument,
} from "../../../apps/web/lib/evidencePackets";

const envelope = {
  metricCodes: ["CENSUS_ACS:acs5:B01003_001"],
  sourceCodes: ["CENSUS_ACS"],
  geoId: "state:55|county:025",
  geoLevel: "COUNTY",
  scope: "as_released",
  release: "2022",
  period: "2022",
  units: "people",
  transformation: "none",
  apiQuery: "/api/v1/observations?scope=as_released&release=2022",
  caveats: ["ACS estimates carry a margin of error"],
};

const query = {
  kind: "observations",
  metric_code: "CENSUS_ACS:acs5:B01003_001",
  scope: "as_released",
  release: "2022",
  filters: { geo_level: "COUNTY", geo_id: "state:55|county:025" },
};

const packet = {
  version: 1,
  title: "Needs assessment",
  purpose: "Describe the need",
  updatedAt: "2026-09-12T00:00:00Z",
  blocks: [
    { id: "summary", type: "text", title: "Summary", content: "The need." },
    { id: "evidence", type: "analysis", title: "Population", content: "Dane", envelope, document: query },
    { id: "condition", type: "analysis", title: "Condition" },
    { id: "limits", type: "caveat", title: "Limits", content: "Associations." },
  ],
};

describe("the packet crosses the account boundary", () => {
  test("every recorded envelope field survives a round trip verbatim", () => {
    const document = packetToDocument(packet);
    expect(document.schema_version).toBe(1);
    expect(document.blocks.map((block) => block.block_id)).toEqual([
      "summary",
      "evidence",
      "condition",
      "limits",
    ]);
    const stored = document.blocks[1];
    expect(stored.envelope).toEqual({
      metric_codes: ["CENSUS_ACS:acs5:B01003_001"],
      source_codes: ["CENSUS_ACS"],
      geo_id: "state:55|county:025",
      geo_level: "COUNTY",
      scope: "as_released",
      release: "2022",
      period: "2022",
      units: "people",
      transformation: "none",
      api_query: "/api/v1/observations?scope=as_released&release=2022",
      caveats: ["ACS estimates carry a margin of error"],
    });
    expect(stored.document).toEqual(query);

    const back = documentToPacket(document, "2026-09-12T01:00:00Z");
    expect(back.blocks[1].envelope).toEqual(envelope);
    expect(back.blocks[1].document).toEqual(query);
    expect(back.updatedAt).toBe("2026-09-12T01:00:00Z");
    // An empty analytical block stays empty in both directions, so the API
    // reports the same gap packetIssues does rather than a guess filling it.
    expect(document.blocks[2].envelope).toBeUndefined();
    expect(back.blocks[2].envelope).toBeUndefined();
  });

  test("a prose block never carries a query or an envelope to the API", () => {
    // The API refuses a caveat that carries a query. A stray envelope on a
    // text block is a composer bug, not content worth preserving.
    const stray = {
      ...packet,
      blocks: [{ id: "limits", type: "caveat", title: "Limits", envelope, document: query }],
    };
    const [block] = packetToDocument(stray).blocks;
    expect(block.envelope).toBeUndefined();
    expect(block.document).toBeUndefined();
    expect(block.content).toBe("");
  });

  test("no observation value is ever in the stored document", () => {
    const serialized = JSON.stringify(packetToDocument(packet));
    expect(serialized).not.toContain("561504");
    expect(serialized).not.toMatch(/"value"/);
  });
});

// The API's read-time verdict for the packet above: one block stale, one
// incomplete. Shared by the merge nodes and the export nodes below.
const validation = {
  valid: false,
  reason: "2 of 2 analytical blocks cannot be read as evidence",
  blocks: [
    { block_id: "evidence", valid: false, reason: "metric_code 'CENSUS_ACS:acs5:B01003_001' is not a published metric", missing: [] },
    {
      block_id: "condition",
      valid: false,
      reason: "this block presents no analysis yet, so it carries no reproducibility envelope",
      missing: ["metric_codes", "source_codes", "geo_id", "period", "api_query"],
    },
  ],
};

describe("merging the API's per-block verdicts", () => {

  test("stale and incomplete are different facts and stay distinct", () => {
    const states = mergeBlockStates(packet, validation);
    const byId = Object.fromEntries(states.map((state) => [state.blockId, state]));
    // Only the API can see a retired measure: the client's own report finds
    // nothing wrong with the filled block, so the API's verdict wins.
    expect(byId.evidence.state).toBe("stale");
    expect(byId.evidence.reason).toContain("not a published metric");
    expect(byId.evidence.missing).toEqual([]);
    // The empty block is incomplete, which the composer can fix.
    expect(byId.condition.state).toBe("incomplete");
    expect(byId.condition.missing).toContain("apiQuery");
  });

  test("without an API verdict the client's own report stands", () => {
    const states = mergeBlockStates(packet, null);
    const byId = Object.fromEntries(states.map((state) => [state.blockId, state]));
    // `unchecked`, not `ok`: the client found nothing wrong with this block
    // and nobody asked the API, which is not the same fact as the API
    // saying it is valid. This node used to assert `ok`, and that
    // conflation reached the exported file (WEB-058).
    expect(byId.evidence.state).toBe("unchecked");
    expect(byId.condition.state).toBe("incomplete");
    expect(states.map((state) => state.blockId)).toEqual(["evidence", "condition"]);
  });
});

// Covers: WEB-058 — the file a reader is handed carries the API's verdict.
//
// ADR-0004's distinction is that a packet leaves the building. The article
// renders the API's failed blocks under "The API reports these blocks can no
// longer be replayed as composed"; the export had one state column,
// `live_or_frozen`, which reports the block's scope and says nothing about
// whether the API can still serve it.
describe("the exported packet says which blocks can no longer be replayed", () => {
  const column = (headings, rows, blockId, name) =>
    rows.find((row) => row[headings.indexOf("block_id")] === blockId)?.[
      headings.indexOf(name)
    ];

  test("a stale block's verdict and reason travel in the file", () => {
    const states = mergeBlockStates(packet, validation);
    const { headings, rows } = packetExport(packet, states);
    expect(headings).toContain("replay_state");
    expect(headings).toContain("replay_reason");
    // The scope column is unchanged: live-or-frozen and replayable-or-stale
    // are different facts about the block.
    expect(column(headings, rows, "evidence", "live_or_frozen")).toContain("frozen to release");
    expect(column(headings, rows, "evidence", "replay_state")).toBe("stale");
    expect(column(headings, rows, "evidence", "replay_reason")).toContain(
      "not a published metric",
    );
    expect(column(headings, rows, "condition", "replay_state")).toBe("incomplete");
  });

  test("no API verdict exports as not checked, never as replayable", () => {
    // A packet read from the browser draft is never checked by the API, and
    // the packet contract says an absent verdict means "not checked", never
    // "valid".
    const states = mergeBlockStates(packet, null);
    const { headings, rows } = packetExport(packet, states);
    expect(column(headings, rows, "evidence", "replay_state")).toBe("not checked");
    expect(column(headings, rows, "evidence", "replay_reason")).toBe("");
  });

  test("an unchecked block is not the same state as a checked valid one", () => {
    const unchecked = mergeBlockStates(packet, null);
    expect(unchecked.find((state) => state.blockId === "evidence").state).toBe("unchecked");

    const checked = mergeBlockStates(packet, {
      valid: true,
      blocks: [{ block_id: "evidence", valid: true, missing: [] }],
    });
    expect(checked.find((state) => state.blockId === "evidence").state).toBe("ok");
  });

  test("a prose block carries no replay verdict, as it carries no scope", () => {
    const states = mergeBlockStates(packet, validation);
    const { headings, rows } = packetExport(packet, states);
    expect(column(headings, rows, "summary", "live_or_frozen")).toBe("");
    expect(column(headings, rows, "summary", "replay_state")).toBe("");
  });
});
