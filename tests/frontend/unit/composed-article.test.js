import { describe, expect, test } from "vitest";

// Covers: WEB-030 — the articles route is a reading surface over composed
// evidence blocks, not a hand-written example. Reading a stored composition
// back keeps three outcomes apart (nothing composed, composed and readable,
// stored but unreadable), never invents a value the composer did not record,
// and never computes an analytical value of its own.

import {
  packetIssues,
  packetIsComplete,
  readComposedPacket,
} from "../../../apps/web/lib/evidencePackets";

const storedEnvelope = {
  metricCodes: ["CENSUS_ACS:acs5:B01003_001"],
  sourceCodes: ["CENSUS_ACS"],
  geoId: "state:55|county:025",
  geoLevel: "COUNTY",
  scope: "latest",
  release: "",
  newestPerGeography: true,
  newestReleasePerPeriod: false,
  period: "2023",
  units: "people",
  transformation: "none",
  apiQuery: "/api/v1/observations?metric_code=CENSUS_ACS%3Aacs5%3AB01003_001",
  caveats: ["ACS estimates carry a margin of error"],
};

function stored(overrides = {}) {
  return JSON.stringify({
    version: 1,
    title: "Needs assessment",
    purpose: "Describe the need",
    blocks: [
      { id: "intro", type: "text", title: "Summary", content: "The need is..." },
      { id: "evidence", type: "analysis", title: "Population context", envelope: storedEnvelope },
    ],
    updatedAt: "2026-09-12T00:00:00Z",
    ...overrides,
  });
}

describe("reading a stored composition", () => {
  test("nothing composed and an unreadable composition are different facts", () => {
    // Collapsing these into one empty state would tell a reader whose draft
    // this build cannot parse that their work is gone.
    const absent = readComposedPacket(null);
    expect(absent.state).toBe("empty");
    expect(absent.packet).toBeNull();
    expect(absent.reason).toContain("nothing has been composed");

    const broken = readComposedPacket("{not json");
    expect(broken.state).toBe("unreadable");
    expect(broken.packet).toBeNull();
    expect(broken.reason).toContain("not readable as a packet");

    expect(readComposedPacket("").state).toBe("empty");
  });

  test("a composition from another composer version is refused, not partially read", () => {
    // A shape this build does not know could carry blocks whose context it
    // would present wrongly, which is worse than presenting nothing.
    const future = readComposedPacket(JSON.stringify({ version: 2, blocks: [] }));
    expect(future.state).toBe("unreadable");
    expect(future.packet).toBeNull();
    expect(future.reason).toContain("different version");

    expect(readComposedPacket(JSON.stringify({ version: 1 })).state).toBe("unreadable");
    expect(readComposedPacket(JSON.stringify([])).state).toBe("unreadable");
  });

  test("a readable composition keeps every recorded envelope field verbatim", () => {
    const read = readComposedPacket(stored());
    expect(read.state).toBe("ready");
    expect(read.unsupported).toEqual([]);
    expect(read.packet.title).toBe("Needs assessment");
    const evidence = read.packet.blocks.find((block) => block.id === "evidence");
    // The envelope is the reason the block can be read as evidence at all,
    // so nothing in it is re-derived on the way to the page.
    expect(evidence.envelope).toEqual(storedEnvelope);
    expect(packetIssues(read.packet)).toEqual([]);
    expect(packetIsComplete(read.packet)).toBe(true);
  });

  test("a block type this build cannot present is named rather than dropped", () => {
    const read = readComposedPacket(
      stored({
        blocks: [
          { id: "intro", type: "text", title: "Summary", content: "The need is..." },
          { id: "future", type: "forecast", title: "Projected demand" },
        ],
      }),
    );
    // A reader who cannot see a block must at least know it was there; a
    // silently dropped block makes the composition look like it never had it.
    expect(read.state).toBe("ready");
    expect(read.unsupported).toEqual(["Projected demand"]);
    expect(read.packet.blocks.map((block) => block.id)).toEqual(["intro"]);
    expect(read.reason).toContain("cannot present");
  });

  test("a malformed envelope is reduced to what it recorded, so the gap is reported", () => {
    const read = readComposedPacket(
      stored({
        blocks: [
          {
            id: "evidence",
            type: "analysis",
            title: "Population context",
            envelope: { metricCodes: ["CENSUS_ACS:acs5:B01003_001"], period: 2023, caveats: "none" },
          },
        ],
      }),
    );
    const block = read.packet.blocks[0];
    // A numeric period and a string caveat list are not coerced into text
    // that would read as recorded context; they stay empty so the block is
    // reported as missing them rather than presented as finished evidence.
    expect(block.envelope.period).toBe("");
    expect(block.envelope.caveats).toEqual([]);
    expect(block.envelope.sourceCodes).toEqual([]);
    const issues = packetIssues(read.packet);
    expect(issues).toHaveLength(1);
    expect(issues[0].missing).toEqual(expect.arrayContaining(["sourceCodes", "period", "apiQuery"]));
    expect(packetIsComplete(read.packet)).toBe(false);
  });

  test("an envelope that is not an object leaves the block with no envelope at all", () => {
    const read = readComposedPacket(
      stored({
        blocks: [{ id: "evidence", type: "analysis", title: "Population context", envelope: "2023" }],
      }),
    );
    // Not an envelope with empty fields — no envelope, which is the state
    // packetIssues already reports as "presents no analysis yet".
    expect(read.packet.blocks[0].envelope).toBeUndefined();
    expect(packetIssues(read.packet)[0].reason).toContain("no reproducibility envelope");
  });

  test("an unpinned composition stays live rather than being read as a fixed release", () => {
    const read = readComposedPacket(
      stored({
        blocks: [
          {
            id: "evidence",
            type: "analysis",
            title: "Population context",
            envelope: { ...storedEnvelope, scope: "whenever", release: "2022" },
          },
        ],
      }),
    );
    // An unrecognized scope is the latest publication, never a pinned one:
    // reading it as frozen would present values that can still change as
    // settled at a release the composer never pinned.
    expect(read.packet.blocks[0].envelope.scope).toBe("latest");
  });
});
