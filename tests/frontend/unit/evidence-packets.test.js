import { describe, expect, test } from "vitest";

// Covers: WEB-023 — evidence packets. Composition is where analytical
// context is most easily lost, so every analytical block carries a
// reproducibility envelope, a block missing one is reported rather than
// rendered as finished evidence, live and frozen blocks are distinguished,
// and the export carries enough for the evidence to be re-derived elsewhere.

import {
  ANALYTICAL_BLOCK_TYPES,
  blockLiveStatus,
  blockReopenHref,
  envelopeFromSavedChart,
  grantNeedsTemplate,
  isAnalyticalBlock,
  packetExport,
  packetIsComplete,
  packetIssues,
} from "../../../apps/web/lib/evidencePackets";

const envelope = {
  metricCodes: ["ACS:acs5:B01003_001"],
  sourceCodes: ["CENSUS_ACS"],
  geoId: "state:55|county:025",
  geoLevel: "COUNTY",
  scope: "latest",
  release: "",
  period: "2023",
  units: "people",
  transformation: "none",
  apiQuery: "/api/v1/observations?metric_code=ACS%3Aacs5%3AB01003_001",
  caveats: ["ACS estimates carry a margin of error"],
};

const completePacket = {
  version: 1,
  title: "Needs assessment",
  purpose: "Describe the need",
  blocks: [
    { id: "intro", type: "text", title: "Summary", content: "The need is..." },
    { id: "evidence", type: "analysis", title: "Population context", envelope },
    { id: "method", type: "methodology", title: "Methodology", content: "Sources and periods" },
  ],
  updatedAt: "2026-09-03T00:00:00Z",
};

describe("the grant needs-assessment template", () => {
  test("ships with methodology and limits already present", () => {
    const packet = grantNeedsTemplate("2026-09-03T00:00:00Z");
    const types = packet.blocks.map((block) => block.type);
    // A packet cannot be assembled without them: they are part of the
    // skeleton rather than an appendix added at the end.
    expect(types).toContain("methodology");
    expect(types).toContain("caveat");
    const limits = packet.blocks.find((block) => block.type === "caveat");
    // The closing note states what the measures cannot establish, so the
    // packet never reads as a causal claim.
    expect(limits.content).toContain("do not establish that a program caused");
    expect(limits.content).toContain("associations as associations");
  });

  test("its analytical blocks start empty and are reported as such", () => {
    const packet = grantNeedsTemplate();
    const issues = packetIssues(packet);
    // Two empty analysis slots: the template is a skeleton, and it says so
    // rather than looking finished.
    expect(issues.map((issue) => issue.blockId)).toEqual([
      "population-evidence",
      "condition-evidence",
    ]);
    expect(issues[0].reason).toContain("no reproducibility envelope");
    expect(packetIsComplete(packet)).toBe(false);
  });
});

describe("every analytical block carries its envelope", () => {
  test("only data-presenting blocks need one", () => {
    expect(ANALYTICAL_BLOCK_TYPES).toEqual(["analysis", "table", "map"]);
    expect(isAnalyticalBlock({ type: "analysis" })).toBe(true);
    expect(isAnalyticalBlock({ type: "map" })).toBe(true);
    // Narrative, methodology, and caveat blocks present no provider values.
    expect(isAnalyticalBlock({ type: "text" })).toBe(false);
    expect(isAnalyticalBlock({ type: "methodology" })).toBe(false);
    expect(isAnalyticalBlock(null)).toBe(false);
  });

  test("a complete packet reports no issues", () => {
    expect(packetIssues(completePacket)).toEqual([]);
    expect(packetIsComplete(completePacket)).toBe(true);
    expect(packetIsComplete(null)).toBe(false);
    expect(packetIsComplete({ ...completePacket, blocks: [] })).toBe(false);
  });

  test("a block missing context is named with exactly what it lacks", () => {
    const stripped = {
      ...completePacket,
      blocks: [
        {
          id: "evidence",
          type: "analysis",
          title: "Population context",
          envelope: { ...envelope, sourceCodes: [], period: "", apiQuery: "" },
        },
      ],
    };
    const [issue] = packetIssues(stripped);
    expect(issue.blockId).toBe("evidence");
    expect(issue.missing).toEqual(["sourceCodes", "period", "apiQuery"]);
    // Nothing is filled in on the author's behalf: a guess where their
    // evidence should be is exactly the failure this prevents.
    expect(issue.reason).toContain("without the context needed to read them");
    expect(packetIsComplete(stripped)).toBe(false);
  });
});

describe("live and frozen blocks are distinguished", () => {
  test("a pinned release is frozen and says what that means", () => {
    const frozen = blockLiveStatus({ ...envelope, scope: "as_released", release: "2022" });
    expect(frozen.label).toBe("frozen to release 2022");
    expect(frozen.detail).toContain("will not change when the source republishes");
  });

  test("a latest-scope block is live and never presented as settled", () => {
    const live = blockLiveStatus(envelope);
    expect(live.label).toBe("live");
    expect(live.state).toBe("warn");
    expect(live.detail).toContain("change when the source republishes");
    // An as-released block with no pinned release is still live: every
    // release answers, so nothing is fixed.
    expect(blockLiveStatus({ ...envelope, scope: "as_released", release: "" }).label).toBe("live");
    expect(blockLiveStatus(null).state).toBe("idle");
  });
});

describe("blocks reopen and export with their evidence intact", () => {
  test("a block reopens into the analysis it replays", () => {
    expect(
      blockReopenHref({
        id: "b",
        type: "analysis",
        title: "t",
        document: { kind: "observations", metric_code: "ACS:acs5:B01003_001", filters: {} },
      }),
    ).toContain("metric=ACS%3Aacs5%3AB01003_001");
    expect(blockReopenHref({ id: "b", type: "text", title: "t" })).toBe("/explore");
  });

  test("a saved view carries its own recorded context, and nothing more", () => {
    const built = envelopeFromSavedChart({
      metricCode: "A",
      metricCodeB: "B",
      source: "CENSUS_ACS",
      sourceB: "CENSUS_PEP",
      geoLevel: "COUNTY",
      apiQuery: "/api/v1/comparison?metric_code_a=A",
      caveats: ["units could not be verified"],
      transformation: "api-derived",
    });
    expect(built.metricCodes).toEqual(["A", "B"]);
    expect(built.sourceCodes).toEqual(["CENSUS_ACS", "CENSUS_PEP"]);
    expect(built.transformation).toBe("api-derived");
    expect(built.caveats).toEqual(["units could not be verified"]);
    // A field the saved view never captured stays empty, so the packet can
    // report it rather than a guess filling it in.
    expect(built.geoId).toBe("");
    expect(built.units).toBe("");
    expect(envelopeFromSavedChart(null).metricCodes).toEqual([]);
  });

  test("the export carries each block's full envelope and live status", () => {
    const exported = packetExport(completePacket);
    expect(exported.headings).toContain("api_query");
    expect(exported.headings).toContain("caveats");
    expect(exported.headings).toContain("live_or_frozen");
    expect(exported.filename).toBe("needs-assessment-evidence.csv");

    const analysisRow = exported.rows.find((row) => row[1] === "evidence");
    expect(analysisRow).toContain("ACS:acs5:B01003_001");
    expect(analysisRow).toContain("CENSUS_ACS");
    expect(analysisRow).toContain("2023");
    expect(analysisRow).toContain("ACS estimates carry a margin of error");
    expect(analysisRow.at(-1)).toBe("live");

    // A narrative block exports its prose and no invented envelope.
    const textRow = exported.rows.find((row) => row[1] === "intro");
    expect(textRow[4]).toBe("The need is...");
    expect(textRow.at(-1)).toBe("");
    expect(packetExport(null).rows).toEqual([]);
  });
});
