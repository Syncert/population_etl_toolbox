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
  documentFromSavedChart,
  envelopeFromSavedChart,
  grantNeedsTemplate,
  isAnalyticalBlock,
  packetExport,
  packetIsComplete,
  packetIssues,
} from "../../../apps/web/lib/evidencePackets";

const envelope = {
  metricCodes: ["CENSUS_ACS:acs5:B01003_001"],
  sourceCodes: ["CENSUS_ACS"],
  geoId: "state:55|county:025",
  geoLevel: "COUNTY",
  scope: "latest",
  release: "",
  period: "2023",
  units: "people",
  transformation: "none",
  apiQuery: "/api/v1/observations?metric_code=CENSUS_ACS%3Aacs5%3AB01003_001",
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
        document: { kind: "observations", metric_code: "CENSUS_ACS:acs5:B01003_001", filters: {} },
      }),
    ).toContain("metric=CENSUS_ACS%3Aacs5%3AB01003_001");
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

  test("a view that captured no period records none, and the packet says so", () => {
    // Covers: WEB-069 — the period fell back to `savedAt`, the moment
    // someone pressed save. No producer wrote `period` at all, so every
    // analytical block attached in the builder claimed a period like
    // `2026-09-13T12:41:03.117Z` -- a timestamp as the period of an annual
    // estimate -- and `packetIssues` saw a filled field and reported the
    // packet complete. A comparison is the case that legitimately has none:
    // it carries two periods, one per side, which WEB-049 exists to keep
    // visible.
    const built = envelopeFromSavedChart({
      metricCode: "A",
      source: "CENSUS_ACS",
      savedAt: "2026-09-13T12:41:03.117Z",
    });
    expect(built.period).toBe("");
    expect(JSON.stringify(built)).not.toContain("2026-09-13T12:41:03");

    const packet = {
      version: 1,
      title: "T",
      purpose: "P",
      blocks: [
        { id: "b", type: "analysis", title: "B", envelope: built },
        { id: "m", type: "methodology", title: "M", content: "how" },
      ],
      updatedAt: "2026-09-13T00:00:00Z",
    };
    const [issue] = packetIssues(packet);
    expect(issue.blockId).toBe("b");
    expect(issue.missing).toContain("period");
  });

  test("a period the view did capture travels as it was captured", () => {
    const built = envelopeFromSavedChart({
      metricCode: "A",
      source: "CENSUS_ACS",
      period: "2023",
      savedAt: "2026-09-13T12:41:03.117Z",
    });
    expect(built.period).toBe("2023");
  });

  test("the export carries each block's full envelope and live status", () => {
    const exported = packetExport(completePacket);
    expect(exported.headings).toContain("api_query");
    expect(exported.headings).toContain("caveats");
    expect(exported.headings).toContain("live_or_frozen");
    expect(exported.filename).toBe("needs-assessment-evidence.csv");

    // Read by heading rather than by position: the columns moved when the
    // export gained the API's replay verdict (WEB-058), and a positional
    // assertion would have read whichever column landed last.
    const cell = (row, name) => row[exported.headings.indexOf(name)];
    const analysisRow = exported.rows.find((row) => row[1] === "evidence");
    expect(analysisRow).toContain("CENSUS_ACS:acs5:B01003_001");
    expect(analysisRow).toContain("CENSUS_ACS");
    expect(analysisRow).toContain("2023");
    expect(analysisRow).toContain("ACS estimates carry a margin of error");
    expect(cell(analysisRow, "live_or_frozen")).toBe("live");
    // Exported with no verdict passed, so the file says nobody checked.
    expect(cell(analysisRow, "replay_state")).toBe("not checked");

    // A narrative block exports its prose and no invented envelope.
    const textRow = exported.rows.find((row) => row[1] === "intro");
    expect(textRow[4]).toBe("The need is...");
    expect(cell(textRow, "live_or_frozen")).toBe("");
    expect(cell(textRow, "replay_state")).toBe("");
    expect(packetExport(null).rows).toEqual([]);
  });
});

describe("a block replays the request its envelope records", () => {
  // Covers: WEB-048 — the packet builder used to hand-build an
  // `AnalysisDocument` for each attached view, which is a second construction
  // of a document `explorerDocument` already knows how to build, and a weaker
  // one: it recorded no reduction, so a map block replayed the source's whole
  // latest publication while the envelope beside it recorded
  // `newest_per_geography=true` in `api_query`.

  test("a map view's document carries the reduction its envelope shows", () => {
    const document = documentFromSavedChart({
      metricCode: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
      source: "CENSUS_PEP",
      geoLevel: "COUNTY",
      stateFips: "55",
      newestPerGeography: true,
      apiQuery: "/api/v1/observations?metric_code=…&newest_per_geography=true",
    });
    expect(document.kind).toBe("observations");
    expect(document.newest_per_geography).toBe(true);
    expect(document.scope).toBe("latest");
    expect(document.filters).toEqual({ geo_level: "COUNTY", state_fips: "55" });
  });

  test("the envelope records the reduction its document asks for", () => {
    // Covers: WEB-071 — the envelope carried `scope` and `release`, the two
    // other duplicated request parameters, and not the reduction. A map block
    // composed from `newest_per_geography=true` records the one period it
    // showed; the API stored it beside a document whose reduction sat at its
    // default, and the block replayed every estimated year of the vintage
    // under an envelope declaring one period. The API cross-checks the two,
    // so the builder reads the reduction once and both sides carry it.
    const chart = {
      metricCode: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
      source: "CENSUS_PEP",
      geoLevel: "COUNTY",
      stateFips: "55",
      period: "2024-07-01",
      newestPerGeography: true,
      apiQuery: "/api/v1/observations?metric_code=…&newest_per_geography=true",
    };
    const built = envelopeFromSavedChart(chart);
    expect(built.newestPerGeography).toBe(true);
    expect(built.newestReleasePerPeriod).toBe(false);
    expect(documentFromSavedChart(chart).newest_per_geography).toBe(true);

    const settled = {
      metricCode: "CENSUS_ACS:acs5:B01003_001",
      source: "CENSUS_ACS",
      scope: "as_released",
      newestReleasePerPeriod: true,
    };
    expect(envelopeFromSavedChart(settled).newestReleasePerPeriod).toBe(true);
    expect(documentFromSavedChart(settled).newest_release_per_period).toBe(true);

    // A view that recorded no reduction asked for none, on both sides, which
    // is what a chart saved before the explorer recorded one says about
    // itself.
    const plain = envelopeFromSavedChart({ metricCode: "M", source: "FRED" });
    expect(plain.newestPerGeography).toBe(false);
    expect(plain.newestReleasePerPeriod).toBe(false);
  });

  test("a stratified view's document carries the stratum its request named", () => {
    // Covers: WEB-081 — the explorer's account save passed its dimension
    // selection into `explorerDocument`; the browser save, which is the store
    // the packet builder attaches from, recorded no dimensions at all. So a
    // CDC measure read for one stratum -- or a NASS one for one domain --
    // attached to a packet as a document asking for every stratum the source
    // publishes, which is a different population, while the envelope's
    // `api_query` beside it still named the one the block was composed from.
    // The reader was handed a file whose recorded request and whose replay
    // answer different questions.
    const document = documentFromSavedChart({
      metricCode: "CDC:nvss:INFANT_MORTALITY",
      source: "CDC",
      geoLevel: "STATE",
      dimensions: { stratum_id: "female-45-54", adjustment_status: "" },
      apiQuery:
        "/api/v1/observations?metric_code=CDC%3Anvss%3AINFANT_MORTALITY"
        + "&geo_level=STATE&stratum_id=female-45-54",
    });
    expect(document.filters).toEqual({
      geo_level: "STATE",
      stratum_id: "female-45-54",
    });
  });

  test("a view that recorded no stratum asks for none", () => {
    // Covers: WEB-081 — an absent field is not "every stratum" filled in by
    // this builder. A chart saved before the explorer recorded its
    // dimensions carries none, and the API refuses a block whose recorded
    // request names a filter its query does not ask for, so such a view is
    // re-saved rather than replayed wider in silence.
    expect(
      documentFromSavedChart({ metricCode: "M", geoLevel: "STATE" }).filters,
    ).toEqual({ geo_level: "STATE" });
    // Not an object, and a non-string value, are both no filter rather than
    // a filter the API would refuse.
    expect(
      documentFromSavedChart({ metricCode: "M", dimensions: ["stratum_id"] }).filters,
    ).toEqual({});
    expect(
      documentFromSavedChart({ metricCode: "M", dimensions: { stratum_id: 7 } }).filters,
    ).toEqual({});
  });

  test("a release without an as-released scope is dropped, not stored", () => {
    // `validate_document` refuses `release` under `scope=latest`, so the hand
    // built literal produced a block the API would not accept.
    const document = documentFromSavedChart({
      metricCode: "M",
      release: "2023",
    });
    expect(document.scope).toBe("latest");
    expect(document.release).toBeNull();

    const released = documentFromSavedChart({
      metricCode: "M",
      scope: "as_released",
      release: "2023",
    });
    expect(released.scope).toBe("as_released");
    expect(released.release).toBe("2023");
  });

  test("a two-measure view is a comparison document", () => {
    const document = documentFromSavedChart({
      metricCode: "A",
      metricCodeB: "B",
      geoLevel: "COUNTY",
      stateFips: "55",
    });
    expect(document.kind).toBe("comparison");
    expect(document.metric_code_a).toBe("A");
    expect(document.metric_code_b).toBe("B");
    // The comparison route serves no scope, so its document records none and
    // the envelope's default `latest` cannot contradict it.
    expect(document.scope).toBeUndefined();
  });

  test("a chart saved before this change attaches exactly as it does today", () => {
    const document = documentFromSavedChart({
      metricCode: "CENSUS_ACS:acs5:B01003_001",
      geoLevel: "COUNTY",
      geoId: "state:55|county:025",
    });
    expect(document.newest_per_geography).toBe(false);
    expect(document.newest_release_per_period).toBe(false);
    expect(document.scope).toBe("latest");
    expect(document.release).toBeNull();
    expect(document.filters).toEqual({
      geo_level: "COUNTY",
      geo_id: "state:55|county:025",
    });
  });
});
