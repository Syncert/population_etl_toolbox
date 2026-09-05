import { describe, expect, test } from "vitest";

// Covers: WEB-024 — source coverage and data quality. The warehouse
// publishes the quality signal and the API serves it; this client presents
// it and never recomputes it. There is no universal score, current/stale/
// retired/unknown stay four distinct facts, a source with no published
// publication time never reads as healthy, and evidence the API publishes
// elsewhere is pointed at rather than fabricated here.

import {
  EVIDENCE_LOCATIONS,
  UNPUBLISHED_EVIDENCE,
  coverageSegments,
  freshnessRow,
  freshnessRows,
  metricQualityRows,
} from "../../../apps/web/lib/dataQuality";

const healthy = {
  source_code: "CENSUS_ACS",
  metric_count: 10,
  current_count: 10,
  stale_count: 0,
  retired_count: 0,
  latest_publication_time: "2026-09-01T00:00:00Z",
  latest_harvested_at: "2026-09-02T00:00:00Z",
};

describe("published counts are presented, never scored", () => {
  test("a fully current source reports its counts and reads as ok", () => {
    const row = freshnessRow(healthy);
    expect(row.state).toBe("ok");
    expect(row.summary).toBe("10 current of 10 published metrics");
    expect(row.unclassifiedCount).toBe(0);
    // No score, index, grade, or percentage stands in for the counts.
    expect(row.summary).not.toMatch(/%|score|grade/i);
  });

  test("any stale metric is a caution naming how many", () => {
    const row = freshnessRow({ ...healthy, current_count: 7, stale_count: 3 });
    expect(row.state).toBe("warn");
    expect(row.summary).toBe("3 stale of 10 published metrics");
  });

  test("metrics the rollup left unclassified are surfaced, not folded in", () => {
    // 10 published, 6 current, 1 retired: three metrics carry no published
    // freshness state at all. A metric whose state the warehouse did not
    // publish is not thereby current.
    const row = freshnessRow({ ...healthy, current_count: 6, stale_count: 0, retired_count: 1 });
    expect(row.unclassifiedCount).toBe(3);
    expect(row.state).toBe("warn");
    expect(row.summary).toContain("no published freshness state");
  });

  test("a source with no published publication time never reads as healthy", () => {
    const row = freshnessRow({ ...healthy, latest_publication_time: null });
    expect(row.state).toBe("idle");
    expect(row.summary).toContain("no publication time published");
    // And a source with nothing published is a fourth distinct state.
    const empty = freshnessRow({ ...healthy, metric_count: 0, current_count: 0 });
    expect(empty.state).toBe("idle");
    expect(empty.summary).toBe("no metrics published for this source");
  });

  test("malformed counts degrade to zero rather than to a wrong total", () => {
    const row = freshnessRow({ source_code: "X", metric_count: "not a number" });
    expect(row.metricCount).toBe(0);
    expect(row.unclassifiedCount).toBe(0);
    expect(freshnessRow(null).sourceCode).toBe("");
    expect(freshnessRows(null)).toEqual([]);
    expect(freshnessRows([healthy, { ...healthy, source_code: "BLS" }]).map((r) => r.sourceCode)).toEqual([
      "BLS",
      "CENSUS_ACS",
    ]);
  });
});

describe("the coverage bar shows counts, not a measurement", () => {
  test("segments carry their own published counts", () => {
    const segments = coverageSegments(
      freshnessRow({ ...healthy, current_count: 6, stale_count: 3, retired_count: 1 }),
    );
    expect(segments.map((segment) => [segment.label, segment.count])).toEqual([
      ["current", 6],
      ["stale", 3],
      ["retired", 1],
    ]);
    // Shares size the bar only; each segment is readable as its own count.
    expect(segments[0].share).toBeCloseTo(0.6);
    expect(segments.reduce((sum, segment) => sum + segment.count, 0)).toBe(10);
  });

  test("a source with nothing published renders no bar", () => {
    expect(coverageSegments(freshnessRow({ ...healthy, metric_count: 0 }))).toEqual([]);
    expect(coverageSegments(null)).toEqual([]);
  });
});

describe("per-metric quality is the publisher's own", () => {
  test("unpublished fields stay empty rather than becoming placeholders", () => {
    const [row] = metricQualityRows([
      {
        metric_code: "ACS:acs5:B01003_001",
        metric_display_name: "Total population",
        freshness_state: "fresh",
        publication_time: "2026-09-01T00:00:00Z",
      },
    ]);
    expect(row.freshness).toBe("fresh");
    // Never a "Pending" or a guessed value where the publisher published none.
    expect(row.harvestedAt).toBe("");
    expect(row.watermark).toBe("");
    expect(row.contractVersion).toBe("");
    expect(metricQualityRows(null)).toEqual([]);
  });
});

describe("evidence the API publishes elsewhere is pointed at, not fabricated", () => {
  test("each kind of quality evidence names its real publisher", () => {
    const kinds = EVIDENCE_LOCATIONS.map((entry) => entry.kind);
    expect(kinds).toContain("Revisions and as-released values");
    expect(kinds).toContain("Suppression and missing values");
    expect(kinds).toContain("Reporting participation");
    expect(kinds).toContain("Definition and contract changes");
    for (const entry of EVIDENCE_LOCATIONS) {
      expect(entry.publishedBy).toBeTruthy();
      expect(entry.inspectHere).toBeTruthy();
      expect(entry.meaning).toBeTruthy();
    }
    // Each meaning states what the evidence is not, so a null is never read
    // as a zero.
    const suppression = EVIDENCE_LOCATIONS.find((entry) =>
      entry.kind.startsWith("Suppression"),
    );
    expect(suppression.meaning).toContain("never a zero");
    const participation = EVIDENCE_LOCATIONS.find((entry) =>
      entry.kind.startsWith("Reporting"),
    );
    expect(participation.meaning).toContain("not zero crime");
  });

  test("what the API does not publish is stated rather than invented", () => {
    expect(UNPUBLISHED_EVIDENCE.join(" ")).toContain("quality score");
    expect(UNPUBLISHED_EVIDENCE.join(" ")).toContain("client-authored judgement");
    expect(UNPUBLISHED_EVIDENCE.length).toBeGreaterThan(0);
  });
});
