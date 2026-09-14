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
import {
  servedContractWords,
  servedFieldNames,
  servesPath,
} from "../support/servedContract.js";

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
        metric_code: "CENSUS_ACS:acs5:B01003_001",
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
  test("each kind of evidence is stated with what it is not", () => {
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

  test("each kind of quality evidence names its real publisher", () => {
    // Covers: WEB-065 — this node's name used to be the whole claim: the
    // check was `expect(entry.publishedBy).toBeTruthy()`, so a route retired
    // or a field renamed would leave the screen pointing a reader at a
    // surface that no longer exists and nothing would fail. It is WEB-043's
    // own finding one layer up — "a fixture that models a weaker API than
    // the one that ships does not fail; it quietly stops testing the
    // behaviour it names" — and the shape of the two unfailable guards
    // WEB-051 and WEB-053 left behind.
    const fields = servedFieldNames();
    const words = servedContractWords();

    for (const entry of EVIDENCE_LOCATIONS) {
      expect(entry.publishedBy, entry.kind).toBeTruthy();
      expect(entry.inspectHere, entry.kind).toBeTruthy();
      expect(entry.meaning, entry.kind).toBeTruthy();

      // A path the prose gives relative to the versioned root, as the guide
      // writes them, must be a route the contract actually serves.
      const paths = entry.publishedBy.match(/\/[a-z0-9/_-]+/g) || [];
      for (const path of paths) {
        expect(servesPath(`/api/v1${path}`), `${entry.kind} names ${path}`).toBe(
          true,
        );
      }

      // Every snake_case name it uses is one the contract uses: a field
      // (`value_status`) or a value a parameter accepts (`as_released`).
      const named = entry.publishedBy.match(/[a-z][a-z0-9]*(?:_[a-z0-9]+)+/g) || [];
      for (const name of named) {
        expect(words.has(name), `${entry.kind} names ${name}`).toBe(true);
      }

      // And it names something concrete. Without this, prose naming nothing
      // would satisfy both checks above by having nothing to check.
      const concrete =
        paths.length > 0 ||
        entry.publishedBy
          .split(/[^A-Za-z0-9_]+/)
          .some((word) => fields.has(word));
      expect(concrete, `${entry.kind} names no published field or route`).toBe(true);
    }
  });

  test("what the API does not publish is stated rather than invented", () => {
    expect(UNPUBLISHED_EVIDENCE.join(" ")).toContain("quality score");
    expect(UNPUBLISHED_EVIDENCE.join(" ")).toContain("client-authored judgement");
    expect(UNPUBLISHED_EVIDENCE.length).toBeGreaterThan(0);
  });
});

// Covers: WEB-041 — the sample can contain the problem the screen reports.
// The table showed the alphabetically first forty of up to 2,487 metrics, so
// a source reported as "12 stale of 2,487" showed forty rows that almost
// certainly held none of the twelve.
describe("the per-measure sample is ordered by the state a reader came for", () => {
  const metric = (code, state) => ({
    metric_code: code,
    metric_display_name: code,
    freshness_state: state,
  });

  test("stale first, then unpublished, then retired, then current", () => {
    const rows = metricQualityRows([
      metric("A:current", "current"),
      metric("B:retired", "retired"),
      metric("C:unpublished", ""),
      metric("D:stale", "stale"),
    ]);
    expect(rows.map((row) => row.metricCode)).toEqual([
      "D:stale",
      "C:unpublished",
      "B:retired",
      "A:current",
    ]);
  });

  test("within one state the order is the metric code, so two loads agree", () => {
    const rows = metricQualityRows([
      metric("Z:stale", "stale"),
      metric("A:stale", "stale"),
      metric("M:stale", "stale"),
    ]);
    expect(rows.map((row) => row.metricCode)).toEqual(["A:stale", "M:stale", "Z:stale"]);
  });

  test("a state the vocabulary adds later sorts last, and nothing is dropped", () => {
    const rows = metricQualityRows([
      metric("A:future", "quarantined"),
      metric("B:stale", "stale"),
      metric("C:current", "current"),
    ]);
    expect(rows).toHaveLength(3);
    expect(rows.map((row) => row.metricCode)).toEqual([
      "B:stale",
      "C:current",
      "A:future",
    ]);
    // The published word travels verbatim; nothing is merged into a known one.
    expect(rows[2].freshness).toBe("quarantined");
  });
});
