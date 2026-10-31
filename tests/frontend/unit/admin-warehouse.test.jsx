import React from "react";

// Covers: WEB-151 — the admin warehouse page renders what the API reports
// about each source and the schema, and explains every refusal.
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";

import AdminWarehousePage from "../../../apps/web/components/AdminWarehousePage";
import {
  coverageByColumn,
  edgePath,
  missingYears,
  runLimited,
  shortType,
  yearSpan,
} from "../../../apps/web/lib/adminWarehouse";
import { TOKEN_SESSION_KEY } from "../../../apps/web/lib/apiToken";
import { PRIVATE_ROUTES } from "../../../apps/web/lib/siteMap";

const SOURCES = {
  grains: [
    ["nation", "Nation"],
    ["state", "State"],
    ["county", "County"],
  ],
  topics: ["Housing"],
  catalog_read_at: "2026-10-31T16:00:00+00:00",
  sources: [
    {
      code: "CENSUS_BPS",
      name: "Census Building Permits Survey",
      publisher: "U.S. Census Bureau",
      topic: "Housing",
      module: "census_bps",
      dag: "census_bps_ingest",
      cadence: "Monthly",
      release: "Monthly",
      grains: [
        { grain: "nation", mark: "n" },
        { grain: "county", mark: "n" },
      ],
      declared_years: [[2000, 2025]],
      years_note: "",
      datasets: ["Units authorized by structure size"],
      measures: "Housing units authorized.",
      raw: ["raw_census_bps.release_file"],
      control: [],
      silver: ["silver_census_bps.observation_revision"],
      gold: ["gold_census_bps.observation_latest"],
      credential: "",
      catalog_source_code: "CENSUS_BPS",
      catalog: {
        metrics: 12,
        current_metrics: 12,
        stale_metrics: 0,
        geo_grains: ["COUNTY", "NATIONAL"],
        time_grains: ["ANNUAL"],
        newest_watermark: "2025-12",
        newest_publication: null,
        last_harvested: "2026-10-31T12:00:00+00:00",
      },
    },
  ],
};

const COVERAGE = {
  source_code: "CENSUS_BPS",
  catalog_source_code: "CENSUS_BPS",
  status: "ok",
  relation: "gold_census_bps.observation_latest",
  method: "scan",
  note: null,
  grains: [
    { grain: "COUNTY", column: "county", first_year: 2004, last_year: 2024, years: [2004, 2005, 2024], row_count: 900 },
    { grain: "NATIONAL", column: "nation", first_year: 2000, last_year: 2024, years: [2000, 2024], row_count: 50 },
  ],
  row_count: 950,
  row_count_is_estimate: false,
  measured_at: "2026-10-31T16:00:00+00:00",
  elapsed_ms: 120,
  cached: false,
};

const SCHEMA = {
  stats: { schemas: 2, tables: 2, views: 0, partitions: 0, foreign_keys: 1 },
  read_at: "2026-10-31T16:00:00Z",
  domains: [
    {
      id: "backbone",
      title: "Capture backbone",
      caption: "Every response lands here first.",
      source_code: null,
      tables: 2,
      views: 0,
      width: 600,
      height: 200,
      boxes: [
        { name: "raw_capture.payload_blob", layer: "capture", kind: "table", shared: false, partitions: 0, x: 10, y: 40, width: 236, height: 66, columns: [["payload_checksum", "text", "PK"]], more_columns: 2 },
        { name: "raw_capture.response_capture", layer: "capture", kind: "table", shared: false, partitions: 0, x: 320, y: 40, width: 236, height: 66, columns: [["capture_id", "uuid", "PK"], ["payload_checksum", "text", "FK"]], more_columns: 0 },
      ],
      edges: [{ from: "raw_capture.payload_blob", to: "raw_capture.response_capture", kind: "fk", columns: ["payload_checksum"] }],
    },
  ],
  relations: [
    { name: "raw_capture.payload_blob", layer: "capture", kind: "table", domain: "backbone", columns: [["payload_checksum", "text", true]], primary_key: ["payload_checksum"], uniques: [], foreign_keys: [], reads_from: [], partition_key: null, partitions: 0, estimated_rows: 10, index_count: 1 },
    { name: "raw_capture.response_capture", layer: "capture", kind: "table", domain: "backbone", columns: [["capture_id", "uuid", true], ["payload_checksum", "text", true]], primary_key: ["capture_id"], uniques: [], foreign_keys: [{ columns: ["payload_checksum"], references: "raw_capture.payload_blob" }], reads_from: [], partition_key: null, partitions: 0, estimated_rows: 10, index_count: 2 },
  ],
};

function json(body, status = 200) {
  return Promise.resolve(
    new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } }),
  );
}

function installFetch(handler) {
  const fetchMock = vi.fn((input, init) => handler(String(input), init));
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

afterEach(() => {
  vi.unstubAllGlobals();
  window.sessionStorage.clear();
  window.history.replaceState(null, "", "/");
});

describe("admin warehouse helpers", () => {
  test("year spans, gaps and folded columns", () => {
    expect(yearSpan(2009, 2024)).toBe("2009–2024");
    expect(yearSpan(2021, 2021)).toBe("2021");
    expect(yearSpan(null, 2021)).toBe("");
    expect(missingYears([2004, 2005, 2008])).toEqual([2006, 2007]);
    const folded = coverageByColumn({
      ...COVERAGE,
      grains: [
        ...COVERAGE.grains,
        { grain: "AGENCY", column: "other", first_year: 2019, last_year: 2020, years: [2019, 2020], row_count: 4 },
        { grain: "REGION", column: "other", first_year: 2010, last_year: 2011, years: [2010, 2011], row_count: 6 },
      ],
    });
    expect(folded.county).toMatchObject({ first: 2004, last: 2024, rows: 900 });
    expect(folded.other).toMatchObject({ first: 2010, last: 2020, rows: 10, grains: ["AGENCY", "REGION"] });
    expect(coverageByColumn({ ...COVERAGE, status: "timed_out" })).toEqual({});
  });

  test("an edge leaves the right side of its source and lands on the named column", () => {
    const [from, to] = SCHEMA.domains[0].boxes;
    // payload_checksum is the second drawn column of response_capture.
    expect(edgePath(from, to, "payload_checksum")).toBe("M246,52 C283,52 283,94 320,94");
    expect(shortType("timestamp with time zone")).toBe("timestamptz");
    expect(shortType("geometry(MultiPolygon,4326)")).toBe("geometry");
  });

  test("runLimited never runs more than its limit at once", async () => {
    let running = 0;
    let peak = 0;
    await runLimited([1, 2, 3, 4, 5, 6, 7], 3, async () => {
      running += 1;
      peak = Math.max(peak, running);
      await new Promise((resolve) => setTimeout(resolve, 1));
      running -= 1;
    });
    expect(peak).toBe(3);
  });

  test("the admin route is excluded from robots and the sitemap", () => {
    expect(PRIVATE_ROUTES).toContain("/admin");
  });
});

describe("AdminWarehousePage", () => {
  test("without a credential it says admin only and makes no request", async () => {
    const fetchMock = installFetch(() => json({}));
    render(<AdminWarehousePage />);
    expect(await screen.findByTestId("admin-warehouse-signed-out")).toHaveTextContent("Admin only");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test("a non-admin account is told why, from the API's 403", async () => {
    window.sessionStorage.setItem(TOKEN_SESSION_KEY, "operator-token");
    installFetch(() => json({ detail: "this account is not on the deployment's admin list" }, 403));
    render(<AdminWarehousePage />);
    expect(await screen.findByTestId("admin-warehouse-refused")).toHaveTextContent(/not on the deployment's admin list/);
  });

  test("a deployment with no admin named says the pages are off", async () => {
    window.sessionStorage.setItem(TOKEN_SESSION_KEY, "operator-token");
    installFetch(() => json({ detail: "Not Found" }, 404));
    render(<AdminWarehousePage />);
    expect(await screen.findByTestId("admin-warehouse-refused")).toHaveTextContent(/API_ADMIN_ACCOUNTS/);
  });

  test("the source map shows live years per grain and the schema atlas draws the catalog", async () => {
    window.sessionStorage.setItem(TOKEN_SESSION_KEY, "operator-token");
    const fetchMock = installFetch((url) => {
      if (url.includes("/admin/warehouse/sources")) return json(SOURCES);
      if (url.includes("/admin/warehouse/coverage/CENSUS_BPS")) return json(COVERAGE);
      if (url.includes("/admin/warehouse/schema")) return json(SCHEMA);
      return json({ detail: "unexpected" }, 500);
    });
    render(<AdminWarehousePage />);

    const table = await screen.findByTestId("admin-source-map");
    expect(table).toHaveTextContent("Census Building Permits Survey");
    await waitFor(() => expect(table).toHaveTextContent("2004–2024"));
    expect(table).toHaveTextContent("2000–2024");
    // Every request carried the credential as a header, never in the URL.
    for (const [url, init] of fetchMock.mock.calls) {
      expect(String(url)).not.toContain("operator-token");
      expect(init.headers.Authorization).toBe("Bearer operator-token");
    }

    fireEvent.click(screen.getByRole("button", { name: /Census Building Permits Survey/ }));
    // Once in the lineage, once as the relation the coverage was measured from.
    expect(await screen.findAllByText("gold_census_bps.observation_latest", { selector: "code" })).toHaveLength(2);

    fireEvent.click(screen.getByRole("tab", { name: "Schema atlas" }));
    expect(await screen.findByRole("img", { name: /Capture backbone: 2 tables/ })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "raw_capture.response_capture, table" }));
    expect(await screen.findByTestId("admin-relation-card")).toHaveTextContent("References: raw_capture.payload_blob");
  });
});
