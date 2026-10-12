import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, expect, test, vi } from "vitest";
import UseCaseSourceReport from "../../../apps/web/components/UseCaseSourceReport";
import { fetchCollectionPages } from "../../../apps/web/lib/api/client";

vi.mock("../../../apps/web/lib/api/client", async (importOriginal) => ({
  ...await importOriginal(),
  fetchCollectionPages: vi.fn(),
}));

const source = {
  key: "fbi", sourceCode: "FBI_UCR", accessShape: "neutral",
  neutralFilters: ["geo_id"], requestFilters: ["geo_id"],
  supportsSettledHistory: true, supportsAsReleased: true,
  neutralDimensionFilters: [], dimensionFilters: [], publishedDimensions: [],
};
const measures = ["absolute_total", "rate"].map((form) => {
  const metricCode = `FBI_UCR:summarized_violent_crime:V:offense:${form}`;
  return {
    available: true, metricCode,
    slot: { id: form, label: form === "rate" ? "Violent crime rate, as published" : "Violent crime count, as published" },
    metric: { metric_code: metricCode, source_code: "FBI_UCR", valid_geo_grains: ["AGENCY", "STATE", "NATIONAL"] },
  };
});
const props = {
  sectionId: "safety", measures, sources: [source],
  geoId: "state:55|county:025", geoLevel: "COUNTY", placeName: "Dane County",
  state: { geo_id: "state:55", state_fips: "55", state_name: "Wisconsin" },
};

beforeEach(() => {
  vi.mocked(fetchCollectionPages).mockReset();
  vi.mocked(fetchCollectionPages).mockResolvedValue({ items: [], complete: true, total: 0 });
});

test("county FBI controls direct counts to the derived panel and read the selected rate only after an explicit state choice", async () => {
  // Covers: WEB-123, WEB-124 — the screenshot's county rate selection has a usable path.
  const view = render(<UseCaseSourceReport {...props} />);
  expect(screen.getByRole("link", { name: "Use derived county counts" })).toHaveAttribute("href", "#county-crime-rollup-title");
  expect(screen.queryByRole("button", { name: "Selected place" })).not.toBeInTheDocument();
  expect(screen.getByRole("status")).toHaveTextContent("County rates are not published");
  expect(fetchCollectionPages).not.toHaveBeenCalled();

  fireEvent.change(screen.getByRole("combobox", { name: "Report measure for safety" }), { target: { value: measures[1].metricCode } });
  expect(fetchCollectionPages).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Load Wisconsin state report" }));
  await waitFor(() => expect(fetchCollectionPages).toHaveBeenCalledTimes(1));
  expect(fetchCollectionPages.mock.calls[0][1].params).toMatchObject({ geo_id: "state:55", metric_code: measures[1].metricCode });
  expect(screen.getByRole("combobox", { name: "Report measure for safety" })).toHaveValue(measures[1].metricCode);
  expect(screen.getByText(/these are not Dane County figures/)).toBeInTheDocument();

  view.rerender(<UseCaseSourceReport {...props} geoId="state:55|county:105" placeName="Rock County" />);
  await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("County rates are not published"));
  expect(fetchCollectionPages).toHaveBeenCalledTimes(1);
});

test("a provider measure published at county grain retains its selected-place report", async () => {
  // Covers: WEB-123 - county publication remains available for supported providers.
  const metricCode = "CDC:places_county:ARTHRITIS:AgeAdjPrv";
  render(<UseCaseSourceReport {...props} sources={[{ ...source, key: "cdc", sourceCode: "CDC" }]} measures={[{
    ...measures[0], metricCode, metric: { metric_code: metricCode, source_code: "CDC", valid_geo_grains: ["COUNTY"] },
  }]} />);
  expect(screen.getByRole("button", { name: "Selected place" })).toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "Use derived county counts" })).not.toBeInTheDocument();
  await waitFor(() => expect(fetchCollectionPages).toHaveBeenCalledTimes(1));
  expect(fetchCollectionPages.mock.calls[0][1].params).toMatchObject({ geo_id: props.geoId, metric_code: metricCode });
});
