import React from "react";

// Covers: WEB-124 — a county safety selection offers the derived roll-up
// with its derivation, coverage, and refusals intact.
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";

import CountyCrimeRollup from "../../../apps/web/components/CountyCrimeRollup";

const DANE = "state:55|county:025";

function rollupRow(overrides = {}) {
  return {
    product_id: "summarized_violent_crime",
    release: "2026-08-15",
    offense_label: "Violent Crime",
    measure_id: "V:offense:absolute_total",
    measure_form: "absolute_total",
    counted_entity_basis: "offense",
    unit: "count",
    geo_id: DANE,
    county_name: "Dane County",
    period: "01-2023",
    period_start: "2023-01-01",
    value: "20",
    contributing_oris: ["WI0130000", "WI0137000", "WI0540300"],
    reporting_agency_count: 3,
    mapped_agency_count: 3,
    includes_multi_county_agency: true,
    derived: true,
    result_label: "derived county roll-up of agency-reported totals",
    methodology_note: "county values are not additive to state totals",
    ...overrides,
  };
}

const CAVEATS = [
  "A derived roll-up of agency-reported totals, not a provider-published county figure.",
  "An agency serving more than one county contributes its whole published count to each of its counties, so county values are not additive to state totals.",
];

function answerWith(status, body) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () =>
      new Response(JSON.stringify(body), {
        status,
        headers: { "content-type": "application/json" },
      }),
    ),
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("the derived county crime roll-up panel", () => {
  test("loads only on an explicit run and keeps every honesty of the contract", async () => {
    answerWith(200, {
      derived: true,
      release_selection: "latest_release",
      caveats: CAVEATS,
      total: 1,
      limit: 200,
      offset: 0,
      items: [rollupRow()],
    });
    render(<CountyCrimeRollup geoId={DANE} placeName="Dane County" />);

    // Nothing is fetched until the reader asks: the derived number never
    // auto-loads beside the provider-published cards.
    expect(globalThis.fetch).not.toHaveBeenCalled();
    expect(screen.getByText(/Derived county roll-up/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Load derived county roll-up/ }));

    await waitFor(() =>
      expect(screen.getByTestId("county-rollup-coverage")).toBeInTheDocument(),
    );
    const url = String(globalThis.fetch.mock.calls[0][0]);
    expect(url).toContain("/api/v1/crime/county-rollup");
    expect(url).toContain(encodeURIComponent(DANE));

    // The sum renders with its contributors, coverage, and the multi-county
    // consequence, and every caveat the API stated survives to the page.
    expect(screen.getByText("20 count")).toBeInTheDocument();
    expect(
      screen.getByText("WI0130000, WI0137000, WI0540300"),
    ).toBeInTheDocument();
    expect(screen.getByTestId("county-rollup-coverage")).toHaveTextContent(
      "3 of 3 mapped agencies reported",
    );
    expect(screen.getByTestId("county-rollup-multi-county")).toHaveTextContent(
      "not additive to state totals",
    );
    expect(screen.getByTestId("county-rollup-caveats")).toHaveTextContent(
      "not a provider-published county figure",
    );
    // The row names itself a derived sum, never a provider observation.
    expect(screen.getByText(/Violent Crime offense \(derived sum\)/)).toBeInTheDocument();
  });

  test("an unmapped county surfaces the API's explicit refusal, not an empty table", async () => {
    answerWith(404, {
      detail:
        "No law-enforcement agency is mapped to state:55|county:078; the roll-up publishes no value for an unmapped county.",
    });
    render(
      <CountyCrimeRollup geoId="state:55|county:078" placeName="Vernon County" />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Load derived county roll-up/ }));

    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        "No law-enforcement agency is mapped",
      ),
    );
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  test("a mapped county whose agencies did not report is never shown as zero", async () => {
    answerWith(200, {
      derived: true,
      release_selection: "latest_release",
      caveats: CAVEATS,
      total: 0,
      limit: 200,
      offset: 0,
      items: [],
    });
    render(<CountyCrimeRollup geoId={DANE} placeName="Dane County" />);
    fireEvent.click(screen.getByRole("button", { name: /Load derived county roll-up/ }));

    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        "nothing is shown as zero",
      ),
    );
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.queryByText("0")).not.toBeInTheDocument();
  });

  test("an answer that lost its derivation labeling is refused", async () => {
    answerWith(200, {
      derived: true,
      release_selection: "latest_release",
      caveats: CAVEATS,
      total: 1,
      limit: 200,
      offset: 0,
      items: [rollupRow({ derived: false })],
    });
    render(<CountyCrimeRollup geoId={DANE} placeName="Dane County" />);
    fireEvent.click(screen.getByRole("button", { name: /Load derived county roll-up/ }));

    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        "the answer was refused",
      ),
    );
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });
});
