import React from "react";

// Covers: WEB-003 — history and source-state components expose accessible context.
import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import TimeSeriesChart from "../../../apps/web/components/TimeSeriesChart";
import SourceNote from "../../../apps/web/components/SourceNote";

describe("frontend history and source-state components", () => {
  test("renders a clear no-data history state", () => {
    render(<TimeSeriesChart items={[]} />);
    expect(screen.getByText(/No time-series observations are available/)).toBeInTheDocument();
  });

  test("renders ordered history with an accessible chart label", () => {
    render(
      <TimeSeriesChart
        items={[
          { observation_date: "2024-01-01", value: "20" },
          { observation_date: "2023-01-01", value: "10" },
        ]}
      />,
    );
    expect(
      screen.getByRole("img", { name: /2 time-series observations from 2023-01-01 to 2024-01-01/ }),
    ).toBeInTheDocument();
  });

  test("an unpublished value is dropped from the series rather than plotted as zero", () => {
    render(
      <TimeSeriesChart
        items={[
          { observation_date: "2023-01-01", value: "10" },
          { observation_date: "2024-01-01", value: null },
        ]}
      />,
    );
    // A period the source published nothing for is not a period with a value
    // of zero, and a trend drawn through one would be a different series.
    expect(
      screen.getByRole("img", { name: /1 time-series observation from 2023-01-01 to 2023-01-01/ }),
    ).toBeInTheDocument();
    // Dropped, but not silently: a reader who cannot see the gap would read
    // the plotted line as the whole published history.
    expect(screen.getByText(/1 period in this history published no value/)).toBeInTheDocument();
  });

  test("renders source context and error/caveat text without hiding it", () => {
    render(
      <SourceNote
        source="CENSUS_ACS"
        dataset="ACS1"
        metric="Population"
        geography="Dane County"
        caveats="Partial coverage; upstream API fallback is active."
      />,
    );
    expect(screen.getByRole("region", { name: "Source and methodology" })).toHaveTextContent("ACS1");
    expect(screen.getByText(/Partial coverage/)).toBeInTheDocument();
  });
});
