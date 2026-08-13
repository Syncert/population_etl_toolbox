import React from "react";

// Covers: WEB-003 — history and source-state components expose accessible context.
import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import MiniLineChart from "../../../apps/web/components/MiniLineChart";
import SourceNote from "../../../apps/web/components/SourceNote";

describe("frontend history and source-state components", () => {
  test("renders a clear no-data history state", () => {
    render(<MiniLineChart items={[]} />);
    expect(screen.getByText(/Not enough history/)).toBeInTheDocument();
  });

  test("renders ordered history with an accessible chart label", () => {
    render(
      <MiniLineChart
        label="County population history"
        items={[
          { observation_date: "2024-01-01", value: "20" },
          { observation_date: "2023-01-01", value: "10" },
        ]}
      />,
    );
    expect(screen.getByRole("img", { name: /County population history, 2023-01-01 to 2024-01-01/ })).toBeInTheDocument();
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
