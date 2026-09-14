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

  // Covers: WEB-042 — the horizontal axis is time. Points were placed by
  // index, so the gap the component was careful not to fill with a zero was
  // closed instead: 1979 and 1981 sat adjacent and evenly spaced, and the
  // line between them sloped as though the measure moved over one ordinary
  // interval.
  function pointXs(container) {
    return [...container.querySelectorAll("circle.chart-point")].map((node) =>
      Number(node.getAttribute("cx")),
    );
  }

  test("a gap in the series is a gap on the axis", () => {
    const { container } = render(
      <TimeSeriesChart
        items={[
          { observation_date: "1979-07-01", value: "10" },
          { observation_date: "1981-07-01", value: "12" },
          { observation_date: "1982-07-01", value: "13" },
        ]}
      />,
    );
    const [first, second, third] = pointXs(container);
    // Two years, then one: the first interval is twice the second.
    expect(second - first).toBeGreaterThan(third - second);
    expect((second - first) / (third - second)).toBeCloseTo(2, 1);
  });

  test("an evenly spaced series is unchanged", () => {
    const { container } = render(
      <TimeSeriesChart
        items={[
          { observation_date: "2021-01-01", value: "1" },
          { observation_date: "2022-01-01", value: "2" },
          { observation_date: "2023-01-01", value: "3" },
        ]}
      />,
    );
    const [first, second, third] = pointXs(container);
    expect(second - first).toBeCloseTo(third - second, 1);
  });

  test("unparseable or identical dates still render, evenly spaced", () => {
    const unparseable = render(
      <TimeSeriesChart
        items={[
          { observation_date: "period one", value: "1" },
          { observation_date: "period two", value: "2" },
        ]}
      />,
    );
    expect(pointXs(unparseable.container)).toHaveLength(2);

    const identical = render(
      <TimeSeriesChart
        items={[
          { observation_date: "2021-01-01", value: "1" },
          { observation_date: "2021-01-01", value: "2" },
        ]}
      />,
    );
    const xs = pointXs(identical.container);
    expect(xs).toHaveLength(2);
    expect(xs.every((value) => Number.isFinite(value))).toBe(true);
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
