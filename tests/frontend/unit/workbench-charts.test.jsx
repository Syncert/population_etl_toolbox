import React from "react";

// Covers: WEB-083 — the workbench's longitudinal charts draw what was
// published and say what was not: a series' time position comes from its
// dates rather than its index, an unpublished period is counted rather than
// drawn at zero, the legend names each series' publisher, measure, grain and
// geography, and the accessible label states how many series and how many
// dropped periods the picture carries.
// Covers: WEB-085 — a bar chart whose value axis does not start at zero says
// so, because a bar whose baseline is not zero exaggerates differences and
// saying nothing about it is the misleading part.

import { render, screen, within } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import BarChart, { barValueScale } from "../../../apps/web/components/BarChart";
import LineChart from "../../../apps/web/components/LineChart";
import {
  assignValueAxes,
  buildPlottedSeries,
  describeChart,
} from "../../../apps/web/lib/workbench";

function series(overrides = {}) {
  return {
    sourceKey: "fred",
    sourceCode: "FRED",
    metricCode: "FRED:UNRATE",
    scope: "latest",
    geoLevel: "NATIONAL",
    geoId: "us:1",
    filters: {},
    ...overrides,
  };
}

function rows(entries) {
  return entries.map(([period, value]) => ({
    period_start: period,
    period_end: period,
    value,
  }));
}

function plottedPair() {
  const unemployment = buildPlottedSeries({
    series: series(),
    rows: rows([
      ["2021-01-01", "6.4"],
      ["2022-01-01", "4.0"],
      ["2024-01-01", "3.7"],
    ]),
    label: "Unemployment rate",
    unit: "Percent",
  });
  const population = buildPlottedSeries({
    series: series({
      sourceKey: "pep",
      sourceCode: "CENSUS_PEP",
      metricCode: "CENSUS_PEP:POP",
      geoLevel: "COUNTY",
      geoId: "county:06001",
    }),
    rows: rows([
      ["2021-01-01", "1648556"],
      ["2022-01-01", null],
      ["2024-01-01", "1649060"],
    ]),
    label: "Resident population",
    unit: "People",
  });
  return [unemployment, population];
}

function assignmentFor(plotted) {
  return assignValueAxes(
    plotted.map((entry) => ({
      key: entry.key,
      unit: entry.unitUnpublished ? null : entry.unit,
    })),
  );
}

describe("the line chart", () => {
  test("draws one path per series and positions points by date", () => {
    const plotted = plottedPair();
    const { container } = render(
      <LineChart plotted={plotted} assignment={assignmentFor(plotted)} />,
    );

    const paths = container.querySelectorAll('[data-testid="workbench-line"]');
    expect(paths).toHaveLength(2);

    // 2021, 2022 and 2024: the missing 2023 must leave the last interval wider
    // than the first, which is the whole point of a time axis (WEB-042).
    const points = paths[0]
      .getAttribute("points")
      .split(" ")
      .map((pair) => Number(pair.split(",")[0]));
    expect(points[1] - points[0]).toBeLessThan(points[2] - points[1]);
  });

  test("an unpublished period is counted on its legend entry, not drawn", () => {
    const plotted = plottedPair();
    render(<LineChart plotted={plotted} assignment={assignmentFor(plotted)} />);

    const dropped = screen.getAllByTestId("legend-dropped-periods");
    expect(dropped).toHaveLength(1);
    expect(dropped[0].textContent).toMatch(/1 period published no value/);
  });

  test("the legend names each series' publisher, measure, grain and geography", () => {
    const plotted = plottedPair();
    render(
      <LineChart
        plotted={plotted}
        assignment={assignmentFor(plotted)}
        geographyNames={{ "county:06001": "Alameda" }}
      />,
    );

    const legend = screen.getByTestId("workbench-legend");
    const entries = within(legend).getAllByRole("listitem");
    expect(entries).toHaveLength(2);
    expect(entries[0].textContent).toContain("FRED");
    expect(entries[0].textContent).toContain("Unemployment rate");
    expect(entries[0].textContent).toContain("Percent");
    expect(entries[1].textContent).toContain("CENSUS_PEP");
    expect(entries[1].textContent).toContain("County: Alameda");
    expect(entries[1].textContent).toContain("People");
  });

  test("two units are two axes, and the caption says the scales differ", () => {
    const plotted = plottedPair();
    render(<LineChart plotted={plotted} assignment={assignmentFor(plotted)} />);

    const axes = screen.getAllByTestId("workbench-axis");
    expect(axes.map((axis) => axis.getAttribute("data-axis-unit"))).toEqual([
      "Percent",
      "People",
    ]);
    expect(screen.getByTestId("workbench-axis-note").textContent).toMatch(
      /relative heights carry no meaning/,
    );
  });

  test("the accessible label says what is drawn and what is not", () => {
    const plotted = plottedPair();
    render(<LineChart plotted={plotted} assignment={assignmentFor(plotted)} />);
    const label = screen.getByRole("img").getAttribute("aria-label");
    expect(label).toBe(describeChart("line", plotted));
    expect(label).toContain("2 series");
    expect(label).toMatch(/1 period published no value/);
  });

  test("a selection that published nothing says so instead of drawing", () => {
    const empty = [
      buildPlottedSeries({
        series: series(),
        rows: rows([["2024-01-01", null]]),
        unit: "Percent",
      }),
    ];
    render(<LineChart plotted={empty} assignment={assignmentFor(empty)} />);
    expect(
      screen.getByTestId("workbench-line-chart-empty").textContent,
    ).toMatch(/not a period with a value of zero/);
  });
});

describe("the bar chart", () => {
  const bars = [
    {
      key: "a-2023",
      category: "2023",
      value: 61.2,
      unit: "Percent",
      groupKey: "a",
      groupLabel: "FRED — Labour force participation",
    },
    {
      key: "a-2024",
      category: "2024",
      value: 63.4,
      unit: "Percent",
      groupKey: "a",
      groupLabel: "FRED — Labour force participation",
    },
  ];

  test("a value axis that does not include zero says so", () => {
    expect(barValueScale([61.2, 63.4]).zeroBased).toBe(false);
    render(<BarChart bars={bars} label="two bars" colorOf={() => "#0b6b57"} />);
    expect(screen.getByTestId("bar-baseline-note").textContent).toMatch(
      /starts at 61.2, not at zero/,
    );
  });

  test("a value axis that spans zero is proportional, and says that instead", () => {
    expect(barValueScale([-1.2, 4.5]).zeroBased).toBe(true);
    expect(barValueScale([0.4, 4.5]).zeroBased).toBe(true);
  });

  test("an unpublished category is counted, never drawn as a zero bar", () => {
    render(
      <BarChart
        bars={bars}
        label="two bars"
        colorOf={() => "#0b6b57"}
        unpublished={3}
      />,
    );
    expect(screen.getAllByTestId("workbench-bar")).toHaveLength(2);
    const note = screen.getByTestId("bar-unpublished").textContent;
    expect(note).toContain("3 periods published no value");
  });

  test("a geography ranking pluralises its own categories", () => {
    render(
      <BarChart
        bars={bars}
        label="two bars"
        orientation="geography"
        colorOf={() => "#0b6b57"}
        unpublished={2}
      />,
    );
    expect(screen.getByTestId("bar-unpublished").textContent).toContain(
      "2 geographies published no value",
    );
  });

  test("nothing published draws nothing, and says why", () => {
    render(<BarChart bars={[]} label="none" colorOf={() => "#0b6b57"} />);
    expect(
      screen.getByTestId("workbench-bar-chart-empty").textContent,
    ).toMatch(/not a value of zero/);
  });
});
