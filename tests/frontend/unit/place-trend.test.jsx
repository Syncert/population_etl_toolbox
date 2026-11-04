import React from "react";

// Covers: WEB-159 — every trend chart on a place page says what it draws: a
// title naming the measure in plain words, its unit, how often it is published
// and the span drawn, its source, a y-axis label and a legend above the plot,
// linked to the headline card it draws (UAT on /metro/madison-wi, where the
// monthly unemployment chart showed only "14" and "1.3").
import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import PlaceTrend from "../../../apps/web/components/PlaceTrend";
import { buildTrend, trendFrequency, trendSourceName } from "../../../apps/web/lib/placeChapters";

const levels = [
  { level: "METRO", geoId: "m", name: "Madison, WI", role: "this place" },
  { level: "STATE", geoId: "s", name: "Wisconsin", role: "parent" },
];

const monthly = (values, year = 1990) =>
  values.map((value, index) => {
    const month = String(index + 1).padStart(2, "0");
    const last = new Date(Date.UTC(year, index + 1, 0)).getUTCDate();
    return { period_start: `${year}-${month}-01`, period_end: `${year}-${month}-${last}`, value };
  });

const yearly = (values, start = 2019) =>
  values.map((value, index) => ({ period_start: `${start + index}-01-01`, period_end: `${start + index}-12-31`, value }));

describe("how often a trend is published", () => {
  it("is read from the rows' own periods", () => {
    expect(trendFrequency(monthly(["1", "2", "3"]))).toBe("monthly");
    expect(trendFrequency(yearly(["1", "2"]))).toBe("yearly");
    expect(trendFrequency([{ period_start: "2020-01-01", period_end: "2024-12-31", value: "1" }])).toBe("five-year estimates");
    expect(trendFrequency([{ period_start: "2024-01-01", period_end: "2024-03-31", value: "1" }])).toBe("quarterly");
  });

  it("falls back to the gap between dated rows, and says nothing when it cannot tell", () => {
    expect(trendFrequency([{ observation_date: "2024-01-01" }, { observation_date: "2024-02-01" }, { observation_date: "2024-03-01" }])).toBe("monthly");
    expect(trendFrequency([{ observation_date: "2024-01-01" }])).toBe("");
  });

  it("is carried on the model, from the page's own place first", () => {
    const trend = buildTrend(levels, new Map([["m", monthly(["3.1", "3.0"])], ["s", monthly(["3.4", "3.3"])]]), "level");
    expect(trend.frequency).toBe("monthly");
  });
});

describe("a trend's source line", () => {
  it("names the program in words", () => {
    expect(trendSourceName("BLS:LAU:UNEMP_RATE", "BLS")).toBe("Bureau of Labor Statistics, Local Area Unemployment Statistics (LAUS)");
    expect(trendSourceName("CENSUS_ACS:acs5:B19013_001", "CENSUS_ACS")).toBe("U.S. Census Bureau, American Community Survey 5-year estimates");
    expect(trendSourceName("CENSUS_PEP:POPESTIMATE")).toBe("U.S. Census Bureau, Population Estimates Program");
  });

  it("falls back to the published source code, never to nothing when one exists", () => {
    expect(trendSourceName("NEW_SOURCE:x", "NEW_SOURCE")).toBe("NEW_SOURCE");
  });
});

describe("a trend chart says what it draws", () => {
  const model = buildTrend(
    levels,
    new Map([["m", monthly(["3.1", "14", "1.3", "2.9"])], ["s", monthly(["3.4", "13", "2.0", "3.1"])]]),
    "level",
  );

  it("has a title, unit, frequency, span, source, y-axis label and legend before the plot", () => {
    render(
      <PlaceTrend
        model={model}
        title="Unemployment rate over time"
        metricName="Unemployment rate"
        unit="percent"
        source={trendSourceName("BLS:LAU:UNEMP_RATE", "BLS")}
        forCard={{ label: "Unemployment rate", anchor: "chapter-work-money-card-unemployment-rate" }}
        testId="trend"
      />,
    );
    const figure = screen.getByTestId("trend");
    expect(screen.getByRole("heading", { name: "Unemployment rate over time" })).toBeTruthy();
    expect(figure.getAttribute("aria-labelledby")).toBe("trend-title");
    // The metric name repeats the title, so it is not said twice.
    expect(screen.getByTestId("trend-meta").textContent).toBe("percent · monthly, Jan 1990 to Apr 1990");
    expect(screen.getByTestId("trend-source").textContent).toBe(
      "Source: Bureau of Labor Statistics, Local Area Unemployment Statistics (LAUS)",
    );
    expect(screen.getByTestId("trend-y-label").textContent).toBe("Percent");
    const link = within(screen.getByTestId("trend-for")).getByRole("link", { name: "Unemployment rate" });
    expect(link.getAttribute("href")).toBe("#chapter-work-money-card-unemployment-rate");
    const legend = figure.querySelector(".place-trend-legend");
    expect(legend.textContent).toContain("Madison, WI (this page)");
    expect(legend.textContent).toContain("Wisconsin (reference)");
    // The legend comes before the plot, so it cannot fall below the card.
    const plot = figure.querySelector('svg[role="img"]');
    expect(legend.compareDocumentPosition(plot) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(plot.getAttribute("aria-label")).toContain("Unemployment rate over time, percent, Jan 1990 to Apr 1990");
  });

  it("states an index's base period as its axis and unit", () => {
    const index = buildTrend(levels, new Map([["m", yearly(["100", "110"])], ["s", yearly(["1000", "1050"])]]), "index");
    render(<PlaceTrend model={index} title="Resident population estimate over time" metricName="Population estimate" unit="people" testId="pop" />);
    expect(screen.getByTestId("pop-y-label").textContent).toBe("Index (2019 = 100)");
    expect(screen.getByTestId("pop-meta").textContent).toBe("Population estimate · index, 2019 = 100 · yearly, 2019 to 2020");
    expect(screen.queryByTestId("pop-source")).toBeNull();
  });
});
