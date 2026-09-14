import React from "react";

// Covers: WEB-042 — an unpublished period is left open rather than plotted
// at zero or closed up by even spacing.
// Covers: WEB-077 — for a source that serves only the periods it published a
// value for, the gap is named; the counted note cannot see it because the row
// does not exist (API-127).
import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import TimeSeriesChart from "../../../apps/web/components/TimeSeriesChart";

const GAP_NOTE = "chart-cadence-gap";

function series(dates) {
  return dates.map((observation_date) => ({ observation_date, value: "1" }));
}

describe("a history from a source that serves only published numbers", () => {
  test("a regular cadence is not reported as a gap", () => {
    render(
      <TimeSeriesChart
        items={series(["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"])}
        publishesValueStatus={false}
      />,
    );
    expect(screen.queryByTestId(GAP_NOTE)).toBeNull();
  });

  test("February beside January is not a gap either", () => {
    // The shortest and longest calendar months side by side: 28 days against
    // 31 is 1.11x, which is why the threshold is not a naive "longer than the
    // one before it".
    render(
      <TimeSeriesChart
        items={series(["2024-01-01", "2024-02-01", "2024-03-01", "2024-05-01", "2024-06-01"])}
        publishesValueStatus={false}
      />,
    );
    // April is missing here, so this one *is* a gap -- the assertion is that
    // the note fires on the skipped month rather than on February's length.
    expect(screen.getByTestId(GAP_NOTE)).toBeInTheDocument();
  });

  test("a skipped month is named as a period without a published value", () => {
    render(
      <TimeSeriesChart
        items={series(["2024-01-01", "2024-02-01", "2024-04-01", "2024-05-01"])}
        publishesValueStatus={false}
      />,
    );
    const note = screen.getByTestId(GAP_NOTE);
    expect(note).toHaveTextContent("without a published value");
    expect(note).toHaveTextContent("not a value of zero");
  });

  test("a skipped year in an annual history is named", () => {
    render(
      <TimeSeriesChart
        items={series(["2020-01-01", "2021-01-01", "2023-01-01", "2024-01-01"])}
        publishesValueStatus={false}
      />,
    );
    expect(screen.getByTestId(GAP_NOTE)).toBeInTheDocument();
  });

  test("two points carry no cadence to compare a gap against", () => {
    render(
      <TimeSeriesChart
        items={series(["2020-01-01", "2024-01-01"])}
        publishesValueStatus={false}
      />,
    );
    expect(screen.queryByTestId(GAP_NOTE)).toBeNull();
  });
});

describe("a history from a source that publishes a value state", () => {
  test("the gap note is not shown, because the row says it instead", () => {
    // CDC, FBI UCR and USDA NASS send the period with `value: null` and a
    // reason, which the counted note reads. Saying both would describe one
    // period twice, and the counted one names how many.
    render(
      <TimeSeriesChart
        items={series(["2024-01-01", "2024-02-01", "2024-04-01", "2024-05-01"])}
        publishesValueStatus
      />,
    );
    expect(screen.queryByTestId(GAP_NOTE)).toBeNull();
  });

  test("an unpublished period it did send is counted, not plotted", () => {
    render(
      <TimeSeriesChart
        items={[
          { observation_date: "2024-01-01", value: "1" },
          { observation_date: "2024-02-01", value: null },
          { observation_date: "2024-03-01", value: "3" },
        ]}
        publishesValueStatus
      />,
    );
    expect(screen.getByText(/1 period in this history published no value/)).toBeInTheDocument();
    expect(screen.queryByTestId(GAP_NOTE)).toBeNull();
  });
});
