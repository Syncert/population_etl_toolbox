import { expect, test } from "vitest";

// Covers: WEB-140 — the explorer offers exactly the calendar grains and
// windows a metric publishes, asks for them with only the parameters the
// resource accepts, says whether a figure is the provider's or derived, and
// shows an incomplete window's reason instead of "No observation".

import { buildExplorerSources, findExplorerSource } from "../../../apps/web/lib/explorerSources";
import {
  buildHistoryObservationRequest,
  buildLatestObservationRequest,
} from "../../../apps/web/lib/observationAccess";
import {
  NATIVE_TIME_VIEW,
  derivationCaption,
  offeredTimeViews,
  refusalLabel,
  refusalReason,
  timeViewParams,
} from "../../../apps/web/lib/timeViews";
import { servedParameters, servedSchemaFields } from "../support/servedContract.js";

const NEUTRAL_PARAMETERS = servedParameters("/api/v1/observations");

const capabilities = [
  {
    source_code: "FBI_UCR",
    display_name: "Federal Bureau of Investigation Uniform Crime Reporting Program",
    route_segment: null,
    served_by_neutral_routes: true,
    datasets: [],
    observation_filters: ["geo_id", "geo_level", "subject_code", "subject_type", "year_from", "year_to"],
    observation_routes: [{ path: "/api/v1/observations", parameters: NEUTRAL_PARAMETERS }],
  },
];

const fbi = () => findExplorerSource(buildExplorerSources(capabilities), "FBI_UCR");

test("the served contract carries the parameters and fields this module reads", () => {
  expect(NEUTRAL_PARAMETERS).toEqual(expect.arrayContaining(["time_grain", "window"]));
  expect(servedSchemaFields("MetricCapability")).toEqual(
    expect.arrayContaining(["time_grains", "time_windows"]),
  );
  expect(servedSchemaFields("ObservationDerivation")).toEqual(
    expect.arrayContaining([
      "kind",
      "method",
      "method_version",
      "expected_periods",
      "present_periods",
      "refusal_reason",
    ]),
  );
});

test("only the views a metric publishes are offered, native first", () => {
  expect(offeredTimeViews(null)).toEqual([NATIVE_TIME_VIEW]);
  expect(offeredTimeViews({ time_grains: ["native"], time_windows: [] })).toEqual(["native"]);
  expect(
    offeredTimeViews({
      time_grains: ["native", "annual", "quarterly"],
      time_windows: ["ytd", "trailing_3", "trailing_12"],
    }),
  ).toEqual(["native", "quarterly", "annual", "trailing_3", "trailing_12", "ytd"]);
  // A grain the API does not name is never offered.
  expect(offeredTimeViews({ time_grains: ["weekly"] })).toEqual(["native"]);
});

test("a grain asks time_grain, a window asks window, native asks neither", () => {
  expect(timeViewParams("native")).toEqual({});
  expect(timeViewParams(undefined)).toEqual({});
  expect(timeViewParams("annual")).toEqual({ time_grain: "annual" });
  expect(timeViewParams("trailing_12")).toEqual({ window: "trailing_12" });
});

test("a time-view read carries only the geography, never a state, scope or reduction", () => {
  const latest = buildLatestObservationRequest(fbi(), {
    metricCode: "FBI_UCR:summarized_arson:ARS:offense:absolute_total",
    geoLevel: "STATE",
    stateFips: "06",
    newestPerGeography: true,
    scope: "latest",
    periodStart: "2023-01-01",
    limit: "500",
    dimensions: { subject_type: "state" },
    timeView: "quarterly",
  });
  expect(latest).toEqual({
    resource: "/observations",
    params: {
      metric_code: "FBI_UCR:summarized_arson:ARS:offense:absolute_total",
      time_grain: "quarterly",
      limit: "500",
      geo_level: "STATE",
    },
  });
  const history = buildHistoryObservationRequest(fbi(), {
    metricCode: "FBI_UCR:summarized_arson:ARS:offense:absolute_total",
    geoId: "state:06",
    timeView: "ytd",
  });
  expect(history.params).toEqual({
    metric_code: "FBI_UCR:summarized_arson:ARS:offense:absolute_total",
    window: "ytd",
    limit: undefined,
    geo_id: "state:06",
  });
  // Native is the unchanged request.
  const native = buildLatestObservationRequest(fbi(), {
    metricCode: "FBI_UCR:x",
    timeView: "native",
  });
  expect(native.params.time_grain).toBeUndefined();
  expect(native.params.window).toBeUndefined();
});

const provider = {
  value: "313.689",
  derivation: { kind: "provider_published" },
};
const derived = {
  value: "312.1",
  derivation: { kind: "derived", method: "mean", method_version: 1, expected_periods: 12, present_periods: 12 },
};
const refused = {
  value: null,
  derivation: {
    kind: "derived",
    method: "mean",
    method_version: 1,
    expected_periods: 12,
    present_periods: 11,
    refusal_reason: "incomplete_window: 11 of 12 periods reported",
  },
};

test("a refused window shows its reason, a complete one shows none", () => {
  expect(refusalReason(refused)).toBe("incomplete_window: 11 of 12 periods reported");
  expect(refusalLabel(refused)).toBe("Incomplete window: 11 of 12 months reported");
  expect(refusalLabel(derived)).toBeNull();
  expect(refusalLabel({ value: null })).toBeNull();
});

test("the caption says whose figures are on screen", () => {
  expect(derivationCaption([])).toBeNull();
  expect(derivationCaption([{ value: "1" }])).toBeNull();
  expect(derivationCaption([provider])).toBe("Provider-published figures.");
  expect(derivationCaption([derived, refused])).toBe(
    "Derived by the warehouse: mean of 12 monthly values (method v1). 1 incomplete window shown without a value.",
  );
  expect(derivationCaption([provider, derived])).toBe(
    "Provider-published figures. Derived by the warehouse: mean of 12 monthly values (method v1).",
  );
});

test("the map keeps each geography's newest complete window, or its refusal", async () => {
  const { mapWindowRows } = await import("../../../apps/web/lib/timeViews");
  const rows = [
    { geo_id: "state:06", period_start: "2024-01-01", value: "10" },
    { geo_id: "state:06", period_start: "2025-01-01", value: null },
    { geo_id: "state:06", period_start: "2023-01-01", value: "9" },
    { geo_id: "state:41", period_start: "2025-01-01", value: null },
    { geo_id: "state:41", period_start: "2024-01-01", value: null },
  ];
  expect(mapWindowRows(rows)).toEqual([
    { geo_id: "state:06", period_start: "2024-01-01", value: "10" },
    { geo_id: "state:41", period_start: "2025-01-01", value: null },
  ]);
  expect(mapWindowRows(null)).toEqual([]);
});
