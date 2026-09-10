import React from "react";

// Covers: WEB-012 — the shared status pill maps the full request-state
// vocabulary to visual classes without letting an unproven state present
// as healthy.
import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import StatusPill, { pillClass } from "../../../apps/web/components/StatusPill";
import {
  REQUEST_STATES,
  RESERVED_REQUEST_STATES,
} from "../../../apps/web/lib/api/requestState";

describe("shared status pill", () => {
  test("only a completed healthy request renders as ok", () => {
    expect(pillClass(REQUEST_STATES.ok)).toBe("pill ok");
    for (const state of ["idle", "loading", "warn"]) {
      expect(pillClass(state)).toBe("pill warn");
    }
  });

  test("failure-shaped states render as errors and the rest as caution", () => {
    for (const state of [
      "bad",
      "unauthorized",
      "forbidden",
      "rate-limited",
      "unavailable",
      "incompatible",
      "conflict",
    ]) {
      expect(pillClass(state)).toBe("pill bad");
    }
    for (const state of ["empty", "partial", "stale", "suppressed"]) {
      expect(pillClass(state)).toBe("pill warn");
    }
  });

  test("every reserved state has a deliberate mapping, never ok", () => {
    for (const state of RESERVED_REQUEST_STATES) {
      expect(["pill warn", "pill bad"]).toContain(pillClass(state));
    }
  });

  test("renders the label and message visibly", () => {
    render(
      <StatusPill state="bad" label="Observations" message="status 503: unavailable" testId="obs" />,
    );
    const pill = screen.getByTestId("obs");
    expect(pill).toHaveTextContent("Observations:");
    expect(pill).toHaveTextContent("status 503: unavailable");
    expect(pill.className).toBe("pill bad");
  });
});
