import React from "react";

// Covers: ENV-018 — a component test's assertions are about its own render.
//
// `@testing-library/react` registers auto-cleanup only when a global
// `afterEach` exists, and this project runs vitest without `globals` (every
// test file imports `describe`/`test` explicitly). So nothing unmounted
// between tests: one document accumulated every render in a file, and
// `screen.getByTestId` could resolve a node an earlier test rendered. A test
// asserting that its own render produced something would then pass when the
// render produced nothing at all.
//
// The two tests below are ordered deliberately: the first renders a marker,
// the second asserts the document no longer holds it. Remove the `cleanup()`
// from `tests/frontend/setup.js` and the second fails.
import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

const MARKER = "render-isolation-marker";

describe("component test isolation", () => {
  test("a render puts its marker in the document", () => {
    render(<p data-testid={MARKER}>rendered by the first test</p>);
    expect(screen.getByTestId(MARKER)).toBeInTheDocument();
  });

  test("the next test does not see the previous test's render", () => {
    expect(screen.queryByTestId(MARKER)).toBeNull();
  });
});
