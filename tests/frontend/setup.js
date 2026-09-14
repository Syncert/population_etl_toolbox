import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";

if (!window.URL.createObjectURL) {
  window.URL.createObjectURL = vi.fn(() => "blob:unit-test");
}
if (!window.URL.revokeObjectURL) {
  window.URL.revokeObjectURL = vi.fn();
}

afterEach(() => {
  // `@testing-library/react` registers its own auto-cleanup only when a
  // global `afterEach` exists, and this project runs vitest without
  // `globals`, so every test in a file shared one accumulating document: a
  // `screen.getByTestId` could resolve an element an *earlier* test rendered,
  // and a test asserting that its own render produced something would pass
  // even when the render produced nothing. Unmounting here is what makes each
  // test's assertions about its own render (WEB-077).
  cleanup();
  window.localStorage.clear();
});
