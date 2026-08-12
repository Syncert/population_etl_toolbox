import "@testing-library/jest-dom/vitest";
import { afterEach, vi } from "vitest";

if (!window.URL.createObjectURL) {
  window.URL.createObjectURL = vi.fn(() => "blob:unit-test");
}
if (!window.URL.revokeObjectURL) {
  window.URL.revokeObjectURL = vi.fn();
}

afterEach(() => {
  window.localStorage.clear();
});
