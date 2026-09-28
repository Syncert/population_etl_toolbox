import { expect, test } from "vitest";

import { PRODUCT_TEMPLATES } from "../../../apps/web/lib/productTemplates";
import { PUBLIC_ROUTES } from "../../../apps/web/lib/siteMap";
import { findUseCasePage, useCasePages } from "../../../apps/web/lib/useCasePages";

test("each reviewed product has one stable use-case address", () => {
  expect(useCasePages.map((entry) => entry.id)).toEqual(
    PRODUCT_TEMPLATES.map((entry) => entry.id),
  );
  expect(new Set(useCasePages.map((entry) => entry.href)).size).toBe(PRODUCT_TEMPLATES.length);
  expect(new Set(useCasePages.map((entry) => entry.title)).size).toBe(PRODUCT_TEMPLATES.length);
  for (const entry of useCasePages) {
    expect(entry.href).toBe(`/use-cases/${entry.id}`);
    expect(entry.title).toBeTruthy();
    expect(entry.summary).toBeTruthy();
    expect(entry.limits).toBeTruthy();
    expect(findUseCasePage(entry.id)).toBe(entry);
    expect(PUBLIC_ROUTES).toContain(entry.href);
  }
  expect(PUBLIC_ROUTES).toContain("/use-cases");
});

test("unknown or malformed use-case identifiers have no page", () => {
  expect(findUseCasePage("unreviewed-idea")).toBeNull();
  expect(findUseCasePage("../../catalog")).toBeNull();
});
