import { expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { PRODUCT_TEMPLATES } from "../../../apps/web/lib/productTemplates";
import { PUBLIC_ROUTES } from "../../../apps/web/lib/siteMap";
import { findUseCasePage, useCaseGroups, useCasePages } from "../../../apps/web/lib/useCasePages";

// Covers: WEB-123 — every documented opportunity has an address, guardrails,
// reusable published measures, and a discoverable navigation group.
test("all twenty markdown use cases have distinct pages in priority order", () => {
  const markdown = readFileSync(resolve("../../docs/product/TOP_20_DATA_PRODUCT_USE_CASES.md"), "utf8");
  const rows = markdown.split("\n").filter((line) => /^\| \d+ \|/.test(line));
  expect(rows).toHaveLength(20);
  expect(useCasePages).toHaveLength(20);
  for (const [index, row] of rows.entries()) {
    const fields = row.split("|").map((field) => field.trim());
    const entry = useCasePages[index];
    expect(entry.rank).toBe(Number(fields[1]));
    expect(entry.title).toBe(fields[2]);
    expect(entry.audience).toBe(fields[3]);
    expect(entry.guardrail).toBe(fields[6]);
    expect(entry.steps).toHaveLength(3);
    expect(entry.question.length).toBeGreaterThan(20);
    expect(entry.sections.length).toBeGreaterThan(0);
  }
  expect(new Set(useCasePages.map((entry) => entry.question)).size).toBe(20);
  const grouped = useCaseGroups.flatMap((group) => group.pages.map((page) => page.id));
  expect(new Set(grouped).size).toBe(20);
  expect(useCaseGroups).toHaveLength(6);
});

test("each reviewed product keeps its stable use-case address", () => {
  for (const template of PRODUCT_TEMPLATES) expect(findUseCasePage(template.id)).toBeTruthy();
  expect(new Set(useCasePages.map((entry) => entry.href)).size).toBe(20);
  expect(new Set(useCasePages.map((entry) => entry.title)).size).toBe(20);
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

test("new compositions use only already verified candidate identities without repeating slots", () => {
  const verified = new Set(PRODUCT_TEMPLATES.flatMap((entry) => entry.sections.flatMap((section) => section.measures.flatMap((slot) => slot.candidates))));
  for (const entry of useCasePages) {
    const slots = entry.sections.flatMap((section) => section.measures);
    expect(new Set(slots.map((slot) => slot.id)).size, entry.id).toBe(slots.length);
    for (const slot of slots) for (const code of slot.candidates) expect(verified.has(code), code).toBe(true);
  }
});

test("unknown or malformed use-case identifiers have no page", () => {
  expect(findUseCasePage("unreviewed-idea")).toBeNull();
  expect(findUseCasePage("../../catalog")).toBeNull();
});
