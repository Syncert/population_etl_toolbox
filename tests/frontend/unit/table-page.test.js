import { describe, expect, test } from "vitest";

// Covers: WEB-110 — the table alternative pages the whole loaded set and says
// how many rows there are, which it is showing, and what orders them.

import {
  TABLE_PAGE_SIZE,
  tableCaption,
  tablePageModel,
  tablePageRows,
} from "../../../apps/web/lib/tablePage";

const ORDER = {
  noun: { one: "row", many: "rows" },
  order: "in the order `/observations` declares",
};

describe("the table's page model", () => {
  test("the first page of many", () => {
    const model = tablePageModel(3144, 0);
    expect(model).toMatchObject({
      pageIndex: 0,
      pageCount: 63,
      firstRow: 1,
      lastRow: 50,
      total: 3144,
      hasPrevious: false,
      hasNext: true,
    });
    expect(tableCaption(model, ORDER)).toBe(
      "Showing 1–50 of 3,144 rows, in the order `/observations` declares.",
    );
  });

  test("the last page, which is short", () => {
    // 3,144 rows in pages of 50 leaves 44 on the last one. `lastRow` is the
    // row that exists, not the row the page size would reach.
    const model = tablePageModel(3144, 62);
    expect(model).toMatchObject({
      pageIndex: 62,
      firstRow: 3101,
      lastRow: 3144,
      hasPrevious: true,
      hasNext: false,
    });
  });

  test("a single page says so rather than counting to itself", () => {
    const model = tablePageModel(12, 0);
    expect(model).toMatchObject({
      pageCount: 1,
      firstRow: 1,
      lastRow: 12,
      hasPrevious: false,
      hasNext: false,
    });
    expect(tableCaption(model, ORDER)).toBe(
      "Showing all 12 rows, in the order `/observations` declares.",
    );
  });

  test("an empty result says there is nothing, not that it shows nothing", () => {
    const model = tablePageModel(0, 0);
    expect(model).toMatchObject({
      pageIndex: 0,
      pageCount: 0,
      firstRow: null,
      lastRow: null,
      total: 0,
      hasPrevious: false,
      hasNext: false,
    });
    expect(tableCaption(model, ORDER)).toBe("No rows to show.");
  });

  test("a page past the end is the last page, not an empty table", () => {
    // The page travels in the URL, so a link written when a filter matched
    // 3,144 rows can be opened when it matches 40. Reporting nothing there
    // would be this client showing an empty table for rows the API published.
    const model = tablePageModel(40, 12);
    expect(model.pageIndex).toBe(0);
    expect(model.firstRow).toBe(1);
    expect(model.lastRow).toBe(40);
    expect(tablePageRows(Array.from({ length: 40 }, (_, i) => i), 12)).toHaveLength(40);
  });

  test("a negative or nonsense page is the first page", () => {
    for (const page of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(tablePageModel(100, page).pageIndex).toBeGreaterThanOrEqual(0);
    }
    expect(tablePageModel(100, -5).pageIndex).toBe(0);
  });

  test("the rows of a page are the rows the model names", () => {
    const rows = Array.from({ length: 120 }, (_, index) => index);
    const model = tablePageModel(rows.length, 1);
    const page = tablePageRows(rows, 1);
    expect(page).toHaveLength(TABLE_PAGE_SIZE);
    expect(page[0]).toBe(model.firstRow - 1);
    expect(page[page.length - 1]).toBe(model.lastRow - 1);
  });

  test("one row is a row, not rows", () => {
    // The counts in this caption are read aloud by the readers it exists for.
    expect(tableCaption(tablePageModel(1, 0), ORDER)).toBe(
      "Showing all 1 row, in the order `/observations` declares.",
    );
  });

  test("the caption names what the rows are", () => {
    const model = tablePageModel(3144, 0);
    expect(
      tableCaption(model, {
        noun: { one: "geography", many: "geographies" },
        order: "in the order `/comparison` declares",
      }),
    ).toContain("3,144 geographies");
  });
});
