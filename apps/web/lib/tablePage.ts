// Paging a table the client already holds.
//
// The explorer's table is the map's accessible alternative: the README says
// every value the map would show "remains available in the observation
// table", and the accessibility suite treats it as that alternative. It
// rendered `observations.slice(0, 12)` under a heading with no caption, no
// count and no way forward, and the comparison table did the same at 25. A
// national county map colours 3,144 geographies, so a reader who cannot use
// the map reached twelve of them.
//
// This is a *render* bound, not a read bound. The rows are already loaded --
// WEB-036, WEB-056 and WEB-059 made the read honest, naming a partial one
// wherever it happens -- so paging here needs no request and can page the
// whole loaded set. What it must not do is imply the loaded set is the whole
// published set; the caption counts what is here, and the screen's own
// partial-read notice says when that is less than everything.

import { formatNumber } from "./format";

/** Rows per page. Enough that a page is worth reading, few enough to scan. */
export const TABLE_PAGE_SIZE = 50;

export interface TablePageModel {
  /** Zero-based, clamped into range: a page beyond the end is the last page. */
  pageIndex: number;
  pageCount: number;
  /** One-based row numbers of this page, or null where there are no rows. */
  firstRow: number | null;
  lastRow: number | null;
  total: number;
  hasPrevious: boolean;
  hasNext: boolean;
}

/**
 * Where a page sits in a loaded set.
 *
 * The requested page is clamped rather than honoured blindly: the page
 * travels in the URL, so a link written when a filter matched 3,144 rows can
 * be opened when it matches 40, and showing an empty table for page 12 would
 * be this client reporting nothing where the API published something.
 */
export function tablePageModel(
  total: number,
  requestedPage: number,
  pageSize: number = TABLE_PAGE_SIZE,
): TablePageModel {
  const rows = Math.max(0, Math.floor(total) || 0);
  const size = Math.max(1, Math.floor(pageSize) || TABLE_PAGE_SIZE);
  const pageCount = rows === 0 ? 0 : Math.ceil(rows / size);
  const pageIndex =
    pageCount === 0
      ? 0
      : Math.min(Math.max(0, Math.floor(requestedPage) || 0), pageCount - 1);
  const offset = pageIndex * size;

  return {
    pageIndex,
    pageCount,
    firstRow: rows === 0 ? null : offset + 1,
    lastRow: rows === 0 ? null : Math.min(offset + size, rows),
    total: rows,
    hasPrevious: pageIndex > 0,
    hasNext: pageIndex + 1 < pageCount,
  };
}

/** The rows of one page, from the same clamping the model applies. */
export function tablePageRows<T>(
  rows: readonly T[],
  requestedPage: number,
  pageSize: number = TABLE_PAGE_SIZE,
): T[] {
  const size = Math.max(1, Math.floor(pageSize) || TABLE_PAGE_SIZE);
  const model = tablePageModel(rows.length, requestedPage, size);
  const offset = model.pageIndex * size;
  return rows.slice(offset, offset + size);
}

/**
 * What the table says about itself.
 *
 * Three facts, because a reader who cannot see the map needs all three: how
 * many rows there are, which of them they are looking at, and what decides
 * the order -- without which "rows 51 to 100" names no particular rows.
 *
 * `noun` is what the rows are, in both forms -- a table holding one row says
 * "1 loaded row", not "1 loaded rows"; the counts here are read aloud by the
 * readers this caption exists for. `order` is the resource's declared order
 * in one phrase; the guide's "Paging a history, and what orders it" table is
 * the authority for the wording.
 */
export interface TableNoun {
  one: string;
  many: string;
}

export function tableCaption(
  model: TablePageModel,
  { noun, order }: { noun: TableNoun; order: string },
): string {
  if (model.total === 0) {
    return `No ${noun.many} to show.`;
  }
  const counted = model.total === 1 ? noun.one : noun.many;
  const range =
    model.pageCount === 1
      ? `Showing all ${formatNumber(model.total)} ${counted}`
      : `Showing ${formatNumber(model.firstRow)}–${formatNumber(model.lastRow)} of ` +
        `${formatNumber(model.total)} ${counted}`;
  return `${range}, ${order}.`;
}
