// A published period in the reader's words (website UX review 2026-11, B1).
//
// The API serves a period as its first and last day. A card that prints
// "2020-01-01 – 2024-12-31" makes the reader work out that it means the
// 2020–2024 survey; this module says "2020–2024", "2024", "September 2026",
// "January–March 2026" or "July 1, 2025". It only rewrites how the same two
// dates are written: a span that is not whole years or whole months keeps
// both of its dates, in words.

const DAY = String.raw`(\d{4})-(\d{2})-(\d{2})`;
const RANGE = new RegExp(`${DAY} – ${DAY}`, "g");
const SINGLE = new RegExp(String.raw`(?<![\d-])${DAY}(?![\d-])`, "g");

const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"] as const;

const monthName = (month: number) => MONTHS[month - 1] || String(month).padStart(2, "0");
const lastDay = (year: number, month: number) => new Date(Date.UTC(year, month, 0)).getUTCDate();
const day = (year: number, month: number, date: number) => `${monthName(month)} ${date}, ${year}`;

function span(y1: number, m1: number, d1: number, y2: number, m2: number, d2: number): string {
  const wholeMonths = d1 === 1 && d2 === lastDay(y2, m2);
  if (wholeMonths && m1 === 1 && m2 === 12) return y1 === y2 ? String(y1) : `${y1}–${y2}`;
  if (wholeMonths && y1 === y2) return m1 === m2 ? `${monthName(m1)} ${y1}` : `${monthName(m1)}–${monthName(m2)} ${y1}`;
  if (wholeMonths) return `${monthName(m1)} ${y1} – ${monthName(m2)} ${y2}`;
  if (y1 === y2 && m1 === m2 && d1 === d2) return day(y1, m1, d1);
  return `${day(y1, m1, d1)} – ${day(y2, m2, d2)}`;
}

/**
 * Every ISO date and date range in `text`, rewritten in words; everything
 * else in the text is left as it is.
 */
export function plainPeriods(text: string | null | undefined): string {
  return String(text || "")
    .replace(RANGE, (_, y1, m1, d1, y2, m2, d2) => span(+y1, +m1, +d1, +y2, +m2, +d2))
    .replace(SINGLE, (_, y, m, d) => day(+y, +m, +d));
}

/**
 * The period a series begins in, from its first day alone: "2020" for a
 * January 1, "July 2025" for any other first of a month, else the day.
 */
export function plainPeriodStart(start: string | null | undefined): string {
  const match = new RegExp(`^${DAY}`).exec(String(start || ""));
  if (!match) return String(start || "");
  const [y, m, d] = [Number(match[1]), Number(match[2]), Number(match[3])];
  if (d !== 1) return day(y, m, d);
  return m === 1 ? String(y) : `${monthName(m)} ${y}`;
}
