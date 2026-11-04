/**
 * How many years of a measure are published, in words a reader can check.
 *
 * The explorer's status row used to count `/observations/releases`, which
 * lists release identities. For some sources that identity is a vintage
 * (Census ACS 2024), but for others it is the moment a load captured the
 * publisher's file: Census SAIPE median household income for Dane County
 * read "11 published releases" when the warehouse held two years, 2023 and
 * 2024, captured by two load runs (OFFICE_LAB, 2026-11-03). A reader takes
 * "11 releases" to mean eleven years of data. What a reader wants to know is
 * which years they can look at, and that is what the period listing says.
 */

/** A period as `/observations/periods` lists it. */
export interface PublishedPeriod {
  period_start: string;
  period_end?: string | null;
}

/**
 * The calendar year a period belongs to: its last year. An ACS five-year
 * period 2020-01-01 – 2024-12-31 is the 2024 estimate, a SAIPE single year
 * is its own year, and a month or a July 1 estimate is the year it falls in.
 */
export function periodYear(period: PublishedPeriod): number | null {
  const text = String(period.period_end || period.period_start || "");
  const year = Number(text.slice(0, 4));
  return /^\d{4}/.test(text) && Number.isFinite(year) ? year : null;
}

/** The distinct years the listed periods cover, oldest first. */
export function publishedYears(periods: readonly PublishedPeriod[]): number[] {
  const years = new Set<number>();
  for (const period of periods) {
    const year = periodYear(period);
    if (year !== null) years.add(year);
  }
  return [...years].sort((left, right) => left - right);
}

/**
 * "2 years published, 2023–2024". A run with missing years says "between"
 * rather than drawing a dash across years nothing was published for, and a
 * listing the page bound cut short says it is a lower bound.
 */
export function describePublishedYears(
  periods: readonly PublishedPeriod[],
  complete = true,
): string {
  const years = publishedYears(periods);
  if (!years.length) {
    return complete ? "no published years" : "no published years loaded yet; the list is incomplete";
  }
  const first = years[0]!;
  const last = years[years.length - 1]!;
  const count = years.length;
  const noun = count === 1 ? "year" : "years";
  const lead = complete ? `${count} ${noun} published` : `at least ${count} ${noun} published`;
  let span: string;
  if (count === 1) {
    span = String(first);
  } else if (last - first + 1 === count) {
    span = `${first}–${last}`;
  } else {
    span = `between ${first} and ${last}, with gaps`;
  }
  const tail = complete ? "" : "; the list is incomplete";
  return `${lead}, ${span}${tail}`;
}
