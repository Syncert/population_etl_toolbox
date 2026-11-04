// Where a place sits beside its state and the nation, drawn on one scale
// (web-level-strip).
//
// A headline card used to list each level's value as text, leaving the reader
// to do the subtraction. The strip puts every level on one shared axis with
// its published margin as a whisker, and one sentence says how the place
// compares, in the record page's own verbs (ADR-0011): "was higher than",
// "was lower than", "was not distinguishable from". A survey value is called
// higher or lower only when the two 90 percent intervals do not overlap.
//
// A count is never drawn: a county's population on the nation's scale is a
// sliver, and the comparison it invites is not a finding.

import { compareVerb, surveyMargin } from "./checkTheRecord";
import type { CompareVerb } from "./checkTheRecord";
import { publishedNumber } from "./explorerViewModel";
import type { CardRow } from "./placeChapters";

/** Headline measures that count things; their levels are never on one scale. */
export const COUNT_MEASURES: ReadonlySet<string> = new Set([
  "population-estimate",
  "bea-real-gdp",
  "qcew-jobs",
  "bps-single-family-year",
  "bps-multifamily-year",
  "bps-single-family-month",
  "corn-acres-harvested",
]);

export interface StripPoint {
  name: string;
  value: number;
  /** The published 90 percent margin, or null when none is published. */
  margin: number | null;
  /** 0 for the page's own place, then its parents in order. */
  index: number;
}

export interface LevelStrip {
  points: StripPoint[];
  domain: [number, number];
  ticks: number[];
  sentence: string;
}

function niceStep(span: number, count: number): number {
  const raw = span / Math.max(1, count);
  const power = 10 ** Math.floor(Math.log10(raw));
  const unit = raw / power;
  return (unit <= 1.5 ? 1 : unit <= 3 ? 2 : unit <= 7 ? 5 : 10) * power;
}

/** About three round ticks inside the domain. */
export function stripTicks(lo: number, hi: number): number[] {
  if (!(hi > lo)) return [lo];
  const step = niceStep(hi - lo, 3);
  const ticks: number[] = [];
  for (let tick = Math.ceil(lo / step) * step; tick <= hi + step * 1e-9; tick += step) {
    ticks.push(Number(tick.toPrecision(12)));
  }
  return ticks;
}

/** "the United States" in running text; every other place reads as named. */
function inText(name: string): string {
  return name === "United States" ? "the United States" : name;
}

function joinNames(names: string[]): string {
  return names.length < 3 ? names.join(" and ") : `${names.slice(0, -1).join(", ")} and ${names.at(-1)}`;
}

/**
 * "Dane County was higher than Wisconsin and the United States." Levels the
 * rule cannot be applied to (one margin known, the other not) are left out;
 * with none left, there is no sentence.
 */
export function stripSentence(points: readonly StripPoint[]): string {
  const [own, ...others] = points;
  if (!own || own.index !== 0 || !others.length) return "";
  const surveyed = points.some((point) => point.margin !== null);
  const groups = new Map<CompareVerb, string[]>();
  for (const other of others) {
    const verb = compareVerb(own.value, other.value, surveyed ? { value: own.margin, other: other.margin } : null);
    if (!verb) continue;
    groups.set(verb, [...(groups.get(verb) || []), inText(other.name)]);
  }
  if (!groups.size) return "";
  const order: CompareVerb[] = ["was higher than", "was lower than", "was not distinguishable from"];
  const clauses = order.filter((verb) => groups.has(verb)).map((verb) => `${verb} ${joinNames(groups.get(verb)!)}`);
  const tail = surveyed ? " at the published margins of error" : "";
  const subject = inText(own.name);
  return `${subject.charAt(0).toUpperCase()}${subject.slice(1)} ${joinNames(clauses)}${tail}.`;
}

/**
 * The strip for one headline card, or null when there is nothing to place:
 * a count, a derived view, or fewer than two levels with a published value.
 */
export function levelStrip(measureId: string, rows: readonly CardRow[]): LevelStrip | null {
  if (COUNT_MEASURES.has(measureId)) return null;
  const points: StripPoint[] = [];
  rows.forEach((entry, index) => {
    if (!entry.row || entry.message) return;
    const value = publishedNumber(entry.row.value);
    if (value === null) return;
    points.push({ name: entry.place.name, value, margin: surveyMargin(entry.row), index });
  });
  if (points.length < 2) return null;
  let lo = Math.min(...points.map((point) => point.value - (point.margin ?? 0)));
  let hi = Math.max(...points.map((point) => point.value + (point.margin ?? 0)));
  const pad = (hi - lo || Math.abs(hi) || 1) * 0.15;
  lo -= pad;
  hi += pad;
  // A rate or a median that cannot go below zero is not drawn below it.
  if (lo < 0 && points.every((point) => point.value >= 0)) lo = 0;
  return { points, domain: [lo, hi], ticks: stripTicks(lo, hi), sentence: stripSentence(points) };
}
