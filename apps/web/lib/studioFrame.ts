// The studio's frames (studio-video-frames).
//
// A frame is one place-page headline measure for a place, its state and the
// nation, drawn as a fixed-size SVG for video: large type, the three level
// colours, and a footer carrying the measure, period, source, uncertainty and
// site name. The footer is part of the one drawing function and has no
// option to leave it out, so a frame clipped out of its context still says
// where its numbers came from. Values are the API's, read by the requests the
// frame records; nothing here recomputes one.

import type { AnalysisDocument } from "./api/types";

export const FRAME_FORMATS = {
  "16:9": { width: 1920, height: 1080 },
  "9:16": { width: 1080, height: 1920 },
  "1:1": { width: 1080, height: 1080 },
} as const;

export type FrameFormat = keyof typeof FRAME_FORMATS;
export type FrameTheme = "light" | "dark";

/** The fixed County, State and Nation colours every frame uses. */
export const LEVEL_COLORS = {
  COUNTY: "#0b6b57",
  STATE: "#8c3b17",
  NATIONAL: "#2b4c8c",
} as const;

const THEMES = {
  light: { background: "#ffffff", ink: "#17232d", muted: "#5b6872", rule: "#d6ddd9" },
  dark: { background: "#101820", ink: "#f2f5f3", muted: "#b4c0c8", rule: "#33424d" },
} as const;

export const SITE_NAME = "Economic Data Studio";

export interface FrameRow {
  level: keyof typeof LEVEL_COLORS;
  name: string;
  /** The value as the page formats it, or a sentence when none was published. */
  valueText: string;
  /** The number, for bar length, or null when none was published. */
  value: number | null;
  uncertainty: string;
}

export interface FrameSpec {
  format: FrameFormat;
  theme: FrameTheme;
  title: string;
  metricCode: string;
  measureName: string;
  unit: string;
  period: string;
  source: string;
  caveat: string;
  rows: FrameRow[];
}

function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

/** Break text into lines of at most `limit` characters, at spaces. */
export function wrapText(text: string, limit: number): string[] {
  const lines: string[] = [];
  let line = "";
  for (const word of text.split(/\s+/).filter(Boolean)) {
    if (line && `${line} ${word}`.length > limit) {
      lines.push(line);
      line = word;
    } else {
      line = line ? `${line} ${word}` : word;
    }
  }
  if (line) lines.push(line);
  return lines;
}

/** The footer's lines, in the order they are drawn. Always all four. */
export function footerLines(spec: FrameSpec): string[] {
  const uncertainty = spec.rows
    .filter((row) => row.uncertainty)
    .map((row) => `${row.name} ${row.uncertainty}`)
    .join("; ");
  return [
    `${spec.measureName} (${spec.metricCode})`,
    `Period: ${spec.period || "not published"} · Source: ${spec.source || "not published"}`,
    `Uncertainty: ${uncertainty || "none published"}`,
    `${spec.caveat ? `${spec.caveat} · ` : ""}${SITE_NAME}`,
  ];
}

/**
 * The frame as an SVG document of the format's exact pixel size.
 *
 * The footer is drawn unconditionally: there is no parameter that removes
 * it, which is what the unit tier asserts for every format.
 */
export function renderFrameSvg(spec: FrameSpec): string {
  const { width, height } = FRAME_FORMATS[spec.format];
  const theme = THEMES[spec.theme];
  const portrait = height > width;
  const margin = Math.round(width * 0.06);
  const titleSize = portrait ? 76 : 64;
  const rowSize = portrait ? 60 : 52;
  const footerSize = portrait ? 30 : 26;
  const footer = footerLines(spec).flatMap((line) => wrapText(line, Math.floor((width - margin * 2) / (footerSize * 0.52))));
  const footerTop = height - margin - footer.length * footerSize * 1.35;
  const titleLines = wrapText(spec.title, Math.floor((width - margin * 2) / (titleSize * 0.55)));
  const max = Math.max(1, ...spec.rows.map((row) => (row.value === null ? 0 : Math.abs(row.value))));
  const rowsTop = margin + titleLines.length * titleSize * 1.15 + titleSize * 0.8;
  const rowGap = Math.min(rowSize * 3.2, (footerTop - rowsTop - rowSize) / Math.max(1, spec.rows.length));
  const barWidth = width - margin * 2;
  const parts: string[] = [
    `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" font-family="Segoe UI, Helvetica, Arial, sans-serif">`,
    `<rect width="${width}" height="${height}" fill="${theme.background}"/>`,
    ...titleLines.map(
      (line, index) =>
        `<text x="${margin}" y="${margin + titleSize * (index + 1)}" font-size="${titleSize}" font-weight="700" fill="${theme.ink}">${escapeXml(line)}</text>`,
    ),
  ];
  spec.rows.forEach((row, index) => {
    const top = rowsTop + index * rowGap;
    const color = LEVEL_COLORS[row.level];
    parts.push(
      `<text x="${margin}" y="${top + rowSize}" font-size="${rowSize}" font-weight="700" fill="${color}">${escapeXml(row.name)}</text>`,
      `<text x="${width - margin}" y="${top + rowSize}" font-size="${rowSize}" font-weight="700" text-anchor="end" fill="${theme.ink}">${escapeXml(row.valueText)}</text>`,
    );
    if (row.value !== null) {
      parts.push(
        `<rect x="${margin}" y="${top + rowSize * 1.35}" width="${Math.max(4, (Math.abs(row.value) / max) * barWidth)}" height="${rowSize * 0.5}" fill="${color}"/>`,
      );
    }
  });
  parts.push(
    `<line x1="${margin}" x2="${width - margin}" y1="${footerTop - footerSize}" y2="${footerTop - footerSize}" stroke="${theme.rule}" stroke-width="2"/>`,
    `<g data-frame-footer="true">`,
    ...footer.map(
      (line, index) =>
        `<text x="${margin}" y="${footerTop + footerSize * 1.35 * index}" font-size="${footerSize}" fill="${theme.muted}">${escapeXml(line)}</text>`,
    ),
    `</g>`,
    `</svg>`,
  );
  return parts.join("");
}

export interface FrameRecordInput {
  spec: FrameSpec;
  placePath: string;
  chapterId: string;
  measureId: string;
  /** The exact observation requests the frame's values came from. */
  requests: { level: string; geoId: string; url: string }[];
  /** The release each row's value came from, by geo_id. */
  releases: Record<string, string>;
  /**
   * Whether the requests asked for one row per geography: only where the
   * source declares that reduction, which the API checks again on save.
   */
  newestPerGeography: boolean;
}

/**
 * The frame record stored to the account: a workbench document whose series
 * are the frame's observation requests, so the API validates each as it
 * validates any saved observations request, with the studio's own fields
 * and the exact request URLs in `visualization`, which the API stores
 * verbatim.
 */
export function frameRecord(input: FrameRecordInput): AnalysisDocument {
  return {
    kind: "workbench",
    series: input.requests.map((request) => ({
      metric_code: input.spec.metricCode,
      scope: "latest",
      newest_per_geography: input.newestPerGeography,
      filters: { geo_id: request.geoId },
    })),
    presentation: { type: "bar", options: {} },
    visualization: {
      studio: {
        version: 1,
        format: input.spec.format,
        theme: input.spec.theme,
        placePath: input.placePath,
        chapterId: input.chapterId,
        measureId: input.measureId,
        metricCode: input.spec.metricCode,
        period: input.spec.period,
        requests: input.requests,
        releases: input.releases,
      },
    },
  } as AnalysisDocument;
}

export interface StudioVisualization {
  version: number;
  format: FrameFormat;
  theme: FrameTheme;
  placePath: string;
  chapterId: string;
  measureId: string;
  metricCode: string;
  period: string;
  requests: { level: string; geoId: string; url: string }[];
  releases: Record<string, string>;
}

/** The studio fields of a stored document, or null when it is not a frame. */
export function studioRecord(document: unknown): StudioVisualization | null {
  const studio = (document as { visualization?: { studio?: StudioVisualization } } | null)?.visualization?.studio;
  if (!studio || studio.version !== 1 || !(studio.format in FRAME_FORMATS) || !Array.isArray(studio.requests)) return null;
  return studio;
}

export interface ScriptNotes {
  definition: string;
  reviewState: "reviewed" | "not reviewed";
  caveats: string[];
  period: string;
  explainer: string | null;
}

export interface ReviewedDefinition {
  text: string;
  reviewedOn: string;
}

/**
 * The voice-over's sources, read-only: the reviewed definition where one
 * exists, otherwise the harvested label marked `not reviewed`
 * (docs/semantics/README.md), the caveats, and the period sentence.
 */
export function scriptNotes(input: {
  harvestedLabel: string;
  definition: ReviewedDefinition | null;
  caveats: string[];
  period: string;
  explainerHref?: string | null;
}): ScriptNotes {
  return {
    definition: input.definition ? input.definition.text : input.harvestedLabel,
    reviewState: input.definition ? "reviewed" : "not reviewed",
    caveats: input.caveats.filter(Boolean),
    period: input.period ? `These figures describe ${input.period}.` : "No period was published for these figures.",
    explainer: input.explainerHref || null,
  };
}

/**
 * Reviewed definitions, by metric_code. `docs/semantics/` holds none yet, so
 * every frame reads `not reviewed`; a definition added there is added here
 * in the same change, and the unit tier covers both states.
 */
export const REVIEWED_DEFINITIONS: Readonly<Record<string, ReviewedDefinition>> = {};
