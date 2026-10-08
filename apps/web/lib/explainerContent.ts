// Explainers: reader-facing answers to the caveats the data carries
// (explainer-pages).
//
// Each explainer is a Markdown file under `content/explainers/` with a small
// frontmatter block and four required sections. This module parses and
// validates that format; it reads no files, so the unit tier and the server
// route share it. The format is deliberately small -- paragraphs and bullet
// lists, no HTML and no inline markup -- so rendering needs no Markdown
// library and nothing in a file can reach the page as markup.

export const EXPLAINER_SECTIONS = [
  "Short answer",
  "What it is not",
  "Worked example",
  "Where it is used",
] as const;

export type ExplainerSectionTitle = (typeof EXPLAINER_SECTIONS)[number];

export type ExplainerBlock =
  | { kind: "paragraph"; text: string }
  | { kind: "list"; items: string[] };

export interface Explainer {
  slug: string;
  title: string;
  summary: string;
  caveatKeys: string[];
  metricCodes: string[];
  sources: string[];
  exampleMetric: string;
  reviewed: string;
  reviewer: string;
  /** Reviewed definitions under `docs/semantics/` this explainer cites. */
  definitions: string[];
  /** The published video, when there is one. */
  videoUrl: string;
  sections: { title: ExplainerSectionTitle; blocks: ExplainerBlock[] }[];
}

export const EXPLAINER_SLUG = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

function parseList(value: string): string[] {
  const trimmed = value.trim();
  if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) return trimmed ? [trimmed] : [];
  return trimmed
    .slice(1, -1)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseFrontmatter(source: string): { fields: Record<string, string>; body: string } {
  const normalized = source.replace(/\r\n/g, "\n");
  const match = /^---\n([\s\S]*?)\n---\n?([\s\S]*)$/.exec(normalized);
  if (!match) throw new Error("missing frontmatter block");
  const fields: Record<string, string> = {};
  for (const line of match[1]!.split("\n")) {
    if (!line.trim()) continue;
    const separator = line.indexOf(":");
    if (separator < 1) throw new Error(`unreadable frontmatter line: ${line}`);
    fields[line.slice(0, separator).trim()] = line.slice(separator + 1).trim();
  }
  return { fields, body: match[2]! };
}

function parseBlocks(lines: string[]): ExplainerBlock[] {
  const blocks: ExplainerBlock[] = [];
  let paragraph: string[] = [];
  let list: string[] | null = null;
  const flush = () => {
    if (paragraph.length) blocks.push({ kind: "paragraph", text: paragraph.join(" ") });
    paragraph = [];
    if (list) blocks.push({ kind: "list", items: list });
    list = null;
  };
  for (const raw of lines) {
    const line = raw.trim();
    if (!line) {
      flush();
    } else if (line.startsWith("- ")) {
      if (paragraph.length) {
        blocks.push({ kind: "paragraph", text: paragraph.join(" ") });
        paragraph = [];
      }
      list = list || [];
      list.push(line.slice(2).trim());
    } else if (list) {
      list[list.length - 1] += ` ${line}`;
    } else {
      paragraph.push(line);
    }
  }
  flush();
  return blocks;
}

/** Parse one explainer file. Throws on a file the format cannot read. */
export function parseExplainer(slug: string, source: string): Explainer {
  const { fields, body } = parseFrontmatter(source);
  const sections: Explainer["sections"] = [];
  let current: { title: string; lines: string[] } | null = null;
  const done = () => {
    if (!current) return;
    if (!(EXPLAINER_SECTIONS as readonly string[]).includes(current.title)) {
      throw new Error(`unknown section "${current.title}"`);
    }
    sections.push({ title: current.title as ExplainerSectionTitle, blocks: parseBlocks(current.lines) });
  };
  for (const line of body.split("\n")) {
    if (line.startsWith("## ")) {
      done();
      current = { title: line.slice(3).trim(), lines: [] };
    } else if (line.startsWith("#")) {
      throw new Error(`only level-2 section headings are allowed: ${line}`);
    } else if (current) {
      current.lines.push(line);
    } else if (line.trim()) {
      throw new Error("text before the first section");
    }
  }
  done();
  return {
    slug,
    title: fields.title || "",
    summary: fields.summary || "",
    caveatKeys: parseList(fields.caveat_keys || ""),
    metricCodes: parseList(fields.metric_codes || ""),
    sources: parseList(fields.sources || ""),
    exampleMetric: fields.example_metric || "",
    reviewed: fields.reviewed || "",
    reviewer: fields.reviewer || "",
    definitions: parseList(fields.definitions || ""),
    videoUrl: fields.video_url || "",
    sections,
  };
}

const KNOWN_FIELDS = new Set([
  "title",
  "summary",
  "caveat_keys",
  "metric_codes",
  "sources",
  "example_metric",
  "reviewed",
  "reviewer",
  "definitions",
  "video_url",
]);

/**
 * Every way a parsed explainer breaks the format, as sentences; empty when
 * it is valid. `knownMetricCodes` is the set of catalog identities the
 * caller can vouch for.
 */
export function explainerProblems(
  explainer: Explainer,
  source: string,
  knownMetricCodes: ReadonlySet<string>,
): string[] {
  const problems: string[] = [];
  const { fields } = parseFrontmatter(source);
  for (const key of Object.keys(fields)) {
    if (!KNOWN_FIELDS.has(key)) problems.push(`unknown frontmatter field "${key}"`);
  }
  if (!EXPLAINER_SLUG.test(explainer.slug)) problems.push("slug is not lower-case words joined by hyphens");
  if (!explainer.title.endsWith("?")) {
    problems.push("title should be the reader's question");
  }
  if (!explainer.summary) problems.push("summary is empty");
  if (!explainer.caveatKeys.length) problems.push("no caveat_keys");
  if (!explainer.metricCodes.length) problems.push("no metric_codes");
  for (const code of explainer.metricCodes) {
    if (!knownMetricCodes.has(code)) problems.push(`metric_code ${code} is not a known catalog identity`);
  }
  if (!explainer.metricCodes.includes(explainer.exampleMetric)) {
    problems.push("example_metric must be one of metric_codes");
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(explainer.reviewed)) problems.push("reviewed is not a YYYY-MM-DD date");
  if (!explainer.reviewer) problems.push("reviewer is empty");
  if (explainer.videoUrl && !/^https:\/\//.test(explainer.videoUrl)) problems.push("video_url must be https");
  const titles = explainer.sections.map((section) => section.title);
  if (titles.join("|") !== EXPLAINER_SECTIONS.join("|")) {
    problems.push(`sections must be exactly, in order: ${EXPLAINER_SECTIONS.join(", ")}`);
  }
  for (const section of explainer.sections) {
    if (!section.blocks.length) problems.push(`section "${section.title}" is empty`);
  }
  if (/<[a-z/!]/i.test(source)) problems.push("explainers carry no HTML");
  return problems;
}
