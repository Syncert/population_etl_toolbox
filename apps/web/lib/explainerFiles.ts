// Reads the explainer files, on the server only.
//
// `next.config.mjs` traces `content/explainers/` into the standalone build,
// so the same relative path resolves under `next start` and in the image.

import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

import { EXPLAINER_SLUG, parseExplainer } from "./explainerContent";
import type { Explainer } from "./explainerContent";

export const EXPLAINER_DIRECTORY = join(process.cwd(), "content", "explainers");

export function explainerSlugs(): string[] {
  return readdirSync(EXPLAINER_DIRECTORY)
    .filter((name) => name.endsWith(".md"))
    .map((name) => name.slice(0, -3))
    .sort();
}

export function loadExplainer(slug: string): Explainer | null {
  if (!EXPLAINER_SLUG.test(slug) || !explainerSlugs().includes(slug)) return null;
  return parseExplainer(slug, readFileSync(join(EXPLAINER_DIRECTORY, `${slug}.md`), "utf8"));
}

export function loadExplainers(): Explainer[] {
  return explainerSlugs().map((slug) => loadExplainer(slug)!);
}
