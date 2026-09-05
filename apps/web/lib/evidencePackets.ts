// Evidence packets: reusable blocks with a reproducibility envelope.
//
// A packet is a composed argument — narrative beside analytical blocks — and
// the risk it exists to manage is that composition strips context. A chart
// lifted out of the explorer and dropped into a document usually loses which
// measure it was, for where, over what period, at what publication, and with
// what caveats; what survives is a shape that looks authoritative.
//
// So every analytical block here carries a reproducibility envelope, and the
// packet reports any block whose envelope is incomplete rather than letting
// it render as finished evidence. Nothing in this module computes an
// analytical value: blocks name queries, and the envelope says exactly what
// each block asked for.

import type { AnalysisDocument } from "./api/types";
import { reopenHref } from "./savedAnalysis";

export const BLOCK_TYPES = [
  "text",
  "analysis",
  "table",
  "map",
  "source-note",
  "methodology",
  "caveat",
] as const;

export type BlockType = (typeof BLOCK_TYPES)[number];

/** Block kinds that present provider data and therefore need an envelope. */
export const ANALYTICAL_BLOCK_TYPES: BlockType[] = ["analysis", "table", "map"];

/**
 * Everything needed to reproduce, and to correctly read, one analytical
 * block outside the page that shows it.
 */
export interface ReproducibilityEnvelope {
  /** Catalog identities the block presents. */
  metricCodes: string[];
  sourceCodes: string[];
  /** The geography the block is about, as the API names it. */
  geoId: string;
  geoLevel: string;
  /** Which publication: the source's latest, or a pinned release. */
  scope: "latest" | "as_released";
  release: string;
  /** The period the presented values describe, as published. */
  period: string;
  units: string;
  /** Any client-side transformation. "none" is the only honest default. */
  transformation: string;
  /** The exact request that reproduces the block. */
  apiQuery: string;
  /** Caveats that must travel with the block. */
  caveats: string[];
}

export const EMPTY_ENVELOPE: ReproducibilityEnvelope = Object.freeze({
  metricCodes: [],
  sourceCodes: [],
  geoId: "",
  geoLevel: "",
  scope: "latest",
  release: "",
  period: "",
  units: "",
  transformation: "none",
  apiQuery: "",
  caveats: [],
}) as ReproducibilityEnvelope;

export interface PacketBlock {
  id: string;
  type: BlockType;
  /** Heading shown above the block. */
  title: string;
  /** Narrative, methodology, or caveat prose. */
  content?: string;
  /** Present on analytical blocks; absent means the block is not evidence. */
  envelope?: ReproducibilityEnvelope;
  /** The saved-analysis document this block replays, when it has one. */
  document?: AnalysisDocument;
}

export interface EvidencePacket {
  version: 1;
  title: string;
  /** What the packet is for, in the author's words. */
  purpose: string;
  blocks: PacketBlock[];
  updatedAt: string;
}

/**
 * The grant needs-assessment starting packet.
 *
 * It is a skeleton of prompts and required context blocks, not a finished
 * argument: the methodology and caveat blocks are present from the start so
 * a packet cannot be assembled without them, and the closing note states
 * what selected measures cannot establish.
 */
export function grantNeedsTemplate(now: string = new Date().toISOString()): EvidencePacket {
  return {
    version: 1,
    title: "Needs assessment",
    purpose:
      "Describe the need this proposal addresses, using published measures that each stand on their own.",
    blocks: [
      {
        id: "summary",
        type: "text",
        title: "Summary of need",
        content:
          "State the need in plain terms, and say which published measures support each part of it.",
      },
      {
        id: "population-evidence",
        type: "analysis",
        title: "Population context",
        content: "Add a saved view from the explorer to fill this block.",
      },
      {
        id: "condition-evidence",
        type: "analysis",
        title: "Condition being addressed",
        content: "Add a saved view from the explorer to fill this block.",
      },
      {
        id: "methodology",
        type: "methodology",
        title: "Methodology",
        content:
          "Name each source, the period each measure covers, the geography basis, and any comparison rules that were checked before combining measures.",
      },
      {
        id: "limits",
        type: "caveat",
        title: "What these measures do not establish",
        content:
          "These measures describe conditions in a place. They do not establish that a program caused a change, and they do not establish that a change would follow from funding. State associations as associations.",
      },
    ],
    updatedAt: now,
  };
}

export function isAnalyticalBlock(block: PacketBlock | null | undefined): boolean {
  return Boolean(block && ANALYTICAL_BLOCK_TYPES.includes(block.type));
}

export interface BlockIssue {
  blockId: string;
  title: string;
  /** The envelope fields the block is missing. */
  missing: string[];
  reason: string;
}

/**
 * Analytical blocks whose envelope is incomplete.
 *
 * Reported rather than repaired or hidden: a block missing its source or
 * period is exactly the failure this module exists to prevent, and quietly
 * filling it in would put a guess where the author's evidence should be.
 */
export function packetIssues(packet: EvidencePacket | null | undefined): BlockIssue[] {
  const issues: BlockIssue[] = [];
  for (const block of packet?.blocks || []) {
    if (!isAnalyticalBlock(block)) {
      continue;
    }
    const envelope = block.envelope;
    if (!envelope) {
      issues.push({
        blockId: block.id,
        title: block.title,
        missing: ["metricCodes", "sourceCodes", "geoId", "period", "apiQuery"],
        reason: "this block presents no analysis yet, so it carries no reproducibility envelope",
      });
      continue;
    }
    const missing: string[] = [];
    if (envelope.metricCodes.length === 0) missing.push("metricCodes");
    if (envelope.sourceCodes.length === 0) missing.push("sourceCodes");
    if (!envelope.geoId && !envelope.geoLevel) missing.push("geoId");
    if (!envelope.period) missing.push("period");
    if (!envelope.apiQuery) missing.push("apiQuery");
    if (missing.length > 0) {
      issues.push({
        blockId: block.id,
        title: block.title,
        missing,
        reason: "this block would present values without the context needed to read them",
      });
    }
  }
  return issues;
}

/** True when every analytical block carries a complete envelope. */
export function packetIsComplete(packet: EvidencePacket | null | undefined): boolean {
  return Boolean(packet && packet.blocks.length > 0 && packetIssues(packet).length === 0);
}

export interface LiveStatus {
  /** Shared request-state vocabulary value, for the status pill. */
  state: string;
  label: string;
  detail: string;
}

/**
 * Whether a block follows the warehouse or is pinned to one publication.
 *
 * Both are legitimate and they mean different things in a proposal: a live
 * block will change when the source republishes, and a frozen one reproduces
 * a specific release. A packet that did not say which would let a reader
 * assume the wrong one.
 */
export function blockLiveStatus(
  envelope: ReproducibilityEnvelope | null | undefined,
): LiveStatus {
  if (!envelope) {
    return { state: "idle", label: "no analysis", detail: "this block presents no analysis yet" };
  }
  if (envelope.scope === "as_released" && envelope.release) {
    return {
      state: "ok",
      label: `frozen to release ${envelope.release}`,
      detail:
        "this block reproduces the values that release published, and will not change when the source republishes",
    };
  }
  return {
    state: "warn",
    label: "live",
    detail:
      "this block replays against the source's latest publication, so its values change when the source republishes",
  };
}

/** Where an analytical block reopens, from the document it replays. */
export function blockReopenHref(block: PacketBlock | null | undefined): string {
  return reopenHref(block?.document);
}

/**
 * The envelope a saved explorer or comparison view carries into a packet.
 *
 * Everything comes from what the view already recorded; nothing is inferred,
 * and a field the view never captured stays empty so `packetIssues` can
 * report it rather than a guess filling it in.
 */
export function envelopeFromSavedChart(
  chart: Record<string, unknown> | null | undefined,
): ReproducibilityEnvelope {
  if (!chart) {
    return EMPTY_ENVELOPE;
  }
  const text = (value: unknown) => (typeof value === "string" && value ? value : "");
  const metricCodes = [text(chart.metricCode), text(chart.metricCodeB)].filter(Boolean);
  const sourceCodes = [text(chart.source), text(chart.sourceB)].filter(Boolean);
  const caveats = Array.isArray(chart.caveats) ? chart.caveats.map(String) : [];
  return {
    metricCodes,
    sourceCodes,
    geoId: text(chart.geoId),
    geoLevel: text(chart.geoLevel),
    scope: chart.scope === "as_released" ? "as_released" : "latest",
    release: text(chart.release),
    period: text(chart.period) || text(chart.savedAt),
    units: text(chart.units),
    transformation: text(chart.transformation) || "none",
    apiQuery: text(chart.apiQuery),
    caveats,
  };
}

export interface PacketExport {
  headings: string[];
  rows: string[][];
  filename: string;
}

/**
 * The packet as a table one row per block, with each analytical block's full
 * envelope alongside it — so the exported file can be read, and its evidence
 * re-derived, without this application.
 */
export function packetExport(packet: EvidencePacket | null | undefined): PacketExport {
  const headings = [
    "packet",
    "block_id",
    "block_type",
    "block_title",
    "content",
    "metric_codes",
    "source_codes",
    "geo_id",
    "geo_level",
    "scope",
    "release",
    "period",
    "units",
    "transformation",
    "api_query",
    "caveats",
    "live_or_frozen",
  ];
  const rows = (packet?.blocks || []).map((block) => {
    const envelope = block.envelope;
    return [
      packet?.title || "",
      block.id,
      block.type,
      block.title,
      block.content || "",
      envelope?.metricCodes.join(" | ") || "",
      envelope?.sourceCodes.join(" | ") || "",
      envelope?.geoId || "",
      envelope?.geoLevel || "",
      envelope?.scope || "",
      envelope?.release || "",
      envelope?.period || "",
      envelope?.units || "",
      envelope?.transformation || "",
      envelope?.apiQuery || "",
      envelope?.caveats.join(" | ") || "",
      isAnalyticalBlock(block) ? blockLiveStatus(envelope).label : "",
    ];
  });
  const slug = (packet?.title || "packet").toLowerCase().replaceAll(/[^a-z0-9]+/g, "-");
  return { headings, rows, filename: `${slug || "packet"}-evidence.csv` };
}

/** The packet as a portable document, for sharing the composition itself. */
export function packetDocument(packet: EvidencePacket | null | undefined): string {
  return JSON.stringify(packet ?? null, null, 2);
}
