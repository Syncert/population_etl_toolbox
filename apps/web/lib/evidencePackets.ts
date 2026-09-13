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
import { comparisonDocument, explorerDocument, reopenHref } from "./savedAnalysis";

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

/**
 * The query one attached view replays.
 *
 * Built by the same functions the explorer saves through, rather than as a
 * literal here. A second construction is a second place every rule about
 * what a document may contain has to be re-learned, and the literal this
 * replaced had learned none of them: it recorded no reduction, so a map
 * block replayed the source's whole latest publication while the envelope
 * beside it recorded `newest_per_geography=true` in its `api_query` -- the
 * block did not reproduce the request its own envelope names, in the one
 * resource whose purpose is that a reader can re-derive the evidence without
 * this application. It also copied a release across unconditionally, which
 * under a latest scope is a document the API refuses (WEB-048).
 *
 * A two-measure view is a comparison, whose route serves no scope: its
 * document records none, and the envelope's default `latest` is what the
 * API's envelope/query cross-check compares against.
 */
export function documentFromSavedChart(
  chart: Record<string, unknown> | null | undefined,
): AnalysisDocument {
  const envelope = envelopeFromSavedChart(chart);
  const text = (value: unknown) => (typeof value === "string" && value ? value : "");
  const stateFips = text(chart?.stateFips);

  if (text(chart?.metricCodeB)) {
    return comparisonDocument({
      metricCodeA: text(chart?.metricCode),
      metricCodeB: text(chart?.metricCodeB),
      geoLevel: envelope.geoLevel || undefined,
      stateFips: stateFips || undefined,
    });
  }

  return explorerDocument({
    metricCode: text(chart?.metricCode),
    scope: envelope.scope,
    release: envelope.release || undefined,
    geoLevel: envelope.geoLevel || undefined,
    stateFips: stateFips || undefined,
    geoId: envelope.geoId || undefined,
    // A view that did not record a reduction asked for none, which is what a
    // chart saved before this change says about itself.
    newestPerGeography: chart?.newestPerGeography === true,
    newestReleasePerPeriod: chart?.newestReleasePerPeriod === true,
  });
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

/** How a stored composition read back: the three states are not the same fact. */
export type ComposedPacketState = "empty" | "ready" | "unreadable";

export interface ComposedPacketRead {
  state: ComposedPacketState;
  packet: EvidencePacket | null;
  /** Why, in the reader's terms. Always populated. */
  reason: string;
  /** Block titles the stored composition carried that this build cannot present. */
  unsupported: string[];
}

function normalizeEnvelope(value: unknown): ReproducibilityEnvelope | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  const source = value as Record<string, unknown>;
  const text = (field: unknown) => (typeof field === "string" ? field : "");
  const list = (field: unknown) =>
    Array.isArray(field) ? field.filter((entry): entry is string => typeof entry === "string") : [];
  return {
    metricCodes: list(source.metricCodes),
    sourceCodes: list(source.sourceCodes),
    geoId: text(source.geoId),
    geoLevel: text(source.geoLevel),
    scope: source.scope === "as_released" ? "as_released" : "latest",
    release: text(source.release),
    period: text(source.period),
    units: text(source.units),
    transformation: text(source.transformation) || "none",
    apiQuery: text(source.apiQuery),
    caveats: list(source.caveats),
  };
}

/**
 * Read a stored composition back as a packet, or say why it could not be.
 *
 * Three outcomes that a reading surface must keep apart. Nothing composed yet
 * is not the same as a stored composition this build cannot parse: the first
 * invites the reader to compose one, the second says their work is still
 * there and this page could not read it. Collapsing them into one empty state
 * would tell someone their packet is gone.
 *
 * A malformed envelope is reduced to the fields it does carry rather than
 * discarded, so `packetIssues` names exactly what the block lacks instead of
 * the block disappearing; and a block of a type this build does not know is
 * reported by title rather than rendered or dropped, because a reader who
 * cannot see it must at least know it was there.
 */
export function readComposedPacket(raw: string | null | undefined): ComposedPacketRead {
  if (raw === null || raw === undefined || raw === "") {
    return {
      state: "empty",
      packet: null,
      reason: "nothing has been composed in this browser yet",
      unsupported: [],
    };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return {
      state: "unreadable",
      packet: null,
      reason: "the stored composition is not readable as a packet, so nothing is shown for it",
      unsupported: [],
    };
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return {
      state: "unreadable",
      packet: null,
      reason: "the stored composition is not readable as a packet, so nothing is shown for it",
      unsupported: [],
    };
  }
  const source = parsed as Record<string, unknown>;
  if (source.version !== 1 || !Array.isArray(source.blocks)) {
    return {
      state: "unreadable",
      packet: null,
      reason:
        "the stored composition was written by a different version of the composer, so this page does not present it",
      unsupported: [],
    };
  }
  const blocks: PacketBlock[] = [];
  const unsupported: string[] = [];
  for (const entry of source.blocks) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      continue;
    }
    const block = entry as Record<string, unknown>;
    const id = typeof block.id === "string" ? block.id : "";
    const title = typeof block.title === "string" ? block.title : "";
    if (!id) {
      continue;
    }
    if (!BLOCK_TYPES.includes(block.type as BlockType)) {
      unsupported.push(title || id);
      continue;
    }
    const normalized: PacketBlock = { id, type: block.type as BlockType, title };
    if (typeof block.content === "string") {
      normalized.content = block.content;
    }
    const envelope = normalizeEnvelope(block.envelope);
    if (envelope) {
      normalized.envelope = envelope;
    }
    if (block.document && typeof block.document === "object" && !Array.isArray(block.document)) {
      normalized.document = block.document as AnalysisDocument;
    }
    blocks.push(normalized);
  }
  const packet: EvidencePacket = {
    version: 1,
    title: typeof source.title === "string" ? source.title : "",
    purpose: typeof source.purpose === "string" ? source.purpose : "",
    blocks,
    updatedAt: typeof source.updatedAt === "string" ? source.updatedAt : "",
  };
  return {
    state: "ready",
    packet,
    reason:
      unsupported.length > 0
        ? "this composition carries blocks this build cannot present; they are named rather than dropped"
        : "this composition was read in full",
    unsupported,
  };
}

// ---------------------------------------------------------------------------
// The account boundary (ADR-0004)
//
// The API is snake_case and stores every envelope field; this module is
// camelCase and lets a field the composer never captured stay absent. The
// two functions below are the whole translation, in one place, so a field
// cannot quietly stop crossing in one direction.
// ---------------------------------------------------------------------------

import type {
  ApiPacketBlock,
  ApiReproducibilityEnvelope,
  BlockValidation,
  EvidencePacketDocument,
  PacketValidation,
} from "./api/types";

function envelopeToApi(envelope: ReproducibilityEnvelope): ApiReproducibilityEnvelope {
  return {
    metric_codes: [...envelope.metricCodes],
    source_codes: [...envelope.sourceCodes],
    geo_id: envelope.geoId,
    geo_level: envelope.geoLevel,
    scope: envelope.scope,
    release: envelope.release,
    period: envelope.period,
    units: envelope.units,
    transformation: envelope.transformation || "none",
    api_query: envelope.apiQuery,
    caveats: [...envelope.caveats],
  };
}

function envelopeFromApi(envelope: ApiReproducibilityEnvelope): ReproducibilityEnvelope {
  return {
    metricCodes: [...(envelope.metric_codes || [])],
    sourceCodes: [...(envelope.source_codes || [])],
    geoId: envelope.geo_id || "",
    geoLevel: envelope.geo_level || "",
    scope: envelope.scope === "as_released" ? "as_released" : "latest",
    release: envelope.release || "",
    period: envelope.period || "",
    units: envelope.units || "",
    transformation: envelope.transformation || "none",
    apiQuery: envelope.api_query || "",
    caveats: [...(envelope.caveats || [])],
  };
}

/**
 * The packet as the API stores it.
 *
 * A prose block's envelope and document are dropped rather than sent: the
 * API refuses a caveat that carries a query, and a stray envelope on a text
 * block is a composer bug, not content worth preserving. Analytical blocks
 * cross exactly as recorded — an empty one stays empty, so the API reports
 * the gap the same way `packetIssues` does.
 */
export function packetToDocument(packet: EvidencePacket): EvidencePacketDocument {
  return {
    schema_version: 1,
    title: packet.title,
    purpose: packet.purpose,
    blocks: packet.blocks.map((block): ApiPacketBlock => {
      const row: ApiPacketBlock = {
        block_id: block.id,
        type: block.type,
        title: block.title,
        content: block.content || "",
      };
      if (isAnalyticalBlock(block)) {
        if (block.envelope) {
          row.envelope = envelopeToApi(block.envelope);
        }
        if (block.document) {
          row.document = block.document;
        }
      }
      return row;
    }),
  };
}

/** The API's stored document as this module's packet, verbatim. */
export function documentToPacket(
  document: EvidencePacketDocument,
  updatedAt: string,
): EvidencePacket {
  return {
    version: 1,
    title: document.title || "",
    purpose: document.purpose || "",
    updatedAt,
    blocks: (document.blocks || []).map((row): PacketBlock => {
      const block: PacketBlock = {
        id: row.block_id,
        type: row.type,
        title: row.title || "",
        content: row.content || "",
      };
      if (row.envelope) {
        block.envelope = envelopeFromApi(row.envelope);
      }
      if (row.document) {
        block.document = row.document;
      }
      return block;
    }),
  };
}

/**
 * The API's per-block verdicts merged with the client's own issue report.
 *
 * The client can see incompleteness; only the API can see staleness — a
 * measure retired since the block was composed. A block the API reports and
 * the client does not is stale, and stale is what a reader must be told
 * first, because it is the thing the composer cannot fix by filling a field.
 */
export interface BlockReadState {
  blockId: string;
  title: string;
  /** "incomplete" | "stale" | "ok" */
  state: "incomplete" | "stale" | "ok";
  reason: string;
  missing: string[];
}

export function mergeBlockStates(
  packet: EvidencePacket | null | undefined,
  validation: PacketValidation | null | undefined,
): BlockReadState[] {
  const issues = new Map(packetIssues(packet).map((issue) => [issue.blockId, issue]));
  const verdicts = new Map<string, BlockValidation>(
    (validation?.blocks || []).map((state) => [state.block_id, state]),
  );
  return (packet?.blocks || []).filter(isAnalyticalBlock).map((block) => {
    const issue = issues.get(block.id);
    const verdict = verdicts.get(block.id);
    if (verdict && !verdict.valid && (verdict.missing || []).length === 0) {
      return {
        blockId: block.id,
        title: block.title,
        state: "stale",
        reason: verdict.reason || "the API reports this block can no longer be replayed",
        missing: [],
      };
    }
    if (issue) {
      return {
        blockId: block.id,
        title: block.title,
        state: "incomplete",
        reason: issue.reason,
        missing: issue.missing,
      };
    }
    if (verdict && !verdict.valid) {
      return {
        blockId: block.id,
        title: block.title,
        state: "incomplete",
        reason: verdict.reason || "this block is missing context",
        missing: verdict.missing || [],
      };
    }
    return { blockId: block.id, title: block.title, state: "ok", reason: "", missing: [] };
  });
}
