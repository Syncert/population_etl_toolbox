// Saved analysis configurations: documents, validation state, and the
// migration path from the browser-local charts that preceded them.
//
// Three rules bound this module.
//
// 1. A saved analysis is *intent*, not data. It names the resource, the
//    measures, and the filters, and is replayed against live publications —
//    so a saved analysis follows the warehouse instead of freezing a
//    snapshot of it. Nothing here may store observation values.
// 2. The API reports a stale configuration rather than repairing it, and so
//    does this module. A document whose measure was retired comes back
//    unmodified with `validation.valid = false` and a reason; presenting it
//    as still valid, or silently rewriting it, would substitute a guess for
//    the user's intent.
// 3. User content and the bearer token are private. Neither may reach a URL,
//    a shared link, a public cache, or a log line. The functions here build
//    request bodies and view models only; the token travels in a header, and
//    the reopen links they produce carry the document's own public
//    selection, never its name, id, owner, or token.

import type {
  AnalysisDocument,
  ConfigurationValidation,
  SavedAnalysisConfiguration,
  SavedAnalysisSummary,
} from "./api/types";
import { explorerHref, comparisonHref } from "./urlState";
import type { GeoLevel } from "./urlState";

export const CONFIGURATION_KINDS = ["observations", "comparison", "distribution"] as const;

/** The document an explorer selection saves as. */
export function explorerDocument(input: {
  metricCode: string;
  scope?: "latest" | "as_released";
  release?: string;
  geoLevel?: string;
  stateFips?: string;
  geoId?: string;
  dimensions?: Record<string, string>;
  /** The view asked the resource for one row per geography (API-066). */
  newestPerGeography?: boolean;
  /** The view read a settled history: newest release per period (API-081). */
  newestReleasePerPeriod?: boolean;
}): AnalysisDocument {
  const filters: Record<string, unknown> = {};
  if (input.geoLevel) {
    filters.geo_level = input.geoLevel;
  }
  if (input.stateFips) {
    filters.state_fips = input.stateFips;
  }
  if (input.geoId) {
    filters.geo_id = input.geoId;
  }
  for (const [name, value] of Object.entries(input.dimensions || {})) {
    if (value) {
      filters[name] = value;
    }
  }
  // A view that asked the resource to reduce must say so, or it reopens as
  // a different set of rows: a map saved without `newest_per_geography`
  // replays as the whole latest publication, which for a source publishing
  // a series per geography colours whichever row arrived last (WEB-047).
  //
  // Each reduction belongs to one scope, and the API refuses the other
  // pairing and the two together. A contradiction is dropped here rather
  // than stored, because a document the live route would reject is one the
  // reader could not reopen.
  const scope = input.scope || "latest";
  const newestPerGeography = Boolean(input.newestPerGeography) && scope === "latest";
  const newestReleasePerPeriod =
    Boolean(input.newestReleasePerPeriod) &&
    scope === "as_released" &&
    !newestPerGeography;
  return {
    kind: "observations",
    metric_code: input.metricCode,
    scope,
    // A release identity is meaningful only under `as_released`; carrying
    // one otherwise would store a request the API refuses. A settled history
    // refuses it too: one pins a release, the other asks for the newest of
    // every period.
    release:
      scope === "as_released" && input.release && !newestReleasePerPeriod
        ? input.release
        : null,
    newest_per_geography: newestPerGeography,
    newest_release_per_period: newestReleasePerPeriod,
    filters,
    visualization: {},
  };
}

/** The document a comparison selection saves as. */
export function comparisonDocument(input: {
  metricCodeA: string;
  metricCodeB: string;
  geoLevel?: string;
  stateFips?: string;
}): AnalysisDocument {
  const filters: Record<string, unknown> = {};
  if (input.geoLevel) {
    filters.geo_level = input.geoLevel;
  }
  if (input.stateFips && input.geoLevel !== "NATIONAL") {
    filters.state_fips = input.stateFips;
  }
  return {
    kind: "comparison",
    metric_code_a: input.metricCodeA,
    metric_code_b: input.metricCodeB,
    filters,
    visualization: {},
  };
}

export interface ValidationState {
  /** Shared request-state vocabulary value, for the status pill. */
  state: string;
  message: string;
}

/**
 * A configuration's live validation state.
 *
 * A stale document is a caution, not a failure: the content is intact and
 * the reader can still see and edit it. It is never `ok`, because replaying
 * it would not produce the analysis it describes.
 */
export function validationState(
  validation: ConfigurationValidation | null | undefined,
): ValidationState {
  if (!validation) {
    return { state: "idle", message: "not checked" };
  }
  if (validation.valid) {
    return { state: "ok", message: "matches live capabilities" };
  }
  return {
    state: "warn",
    message: validation.reason || "no longer matches live capabilities",
  };
}

/**
 * Where a saved configuration reopens.
 *
 * The link carries the document's own public selection and nothing else —
 * not the configuration's name, id, version, or owner — so opening a saved
 * analysis never puts private content into a URL, a referrer, or history.
 */
export function reopenHref(document: AnalysisDocument | null | undefined): string {
  if (!document) {
    return "/explore";
  }
  const filters = (document.filters || {}) as Record<string, unknown>;
  const geoLevel = typeof filters.geo_level === "string" ? filters.geo_level : undefined;
  const stateFips = typeof filters.state_fips === "string" ? filters.state_fips : undefined;
  const geoId = typeof filters.geo_id === "string" ? filters.geo_id : undefined;

  if (document.kind === "comparison") {
    return comparisonHref({
      metricA: document.metric_code_a || undefined,
      metricB: document.metric_code_b || undefined,
      geoLevel: geoLevel as GeoLevel | undefined,
      stateFips,
    });
  }
  return explorerHref({
    metric: document.metric_code || undefined,
    geoLevel: geoLevel as GeoLevel | undefined,
    stateFips,
    geoId,
    scope: document.scope,
    release: document.scope === "as_released" ? document.release || undefined : undefined,
  });
}

/** A one-line description of what a document asks for. */
export function describeDocument(document: AnalysisDocument | null | undefined): string {
  if (!document) {
    return "";
  }
  if (document.kind === "comparison") {
    return `${document.metric_code_a || "?"} vs ${document.metric_code_b || "?"}`;
  }
  const scope = document.scope === "as_released"
    ? document.release
      ? ` as released ${document.release}`
      : " across every published release"
    : "";
  return `${document.metric_code || "?"}${scope}`;
}

export interface ConflictState {
  /** True when the API refused an update because the version moved. */
  conflicted: boolean;
  /** The version this client last read. */
  expectedVersion: number | null;
  message: string;
}

/**
 * A 409 from an update, turned into something a reader can act on.
 *
 * The API refuses rather than merging, and this module does not merge
 * either: overwriting a version another session wrote would discard content
 * this client never saw.
 */
export function describeConflict(
  status: number | null | undefined,
  detail: string | null | undefined,
  expectedVersion: number | null,
): ConflictState {
  if (status !== 409) {
    return { conflicted: false, expectedVersion, message: "" };
  }
  return {
    conflicted: true,
    expectedVersion,
    message:
      detail ||
      "this configuration changed elsewhere; reload it before saving so no other change is lost",
  };
}

export interface LocalChart {
  id?: string;
  title?: string;
  chartType?: string;
  metricCode?: string;
  metricCodeB?: string;
  geoLevel?: string;
  stateFips?: string | null;
  geoId?: string | null;
  [key: string]: unknown;
}

export interface MigrationCandidate {
  localId: string;
  name: string;
  document: AnalysisDocument;
}

export interface MigrationPlan {
  candidates: MigrationCandidate[];
  /** Local charts that cannot become a configuration, and why. */
  skipped: { localId: string; name: string; reason: string }[];
}

/**
 * Which browser-local charts can become saved configurations.
 *
 * The local store predates the API's contract and holds shapes it does not
 * accept — a profile is a reading order, not an analysis request. Those are
 * listed as skipped with a reason rather than being coerced into a document
 * the API would refuse, or silently dropped so a user believes their work
 * moved when it did not.
 */
export function planLocalMigration(
  charts: LocalChart[] | null | undefined,
): MigrationPlan {
  const candidates: MigrationCandidate[] = [];
  const skipped: MigrationPlan["skipped"] = [];

  for (const chart of Array.isArray(charts) ? charts : []) {
    const localId = String(chart?.id ?? "");
    const name = String(chart?.title || localId || "Untitled");
    if (!localId) {
      continue;
    }

    if (chart.chartType === "comparison") {
      if (!chart.metricCode || !chart.metricCodeB) {
        skipped.push({ localId, name, reason: "the saved comparison names only one measure" });
        continue;
      }
      candidates.push({
        localId,
        name,
        document: comparisonDocument({
          metricCodeA: String(chart.metricCode),
          metricCodeB: String(chart.metricCodeB),
          geoLevel: chart.geoLevel ? String(chart.geoLevel) : undefined,
          stateFips: chart.stateFips ? String(chart.stateFips) : undefined,
        }),
      });
      continue;
    }

    if (chart.chartType === "profile") {
      skipped.push({
        localId,
        name,
        reason:
          "a saved profile is a reading order over several measures, which the configuration contract does not describe",
      });
      continue;
    }

    if (!chart.metricCode) {
      skipped.push({ localId, name, reason: "the saved chart names no measure" });
      continue;
    }

    candidates.push({
      localId,
      name,
      // What the view asked, where the chart recorded it. A chart saved
      // before it recorded any of this carries none of these fields, and
      // migrates exactly as it did: `latest`, no release, no reduction
      // (WEB-048).
      document: explorerDocument({
        metricCode: String(chart.metricCode),
        scope: chart.scope === "as_released" ? "as_released" : "latest",
        release: chart.release ? String(chart.release) : undefined,
        geoLevel: chart.geoLevel ? String(chart.geoLevel) : undefined,
        stateFips: chart.stateFips ? String(chart.stateFips) : undefined,
        geoId: chart.geoId ? String(chart.geoId) : undefined,
        newestPerGeography: chart.newestPerGeography === true,
        newestReleasePerPeriod: chart.newestReleasePerPeriod === true,
      }),
    });
  }

  return { candidates, skipped };
}

/** Summary rows sorted for deterministic rendering. */
export function sortConfigurations(
  items: SavedAnalysisSummary[] | null | undefined,
): SavedAnalysisSummary[] {
  return [...(items || [])].sort((left, right) => left.name.localeCompare(right.name));
}

/** True when a loaded configuration is the one a summary row names. */
export function isSameConfiguration(
  summary: SavedAnalysisSummary | null | undefined,
  configuration: SavedAnalysisConfiguration | null | undefined,
): boolean {
  return Boolean(
    summary && configuration && summary.configuration_id === configuration.configuration_id,
  );
}

// ---------------------------------------------------------------------------
// Where a save goes
// ---------------------------------------------------------------------------

/**
 * The two places a view can be saved, which are different facts and not two
 * spellings of "saved".
 *
 * - `account` — a configuration on `/analysis-configurations`. It is the
 *   user's, it survives the tab, and it is replayed against live
 *   publications.
 * - `browser` — the local chart store that predates the contract. It is this
 *   browser's only, invisible to the account, and it is what the evidence
 *   packet builder still composes from.
 *
 * A reader who is told only "Saved" cannot tell which happened, and the two
 * have very different consequences for whether the work still exists
 * tomorrow. Every caller reports the destination.
 */
export type SaveDestination = "account" | "browser";

export interface SaveOutcome {
  /** Shared request-state vocabulary value, for the status surface. */
  state: string;
  message: string;
  /** Null when the save did not happen. */
  destination: SaveDestination | null;
}

/**
 * Where a save will go, decided only by whether a token is held.
 *
 * This is one decision in one place on purpose: the explorer and the
 * comparison workspace must not drift into different ideas of when the
 * account is used.
 */
export function saveDestination(token: string | null | undefined): SaveDestination {
  return token ? "account" : "browser";
}

/**
 * Report an account save that failed.
 *
 * A `401` is deliberately indistinguishable between a missing, malformed,
 * unknown, and revoked token, so this names what the API said rather than
 * guessing which. Nothing here echoes the token itself.
 */
export function describeSaveFailure(error: unknown): SaveOutcome {
  const status = (error as { status?: number } | null)?.status;
  if (status === 401) {
    return {
      state: "unauthorized",
      message: "the token was not accepted — the view was not saved",
      destination: null,
    };
  }
  if (status === 409) {
    return {
      state: "conflict",
      message: "a saved analysis with that name already exists",
      destination: null,
    };
  }
  const detail = (error as { message?: string } | null)?.message;
  return {
    state: "bad",
    message: detail ? `not saved: ${detail}` : "not saved",
    destination: null,
  };
}

/** Report a successful save, naming where it went and what that means. */
export function describeSaveSuccess(destination: SaveDestination, name: string): SaveOutcome {
  if (destination === "account") {
    return { state: "ok", message: `Saved to your account as “${name}”`, destination };
  }
  return {
    state: "warn",
    message: "Saved in this browser only — sign in on Saved analyses to keep it",
    destination,
  };
}

// --- Account libraries ---------------------------------------------------
//
// An account's saved analyses and evidence packets are paged collections:
// `/analysis-configurations` and `/evidence-packets` cap `limit` at 200 and
// declare `offset`. Three screens read them with one request at that maximum
// and reported the result in green, so a library past two hundred entries was
// shown two hundred of them with no way to open, edit, or compose from the
// rest (WEB-044).

/** One page of an account library, and how many of them to read. */
export const LIBRARY_PAGE_SIZE = 200;
export const LIBRARY_PAGE_LIMIT = 10;

/**
 * The library status line, honest about the page bound.
 *
 * Follows WEB-036's wording: a bound-limited read names the shortfall and is
 * reported as a failure, and a resource that published no total is never
 * reported as short, because there is no shortfall to state.
 */
export function describeLibraryLoad(
  loaded: number,
  total: number | null,
  complete: boolean,
  one: string,
  many: string,
): string {
  const noun = loaded === 1 ? one : many;
  if (!complete && total !== null) {
    return (
      `loaded ${loaded} of ${total} ${many}; the page bound cut the answer ` +
      "short, so this list is incomplete"
    );
  }
  return `${loaded} ${noun}`;
}
