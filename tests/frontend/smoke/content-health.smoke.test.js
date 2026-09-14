import { beforeAll, describe, expect, test } from "vitest";

// Covers: WEB-102 — the deployed content report is read against the deployed
// catalog, so a deployment that serves nothing fails a check instead of
// rendering an empty screen.
//
// Why this sits in the smoke tier rather than beside the API's own tests.
// API-137 proves the resource's rule and its query. Neither can prove the
// thing an operator actually wants to know: that *this* deployment, right
// now, has content behind the routes the explorer reads. That is a fact
// about a running stack, and only a running stack can answer it.
//
// The failure it exists to catch has no other signal. `/health/ready`
// answers `SELECT 1`, so an API pointed at an empty or half-loaded warehouse
// reports itself healthy, answers `total: 0` for every metric, and draws a
// blank chart on every screen with no error anywhere. Every tier was green
// while that was true, because every tier seeds its own content.
//
// Two bounds, deliberately different:
//
//   - `status` must not be `empty`, always. A deployment publishing no
//     current measure at all cannot draw anything, and no seeding story
//     makes that acceptable.
//   - `silent_sources` must be empty only under SMOKE_REQUIRE_ALL_SOURCES,
//     the way the end-to-end tier grades against its product inventory under
//     E2E_REQUIRE_ALL_PRODUCTS. The Compose stack this tier also runs
//     against seeds one ACS measure, so six of seven sources are legitimately
//     silent there; a fully loaded deployment has no excuse.

import { apiFetch } from "../../../apps/web/lib/api/client";
import { reportUnhandledErrors } from "./unhandledErrors";

const BASE_URL = (process.env.SMOKE_BASE_URL || "").replace(/\/+$/, "");

/** Every source must publish a current measure, not merely some source. */
const REQUIRE_ALL_SOURCES = process.env.SMOKE_REQUIRE_ALL_SOURCES === "1";

/** The words the resource publishes, as `apps/api/services/content_health.py` declares them. */
const SERVING = "serving";
const DEGRADED = "degraded";
const EMPTY = "empty";

function installOriginResolvingFetch() {
  const realFetch = globalThis.fetch;
  globalThis.fetch = (input, init) => {
    const target = typeof input === "string" ? input : input.url;
    return realFetch(target.startsWith("/") ? `${BASE_URL}${target}` : target, init);
  };
}

describe.skipIf(!BASE_URL)("deployed content health", () => {
  /** @type {{status: string, sources: Array<Record<string, unknown>>, silent_sources: string[]}} */
  let report;

  beforeAll(async () => {
    installOriginResolvingFetch();
    report = await apiFetch("/health/content");
  }, 60_000);

  test("the deployment publishes a measure something could ask for", () => {
    expect([SERVING, DEGRADED, EMPTY]).toContain(report.status);

    const silent = report.silent_sources.join(", ");
    expect(
      report.status,
      `the deployment publishes no current measure for any source (silent: ${silent}). ` +
        "Every observation request answers no rows and every chart draws nothing; " +
        "/health/ready cannot see this.",
    ).not.toBe(EMPTY);
  });

  test("every source's catalog is fully accounted for", () => {
    expect(report.sources.length).toBeGreaterThan(0);

    // A source whose counted states do not sum to its total is one the
    // warehouse has given a freshness word this API has never heard of. The
    // counts are then a partial tally, and a partial tally reported as a
    // whole one is how a shrinking catalog stays invisible.
    const partial = report.sources
      .filter((source) => source.counts_are_complete !== true)
      .map((source) => `${source.source_code} (${source.metrics_total} measures)`);
    expect(partial, "these sources' freshness counts do not account for their catalog").toEqual(
      [],
    );
  });

  test("the report agrees with the catalog the explorer reads", async () => {
    // `/catalog/metrics?active_only=true` filters on `is_active`, which
    // migration 003 defines as `freshness_state = 'current'` -- the exact
    // predicate `metrics_current` counts. So this is an equality check
    // between two independent readings of one fact, not an approximation:
    // the resource an operator trusts and the resource the explorer queries
    // must not disagree about whether a source has anything to offer.
    let checked = 0;
    const disagreements = [];

    for (const source of report.sources) {
      if (!source.registered) {
        continue;
      }
      const page = await apiFetch("/catalog/metrics", {
        params: { source_code: source.source_code, active_only: "true", limit: 1 },
      });
      checked += 1;
      const catalogHasCurrent = Number(page.total) > 0;
      const reportSaysServing = source.status === SERVING;
      if (catalogHasCurrent !== reportSaysServing) {
        disagreements.push(
          `${source.source_code}: /health/content says ${source.status} ` +
            `(${source.metrics_current} current) while /catalog/metrics answers ` +
            `${page.total} active metrics`,
        );
      }
    }

    // A report listing no registered source would make the loop vacuous, and
    // a vacuous pass here is the reassurance this tier must not give.
    expect(checked, "the content report named no registered source").toBeGreaterThan(0);
    expect(disagreements).toEqual([]);
  }, 60_000);

  test.skipIf(!REQUIRE_ALL_SOURCES)("every registered source publishes a measure", () => {
    // Only where the deployment claims to be fully loaded. Against the
    // Compose stack, which seeds one ACS measure, six sources are silent for
    // a reason that is not a defect -- so this is opt-in rather than a bound
    // the tier would have to weaken to stay green.
    expect(
      report.silent_sources,
      "SMOKE_REQUIRE_ALL_SOURCES=1 but these sources publish no current measure, " +
        "so every chart over them draws nothing",
    ).toEqual([]);
    expect(report.status).toBe(SERVING);
  });
});

// Declared last on purpose: it reports on every request the tests above made.
reportUnhandledErrors();
