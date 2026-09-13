import { describe, expect, test, vi } from "vitest";

// Covers: WEB-022 — saved analysis configurations. A configuration is
// intent replayed against live publications, never a copy of observation
// data; a stale document is reported unmodified rather than repaired; a
// version conflict is refused rather than merged; the bearer token travels
// only in a header and never in a URL; and the browser-local charts that
// preceded the contract migrate only where the contract can describe them.

import {
  describeConflict,
  describeLibraryLoad,
  describeDocument,
  describeSaveFailure,
  describeSaveSuccess,
  saveDestination,
  comparisonDocument,
  explorerDocument,
  planLocalMigration,
  reopenHref,
  sortConfigurations,
  validationState,
} from "../../../apps/web/lib/savedAnalysis";
import { apiFetch, buildApiPath } from "../../../apps/web/lib/api/client";

function jsonResponse(payload, { status = 200 } = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => payload,
  };
}

describe("a configuration is intent, not data", () => {
  test("an explorer selection saves its query, never its values", () => {
    const document = explorerDocument({
      metricCode: "CENSUS_ACS:acs5:B01003_001",
      geoLevel: "COUNTY",
      stateFips: "55",
      geoId: "state:55|county:025",
      dimensions: { stratum_id: "overall", adjustment_status: "" },
    });
    expect(document).toEqual({
      kind: "observations",
      metric_code: "CENSUS_ACS:acs5:B01003_001",
      scope: "latest",
      release: null,
      filters: {
        geo_level: "COUNTY",
        state_fips: "55",
        geo_id: "state:55|county:025",
        stratum_id: "overall",
      },
      visualization: {},
    });
    // Nothing observation-shaped is stored: a saved analysis follows the
    // warehouse rather than freezing a snapshot of it.
    const serialized = JSON.stringify(document);
    expect(serialized).not.toContain("value");
    expect(serialized).not.toContain("period");
  });

  test("a release is stored only under the scope that accepts it", () => {
    expect(
      explorerDocument({ metricCode: "M", scope: "as_released", release: "2022" }).release,
    ).toBe("2022");
    // `release` without `scope=as_released` is a request the API refuses, so
    // it is never stored.
    expect(explorerDocument({ metricCode: "M", release: "2022" }).release).toBeNull();
  });

  test("a comparison saves both identities and drops a contradictory scope", () => {
    expect(
      comparisonDocument({
        metricCodeA: "A",
        metricCodeB: "B",
        geoLevel: "NATIONAL",
        stateFips: "55",
      }),
    ).toEqual({
      kind: "comparison",
      metric_code_a: "A",
      metric_code_b: "B",
      filters: { geo_level: "NATIONAL" },
      visualization: {},
    });
    expect(describeDocument({ kind: "comparison", metric_code_a: "A", metric_code_b: "B" })).toBe(
      "A vs B",
    );
    expect(
      describeDocument({ kind: "observations", metric_code: "M", scope: "as_released" }),
    ).toContain("across every published release");
    expect(describeDocument(null)).toBe("");
  });
});

describe("stale and conflicting configurations are reported, not repaired", () => {
  test("a stale document is a caution that never reads as healthy", () => {
    expect(validationState({ valid: true })).toEqual({
      state: "ok",
      message: "matches live capabilities",
    });
    // The content is intact and editable; replaying it would simply not
    // produce the analysis it describes.
    expect(validationState({ valid: false, reason: "metric_code was retired" })).toEqual({
      state: "warn",
      message: "metric_code was retired",
    });
    expect(validationState({ valid: false })).toEqual({
      state: "warn",
      message: "no longer matches live capabilities",
    });
    expect(validationState(null).state).toBe("idle");
  });

  test("a version conflict is surfaced with the API's own explanation", () => {
    const conflict = describeConflict(
      409,
      "configuration was modified; expected version 2, current version 3",
      2,
    );
    expect(conflict.conflicted).toBe(true);
    expect(conflict.expectedVersion).toBe(2);
    expect(conflict.message).toContain("current version 3");
    // Nothing is merged: overwriting a version this client never read would
    // discard someone else's change.
    expect(conflict.message).not.toContain("overwrit");
    expect(describeConflict(200, null, 2).conflicted).toBe(false);
    expect(describeConflict(422, "invalid", 2).conflicted).toBe(false);
    expect(describeConflict(409, null, null).message).toContain("reload it before saving");
  });
});

describe("private content stays out of URLs", () => {
  test("a reopen link carries the selection and nothing identifying", () => {
    const href = reopenHref({
      kind: "observations",
      metric_code: "CENSUS_ACS:acs5:B01003_001",
      scope: "as_released",
      release: "2022",
      filters: { geo_level: "COUNTY", state_fips: "55", geo_id: "state:55|county:025" },
    });
    expect(href).toContain("/explore?");
    expect(href).toContain("metric=CENSUS_ACS");
    expect(href).toContain("scope=as_released");
    expect(href).toContain("release=2022");
    // Not the configuration's own identity, name, version, or owner.
    expect(href).not.toContain("configuration");
    expect(href).not.toContain("name=");
    expect(href).not.toContain("version");

    expect(
      reopenHref({ kind: "comparison", metric_code_a: "A", metric_code_b: "B", filters: {} }),
    ).toContain("/compare?");
    expect(reopenHref(null)).toBe("/explore");
  });

  test("the bearer token travels in a header, never in the request URL", async () => {
    const fetchImpl = vi.fn(async () => jsonResponse({ total: 0, items: [] }));
    await apiFetch("/analysis-configurations", {
      token: "secret-token-value",
      fetchImpl,
    });
    const [url, init] = fetchImpl.mock.calls[0];
    expect(url).toBe(buildApiPath("/analysis-configurations"));
    expect(url).not.toContain("secret-token-value");
    expect(init.headers.Authorization).toBe("Bearer secret-token-value");
    expect(init.cache).toBe("no-store");
  });

  test("a write sends a JSON body and a delete decodes no content", async () => {
    const create = vi.fn(async () => jsonResponse({ configuration_id: 1 }, { status: 201 }));
    await apiFetch("/analysis-configurations", {
      token: "t",
      method: "POST",
      body: { name: "My analysis", document: { kind: "observations" } },
      fetchImpl: create,
    });
    const [, init] = create.mock.calls[0];
    expect(init.method).toBe("POST");
    expect(init.headers["Content-Type"]).toBe("application/json");
    expect(JSON.parse(init.body).name).toBe("My analysis");

    // 204 has no body; decoding one would throw on a successful delete.
    const remove = vi.fn(async () => ({
      ok: true,
      status: 204,
      headers: { get: () => null },
      json: async () => {
        throw new Error("no body");
      },
    }));
    await expect(
      apiFetch("/analysis-configurations/1", { token: "t", method: "DELETE", fetchImpl: remove }),
    ).resolves.toBeUndefined();
  });
});

describe("browser-local charts migrate only where the contract describes them", () => {
  test("charts and comparisons become documents; a profile is skipped with a reason", () => {
    const plan = planLocalMigration([
      {
        id: "chart:1",
        title: "County population",
        chartType: "choropleth",
        metricCode: "CENSUS_ACS:acs5:B01003_001",
        geoLevel: "COUNTY",
        stateFips: "55",
      },
      {
        id: "comparison:1",
        title: "ACS vs PEP",
        chartType: "comparison",
        metricCode: "A",
        metricCodeB: "B",
        geoLevel: "COUNTY",
      },
      { id: "profile:1", title: "Community profile", chartType: "profile" },
      { id: "chart:2", title: "Nameless", chartType: "choropleth" },
      { id: "comparison:2", title: "Half a pair", chartType: "comparison", metricCode: "A" },
    ]);

    expect(plan.candidates.map((candidate) => candidate.localId)).toEqual([
      "chart:1",
      "comparison:1",
    ]);
    expect(plan.candidates[0].document.kind).toBe("observations");
    expect(plan.candidates[0].document.filters).toEqual({
      geo_level: "COUNTY",
      state_fips: "55",
    });
    expect(plan.candidates[1].document.kind).toBe("comparison");

    // Nothing is coerced into a document the API would refuse, and nothing
    // is dropped silently so a user believes their work moved when it did not.
    expect(plan.skipped.map((entry) => entry.localId)).toEqual([
      "profile:1",
      "chart:2",
      "comparison:2",
    ]);
    expect(plan.skipped[0].reason).toContain("reading order");
    expect(plan.skipped[1].reason).toContain("names no measure");
    expect(plan.skipped[2].reason).toContain("only one measure");
  });

  test("an empty or malformed local store plans nothing", () => {
    expect(planLocalMigration([])).toEqual({ candidates: [], skipped: [] });
    expect(planLocalMigration(null)).toEqual({ candidates: [], skipped: [] });
    expect(planLocalMigration([{ title: "no id" }]).candidates).toEqual([]);
  });

  test("configurations sort deterministically by name", () => {
    expect(
      sortConfigurations([
        { configuration_id: 2, name: "Beta", version: 1 },
        { configuration_id: 1, name: "Alpha", version: 1 },
      ]).map((item) => item.name),
    ).toEqual(["Alpha", "Beta"]);
    expect(sortConfigurations(null)).toEqual([]);
  });
});

// Covers: WEB-022 — where a save goes is a decision the explorer and the
// comparison workspace must make identically, and a reader must be told
// which of the two destinations answered.
describe("save destination", () => {
  test("the account answers whenever a token is held, and the browser otherwise", () => {
    expect(saveDestination("operator-token")).toBe("account");
    // Every falsy token shape is the same fact: nobody is signed in.
    expect(saveDestination("")).toBe("browser");
    expect(saveDestination(null)).toBe("browser");
    expect(saveDestination(undefined)).toBe("browser");
  });

  test("a browser save is reported as weaker than an account save, not as equal", () => {
    const account = describeSaveSuccess("account", "Total population by county");
    expect(account.state).toBe("ok");
    expect(account.destination).toBe("account");
    expect(account.message).toContain("your account");

    // "Saved" alone would let a reader believe work survives the tab when it
    // does not, so the local outcome names its limit and the way out of it.
    const browser = describeSaveSuccess("browser", "Total population by county");
    expect(browser.state).toBe("warn");
    expect(browser.destination).toBe("browser");
    expect(browser.message).toContain("this browser only");
  });

  test("a failed save reports the API's own refusal and claims no destination", () => {
    // A 401 is deliberately identical for a missing, malformed, unknown, and
    // revoked token; the message says the token was not accepted rather than
    // guessing which, and never echoes the token itself.
    const unauthorized = describeSaveFailure({ status: 401, message: "boom" });
    expect(unauthorized.state).toBe("unauthorized");
    expect(unauthorized.destination).toBeNull();
    expect(unauthorized.message).toContain("not accepted");
    expect(unauthorized.message).not.toContain("boom");

    const conflict = describeSaveFailure({ status: 409 });
    expect(conflict.state).toBe("conflict");
    expect(conflict.destination).toBeNull();

    const other = describeSaveFailure({ status: 503, message: "status 503" });
    expect(other.state).toBe("bad");
    expect(other.destination).toBeNull();
    expect(other.message).toContain("status 503");

    // A failed account save never silently becomes a local one: telling a
    // user their work is safe somewhere they did not choose is worse than
    // telling them it was not saved.
    expect(describeSaveFailure(new Error("network")).destination).toBeNull();
  });
});

// Covers: WEB-044 — an account library is a paged collection, and a partial
// answer is never green. Three screens read theirs with one request at the
// route's maximum of 200 and reported the result as ok, so a library past
// two hundred entries was shown two hundred with no way to reach the rest.
describe("the account library status line", () => {
  test("a complete library reads as what it is, and counts one correctly", () => {
    expect(describeLibraryLoad(7, 7, true, "packet", "packets")).toBe("7 packets");
    expect(describeLibraryLoad(1, 1, true, "packet", "packets")).toBe("1 packet");
    expect(describeLibraryLoad(0, 0, true, "packet", "packets")).toBe("0 packets");
  });

  test("a bound-limited read names the shortfall and calls the list incomplete", () => {
    expect(describeLibraryLoad(2000, 2400, false, "packet", "packets")).toBe(
      "loaded 2000 of 2400 packets; the page bound cut the answer short, so " +
        "this list is incomplete",
    );
  });

  test("a resource that published no total is not reported as short", () => {
    expect(describeLibraryLoad(3, null, false, "packet", "packets")).toBe("3 packets");
  });
});
