import { describe, expect, it } from "vitest";

import { metroAddressIsUnknown, placeAddressIsUnknown } from "../../../apps/web/lib/placeExists";

// Covers: WEB-185 — the web server answers 404 for a place address the
// catalog does not publish, and only then: a failed, partial or unexpected
// catalog read leaves the address to the browser (website UX review F6).

const STATES = [
  { geo_id: "state:55", geo_level: "STATE", geo_name: "Wisconsin", state_fips: "55" },
  { geo_id: "state:27", geo_level: "STATE", geo_name: "Minnesota", state_fips: "27" },
];
const COUNTIES = [
  { geo_id: "state:55|county:025", geo_level: "COUNTY", geo_name: "Dane County", county_name: "Dane County", state_fips: "55", county_fips: "025" },
  { geo_id: "state:55|county:105", geo_level: "COUNTY", geo_name: "Rock County", county_name: "Rock County", state_fips: "55", county_fips: "105" },
];
const PLACES = [
  { geo_id: "state:55|place:48000", geo_level: "PLACE", geo_name: "Madison city", place_name: "Madison city", state_fips: "55", place_fips: "48000" },
];

function catalog({ counties = COUNTIES, places = PLACES, broken = "" } = {}) {
  const calls = [];
  const fetcher = async (url) => {
    const address = new URL(url);
    calls.push(`${address.pathname}?${address.searchParams.get("geo_level")}`);
    const level = address.searchParams.get("geo_level");
    if (level === broken) return { ok: false, status: 503, json: async () => ({}) };
    const items = level === "STATE" ? STATES : level === "COUNTY" ? counties : places;
    return { ok: true, status: 200, json: async () => ({ items, total: items.length }) };
  };
  return { calls, fetcher, origin: "http://api:8000" };
}

describe("a place address the catalog does not publish", () => {
  it("is unknown when no state answers to the segment, after one read", async () => {
    const api = catalog();
    expect(await placeAddressIsUnknown("atlantis", undefined, api)).toBe(true);
    expect(await placeAddressIsUnknown("atlantis", "dane-county", api)).toBe(true);
    expect(api.calls).toEqual(["/api/v1/catalog/geographies?STATE", "/api/v1/catalog/geographies?STATE"]);
  });

  it("is known for a state, a county and a city, by name or by code", async () => {
    const api = catalog();
    expect(await placeAddressIsUnknown("wisconsin", undefined, api)).toBe(false);
    expect(await placeAddressIsUnknown("55", undefined, api)).toBe(false);
    expect(await placeAddressIsUnknown("wisconsin", "dane-county", api)).toBe(false);
    expect(await placeAddressIsUnknown("wisconsin", "55025", api)).toBe(false);
    expect(await placeAddressIsUnknown("wisconsin", "madison", api)).toBe(false);
  });

  it("is unknown when neither a county nor a city of the state answers", async () => {
    const api = catalog();
    expect(await placeAddressIsUnknown("wisconsin", "no-such-county", api)).toBe(true);
    expect(api.calls.map((call) => call.split("?")[1])).toEqual(["STATE", "COUNTY", "PLACE"]);
  });

  it("leaves the address to the browser when any read fails or is cut short", async () => {
    expect(await placeAddressIsUnknown("atlantis", undefined, catalog({ broken: "STATE" }))).toBe(false);
    expect(await placeAddressIsUnknown("wisconsin", "nowhere", catalog({ broken: "COUNTY" }))).toBe(false);
    expect(await placeAddressIsUnknown("wisconsin", "nowhere", catalog({ broken: "PLACE" }))).toBe(false);
    const refused = async () => { throw new Error("refused"); };
    expect(await placeAddressIsUnknown("atlantis", undefined, { fetcher: refused, origin: "x" })).toBe(false);
    const short = async () => ({ ok: true, status: 200, json: async () => ({ items: [], total: 5 }) });
    expect(await placeAddressIsUnknown("atlantis", undefined, { fetcher: short, origin: "x" })).toBe(false);
    const shapeless = async () => ({ ok: true, status: 200, json: async () => ({ rows: [] }) });
    expect(await placeAddressIsUnknown("atlantis", undefined, { fetcher: shapeless, origin: "x" })).toBe(false);
  });

  it("is unknown for a metro only when the API answers 404", async () => {
    const answering = (status) => async () => ({ ok: status === 200, status, json: async () => ({}) });
    expect(await metroAddressIsUnknown("no-such-metro", { fetcher: answering(404), origin: "x" })).toBe(true);
    expect(await metroAddressIsUnknown("madison-wi", { fetcher: answering(200), origin: "x" })).toBe(false);
    expect(await metroAddressIsUnknown("madison-wi", { fetcher: answering(503), origin: "x" })).toBe(false);
    const refused = async () => { throw new Error("refused"); };
    expect(await metroAddressIsUnknown("madison-wi", { fetcher: refused, origin: "x" })).toBe(false);
  });
});
