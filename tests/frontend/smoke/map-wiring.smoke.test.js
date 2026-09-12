import { beforeAll, describe, expect, test } from "vitest";

// Covers: WEB-033 — the URL the comparison map hands MapLibre, fetched from
// real Martin the way MapLibre fills it.
//
// The browser tier cannot see this. Headless Chromium has no GL context, so
// MapLibre requests no tiles there at all, and the map's legend and coloured
// count come from the rows rather than the tiles — a comparison map whose
// every tile 404s stays green in that tier. `new URL(template, origin)`
// percent-encodes the template's braces (Chromium honours the WHATWG path
// set), and MapLibre fills a template by literal `replace(/{z}/g, …)`, so a
// source URL built that way is never filled. Only a real tile server can
// show the difference, so this runs against one: the template resolved
// textually yields a vector tile, and the `new URL` form of the same
// template does not.
//
// Opt-in like the rest of the smoke tier: without SMOKE_BASE_URL it skips.

import { boundaryTileUrl } from "../../../apps/web/lib/mapWiring";
import { discoverTileMetadata, isVectorTileContentType } from "../../../apps/web/lib/tiles";
import { reportUnhandledErrors } from "./unhandledErrors";

const BASE_URL = (process.env.SMOKE_BASE_URL || "").replace(/\/+$/, "");

/** Exactly how MapLibre fills a tile template: literal braces, nothing decoded. */
function fillAsMapLibreDoes(template, z, x, y) {
  return template
    .replace(/{z}/g, String(z))
    .replace(/{x}/g, String(x))
    .replace(/{y}/g, String(y));
}

describe.skipIf(!BASE_URL)("the comparison map's source URL against real Martin", () => {
  let template;

  beforeAll(async () => {
    const realFetch = globalThis.fetch;
    globalThis.fetch = (input, init) => {
      const target = typeof input === "string" ? input : input.url;
      return realFetch(target.startsWith("/") ? `${BASE_URL}${target}` : target, init);
    };
    const tiles = await discoverTileMetadata();
    template = tiles.tileTemplate;
    expect(template).toMatch(/\{z\}\/\{x\}\/\{y\}/);
  }, 60_000);

  test("the textually resolved template, filled as MapLibre fills it, fetches a vector tile", async () => {
    const source = boundaryTileUrl(template, BASE_URL);
    expect(source).toContain("{z}/{x}/{y}");
    const response = await fetch(fillAsMapLibreDoes(source, 0, 0, 0));
    expect(response.status).toBe(200);
    expect(isVectorTileContentType(response.headers.get("content-type"))).toBe(true);
    expect((await response.arrayBuffer()).byteLength).toBeGreaterThan(0);
  });

  test("the same template through new URL cannot be filled, and the server has no such tile", async () => {
    const encoded = new URL(template, BASE_URL).toString();
    // The placeholders are gone; MapLibre's replace finds nothing to fill.
    expect(encoded).toContain("%7Bz%7D");
    const filled = fillAsMapLibreDoes(encoded, 0, 0, 0);
    expect(filled).toBe(encoded);
    const response = await fetch(filled);
    // Whatever the server answers for the row "%7By%7D", it is not a tile.
    const isTile =
      response.status === 200 && isVectorTileContentType(response.headers.get("content-type"));
    // Drained even though the verdict is decided from the head: an abandoned
    // body holds its socket, and a socket that ends with its parser still
    // paused takes the whole process down with it.
    await response.arrayBuffer();
    expect(isTile, `${filled} answered ${response.status} as a tile`).toBe(false);
  });
});

// Declared last on purpose: it reports on every request the tests above made.
reportUnhandledErrors();
