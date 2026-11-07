// Synthetic Mapbox vector tiles for browser fixtures (web-place-maps).
//
// The browser tier has no Martin, so a map that should draw cities or metros
// is served tiles built here: a few hand-placed polygons, projected into
// whichever tile the map asks for. The shapes are fixture shapes (a small
// polygon around a point), not boundaries, and every test that uses them
// says so; what they prove is that the page asks for the layer and vintage
// it names and colours by joining published values on `geo_id`.

import Pbf from "../../../apps/web/node_modules/pbf/index.js";

const EXTENT = 4096;

function project(lon, lat, z, x, y) {
  const n = 2 ** z;
  const px = ((lon + 180) / 360) * n;
  const latRad = (lat * Math.PI) / 180;
  const py = ((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2) * n;
  return [Math.round((px - x) * EXTENT), Math.round((py - y) * EXTENT)];
}

const zigzag = (n) => (n << 1) ^ (n >> 31);

/** A polygon of `sides` corners around a point, `radius` degrees across. */
export function polygonAround(lon, lat, radius, sides = 8) {
  const ring = [];
  for (let corner = 0; corner < sides; corner += 1) {
    const angle = (2 * Math.PI * corner) / sides;
    ring.push([lon + radius * Math.cos(angle), lat + radius * Math.sin(angle)]);
  }
  return ring;
}

function ringGeometry(points) {
  // MVT v2: an exterior ring has positive area in tile (y-down) coordinates.
  let area = 0;
  for (let index = 0; index < points.length; index += 1) {
    const [x1, y1] = points[index];
    const [x2, y2] = points[(index + 1) % points.length];
    area += x1 * y2 - x2 * y1;
  }
  const ring = area < 0 ? [...points].reverse() : points;
  const commands = [];
  let cursorX = 0;
  let cursorY = 0;
  ring.forEach(([px, py], index) => {
    if (index === 0) commands.push(9);
    else if (index === 1) commands.push(2 | ((ring.length - 1) << 3));
    commands.push(zigzag(px - cursorX), zigzag(py - cursorY));
    cursorX = px;
    cursorY = py;
  });
  commands.push(15);
  return commands;
}

function inTile(points) {
  const margin = EXTENT / 8;
  return points.some(([px, py]) => px > -margin && px < EXTENT + margin && py > -margin && py < EXTENT + margin);
}

/**
 * One tile holding each layer's features that fall in it.
 *
 * `layers` is `[{ name, features: [{ ring: [[lon, lat], ...], properties }] }]`.
 */
export function encodeTile(layers, z, x, y) {
  const pbf = new Pbf();
  for (const layer of layers) {
    const keys = [];
    const values = [];
    const features = [];
    for (const feature of layer.features) {
      const points = feature.ring.map(([lon, lat]) => project(lon, lat, z, x, y));
      if (!inTile(points)) continue;
      const tags = [];
      for (const [key, value] of Object.entries(feature.properties)) {
        if (value === null || value === undefined) continue;
        let keyIndex = keys.indexOf(key);
        if (keyIndex < 0) keyIndex = keys.push(key) - 1;
        let valueIndex = values.findIndex((item) => item === value);
        if (valueIndex < 0) valueIndex = values.push(value) - 1;
        tags.push(keyIndex, valueIndex);
      }
      features.push({ tags, geometry: ringGeometry(points) });
    }
    pbf.writeMessage(3, (_, out) => {
      out.writeVarintField(15, 2);
      out.writeStringField(1, layer.name);
      for (const feature of features) {
        out.writeMessage(2, (__, featureOut) => {
          featureOut.writePackedVarint(2, feature.tags);
          featureOut.writeVarintField(3, 3);
          featureOut.writePackedVarint(4, feature.geometry);
        });
      }
      for (const key of keys) out.writeStringField(3, key);
      for (const value of values) {
        out.writeMessage(4, (___, valueOut) => {
          if (typeof value === "number") valueOut.writeDoubleField(3, value);
          else valueOut.writeStringField(1, String(value));
        });
      }
      out.writeVarintField(5, EXTENT);
    });
  }
  return Buffer.from(pbf.finish());
}

/** Answer `/tiles/<layer>/{z}/{x}/{y}` with tiles built from `layers`. */
export async function routeSyntheticTiles(page, layerId, layers, requests = []) {
  await page.route(new RegExp(`/tiles/${layerId}/\\d+/\\d+/\\d+`), (route) => {
    const url = new URL(route.request().url());
    requests.push(`${url.pathname}${url.search}`);
    const [z, x, y] = url.pathname.split("/").slice(-3).map(Number);
    return route.fulfill({ status: 200, contentType: "application/x-protobuf", body: encodeTile(layers, z, x, y) });
  });
}
