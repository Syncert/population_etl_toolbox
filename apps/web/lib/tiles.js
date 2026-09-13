// Martin vector-tile boundary: discover tile layers from /tiles
// catalogs, normalize TileJSON templates to same-origin paths, and load
// preview features from a real decoded MVT sample.

import { VectorTile } from "@mapbox/vector-tile";
import Protobuf from "pbf";
import { isDrawableTileGrain } from "./tileGrains";

/**
 * Catalog keys that name a section of the catalog rather than a source.
 *
 * Martin groups what it serves into sections — `tiles`, `sprites`, `fonts`,
 * `styles` — beside a `settings` object, so source ids live one level down.
 * Treating these as layer ids makes discovery probe the catalog's own
 * structure, find nothing, and report that the deployment publishes no
 * spatial layer, which is a very different claim from "this build could not
 * read the catalog".
 */
const CATALOG_SECTIONS = new Set(["tiles", "sprites", "fonts", "styles", "settings"]);

/** The catalog sections whose entries are vector-tile sources. */
const TILE_SECTIONS = ["tiles"];

export function collectTileCandidates(catalogPayload) {
  const candidates = [];

  if (Array.isArray(catalogPayload)) {
    for (const entry of catalogPayload) {
      if (typeof entry === "string") {
        candidates.push(entry);
      } else if (entry && typeof entry.id === "string") {
        candidates.push(entry.id);
      }
    }
  } else if (catalogPayload && typeof catalogPayload === "object") {
    if (Array.isArray(catalogPayload.collections)) {
      for (const entry of catalogPayload.collections) {
        if (entry && typeof entry.id === "string") {
          candidates.push(entry.id);
        }
      }
    }

    // Section-keyed catalogs: only the tile sections hold something the map
    // can join observations to. A font or sprite id is not a vector layer.
    for (const section of TILE_SECTIONS) {
      const entries = catalogPayload[section];
      if (entries && typeof entries === "object" && !Array.isArray(entries)) {
        candidates.push(...Object.keys(entries));
      }
    }

    // Flat catalogs list their layer ids at the top level. A section name is
    // never one of them, so it is excluded here rather than probed.
    for (const key of Object.keys(catalogPayload)) {
      if (key !== "collections" && !CATALOG_SECTIONS.has(key)) {
        candidates.push(key);
      }
    }
  }

  return [...new Set(candidates)].filter(Boolean);
}

export function prioritizeTileCandidates(candidates) {
  const preferredOrder = ["dim_geo", "dim_geo_latest", "counties"];
  const remaining = [];
  const seen = new Set();

  for (const candidate of Array.isArray(candidates) ? candidates : []) {
    if (typeof candidate !== "string" || !candidate || seen.has(candidate)) {
      continue;
    }
    seen.add(candidate);
    remaining.push(candidate);
  }

  const prioritized = [];
  for (const preferred of preferredOrder) {
    if (seen.has(preferred)) {
      prioritized.push(preferred);
    }
  }

  for (const candidate of remaining) {
    if (!prioritized.includes(candidate)) {
      prioritized.push(candidate);
    }
  }

  return prioritized;
}

export function pickJoinKey(fields = {}) {
  const fieldKeys = Array.isArray(fields)
    ? fields
    : Object.keys(fields || {});
  const preferred = ["geo_id", "geoid", "GEOID", "county_fips", "state_fips"];

  for (const preferredKey of preferred) {
    const matched = fieldKeys.find(
      (key) => typeof key === "string" && key.toLowerCase() === preferredKey.toLowerCase(),
    );
    if (matched) {
      return matched;
    }
  }

  return "geo_id";
}

export function normalizeTileTemplate(layerId) {
  return `/tiles/${layerId}/{z}/{x}/{y}`;
}

export function normalizeTileJsonUrl(layerId) {
  return `/tiles/${layerId}`;
}

export function isVectorTileContentType(contentType) {
  const normalized = (contentType || "").toLowerCase();
  return (
    normalized.includes("application/x-protobuf") ||
    normalized.includes("application/vnd.mapbox-vector-tile") ||
    normalized.includes("application/octet-stream")
  );
}

export function normalizeTileTemplateFromTileJson(rawTemplate) {
  if (typeof rawTemplate !== "string" || !rawTemplate) {
    return "";
  }

  let path = rawTemplate;

  // The origin is stripped textually rather than by parsing the URL.
  //
  // A TileJSON `tiles` entry is a *template*, and `new URL(...).pathname`
  // percent-encodes its `{z}/{x}/{y}` placeholders into `%7Bz%7D/...`. The
  // result still looks like a template and still yields a URL, but the
  // substitution that builds a real tile request matches literal braces, so
  // it silently fills nothing and asks the tile server for the row `{y}`.
  // Martin answers that with a 404, discovery rejects a layer that was
  // healthy, and the map is empty on exactly the deployments whose TileJSON
  // was telling the truth about where its tiles live.
  const absolute = /^https?:\/\/[^/?#]*(.*)$/i.exec(rawTemplate);
  if (absolute) {
    path = absolute[1] || "/";
  }

  if (!path.startsWith("/")) {
    path = `/${path}`;
  }

  if (path.startsWith("/tiles/")) {
    return path;
  }

  return `/tiles${path}`;
}

export function buildSampleUrlFromTemplate(tileTemplate) {
  return tileTemplate
    .replaceAll("{z}", "0")
    .replaceAll("{x}", "0")
    .replaceAll("{y}", "0")
    .replaceAll(
      "{bbox-epsg-3857}",
      "-20037508.342789244,-20037508.342789244,20037508.342789244,20037508.342789244",
    );
}

/**
 * Release a response whose body this module will not read.
 *
 * Discovery probes several endpoints and reads the body of at most one of
 * them: a rejected status, and every tile sample probed only for its
 * content type, are decided from the head alone. An unread body is not free
 * -- it holds its connection open until the response is collected, and the
 * tile samples in particular are whole world tiles. Cancelling says the
 * caller is done with it, so the connection is returned immediately.
 *
 * Errors are swallowed deliberately: a body already consumed, already
 * cancelled, or absent (a 304, a HEAD) is nothing to report, and a discovery
 * probe must not fail over how its own discarded response was disposed of.
 */
function releaseBody(response) {
  try {
    return response?.body?.cancel?.()?.catch?.(() => {});
  } catch {
    return undefined;
  }
}

export async function discoverTileMetadata() {
  const discoveryPaths = ["/tiles/catalog", "/tiles/"];
  let prioritizedCandidates = [];

  for (const path of discoveryPaths) {
    try {
      const response = await fetch(path, { cache: "no-store" });
      if (!response.ok) {
        releaseBody(response);
        continue;
      }

      const payload = await response.json();
      const candidates = collectTileCandidates(payload);
      if (candidates.length > 0) {
        prioritizedCandidates = prioritizeTileCandidates(candidates);
        break;
      }
    } catch {
      // Continue to fallback discovery endpoint.
    }
  }

  if (prioritizedCandidates.length === 0) {
    throw new Error("No tile layer ids discovered from /tiles/catalog or /tiles/");
  }

  for (const id of prioritizedCandidates) {
    try {
      const tileJsonResponse = await fetch(`/tiles/${id}`, { cache: "no-store" });
      if (!tileJsonResponse.ok) {
        releaseBody(tileJsonResponse);
        continue;
      }

      const tileJson = await tileJsonResponse.json();
      const vectorLayer =
        Array.isArray(tileJson.vector_layers) && tileJson.vector_layers.length > 0
          ? tileJson.vector_layers[0]
          : null;
      const sourceLayerCandidates = [];

      if (Array.isArray(tileJson.vector_layers)) {
        for (const item of tileJson.vector_layers) {
          if (item && typeof item.id === "string") {
            sourceLayerCandidates.push(item.id);
          }
        }
      }

      if (typeof tileJson.name === "string") {
        sourceLayerCandidates.push(tileJson.name);
      }

      sourceLayerCandidates.push(id);

      const dedupedSourceLayerCandidates = [];
      const seenCandidates = new Set();
      for (const candidate of sourceLayerCandidates) {
        if (!candidate || seenCandidates.has(candidate)) {
          continue;
        }
        seenCandidates.add(candidate);
        dedupedSourceLayerCandidates.push(candidate);
      }

      const tileTemplateCandidates = [];

      if (Array.isArray(tileJson.tiles)) {
        for (const rawTemplate of tileJson.tiles) {
          const normalizedTemplate = normalizeTileTemplateFromTileJson(rawTemplate);
          if (normalizedTemplate) {
            tileTemplateCandidates.push(normalizedTemplate);
          }
        }
      }

      tileTemplateCandidates.push(normalizeTileTemplate(id));
      tileTemplateCandidates.push(`/${id}/{z}/{x}/{y}`);
      tileTemplateCandidates.push(`/${id}/{z}/{x}/{y}.pbf`);
      tileTemplateCandidates.push(`/tiles/${id}/{z}/{x}/{y}`);
      tileTemplateCandidates.push(`/tiles/${id}/{z}/{x}/{y}.pbf`);

      const dedupedTileTemplateCandidates = [];
      const seenTileTemplates = new Set();
      for (const candidateTemplate of tileTemplateCandidates) {
        if (!candidateTemplate || seenTileTemplates.has(candidateTemplate)) {
          continue;
        }
        seenTileTemplates.add(candidateTemplate);
        dedupedTileTemplateCandidates.push(candidateTemplate);
      }

      let selectedTileTemplate = null;
      for (const candidateTemplate of dedupedTileTemplateCandidates) {
        const sampleUrl = buildSampleUrlFromTemplate(candidateTemplate);
        const sampleTileResponse = await fetch(sampleUrl, { cache: "no-store" });
        const sampleContentType = sampleTileResponse.headers.get("content-type") || "";
        // The probe asks one question -- does this template serve vector
        // tiles -- and the headers answer it. The tile itself is refetched
        // by loadPreviewTileFeatures when one is actually drawn.
        releaseBody(sampleTileResponse);

        if (sampleTileResponse.ok && isVectorTileContentType(sampleContentType)) {
          selectedTileTemplate = candidateTemplate;
          break;
        }
      }

      if (!selectedTileTemplate) {
        continue;
      }

      const sourceLayerId = dedupedSourceLayerCandidates[0] || id;
      const layerFields = vectorLayer?.fields || {};
      const joinKey = pickJoinKey(layerFields);

      return {
        layerId: id,
        sourceLayer: sourceLayerId,
        sourceLayerCandidates: dedupedSourceLayerCandidates,
        joinKey,
        // The field names the layer publishes. They are what says which
        // geography grains this boundary can actually draw; a grain it
        // publishes nothing to identify has no spatial presentation.
        fields: Array.isArray(layerFields) ? [...layerFields] : Object.keys(layerFields),
        tileJsonUrl: normalizeTileJsonUrl(id),
        tileTemplate: selectedTileTemplate,
      };
    } catch {
      // Try next layer id.
    }
  }

  throw new Error("No healthy vector tile endpoint found from discovered /tiles/{id} candidates");
}

/** Whether a grain is drawable, with the empty grain meaning "every one". */
function grainCanBeDrawn(geoLevel) {
  const wanted = String(geoLevel || "").toUpperCase();
  return !wanted || isDrawableTileGrain(wanted);
}

/**
 * The features of a decoded layer at one grain.
 *
 * The grain is matched against each feature's published `geo_level` -- the
 * same rule the layer filter applies (`tileFilterForGeoLevel`), against the
 * same declaration of which grains the boundary can draw
 * (`DRAWABLE_TILE_GRAINS`), so the collection handed to the map is the
 * collection the map draws. This used to infer the grain from `county_fips`
 * instead, which made "not a county" mean "a state" and handed the state
 * grain every place polygon for the layer filter to hide again.
 *
 * An empty grain is every feature the layer carries, which is what checking
 * a decoded tile needs. That is the absence of a grain rather than a grain
 * of its own, so it is spelled as one instead of borrowing a selectable
 * grain the boundary cannot draw.
 */
export function featuresAtGrain(layer, geoLevel) {
  if (!grainCanBeDrawn(geoLevel)) {
    return [];
  }
  const wanted = String(geoLevel || "").toUpperCase();
  const features = [];
  for (let index = 0; index < layer.length; index += 1) {
    const feature = layer.feature(index).toGeoJSON(0, 0, 0);
    const published = String(feature.properties?.geo_level || "").toUpperCase();
    if (!wanted || published === wanted) {
      features.push(feature);
    }
  }
  return features;
}

/**
 * One sample tile decoded to the GeoJSON the choropleth source is given,
 * carrying the grain `featuresAtGrain` selects and nothing else.
 */
export async function loadPreviewTileFeatures(tileTemplate, sourceLayer, geoLevel) {
  if (!grainCanBeDrawn(geoLevel)) {
    // No map is drawn at this grain, so there is no tile worth a request.
    return { type: "FeatureCollection", features: [] };
  }

  const sampleUrl = buildSampleUrlFromTemplate(tileTemplate);
  const response = await fetch(sampleUrl, { cache: "no-store" });

  if (!response.ok) {
    releaseBody(response);
    throw new Error(`tile sample status ${response.status}`);
  }

  const tile = new VectorTile(new Protobuf(new Uint8Array(await response.arrayBuffer())));
  const layer = tile.layers[sourceLayer] || tile.layers[Object.keys(tile.layers)[0]];

  if (!layer) {
    throw new Error("tile sample contained no vector layers");
  }

  return {
    type: "FeatureCollection",
    features: featuresAtGrain(layer, geoLevel),
  };
}
