"use client";

// A read-only choropleth over the Martin vector boundary.
//
// The colouring logic is not new: this renders `buildChoroplethModel` from
// lib/explorerViewModel — the same model the explorer uses, with the same
// join key discovery and the same rule that a row without a usable number is
// left uncoloured rather than coloured as zero. The map instance and the
// layer syncing are not new either: `useMapLibre` and `lib/mapWiring` are the
// same construction and the same sync the explorer uses. What is local here
// is only which source and layer exist — one fill straight off the vector
// source, with no hover, selection, or extrusion, which is all a comparison
// map needs.
//
// The legend states what is being coloured, including whether the value is
// API-derived, and the caller always renders a table alongside so the map is
// never the only way to retrieve a value.

import { useEffect, useMemo, useRef } from "react";
import type { ExpressionSpecification, FilterSpecification } from "maplibre-gl";
import ChoroplethLegend from "./ChoroplethLegend";
import { useMapLibre } from "./useMapLibre";
import { buildChoroplethModel, tileFilterForGeoLevel } from "../lib/explorerViewModel";
import type { ObservationRow } from "../lib/explorerViewModel";
import {
  COMPARISON_LAYER,
  COMPARISON_SOURCE,
  boundaryTileUrl,
  syncLayerFilter,
  syncLayerPaint,
} from "../lib/mapWiring";
import type { discoverTileMetadata } from "../lib/tiles";

type TileMetadata = Awaited<ReturnType<typeof discoverTileMetadata>>;

export default function ChoroplethMap({
  rows,
  tileMetadata,
  geoLevel,
  legendTitle,
  missingLabel = "Not published on both sides",
  testId = "comparison-map",
}: {
  rows: ObservationRow[];
  tileMetadata: TileMetadata | null;
  geoLevel: string;
  legendTitle: string;
  missingLabel?: string;
  testId?: string;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const { mapRef, ready } = useMapLibre(containerRef, true);

  const model = useMemo(
    () => buildChoroplethModel(rows, tileMetadata?.joinKey || "geo_id", null, missingLabel),
    [rows, tileMetadata, missingLabel],
  );

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready || !tileMetadata) {
      return;
    }
    if (!map.getSource(COMPARISON_SOURCE)) {
      map.addSource(COMPARISON_SOURCE, {
        type: "vector",
        tiles: [boundaryTileUrl(tileMetadata.tileTemplate, window.location.origin)],
        minzoom: 0,
        maxzoom: 12,
      });
    }
    const expression = model.expression as unknown as ExpressionSpecification;
    if (!map.getLayer(COMPARISON_LAYER)) {
      map.addLayer({
        id: COMPARISON_LAYER,
        type: "fill",
        source: COMPARISON_SOURCE,
        "source-layer": tileMetadata.sourceLayer,
        paint: {
          "fill-color": expression,
          "fill-opacity": 0.85,
          "fill-outline-color": "#ffffff",
        },
      });
    } else {
      syncLayerPaint(map, [COMPARISON_LAYER], "fill-color", expression);
    }
    syncLayerFilter(
      map,
      [COMPARISON_LAYER],
      tileFilterForGeoLevel(geoLevel) as unknown as FilterSpecification,
    );
  }, [mapRef, ready, tileMetadata, model, geoLevel]);

  return (
    <div className="map-shell">
      <div
        className="map-canvas"
        data-testid={testId}
        data-map-ready={ready ? "true" : "false"}
        data-colored-values={model.valueCount}
        ref={containerRef}
        role="region"
        aria-label={`${legendTitle}. The comparison table lists every value, including the geographies this map leaves uncoloured.`}
      />
      <ChoroplethLegend
        title={legendTitle}
        items={model.legendItems}
        ariaLabel={`${legendTitle} legend`}
      />
    </div>
  );
}
