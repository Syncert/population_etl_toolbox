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
import type { DistributionResponse } from "../lib/api/types";
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
  distribution = null,
  ariaLabel,
  onFeatureClick,
}: {
  rows: ObservationRow[];
  tileMetadata: TileMetadata | null;
  geoLevel: string;
  legendTitle: string;
  missingLabel?: string;
  testId?: string;
  /** Bins to colour by, in the distribution resource's shape; their counts reach the legend. */
  distribution?: DistributionResponse | null;
  /** Replaces the comparison's description for a map with another job. */
  ariaLabel?: string;
  /** Called with a clicked boundary's tile properties. */
  onFeatureClick?: (properties: Record<string, unknown>) => void;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const { mapRef, ready, loadFailed } = useMapLibre(containerRef, true);

  const model = useMemo(
    () => buildChoroplethModel(rows, tileMetadata?.joinKey || "geo_id", distribution, missingLabel),
    [rows, tileMetadata, missingLabel, distribution],
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

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready || !tileMetadata || !onFeatureClick) {
      return;
    }
    const handleClick = (event: { features?: { properties?: Record<string, unknown> | null }[] }) => {
      const properties = event.features?.[0]?.properties;
      if (properties) onFeatureClick(properties);
    };
    const pointer = () => { map.getCanvas().style.cursor = "pointer"; };
    const reset = () => { map.getCanvas().style.cursor = ""; };
    map.on("click", COMPARISON_LAYER, handleClick);
    map.on("mouseenter", COMPARISON_LAYER, pointer);
    map.on("mouseleave", COMPARISON_LAYER, reset);
    return () => {
      map.off("click", COMPARISON_LAYER, handleClick);
      map.off("mouseenter", COMPARISON_LAYER, pointer);
      map.off("mouseleave", COMPARISON_LAYER, reset);
    };
  }, [mapRef, ready, tileMetadata, onFeatureClick]);

  if (loadFailed) {
    // The library's chunk did not arrive. An empty rectangle labelled as a
    // map would be worse than a sentence, and the caller always renders a
    // table beside this component.
    return (
      <p className="status-line" role="status" data-testid={`${testId}-load-failed`}>
        The map could not be loaded. Every value it would colour is in the table
        below.
      </p>
    );
  }

  return (
    <div className="map-shell">
      <div
        className="map-canvas"
        data-testid={testId}
        data-map-ready={ready ? "true" : "false"}
        data-colored-values={model.valueCount}
        ref={containerRef}
        role="region"
        aria-label={ariaLabel || `${legendTitle}. The comparison table lists every value, including the geographies this map leaves uncoloured.`}
      />
      <ChoroplethLegend
        title={legendTitle}
        items={model.legendItems}
        ariaLabel={`${legendTitle} legend`}
      />
    </div>
  );
}
