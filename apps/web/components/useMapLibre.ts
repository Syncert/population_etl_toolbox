"use client";

// One way to bring a MapLibre map up and take it down.
//
// Both maps built their instance the same way -- the same empty style, the
// same opening view, the same navigation control, ready on `load`, removed on
// cleanup -- in two copies. This is the one copy. What differs between the
// screens (which sources and layers exist) is the caller's, through `onLoad`.

import { useEffect, useRef, useState } from "react";
import type { RefObject } from "react";
import * as maplibregl from "maplibre-gl";
import type { StyleSpecification } from "maplibre-gl";
import { US_MAP_VIEW, baseMapStyle } from "../lib/mapWiring";

export interface MapLibreHandle {
  mapRef: RefObject<maplibregl.Map | null>;
  /** True once the map has loaded its style and `onLoad` has run. */
  ready: boolean;
}

/**
 * Mount a map in `containerRef` while `enabled` holds.
 *
 * When `enabled` turns false the map is removed rather than hidden: a hidden
 * WebGL context still costs memory and still answers events, and the
 * explorer only offers the map while the boundary can draw the selection.
 * `onLoad` runs once per instance, after the style loads, and is where a
 * caller adds the sources and layers that are its own.
 */
export function useMapLibre(
  containerRef: RefObject<HTMLDivElement | null>,
  enabled: boolean,
  onLoad?: (map: maplibregl.Map) => void,
): MapLibreHandle {
  const mapRef = useRef<maplibregl.Map | null>(null);
  const onLoadRef = useRef(onLoad);
  onLoadRef.current = onLoad;
  const [ready, setReady] = useState(false);

  useEffect(() => {
    if (!enabled || !containerRef.current || mapRef.current) {
      return;
    }
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: baseMapStyle() as StyleSpecification,
      center: US_MAP_VIEW.center,
      zoom: US_MAP_VIEW.zoom,
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    map.on("load", () => {
      onLoadRef.current?.(map);
      setReady(true);
    });
    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
      setReady(false);
    };
  }, [containerRef, enabled]);

  return { mapRef, ready };
}
