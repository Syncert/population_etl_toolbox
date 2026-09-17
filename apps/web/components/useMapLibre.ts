"use client";

// One way to bring a MapLibre map up and take it down.
//
// Both maps built their instance the same way -- the same empty style, the
// same opening view, the same navigation control, ready on `load`, removed on
// cleanup -- in two copies. This is the one copy. What differs between the
// screens (which sources and layers exist) is the caller's, through `onLoad`.
//
// The library is fetched by `import()` inside the effect rather than imported
// at the top of this file, so its bytes belong to a chunk that is requested
// when a map is actually drawn. MapLibre is by far the largest dependency the
// application has, the explorer's map is only one of its tabs, and the map is
// not mounted at all where WebGL is unavailable -- a reader in any of those
// cases used to download it anyway. The import is the only asynchronous step:
// everything after it is the construction that was here before.

import { useEffect, useRef, useState } from "react";
import type { RefObject } from "react";
import type { Map as MapLibreMap, StyleSpecification } from "maplibre-gl";
import { US_MAP_VIEW, baseMapStyle } from "../lib/mapWiring";

export interface MapLibreHandle {
  mapRef: RefObject<MapLibreMap | null>;
  /** True once the map has loaded its style and `onLoad` has run. */
  ready: boolean;
  /**
   * True when the map library itself could not be fetched.
   *
   * A static import could not fail at runtime; a chunk request can, on a
   * flaky network or a part-deployed build. A caller that never says so
   * leaves an empty rectangle that claims to be a map, so the state is
   * published here and every caller states it and points at its table.
   */
  loadFailed: boolean;
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
  onLoad?: (map: MapLibreMap) => void,
): MapLibreHandle {
  const mapRef = useRef<MapLibreMap | null>(null);
  const onLoadRef = useRef(onLoad);
  onLoadRef.current = onLoad;
  const [ready, setReady] = useState(false);
  const [loadFailed, setLoadFailed] = useState(false);

  useEffect(() => {
    if (!enabled || !containerRef.current || mapRef.current) {
      return;
    }
    // `cancelled` covers the window between asking for the chunk and being
    // handed it: an unmount in that window must not leave a map nobody holds.
    let cancelled = false;

    void import("maplibre-gl")
      .then((maplibregl) => {
        const container = containerRef.current;
        if (cancelled || !container || mapRef.current) {
          return;
        }
        const map = new maplibregl.Map({
          container,
          style: baseMapStyle() as StyleSpecification,
          center: US_MAP_VIEW.center,
          zoom: US_MAP_VIEW.zoom,
        });
        map.addControl(
          new maplibregl.NavigationControl({ showCompass: false }),
          "top-right",
        );
        map.on("load", () => {
          onLoadRef.current?.(map);
          setReady(true);
        });
        mapRef.current = map;
      })
      .catch(() => {
        if (!cancelled) {
          setLoadFailed(true);
        }
      });

    return () => {
      cancelled = true;
      const map = mapRef.current;
      if (map) {
        map.remove();
        mapRef.current = null;
      }
      setReady(false);
    };
  }, [containerRef, enabled]);

  return { mapRef, ready, loadFailed };
}
