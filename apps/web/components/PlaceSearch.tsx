"use client";

// The home page's one job: get a reader to their place page in one action.
//
// A combobox over the API's place search (D-1, over A-1): typing "Austin"
// offers Austin city, Travis County and the Austin metro, ranked by
// population. Arrow keys move through the results that have a page, Enter
// opens the highlighted one, and the count is announced in a live region.
// Where the search is unavailable, the catalog's counties and states are
// matched here in the page instead, as before (find-your-place-home).
//
// "Use my location" asks the browser only when pressed, asks the API which
// city or county contains the point, and keeps the coordinate nowhere -- not
// in the page's address, not in storage. It is rounded to about 11 meters
// before it is sent, and the API's answer does not echo it.

import { useEffect, useId, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { LocateFixed, Search } from "lucide-react";
import { ApiError, apiErrorMessage, fetchAllPages, locateGeography, searchGeographies } from "../lib/api/client";
import type { GeographySummary } from "../lib/api/types";
import { ACTIVE_GEOGRAPHIES_ONLY } from "../lib/observationAccess";
import { buildPlaceDirectory, nearestCounty, searchPlaces } from "../lib/placeDirectory";
import type { PlaceEntry } from "../lib/placeDirectory";
import { directoryPlaces, foundPlaces, locateLanding, nationMatch, roundCoordinate, searchable } from "../lib/placeSearch";
import type { FoundPlace } from "../lib/placeSearch";

const PAGE_SIZE = 1000;
const SEARCH_LIMIT = 10;
const SEARCH_DELAY_MS = 200;

export interface PlaceCatalog {
  nation: GeographySummary | null;
  states: GeographySummary[];
  counties: GeographySummary[];
  entries: PlaceEntry[];
}

/** The catalog's places, read once; shared by the search and the map. */
export function usePlaceCatalog(): { catalog: PlaceCatalog | null; error: string } {
  const [catalog, setCatalog] = useState<PlaceCatalog | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    const read = (geo_level: string) =>
      fetchAllPages<GeographySummary>("/catalog/geographies", {
        params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level },
        pageSize: PAGE_SIZE,
        signal: controller.signal,
      });
    Promise.all([read("NATIONAL"), read("STATE"), read("COUNTY")])
      .then(([nations, states, counties]) => {
        if (controller.signal.aborted) return;
        const nation = nations[0] || null;
        setCatalog({ nation, states, counties, entries: buildPlaceDirectory(nation, states, counties) });
      })
      .catch((reason) => {
        if (!controller.signal.aborted) setError(apiErrorMessage(reason));
      });
    return () => controller.abort();
  }, []);
  return { catalog, error };
}

export default function PlaceSearch({ catalog, error }: { catalog: PlaceCatalog | null; error: string }) {
  const router = useRouter();
  const id = useId();
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(-1);
  const [locating, setLocating] = useState("");
  const [canLocate, setCanLocate] = useState(false);
  // The API's answer and the query it answered; `fallback` once the search
  // itself has failed, after which the catalog's rows answer instead.
  const [answer, setAnswer] = useState<{ query: string; places: FoundPlace[] } | null>(null);
  const [fallback, setFallback] = useState(false);
  const [searching, setSearching] = useState(false);

  useEffect(() => {
    setCanLocate(typeof navigator !== "undefined" && "geolocation" in navigator);
  }, []);

  useEffect(() => {
    const typed = query.trim();
    if (fallback || !searchable(typed)) {
      setSearching(false);
      return;
    }
    const controller = new AbortController();
    setSearching(true);
    const timer = setTimeout(() => {
      searchGeographies({ q: typed, limit: SEARCH_LIMIT }, { signal: controller.signal })
        .then((response) => {
          if (controller.signal.aborted) return;
          setAnswer({ query: typed, places: foundPlaces(response.items || []) });
          setSearching(false);
        })
        .catch((reason) => {
          if (controller.signal.aborted) return;
          // A 422 is this query refused (too short once punctuation is
          // dropped): nothing matches it. Anything else means the search
          // itself is unavailable, and the catalog's own rows answer.
          if (reason instanceof ApiError && reason.status === 422) {
            setAnswer({ query: typed, places: [] });
          } else {
            setFallback(true);
          }
          setSearching(false);
        });
    }, SEARCH_DELAY_MS);
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [query, fallback]);

  const results = useMemo<FoundPlace[]>(() => {
    const typed = query.trim();
    if (!typed) return [];
    if (fallback) return catalog ? directoryPlaces(searchPlaces(catalog.entries, typed)) : [];
    const nation = nationMatch(typed);
    const served = answer && answer.query === typed ? answer.places : [];
    return nation ? [nation, ...served] : served;
  }, [query, fallback, catalog, answer]);

  const openable = useMemo(() => results.map((place, index) => (place.href ? index : -1)).filter((index) => index >= 0), [results]);

  useEffect(() => setActive(openable[0] ?? -1), [openable]);

  const step = (direction: 1 | -1) => {
    if (!openable.length) return;
    const position = openable.indexOf(active);
    const next = position < 0 ? 0 : Math.min(Math.max(position + direction, 0), openable.length - 1);
    setActive(openable[next]!);
  };

  const open = (place: FoundPlace | undefined) => {
    if (place?.href) router.push(place.href);
  };

  const nearestCountyLanding = (latitude: number, longitude: number) => {
    const county = catalog ? nearestCounty(latitude, longitude, catalog.counties) : null;
    const entry = county ? catalog!.entries.find((item) => item.geoId === county.geo_id) : undefined;
    if (entry) {
      setLocating(`Opening ${entry.name}, the county whose center is nearest you.`);
      router.push(entry.href);
    } else {
      setLocating("Your place could not be found from your location. Search for your place instead.");
    }
  };

  const locate = () => {
    setLocating("Asking your browser for your location…");
    navigator.geolocation.getCurrentPosition(
      (position) => {
        const latitude = roundCoordinate(position.coords.latitude);
        const longitude = roundCoordinate(position.coords.longitude);
        setLocating("Finding the city or county you are in…");
        locateGeography(latitude, longitude)
          .then((located) => {
            const landing = locateLanding(located);
            if (landing) {
              setLocating(landing.message);
              router.push(landing.href);
            } else {
              setLocating("Your place has no page here yet. Search for your place instead.");
            }
          })
          .catch((reason) => {
            if (reason instanceof ApiError && reason.status === 404) {
              setLocating("Your location is outside the United States places this site covers. Search for your place instead.");
            } else {
              // The lookup is unavailable: the nearest county center is an
              // approximation, and says it is one.
              nearestCountyLanding(latitude, longitude);
            }
          });
      },
      () => setLocating("Your location is not available. Search for your place instead."),
      { maximumAge: 600_000, timeout: 15_000 },
    );
  };

  const typed = query.trim();
  const announcement = fallback && !catalog
    ? error
      ? `Places could not be loaded: ${error}`
      : "Loading places…"
    : !typed
      ? ""
      : !fallback && !searchable(typed)
        ? "Type at least two letters."
        : searching && !results.length
          ? "Searching…"
          : `${results.length} place${results.length === 1 ? "" : "s"} found`;

  return (
    <div className="place-finder" data-testid="place-finder" data-search={fallback ? "catalog" : "api"}>
      <label htmlFor={`${id}-input`} className="place-finder-label">
        Find your city, county, or state
      </label>
      <div className="place-finder-row">
        <Search size={18} aria-hidden="true" />
        <input
          id={`${id}-input`}
          type="search"
          role="combobox"
          aria-expanded={results.length > 0}
          aria-controls={`${id}-results`}
          aria-autocomplete="list"
          aria-activedescendant={active >= 0 && results[active] ? `${id}-option-${active}` : undefined}
          placeholder="Austin, Texas"
          autoComplete="off"
          value={query}
          data-testid="place-finder-input"
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "ArrowDown") {
              event.preventDefault();
              step(1);
            } else if (event.key === "ArrowUp") {
              event.preventDefault();
              step(-1);
            } else if (event.key === "Enter") {
              event.preventDefault();
              open(results[active]);
            } else if (event.key === "Escape") {
              setQuery("");
            }
          }}
        />
        {canLocate ? (
          <button type="button" className="button secondary" onClick={locate} data-testid="place-finder-locate">
            <LocateFixed size={16} aria-hidden="true" /> Use my location
          </button>
        ) : null}
      </div>
      <ul id={`${id}-results`} role="listbox" className="place-finder-results" aria-label="Matching places">
        {results.map((place, index) => (
          <li
            key={`${place.geoId}-${index}`}
            id={`${id}-option-${index}`}
            role="option"
            aria-selected={index === active}
            aria-disabled={place.href ? undefined : true}
            data-geo-level={place.level}
            className={index === active ? "active" : undefined}
            style={place.href ? undefined : { cursor: "default" }}
            onMouseDown={(event) => {
              event.preventDefault();
              open(place);
            }}
          >
            {place.name}
            <span className="subtle"> · {place.kind}</span>
            {place.via ? <span className="subtle"> · {place.via}</span> : null}
            {place.population ? <span className="subtle"> · {place.population}</span> : null}
            {place.href ? null : <span className="subtle"> · no page yet</span>}
          </li>
        ))}
      </ul>
      {fallback ? (
        <p className="subtle" data-testid="place-finder-fallback">City search is not available right now, so only counties, states and the nation are searched.</p>
      ) : null}
      <p className="sr-only" role="status" data-testid="place-finder-status">{announcement}</p>
      {locating ? <p className="subtle" role="status" data-testid="place-finder-locating">{locating}</p> : null}
    </div>
  );
}
