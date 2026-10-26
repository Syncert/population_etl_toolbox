"use client";

// The home page's one job: get a reader to their place page in one action.
//
// A combobox over the geography catalog (find-your-place-home). Arrow keys
// move through the results, Enter opens the highlighted one, and the count is
// announced in a live region. Typing asks the API's place search (A-1), so a
// city or town is found by name as well as a county or state; the catalog's
// nation, states and counties answer at once while it does, and alone if it
// cannot. "Use my location" asks the browser only when pressed, asks the API
// which places contain that point and opens the city (or else the county),
// and keeps the coordinate nowhere -- not in the URL, not in storage.

import { useEffect, useId, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { LocateFixed, Search } from "lucide-react";
import { apiErrorMessage, apiFetch, fetchAllPages } from "../lib/api/client";
import type { GeographySummary } from "../lib/api/types";
import { ACTIVE_GEOGRAPHIES_ONLY } from "../lib/observationAccess";
import {
  buildPlaceDirectory,
  nearestCounty,
  placeMatchEntry,
  searchPlaces,
} from "../lib/placeDirectory";
import type { PlaceEntry, PlaceMatch } from "../lib/placeDirectory";

const PAGE_SIZE = 1000;

const LEVEL_WORDS: Record<PlaceEntry["level"], string> = {
  NATIONAL: "nation",
  STATE: "state",
  COUNTY: "county",
  PLACE: "city or town",
  METRO: "metro area",
};

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
  const [active, setActive] = useState(0);
  const [locating, setLocating] = useState("");
  const [canLocate, setCanLocate] = useState(false);
  const [served, setServed] = useState<{ query: string; entries: PlaceEntry[] } | null>(null);
  const local = useMemo(() => (catalog ? searchPlaces(catalog.entries, query) : []), [catalog, query]);

  // The API's search, a moment after typing stops. Its answer replaces the
  // local one only for the query it was asked for.
  useEffect(() => {
    const q = query.trim();
    if (!catalog || q.length < 2) return;
    const controller = new AbortController();
    const timer = setTimeout(() => {
      apiFetch<{ items: PlaceMatch[] }>("/catalog/geographies/search", {
        params: { q, limit: 16 },
        signal: controller.signal,
      })
        .then((body) => {
          const entries = body.items
            .map((match) => placeMatchEntry(match, catalog.states))
            .filter((entry): entry is PlaceEntry => entry !== null);
          setServed({ query: q, entries });
        })
        .catch(() => undefined);
    }, 200);
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [catalog, query]);

  const results = useMemo(() => {
    const q = query.trim();
    if (!served || served.query !== q) return local;
    // The nation is not a search result; keep it when the reader asked for it.
    const nation = local.filter((entry) => entry.level === "NATIONAL");
    return [...nation, ...served.entries].slice(0, 8);
  }, [local, served, query]);

  useEffect(() => {
    setCanLocate(typeof navigator !== "undefined" && "geolocation" in navigator);
  }, []);

  useEffect(() => setActive(0), [query]);

  const open = (entry: PlaceEntry | undefined) => {
    if (entry) router.push(entry.href);
  };

  const locate = () => {
    if (!catalog) return;
    setLocating("Asking your browser for your location…");
    navigator.geolocation.getCurrentPosition(
      (position) => {
        const { latitude, longitude } = position.coords;
        const nearest = () => {
          const county = nearestCounty(latitude, longitude, catalog.counties);
          const entry = county ? catalog.entries.find((item) => item.geoId === county.geo_id) : undefined;
          if (entry) {
            setLocating(`Opening ${entry.name}, the county whose center is nearest you.`);
            router.push(entry.href);
          } else {
            setLocating("No published county is near that location. Search for your place instead.");
          }
        };
        apiFetch<{ items: PlaceMatch[] }>("/catalog/geographies/locate", { params: { latitude, longitude } })
          .then((body) => {
            const entries = body.items
              .map((match) => placeMatchEntry(match, catalog.states))
              .filter((entry): entry is PlaceEntry => entry !== null);
            const entry = entries.find((item) => item.level === "PLACE") || entries.find((item) => item.level === "COUNTY");
            if (!entry) return nearest();
            setLocating(`Opening ${entry.name}, which contains your location.`);
            router.push(entry.href);
          })
          .catch(nearest);
      },
      () => setLocating("Your location is not available. Search for your place instead."),
      { maximumAge: 600_000, timeout: 15_000 },
    );
  };

  const announcement = !catalog
    ? error
      ? `Places could not be loaded: ${error}`
      : "Loading places…"
    : query.trim()
      ? `${results.length} place${results.length === 1 ? "" : "s"} found`
      : "";

  return (
    <div className="place-finder" data-testid="place-finder">
      <label htmlFor={`${id}-input`} className="place-finder-label">
        Find your city or town, county, state, or the nation
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
          aria-activedescendant={results.length ? `${id}-option-${active}` : undefined}
          placeholder="Dane County, Wisconsin"
          autoComplete="off"
          value={query}
          disabled={!catalog}
          data-testid="place-finder-input"
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "ArrowDown") {
              event.preventDefault();
              setActive((index) => Math.min(index + 1, results.length - 1));
            } else if (event.key === "ArrowUp") {
              event.preventDefault();
              setActive((index) => Math.max(index - 1, 0));
            } else if (event.key === "Enter") {
              event.preventDefault();
              open(results[active]);
            } else if (event.key === "Escape") {
              setQuery("");
            }
          }}
        />
        {canLocate ? (
          <button type="button" className="button secondary" onClick={locate} disabled={!catalog} data-testid="place-finder-locate">
            <LocateFixed size={16} aria-hidden="true" /> Use my location
          </button>
        ) : null}
      </div>
      <ul id={`${id}-results`} role="listbox" className="place-finder-results" aria-label="Matching places">
        {results.map((entry, index) => (
          <li
            key={entry.geoId}
            id={`${id}-option-${index}`}
            role="option"
            aria-selected={index === active}
            className={index === active ? "active" : undefined}
            onMouseDown={(event) => {
              event.preventDefault();
              open(entry);
            }}
          >
            {entry.name}
            <span className="subtle"> · {LEVEL_WORDS[entry.level]}</span>
          </li>
        ))}
      </ul>
      <p className="sr-only" role="status" data-testid="place-finder-status">{announcement}</p>
      {locating ? <p className="subtle" role="status" data-testid="place-finder-locating">{locating}</p> : null}
    </div>
  );
}
