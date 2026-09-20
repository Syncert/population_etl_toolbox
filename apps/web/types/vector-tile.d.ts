// Ambient declarations for the two untyped packages the tile boundary uses.
//
// `@mapbox/vector-tile` and `pbf` ship no types and have no `@types` package
// in this dependency set. `lib/tiles.ts` is a contract boundary and is
// compiled `strict`, so the alternative to declaring them is `any` at the one
// place a malformed tile would first be noticed.
//
// Only the surface this application calls is declared. A wider declaration
// would be a guess about a library's shape rather than a statement about how
// this code uses it, and a wrong guess types as confidently as a right one.

declare module "pbf" {
  export default class Protobuf {
    constructor(buffer: Uint8Array | ArrayBuffer);
  }
}

declare module "@mapbox/vector-tile" {
  import type { Feature, Geometry, GeoJsonProperties } from "geojson";
  import type Protobuf from "pbf";

  export interface VectorTileFeature {
    /** Decode to GeoJSON in tile coordinates; `0, 0, 0` yields the raw tile. */
    toGeoJSON(x: number, y: number, z: number): Feature<Geometry, GeoJsonProperties>;
  }

  export interface VectorTileLayer {
    readonly length: number;
    feature(index: number): VectorTileFeature;
  }

  export class VectorTile {
    constructor(protobuf: Protobuf);
    readonly layers: Record<string, VectorTileLayer>;
  }
}
