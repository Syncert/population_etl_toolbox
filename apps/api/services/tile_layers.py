"""The tile layers a map can draw, and the vintages each holds (tiles-places-and-metros).

The layers themselves are declared here once for the API, beside the Martin
configuration that serves them (`infra/martin/martin.yml`); a unit test holds
the two to the same ids, zooms and kind. What varies with the warehouse --
which boundary vintages the place and metro layers hold -- is read from
`gold.tile_layer_vintage`, so a map never offers a vintage that would draw
nothing, and a versioned layer with no boundary loaded says so.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from apps.api.schemas import TileLayer, TileLayerCatalogResponse, TileLayerVintage
from data_ingestion_toolbox.sql.catalog_queries import TILE_LAYER_VINTAGES_QUERY

#: The rule every map drawn from these layers follows. Features carry no
#: values, so colour comes only from a published value joined on `geo_id`.
UNCOLOURED_RULE = (
    "A geography with no published value is left uncoloured, never drawn as "
    "zero. Tiles carry boundaries only; a map colours a geography by joining "
    "its published value on geo_id, and says how many it left uncoloured."
)


@dataclass(frozen=True)
class DeclaredTileLayer:
    layer_id: str
    grains: tuple[str, ...]
    versioned: bool
    minzoom: int
    maxzoom: int
    name_field: str

    @property
    def tile_template(self) -> str:
        return f"/tiles/{self.layer_id}/{{z}}/{{x}}/{{y}}"

    def vintage_template(self, vintage: int) -> str:
        return f"{self.tile_template}?vintage={vintage}"


#: Every layer Martin publishes. `counties` and `tracts` are table sources
#: drawing current boundaries; `places` and `metros` are function sources
#: drawing the vintage a request names.
TILE_LAYERS: tuple[DeclaredTileLayer, ...] = (
    DeclaredTileLayer("counties", ("STATE", "COUNTY"), False, 0, 12, "county_name"),
    DeclaredTileLayer("tracts", ("TRACT",), False, 6, 14, "area_name"),
    DeclaredTileLayer("places", ("PLACE",), True, 6, 14, "place_name"),
    DeclaredTileLayer("metros", ("METRO",), True, 0, 12, "metro_name"),
)


def list_tile_layers(db: Session) -> TileLayerCatalogResponse:
    held: dict[str, list[dict]] = {}
    for row in db.execute(TILE_LAYER_VINTAGES_QUERY).mappings().all():
        held.setdefault(str(row["geo_level"]).upper(), []).append(dict(row))

    layers: list[TileLayer] = []
    for declared in TILE_LAYERS:
        vintages: list[TileLayerVintage] = []
        if declared.versioned:
            for grain in declared.grains:
                for row in held.get(grain, []):
                    vintage = int(row["boundary_vintage"])
                    vintages.append(
                        TileLayerVintage(
                            boundary_vintage=vintage,
                            feature_count=int(row["feature_count"]),
                            is_default=bool(row["is_default"]),
                            tile_template=declared.vintage_template(vintage),
                        )
                    )
        default = next((v.boundary_vintage for v in vintages if v.is_default), None)
        layers.append(
            TileLayer(
                layer_id=declared.layer_id,
                source_layer=declared.layer_id,
                grains=list(declared.grains),
                versioned=declared.versioned,
                minzoom=declared.minzoom,
                maxzoom=declared.maxzoom,
                join_key="geo_id",
                name_field=declared.name_field,
                tile_template=(
                    declared.vintage_template(default)
                    if declared.versioned and default is not None
                    else declared.tile_template
                ),
                vintages=vintages,
                default_vintage=default,
                available=(not declared.versioned) or bool(vintages),
            )
        )
    return TileLayerCatalogResponse(layers=layers, uncoloured_rule=UNCOLOURED_RULE)
