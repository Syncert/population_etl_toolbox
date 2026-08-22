-- GEO-005 beta cutover: shared reference geography is the sole owner.
DROP TABLE IF EXISTS raw_census.geo_dim;
COMMENT ON VIEW silver_ref.dim_geo IS
    'Read-only compatibility projection over versioned shared geography.';
COMMENT ON TABLE silver_ref.dim_geo_entity IS
    'Stable geography identities; attributes and boundaries live in version tables.';
