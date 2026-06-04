# API Contract (First Pass)

Base routes:

- `GET /health`
- `GET /api/catalog/sources`
- `GET /api/catalog/metrics`
- `GET /api/catalog/geographies`
- `GET /api/observations/latest?metric_id=population&geo_level=county`
- `GET /api/observations/timeseries?metric_id=population&geo_id=55025`
- `GET /api/distribution/bins?metric_id=population&geo_level=county&method=quantile`
- `GET /api/comparison?metric_a=population&metric_b=unemployment_rate&geo_level=county`

The API prefers DB-backed responses and falls back to explicit placeholder/mock records when views are not yet populated.
