import logging
from bls.ingest import ingest_slice, expand_laus_series_ids, get_curated_series_for_program

logging.basicConfig(level=logging.INFO)

# Test what series IDs are generated
measure_codes = get_curated_series_for_program("la")
print(f"Measure codes for LAUS: {measure_codes}")

series_ids = expand_laus_series_ids(
    measure_codes=measure_codes,
    geo_level="us",
    state_fips=None,
    seasonal="U"
)
print(f"\nGenerated {len(series_ids)} series IDs for US level:")
for sid in series_ids[:5]:
    print(f"  {sid} (length: {len(sid)})")

# Try to ingest
print("\n=== Attempting ingestion ===")
try:
    rows = ingest_slice(
        program="la",
        start_year=2023,
        end_year=2023,
        geo_level="us"
    )
    print(f"Ingested {rows} rows")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
