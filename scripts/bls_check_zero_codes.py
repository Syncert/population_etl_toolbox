"""Operational diagnostic for zero-filled BLS codes."""

import psycopg2
from data_ingestion_toolbox.utility.db_connection import PostgresConnectionFactory

conn = psycopg2.connect(
    **PostgresConnectionFactory.auto(
        prefix="POSTGRES_", database="public_data"
    ).psycopg_kwargs()
)
cur = conn.cursor()

# Check for series with area codes containing mostly zeros
cur.execute("""
    SELECT series_id, area_code
    FROM raw_bls.bls_series 
    WHERE program = 'la' AND area_code LIKE '%000000000000%'
    LIMIT 10
""")
print("Series with many zeros in area code:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

# Let's look at the actual series ID patterns - maybe US level doesn't exist in metadata
# Check first few series IDs
cur.execute("""
    SELECT series_id, seasonal, measure, area_code
    FROM raw_bls.bls_series 
    WHERE program = 'la'
    ORDER BY series_id
    LIMIT 20
""")
print("\nFirst 20 LAUS series (sorted):")
for row in cur.fetchall():
    print(f"  {row[0]}: S={row[1]}, M={row[2]}, A={row[3]}")

conn.close()
