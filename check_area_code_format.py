import psycopg2
from utility.db_connection import PostgresConnectionFactory

conn = psycopg2.connect(**PostgresConnectionFactory.auto(prefix='POSTGRES_', database='public_data').psycopg_kwargs())
cur = conn.cursor()

# Check area code lengths
cur.execute("""
    SELECT DISTINCT LENGTH(area_code) as len, COUNT(*)
    FROM raw_bls.bls_series 
    WHERE program = 'la'
    GROUP BY LENGTH(area_code)
""")
print("Area code lengths in database:")
for row in cur.fetchall():
    print(f"  Length {row[0]}: {row[1]} series")

# Check series ID format
cur.execute("""
    SELECT series_id, seasonal, area_code, measure
    FROM raw_bls.bls_series 
    WHERE program = 'la'
    LIMIT 5
""")
print("\nSample series structure:")
for row in cur.fetchall():
    print(f"  Series: {row[0]}, S={row[1]}, Area={row[2]} (len={len(row[2])}), Measure={row[3]}")
    print(f"    Format check: LA{row[1]}{row[2]}{row[3]} = {len('LA' + str(row[1]) + str(row[2]) + str(row[3]))} chars")

conn.close()
