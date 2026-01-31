import psycopg2
from utility.db_connection import PostgresConnectionFactory

conn = psycopg2.connect(**PostgresConnectionFactory.auto(prefix='POSTGRES_', database='public_data').psycopg_kwargs())
cur = conn.cursor()

# Check for US-level area codes (should start with 00)
cur.execute("""
    SELECT DISTINCT area_code 
    FROM raw_bls.bls_series 
    WHERE program = 'la' AND area_code LIKE '00%'
    ORDER BY area_code 
    LIMIT 20
""")
print("Area codes starting with 00:")
for row in cur.fetchall():
    print(f"  {row[0]}")

# Check what patterns exist
cur.execute("""
    SELECT LEFT(area_code, 2) as prefix, COUNT(*) 
    FROM raw_bls.bls_series 
    WHERE program = 'la' 
    GROUP BY LEFT(area_code, 2)
    ORDER BY prefix
""")
print("\nArea code prefixes:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} series")

conn.close()
