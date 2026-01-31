import psycopg2
from utility.db_connection import PostgresConnectionFactory

conn = psycopg2.connect(**PostgresConnectionFactory.auto(prefix='POSTGRES_', database='public_data').psycopg_kwargs())
cur = conn.cursor()

# Check area code lengths and patterns
cur.execute("""
    SELECT LENGTH(area_code) as len, LEFT(area_code, 2) as prefix, COUNT(*) 
    FROM raw_bls.bls_series 
    WHERE program = 'la'
    GROUP BY LENGTH(area_code), LEFT(area_code, 2)
    ORDER BY len, prefix
""")
print("Area code lengths and prefixes:")
for row in cur.fetchall():
    print(f"  Length {row[0]}, Prefix {row[1]}: {row[2]} series")

# Check for 15-character area codes that might be US level
cur.execute("""
    SELECT DISTINCT area_code 
    FROM raw_bls.bls_series 
    WHERE program = 'la' AND LENGTH(area_code) = 15
    ORDER BY area_code
    LIMIT 10
""")
print("\n15-character area codes:")
for row in cur.fetchall():
    print(f"  {row[0]}")

conn.close()
