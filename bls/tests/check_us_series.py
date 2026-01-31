import psycopg2
from utility.db_connection import PostgresConnectionFactory

conn = psycopg2.connect(**PostgresConnectionFactory.auto(prefix='POSTGRES_', database='public_data').psycopg_kwargs())
cur = conn.cursor()

# Check for possible national level series  
cur.execute("""
    SELECT series_id, area_code, area_text
    FROM raw_bls.bls_series 
    WHERE program = 'la' 
    AND (area_text ILIKE '%united states%' OR area_text ILIKE '%u.s.%' OR area_text ILIKE '%national%')
    LIMIT 10
""")
print("Possible national level series:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} - {row[2]}")

# Check sample ST series
cur.execute("""
    SELECT series_id, area_code, area_text
    FROM raw_bls.bls_series 
    WHERE program = 'la' AND area_code LIKE 'ST%'
    LIMIT 10
""")
print("\nSample ST series:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} - {row[2]}")

conn.close()
