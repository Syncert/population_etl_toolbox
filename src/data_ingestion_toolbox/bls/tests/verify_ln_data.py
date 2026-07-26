import psycopg2
from data_ingestion_toolbox.utility.db_connection import PostgresConnectionFactory

conn = psycopg2.connect(**PostgresConnectionFactory.auto(prefix='POSTGRES_', database='public_data').psycopg_kwargs())
cur = conn.cursor()

# Check LN series ingested
cur.execute("""
    SELECT series_id, COUNT(*) as obs_count, MIN(year), MAX(year)
    FROM raw_bls.bls_long 
    WHERE program = 'ln' AND year BETWEEN 2022 AND 2023 
    GROUP BY series_id 
    ORDER BY series_id
""")

rows = cur.fetchall()
print('\nCPS/LN Series Ingested (2022-2023):')
print('=' * 70)
for row in rows:
    print(f'{row[0]}: {row[1]} observations ({row[2]}-{row[3]})')

print(f'\nTotal: {sum(r[1] for r in rows)} observations across {len(rows)} series')

# Show sample data for LNS14000000
print('\n' + '=' * 70)
print('Sample Data: LNS14000000 (National Unemployment Rate)')
print('=' * 70)
cur.execute("""
    SELECT year, period, value 
    FROM raw_bls.bls_long 
    WHERE series_id = 'LNS14000000' 
    ORDER BY year DESC, period DESC 
    LIMIT 24
""")
for row in cur.fetchall():
    print(f'{row[0]}-{row[1]}: {row[2]}%')

conn.close()
