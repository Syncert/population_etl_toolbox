from db_connection import PostgresConnectionFactory
from sqlalchemy import create_engine, text

def main():
    try:
        # Use env vars with prefix POSTGRES_ and override the DB if you want
        details = PostgresConnectionFactory.auto(
            conn_id=None,          # no Airflow in local dev
            prefix="POSTGRES_",    # matches your from_env expectations
            database="public_data",  # or None to use POSTGRES_DB
        )

        print("Connection details:")
        print(f"  host: {details.host}")
        print(f"  port: {details.port}")
        print(f"  user: {details.user}")
        print(f"  db:   {details.database}")

        # Build SQLAlchemy engine
        engine = create_engine(details.sqlalchemy_url())

        # Smoke test: connect and run two simple queries
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version();")).scalar()
            db_name = conn.execute(text("SELECT current_database();")).scalar()

        print("\n✅ Connection successful!")
        print(f"Postgres version: {version}")
        print(f"Connected to DB: {db_name}")

    except Exception as e:
        print("\n❌ Connection failed")
        print(type(e).__name__)
        print(e)


if __name__ == "__main__":
    main()