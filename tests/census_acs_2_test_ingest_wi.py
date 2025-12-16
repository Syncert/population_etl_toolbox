from census_acs.ingest import ingest_slice

def run_for(dataset: str):
    year = 2022

    print(f"\n=== Ingesting {dataset} {year} US/WI ===")

    rows_us = ingest_slice(year=year, dataset=dataset, geo_level="us")
    print(f"Ingested {rows_us} rows for {dataset} {year} us")

    rows_wi_state = ingest_slice(
        year=year,
        dataset=dataset,
        geo_level="state",
        state_fips="55"
    )
    print(f"Ingested {rows_wi_state} rows for {dataset} {year} state level")

    rows_wi_county = ingest_slice(
        year=year,
        dataset=dataset,
        geo_level="county",
        state_fips="55",
    )
    print(f"Ingested {rows_wi_county} rows for {dataset} {year} WI counties")


def main():
    run_for("acs5")
    run_for("acs1")


if __name__ == "__main__":
    main()