# G:\population_toolbox\test_ingest_wi.py

from Census.include.ingest import ingest_slice

def main():
    year = 2022
    dataset = "acs5"

    # Tiny smoke test – US level
    rows_us = ingest_slice(year=year, dataset=dataset, geo_level="us")
    print(f"Ingested {rows_us} rows for {dataset} {year} us")

    # WI State
    rows_wi = ingest_slice(
        year=year,
        dataset=dataset,
        geo_level="state",
        state_fips="55",  # Wisconsin
    )
    print(f"Ingested {rows_wi} rows for {dataset} {year} WI state")

    # Realistic test – all WI counties
    rows_wi = ingest_slice(
        year=year,
        dataset=dataset,
        geo_level="county",
        state_fips="55",  # Wisconsin
    )
    print(f"Ingested {rows_wi} rows for {dataset} {year} WI counties")

if __name__ == "__main__":
    main()
