# G:\population_toolbox\test_curated_and_metadata.py

from census_acs.metadata import sync_variable_metadata_for_year
from census_acs.config import CONFIG

def update_metadata_all(dataset: str):
    year = 2022

    print(f"\n=== Updating metadata for {dataset} {year} ===")
    sync_variable_metadata_for_year(year, dataset)
    print("Done.")

if __name__ == "__main__":
    update_metadata_all("acs5")
    update_metadata_all("acs1")