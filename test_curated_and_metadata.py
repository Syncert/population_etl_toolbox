# G:\population_toolbox\test_curated_and_metadata.py

from Census.include.metadata import sync_variable_metadata_for_year
from Census.include.config import CONFIG

def main():
    print("Curated tables from CONFIG.curated_tables:")
    print(CONFIG.curated_tables)

    year = 2022
    dataset = "acs5"

    print(f"\nSyncing variable metadata for {dataset} {year} ...")
    sync_variable_metadata_for_year(year, dataset)
    print("Done.")

if __name__ == "__main__":
    main()
