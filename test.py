from Census.include.metadata import sync_acs_dataset_table

def main():
    sync_acs_dataset_table()
    print("Synced acs_datasets")

if __name__ == "__main__":
    main()
