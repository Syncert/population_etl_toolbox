import importlib
import warnings

warnings.filterwarnings("ignore")

mods = [
    "data_ingestion_toolbox",
    "data_ingestion_toolbox.cdc",
    "data_ingestion_toolbox.cdc.config",
    "data_ingestion_toolbox.cdc.registry",
    "data_ingestion_toolbox.cdc.schemas",
    "data_ingestion_toolbox.cdc.fixtures",
    "data_ingestion_toolbox.cdc.metadata",
    "data_ingestion_toolbox.cdc.client",
    "data_ingestion_toolbox.cdc.capture",
    "data_ingestion_toolbox.cdc.silver_cdc",
    "data_ingestion_toolbox.cdc.gold_cdc",
    "data_ingestion_toolbox.cdc.silver_cdc.transform",
    "data_ingestion_toolbox.cdc.gold_cdc.publisher",
    "data_ingestion_toolbox.utility.gold_schema",
    "data_ingestion_toolbox.normalization",
    "data_ingestion_toolbox.glossary",
]

# symbols the DAG and tests actually import
symbols = {
    "data_ingestion_toolbox.cdc.config": ["CONFIG", "CDCConfig"],
    "data_ingestion_toolbox.cdc.registry": ["CDCRegistry", "CDC_REGISTRY_ENTRIES"],
    "data_ingestion_toolbox.cdc.schemas": ["CDCSchema"],
    "data_ingestion_toolbox.cdc.fixtures": ["CDCFixtures"],
    "data_ingestion_toolbox.cdc.metadata": [
        "sync_cdc_dataset_table",
        "sync_variable_metadata_for_year",
    ],
    "data_ingestion_toolbox.cdc.client": ["make_request"],
    "data_ingestion_toolbox.cdc.capture": ["commit_capture"],
    "data_ingestion_toolbox.cdc.silver_cdc.transform": ["transform_cdc_to_silver"],
    "data_ingestion_toolbox.cdc.gold_cdc.publisher": [
        "publish_glossary",
        "publish_state",
        "ensure_cdc_gold_schema",
        "refresh_cdc_elements",
    ],
    "data_ingestion_toolbox.utility.gold_schema": [
        "ServingRefreshChunkConfig",
        "refresh_serving_layer_in_year_chunks",
    ],
}

for m in mods:
    try:
        mod = importlib.import_module(m)
        f = getattr(mod, "__file__", None)
        print(f"OK    {m}  file={f}")
        for sym in symbols.get(m, []):
            if hasattr(mod, sym):
                print(f"      .has {sym}")
            else:
                print(f"      .MISSING {sym}")
    except Exception as e:
        print(f"FAIL  {m}  {type(e).__name__}: {e}")
