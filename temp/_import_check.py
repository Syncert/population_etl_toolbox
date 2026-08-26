import importlib

mods = [
    "data_ingestion_toolbox",
    "data_ingestion_toolbox.cdc",
    "data_ingestion_toolbox.cdc.config",
    "data_ingestion_toolbox.cdc.client",
    "data_ingestion_toolbox.cdc.capture",
    "data_ingestion_toolbox.cdc.metadata",
    "data_ingestion_toolbox.cdc.registry",
    "data_ingestion_toolbox.cdc.schemas",
    "data_ingestion_toolbox.cdc.fixtures",
    "data_ingestion_toolbox.cdc.silver_cdc",
    "data_ingestion_toolbox.cdc.gold_cdc",
]
for m in mods:
    try:
        importlib.import_module(m)
        print("OK   ", m)
    except Exception as e:
        print("FAIL ", m, "->", type(e).__name__, str(e)[:130])

try:
    import airflow  # noqa

    print("airflow", airflow.__version__)
except Exception as e:
    print("airflow NOT importable:", type(e).__name__, str(e)[:100])
