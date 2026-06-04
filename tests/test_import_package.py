def test_import_population_package() -> None:
    import population_etl_toolbox

    assert population_etl_toolbox.__name__ == "population_etl_toolbox"
