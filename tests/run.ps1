[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("unit", "etl", "api", "dags", "dag-pipeline", "integration", "external", "e2e", "martin-unit", "martin-integration", "performance", "resilience", "web-unit", "web-browser", "web-build", "compose-smoke")]
    [string]$Tier = "unit"
)

$ErrorActionPreference = "Stop"

function Invoke-Pytest {
    param([string[]]$Arguments)
    & python -m pytest @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "pytest failed for tier '$Tier' with exit code $LASTEXITCODE"
    }
}

switch ($Tier) {
    "unit" {
        Invoke-Pytest -Arguments @("tests/unit")
    }
    "etl" {
        Invoke-Pytest -Arguments @(
            "-m", "unit and not api",
            "tests/unit/census", "tests/unit/bls", "tests/unit/fred",
            "tests/unit/usda_nass", "tests/unit/shared"
        )
    }
    "api" {
        Invoke-Pytest -Arguments @("-m", "unit and api", "tests/unit/api")
    }
    "dags" {
        $env:RUN_DAG_TESTS = "1"
        try { Invoke-Pytest -Arguments @("-m", "dag", "tests/dags") }
        finally { Remove-Item Env:RUN_DAG_TESTS -ErrorAction SilentlyContinue }
    }
    "dag-pipeline" {
        $env:RUN_DAG_TESTS = "1"
        $env:RUN_INTEGRATION_TESTS = "1"
        try {
            Invoke-Pytest -Arguments @(
                "-m", "dag and integration and database",
                "tests/dags/test_dag_pipeline_execution.py"
            )
        }
        finally {
            Remove-Item Env:RUN_DAG_TESTS -ErrorAction SilentlyContinue
            Remove-Item Env:RUN_INTEGRATION_TESTS -ErrorAction SilentlyContinue
        }
    }
    "integration" {
        $env:RUN_INTEGRATION_TESTS = "1"
        try { Invoke-Pytest -Arguments @("-m", "integration and not e2e", "tests/integration") }
        finally { Remove-Item Env:RUN_INTEGRATION_TESTS -ErrorAction SilentlyContinue }
    }
    "external" {
        $env:RUN_EXTERNAL_TESTS = "1"
        $env:RUN_INTEGRATION_TESTS = "1"
        try {
            Invoke-Pytest -Arguments @(
                "-m", "external", "tests/external", "tests/integration/database/legacy"
            )
        }
        finally {
            Remove-Item Env:RUN_EXTERNAL_TESTS -ErrorAction SilentlyContinue
            Remove-Item Env:RUN_INTEGRATION_TESTS -ErrorAction SilentlyContinue
        }
    }
    "e2e" {
        $env:RUN_E2E_TESTS = "1"
        try { Invoke-Pytest -Arguments @("-m", "e2e", "tests/e2e") }
        finally { Remove-Item Env:RUN_E2E_TESTS -ErrorAction SilentlyContinue }
    }
    "martin-unit" {
        Invoke-Pytest -Arguments @("-m", "unit", "tests/unit/martin")
    }
    "martin-integration" {
        $env:RUN_INTEGRATION_TESTS = "1"
        $env:RUN_E2E_TESTS = "1"
        $env:RUN_MARTIN_TESTS = "1"
        $env:TEST_POSTGRES_HOST = "127.0.0.1"
        $env:TEST_POSTGRES_PORT = "55432"
        $env:TEST_POSTGRES_USER = "population_test"
        $env:TEST_POSTGRES_PASSWORD = "population_test"
        $env:TEST_POSTGRES_DATABASE = "population_etl_test"
        $env:TEST_REDIS_URL = "redis://127.0.0.1:56379/15"
        try {
            & docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres redis martin proxy
            if ($LASTEXITCODE -ne 0) { throw "Martin test stack failed to start" }
            Invoke-Pytest -Arguments @(
                "-m", "martin", "tests/integration/martin", "tests/e2e/test_martin_api_join.py"
            )
        }
        finally {
            & docker compose -f infra/docker/docker-compose.test.yml down --volumes --remove-orphans
            Remove-Item Env:RUN_INTEGRATION_TESTS -ErrorAction SilentlyContinue
            Remove-Item Env:RUN_E2E_TESTS -ErrorAction SilentlyContinue
            Remove-Item Env:RUN_MARTIN_TESTS -ErrorAction SilentlyContinue
            @(
                "TEST_POSTGRES_HOST", "TEST_POSTGRES_PORT", "TEST_POSTGRES_USER",
                "TEST_POSTGRES_PASSWORD", "TEST_POSTGRES_DATABASE", "TEST_REDIS_URL"
            ) | ForEach-Object { Remove-Item "Env:$_" -ErrorAction SilentlyContinue }
        }
    }
    "performance" {
        $env:RUN_PERFORMANCE_TESTS = "1"
        try { Invoke-Pytest -Arguments @("-m", "performance", "tests/performance") }
        finally { Remove-Item Env:RUN_PERFORMANCE_TESTS -ErrorAction SilentlyContinue }
    }
    "resilience" {
        $env:RUN_INTEGRATION_TESTS = "1"
        $env:RUN_E2E_TESTS = "1"
        $env:RUN_PERFORMANCE_TESTS = "1"
        try {
            Invoke-Pytest -Arguments @(
                "-m", "integration or e2e or performance", "tests/resilience",
                "tests/integration/database/test_production_resilience.py",
                "tests/integration/api/test_connection_capacity.py"
            )
        }
        finally {
            Remove-Item Env:RUN_INTEGRATION_TESTS -ErrorAction SilentlyContinue
            Remove-Item Env:RUN_E2E_TESTS -ErrorAction SilentlyContinue
            Remove-Item Env:RUN_PERFORMANCE_TESTS -ErrorAction SilentlyContinue
        }
    }
    "web-unit" {
        & npm --prefix apps/web run test:unit
        if ($LASTEXITCODE -ne 0) { throw "web unit tests failed" }
    }
    "web-browser" {
        & npm --prefix apps/web run test:browser
        if ($LASTEXITCODE -ne 0) { throw "web browser tests failed" }
    }
    "web-build" {
        & npm --prefix apps/web run lint
        if ($LASTEXITCODE -ne 0) { throw "web lint failed" }
        & npm --prefix apps/web run build
        if ($LASTEXITCODE -ne 0) { throw "web build failed" }
    }
    "compose-smoke" {
        $env:RUN_INTEGRATION_TESTS = "1"
        $env:RUN_COMPOSE_TESTS = "1"
        $env:RUN_MARTIN_TESTS = "1"
        $env:TEST_POSTGRES_HOST = "127.0.0.1"
        $env:TEST_POSTGRES_PORT = "55432"
        $env:TEST_POSTGRES_USER = "population_test"
        $env:TEST_POSTGRES_PASSWORD = "population_test"
        $env:TEST_POSTGRES_DATABASE = "population_etl_test"
        $env:TEST_REDIS_URL = "redis://127.0.0.1:56379/15"
        try {
            & docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres redis martin proxy
            if ($LASTEXITCODE -ne 0) { throw "Compose test stack failed to start" }
            Invoke-Pytest -Arguments @("-m", "integration and deployment", "tests/integration/deployment")
        }
        finally {
            & docker compose -f infra/docker/docker-compose.test.yml down --volumes --remove-orphans
            @(
                "RUN_INTEGRATION_TESTS", "RUN_COMPOSE_TESTS", "RUN_MARTIN_TESTS",
                "TEST_POSTGRES_HOST", "TEST_POSTGRES_PORT", "TEST_POSTGRES_USER",
                "TEST_POSTGRES_PASSWORD", "TEST_POSTGRES_DATABASE", "TEST_REDIS_URL"
            ) | ForEach-Object { Remove-Item "Env:$_" -ErrorAction SilentlyContinue }
        }
    }
}
