[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("unit", "etl", "api", "dags", "integration", "external", "e2e", "performance")]
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
            "tests/unit/census", "tests/unit/bls", "tests/unit/fred", "tests/unit/shared"
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
    "integration" {
        $env:RUN_INTEGRATION_TESTS = "1"
        try { Invoke-Pytest -Arguments @("-m", "integration and not e2e", "tests/integration") }
        finally { Remove-Item Env:RUN_INTEGRATION_TESTS -ErrorAction SilentlyContinue }
    }
    "external" {
        $env:RUN_EXTERNAL_TESTS = "1"
        try { Invoke-Pytest -Arguments @("-m", "external", "tests/external") }
        finally { Remove-Item Env:RUN_EXTERNAL_TESTS -ErrorAction SilentlyContinue }
    }
    "e2e" {
        $env:RUN_E2E_TESTS = "1"
        try { Invoke-Pytest -Arguments @("-m", "e2e", "tests/e2e") }
        finally { Remove-Item Env:RUN_E2E_TESTS -ErrorAction SilentlyContinue }
    }
    "performance" {
        $env:RUN_PERFORMANCE_TESTS = "1"
        try { Invoke-Pytest -Arguments @("-m", "performance", "tests/performance") }
        finally { Remove-Item Env:RUN_PERFORMANCE_TESTS -ErrorAction SilentlyContinue }
    }
}
