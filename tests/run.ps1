[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("unit", "etl", "api", "dags", "dag-pipeline", "integration", "external", "e2e", "linux", "martin-unit", "martin-integration", "performance", "resilience", "web-unit", "web-browser", "web-smoke", "web-maps", "web-build", "compose-smoke")]
    [string]$Tier = "unit",

    # Extra pytest arguments for the "linux" tier, e.g.
    #   .\tests\run.ps1 linux -m "integration and database" tests/integration/database
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArgs = @()
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
            "tests/unit/cdc",
            "tests/unit/fbi_ucr", "tests/unit/usda_nass", "tests/unit/shared"
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
    "linux" {
        # Airflow refuses to initialize outside a POSIX-compliant OS, so the
        # "dags" and "dag-pipeline" tiers above cannot run on a Windows host:
        # every module importing airflow dies at collection. This tier runs
        # pytest inside the pinned Airflow 2.9.3 + Python 3.11 image against
        # the disposable PostGIS service, which is what CI grades.
        #
        # Sources are mounted read-only, so an edit needs no rebuild:
        # Compose builds the image on first use and reuses it after. After a
        # dependency change, rebuild with:
        #   docker compose -f infra/docker/docker-compose.test.yml -f infra/docker/docker-compose.pytest.yml build pytest
        #
        # Defaults to the DAG tier; pass pytest arguments to run anything else.
        $compose = @(
            "compose",
            "-f", "infra/docker/docker-compose.test.yml",
            "-f", "infra/docker/docker-compose.pytest.yml"
        )
        try {
            & docker @compose run --rm pytest @PytestArgs
            if ($LASTEXITCODE -ne 0) {
                throw "pytest failed for tier 'linux' with exit code $LASTEXITCODE"
            }
        }
        finally {
            # CI grades these suites in separate jobs against separate
            # databases; leaving one suite's residue behind reports failures
            # CI will not reproduce.
            #
            # Windows PowerShell wraps a native command's stderr in an
            # ErrorRecord, so Compose's ordinary teardown progress would
            # terminate here and replace the pytest verdict this tier exists
            # to report. Teardown reports through its own exit code instead.
            $ErrorActionPreference = "Continue"
            & docker @compose down --volumes --remove-orphans
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "Compose teardown exited $LASTEXITCODE; check for leftover population-testing containers."
            }
        }
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
    "web-maps" {
        # The explorer map checks against a running full stack (WEB-118): the
        # data-path sweep over every source's maps, then the painted-pixel
        # check in Chromium with software WebGL. Both need the web app as well
        # as /api/v1 and /tiles on one origin, which the composed web-smoke
        # stack does not serve, so this points at a stack already up --
        # `http://localhost:3001` for the local development stack.
        if (-not $env:SMOKE_BASE_URL) { $env:SMOKE_BASE_URL = "http://localhost:3001" }
        $env:SMOKE_REQUIRED = "1"
        & npm --prefix apps/web run test:smoke -- ../../tests/frontend/smoke/map-display.smoke.test.js
        if ($LASTEXITCODE -ne 0) { throw "map-display sweep failed" }
        & npm --prefix apps/web run test:maps
        if ($LASTEXITCODE -ne 0) { throw "map paint check failed" }
    }
    "web-smoke" {
        # The live-stack frontend tier, composed exactly as CI composes it:
        # the real API over the disposable warehouse, Martin, and the nginx
        # proxy that serves /api/v1 and /tiles through the same rewrites a
        # browser uses.
        #
        # The origin is part of the tier, not a convenience. Node 24's bundled
        # undici asserts (`assert(!this.paused)`) when a large response
        # arrives over a socket the origin closes, so running this against
        # `next dev`'s rewrite origin -- which answers `connection: close` on
        # every response -- exits the tier non-zero on whole-world tiles no
        # matter how the client reads them, with every test green. The
        # composed proxy answers keep-alive, and the tier's own unhandled-error
        # test reports the difference rather than leaving it to the exit code.
        $env:SMOKE_BASE_URL = "http://127.0.0.1:33001"
        # Refuse to pass by skipping: this tier is required wherever it runs.
        $env:SMOKE_REQUIRED = "1"
        # The same two bounds `.github/workflows/frontend-smoke.yml` sets. This
        # block claims to compose the tier exactly as CI composes it, and for a
        # while it did not: the workflow graded the stack against every
        # registered source and against drift, and this runner did not, so
        # reproducing a CI failure locally meant knowing to set them by hand.
        # The seed publishes one current measure per source, so both are free
        # here -- which is the point. A violation can only mean the seed, the
        # rule, or the report broke.
        $env:SMOKE_REQUIRE_ALL_SOURCES = "1"
        $env:SMOKE_REQUIRE_FRESH_SOURCES = "1"
        $compose = @(
            "compose",
            "-f", "infra/docker/docker-compose.test.yml",
            "-f", "infra/docker/docker-compose.smoke.yml"
        )
        try {
            # `--build` is not optional here. The API image this tier grades is
            # built from the working tree, and Compose reuses an existing image
            # by name: on 2026-09-12 a four-day-old `population-etl-api:smoke`
            # answered every ACS observation request with the pre-ARC-005
            # identity, and two WEB-027 tests failed against code that had been
            # correct for days. A tier that can grade a stale image is not a
            # live-stack tier.
            & docker @compose up --detach --wait --build postgres martin api proxy
            if ($LASTEXITCODE -ne 0) { throw "Smoke stack failed to start" }
            & npm --prefix apps/web run test:smoke
            if ($LASTEXITCODE -ne 0) { throw "web smoke tests failed" }
        }
        finally {
            # Windows PowerShell wraps a native command's stderr in an
            # ErrorRecord, so Compose's ordinary teardown progress would
            # terminate here and replace the verdict this tier reports.
            $ErrorActionPreference = "Continue"
            & docker @compose down --volumes --remove-orphans --timeout 15
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "Compose teardown exited $LASTEXITCODE; check for leftover population-testing containers."
            }
            @(
                "SMOKE_BASE_URL", "SMOKE_REQUIRED",
                "SMOKE_REQUIRE_ALL_SOURCES", "SMOKE_REQUIRE_FRESH_SOURCES"
            ) | ForEach-Object { Remove-Item "Env:$_" -ErrorAction SilentlyContinue }
        }
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
