param(
    [ValidateSet('internal', 'external')]
    [string]$Mode = 'internal',

    [ValidateSet('init', 'up', 'smoke', 'down', 'all')]
    [string]$Action = 'all',

    [string]$EnvFile,

    [switch]$UseHostEnv,

    [switch]$WithLocalAirflow
)

$ErrorActionPreference = 'Stop'

function Write-Log {
    param([string]$Message)
    Write-Host "[deploy:$Mode/$Action] $Message"
}

function Get-DefaultEnvFile {
    if ($Mode -eq 'external') {
        return 'infra/docker/stack.external.env'
    }

    return 'infra/docker/stack.env'
}

function Get-ExampleEnvFile {
    if ($Mode -eq 'external') {
        return 'infra/docker/stack.external.env.example'
    }

    return 'infra/docker/stack.env.example'
}

function Get-ComposeFile {
    if ($Mode -eq 'external') {
        return 'infra/docker/docker-compose.external.yml'
    }

    return 'infra/docker/docker-compose.yml'
}

function Get-ExternalServiceSet {
    return @('redis', 'api', 'martin', 'web')
}

function Resolve-ComposeContext {
    $composeFile = Get-ComposeFile
    $effectiveEnvFile = if ([string]::IsNullOrWhiteSpace($EnvFile)) { Get-DefaultEnvFile } else { $EnvFile }

    if (-not $UseHostEnv) {
        if (-not (Test-Path -Path $effectiveEnvFile)) {
            $exampleEnvFile = Get-ExampleEnvFile
            throw "Missing env file '$effectiveEnvFile'. Copy '$exampleEnvFile' to '$effectiveEnvFile' and fill required secrets, or rerun with -UseHostEnv."
        }
    }

    return @{
        ComposeFile = $composeFile
        EnvFile = $effectiveEnvFile
    }
}

function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$ComposeArgs)

    $composeContext = Resolve-ComposeContext
    $composeCliArgs = @('-f', $composeContext.ComposeFile)

    if (-not $UseHostEnv) {
        $composeCliArgs = @('--env-file', $composeContext.EnvFile) + $composeCliArgs
    }

    $composeCliArgs += $ComposeArgs
    Write-Log ("docker compose " + ($composeCliArgs -join ' '))
    & docker compose @composeCliArgs
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Init {
    if ($Mode -eq 'external' -and -not $WithLocalAirflow) {
        Write-Log 'External service-only init: starting redis/api/martin/web'
        $services = Get-ExternalServiceSet
        Invoke-Compose up '-d' @services
        return
    }

    Write-Log 'Running airflow-init'
    Invoke-Compose up airflow-init
}

function Invoke-Up {
    if ($Mode -eq 'external' -and -not $WithLocalAirflow) {
        Write-Log 'Starting external service-only stack in detached mode'
        $services = Get-ExternalServiceSet
        Invoke-Compose up '-d' @services
        return
    }

    Write-Log 'Starting stack in detached mode'
    Invoke-Compose up '-d'
}

function Invoke-Smoke {
    Write-Log 'Checking API health endpoint'
    try {
        $resp = Invoke-WebRequest -Uri 'http://localhost:8000/health' -Method GET -TimeoutSec 20
        Write-Log ("Health check status: " + [int]$resp.StatusCode)
    }
    catch {
        Write-Log ("Health check failed: " + $_.Exception.Message)
        throw 'API health check failed'
    }

    if ($Mode -eq 'external' -and -not $WithLocalAirflow) {
        Write-Log 'Checking Martin health endpoint'
        try {
            $martinResp = Invoke-WebRequest -Uri 'http://localhost:3000/health' -Method GET -TimeoutSec 20
            Write-Log ("Martin health status: " + [int]$martinResp.StatusCode)
        }
        catch {
            Write-Log 'Martin /health unavailable, trying root endpoint'
            try {
                $martinRootResp = Invoke-WebRequest -Uri 'http://localhost:3000/' -Method GET -TimeoutSec 20
                Write-Log ("Martin root status: " + [int]$martinRootResp.StatusCode)
            }
            catch {
                Write-Log ("Martin check failed: " + $_.Exception.Message)
                throw 'Martin health/root check failed'
            }
        }

        return
    }

    Write-Log 'Listing Airflow DAGs from airflow-webserver'
    Invoke-Compose exec airflow-webserver airflow dags list
}

function Invoke-Down {
    Write-Log 'Stopping stack'
    Invoke-Compose down
}

try {
    switch ($Action) {
        'init' { Invoke-Init }
        'up' { Invoke-Up }
        'smoke' { Invoke-Smoke }
        'down' { Invoke-Down }
        'all' {
            Invoke-Init
            Invoke-Up
            Invoke-Smoke
        }
    }

    Write-Log 'Completed successfully'
    exit 0
}
catch {
    Write-Error $_
    exit 1
}
