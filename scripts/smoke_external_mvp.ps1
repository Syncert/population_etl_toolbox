param(
    [bool]$StartServices = $true,
    [string]$EnvFile = 'infra/docker/stack.external.env',
    [string]$ComposeFile = 'infra/docker/docker-compose.external.yml'
)

$ErrorActionPreference = 'Stop'

function Write-Pass {
    param([string]$Name, [string]$Detail)
    Write-Host ("PASS {0}: {1}" -f $Name, $Detail)
}

function Write-Fail {
    param([string]$Name, [string]$Detail)
    Write-Host ("FAIL {0}: {1}" -f $Name, $Detail)
}

function Write-Warn {
    param([string]$Name, [string]$Detail)
    Write-Host ("WARN {0}: {1}" -f $Name, $Detail)
}

function Invoke-ComposeUp {
    if (-not (Test-Path -Path $EnvFile)) {
        throw "Missing env file '$EnvFile'. Copy infra/docker/stack.external.env.example and fill required values."
    }

    $composeParameters = @('--env-file', $EnvFile, '-f', $ComposeFile, 'up', '-d', 'redis', 'api', 'martin', 'web')
    & docker compose @composeParameters
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose up failed with exit code $LASTEXITCODE"
    }
}

function Invoke-JsonGet {
    param([string]$Name, [string]$Uri)

    try {
        $response = Invoke-WebRequest -Uri $Uri -Method GET -TimeoutSec 30 -UseBasicParsing
        $statusCode = [int]$response.StatusCode
        if ($statusCode -lt 200 -or $statusCode -ge 300) {
            throw "HTTP $statusCode"
        }

        $json = $null
        try {
            $json = $response.Content | ConvertFrom-Json -ErrorAction Stop
        }
        catch {
            $json = $null
        }

        Write-Pass -Name $Name -Detail ("HTTP $statusCode ($Uri)")
        return @{
            Ok = $true
            StatusCode = $statusCode
            Json = $json
            Error = ''
        }
    }
    catch {
        $message = $_.Exception.Message
        Write-Fail -Name $Name -Detail ("$Uri -> $message")
        return @{
            Ok = $false
            StatusCode = 0
            Json = $null
            Error = $message
        }
    }
}

function Invoke-WebGet {
    param([string]$Name, [string]$Uri)

    try {
        $response = Invoke-WebRequest -Uri $Uri -Method GET -TimeoutSec 30 -UseBasicParsing
        $statusCode = [int]$response.StatusCode
        if ($statusCode -lt 200 -or $statusCode -ge 300) {
            throw "HTTP $statusCode"
        }

        Write-Pass -Name $Name -Detail ("HTTP $statusCode ($Uri)")
        return $true
    }
    catch {
        Write-Fail -Name $Name -Detail ("$Uri -> " + $_.Exception.Message)
        return $false
    }
}

function Invoke-BrowserMapAssert {
    $edgeCandidates = @()
    if (${env:ProgramFiles(x86)} -and -not [string]::IsNullOrWhiteSpace(${env:ProgramFiles(x86)})) {
        $edgeCandidates += Join-Path -Path ${env:ProgramFiles(x86)} -ChildPath 'Microsoft\Edge\Application\msedge.exe'
    }
    if ($env:ProgramFiles -and -not [string]::IsNullOrWhiteSpace($env:ProgramFiles)) {
        $edgeCandidates += Join-Path -Path $env:ProgramFiles -ChildPath 'Microsoft\Edge\Application\msedge.exe'
    }

    $edgePath = $edgeCandidates | Where-Object { Test-Path -Path $_ } | Select-Object -First 1
    if (-not $edgePath) {
        Write-Warn -Name 'browser-map-render' -Detail 'Microsoft Edge not found on host; skipping browser-level map assertion.'
        return $true
    }

    try {
        $domDump = & $edgePath '--headless' '--disable-gpu' '--virtual-time-budget=15000' '--dump-dom' 'http://localhost:3001/' 2>&1 | Out-String
        if ($LASTEXITCODE -ne 0) {
            Write-Fail -Name 'browser-map-render' -Detail ("Edge headless DOM dump failed with exit code $LASTEXITCODE")
            return $false
        }

        if ([string]::IsNullOrWhiteSpace($domDump)) {
            Write-Fail -Name 'browser-map-render' -Detail 'Edge headless DOM dump returned no content.'
            return $false
        }

        $mapStatePattern = '(?is)<[^>]*id=["'']mapState["''][^>]*>[^<]*(PASS|ok)\b'
        if ($domDump -notmatch $mapStatePattern) {
            Write-Fail -Name 'browser-map-render' -Detail 'Map status assertion failed: mapState PASS/ok marker not found.'
            return $false
        }

        $renderHintPattern = '(?is)(Map\s*tile\s*layer\s*rendered|Rendered\s*source-layer)'
        if ($domDump -notmatch $renderHintPattern) {
            Write-Fail -Name 'browser-map-render' -Detail 'Map render hint assertion failed: rendered tile-layer hint not found.'
            return $false
        }

        Write-Pass -Name 'browser-map-render' -Detail 'Headless Edge DOM shows mapState PASS/ok and rendered tile-layer hint.'
        return $true
    }
    catch {
        Write-Fail -Name 'browser-map-render' -Detail $_.Exception.Message
        return $false
    }
}

try {
    $shouldStartServices = $StartServices

    if ($shouldStartServices) {
        Write-Host 'Starting external MVP services...'
        Invoke-ComposeUp
    }

    if (-not (Invoke-WebGet -Name 'web-root' -Uri 'http://localhost:3001/')) {
        exit 1
    }

    if (-not (Invoke-JsonGet -Name 'api-health' -Uri 'http://localhost:3001/api/health').Ok) {
        exit 1
    }

    $catalogResult = Invoke-JsonGet -Name 'catalog-metrics' -Uri 'http://localhost:3001/api/catalog/metrics?limit=5'
    if (-not $catalogResult.Ok) {
        exit 1
    }

    $metricCode = 'population'
    $obsPrimary = Invoke-JsonGet -Name 'observations-latest-population' -Uri 'http://localhost:3001/api/observations/latest?metric_code=population&geo_level=county&limit=5'
    if (-not $obsPrimary.Ok) {
        $catalogItems = @()
        if ($catalogResult.Json -and $catalogResult.Json.items) {
            $catalogItems = @($catalogResult.Json.items)
        }

        $firstMetricCode = $null
        if ($catalogItems.Count -gt 0) {
            $firstMetricCode = [string]$catalogItems[0].metric_code
        }

        if ([string]::IsNullOrWhiteSpace($firstMetricCode)) {
            Write-Fail -Name 'observations-latest-fallback' -Detail 'Population metric failed and no fallback metric available from catalog.'
            exit 1
        }

        $metricCode = $firstMetricCode
        $fallbackUri = "http://localhost:3001/api/observations/latest?metric_code=$metricCode&geo_level=county&limit=5"
        $obsFallback = Invoke-JsonGet -Name 'observations-latest-fallback' -Uri $fallbackUri
        if (-not $obsFallback.Ok) {
            exit 1
        }
        Write-Pass -Name 'observations-selected-metric' -Detail ("Using fallback metric_code=$metricCode")
    }
    else {
        Write-Pass -Name 'observations-selected-metric' -Detail ("Using metric_code=$metricCode")
    }

    $tileHealth = Invoke-JsonGet -Name 'tiles-health' -Uri 'http://localhost:3001/tiles/health'
    if (-not $tileHealth.Ok) {
        if (-not (Invoke-WebGet -Name 'tiles-root-fallback' -Uri 'http://localhost:3001/tiles/')) {
            exit 1
        }
    }

    if (-not (Invoke-BrowserMapAssert)) {
        exit 1
    }

    Write-Host 'PASS smoke_external_mvp complete'
    exit 0
}
catch {
    Write-Fail -Name 'smoke-external-mvp' -Detail $_.Exception.Message
    exit 1
}
