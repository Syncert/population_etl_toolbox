param(
    [bool]$StartServices = $true,
    [string]$EnvFile = 'infra/docker/stack.external.env',
    [string]$ComposeFile = 'infra/docker/docker-compose.external.yml',
    [string]$PythonExecutable = 'python',
    [bool]$SkipGeoTileJoinCheck = $false
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

function Invoke-NextWebAssert {
    param([string]$Uri)

    try {
        $response = Invoke-WebRequest -Uri $Uri -Method GET -TimeoutSec 30 -UseBasicParsing
        $statusCode = [int]$response.StatusCode
        $poweredBy = [string]$response.Headers['X-Powered-By']
        if ($statusCode -lt 200 -or $statusCode -ge 300) {
            throw "HTTP $statusCode"
        }
        if ($poweredBy -notmatch 'Next\.js') {
            throw "Expected X-Powered-By: Next.js, received '$poweredBy'"
        }
        if ($response.Content -notmatch '/_next/static/') {
            throw 'Next.js static asset references were not found in the web response.'
        }

        Write-Pass -Name 'web-root-nextjs' -Detail ("HTTP $statusCode with Next.js assets ($Uri)")
        return $true
    }
    catch {
        Write-Fail -Name 'web-root-nextjs' -Detail ("$Uri -> " + $_.Exception.Message)
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
        $browserProfile = Join-Path -Path $env:TEMP -ChildPath ("population-etl-smoke-" + [guid]::NewGuid().ToString('N'))
        $browserStdout = Join-Path -Path $env:TEMP -ChildPath ("population-etl-smoke-" + [guid]::NewGuid().ToString('N') + '.html')
        $browserStderr = Join-Path -Path $env:TEMP -ChildPath ("population-etl-smoke-" + [guid]::NewGuid().ToString('N') + '.log')
        $browserArgs = @(
            '--headless=new',
            '--disable-gpu',
            '--no-first-run',
            "--user-data-dir=$browserProfile",
            '--virtual-time-budget=30000',
            '--dump-dom',
            'http://localhost:3001/'
        )
        $browserProcess = Start-Process -FilePath $edgePath -ArgumentList $browserArgs -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput $browserStdout -RedirectStandardError $browserStderr
        if ($browserProcess.ExitCode -ne 0) {
            Write-Fail -Name 'browser-map-render' -Detail ("Edge headless DOM dump failed with exit code " + $browserProcess.ExitCode)
            return $false
        }
        $domDump = Get-Content -Path $browserStdout -Raw

        if ([string]::IsNullOrWhiteSpace($domDump)) {
            Write-Fail -Name 'browser-map-render' -Detail 'Edge headless DOM dump returned no content.'
            return $false
        }

        if ($domDump -notmatch '(?is)data-testid=["'']map-canvas["'']') {
            Write-Fail -Name 'browser-map-render' -Detail 'Next.js map container marker was not found.'
            return $false
        }

        if ($domDump -notmatch '(?is)class=["''][^"'']*maplibregl-canvas[^"'']*["'']') {
            Write-Fail -Name 'browser-map-render' -Detail 'MapLibre canvas was not rendered.'
            return $false
        }

        if ($domDump -notmatch '(?is)data-testid=["'']tiles-status["''][\s\S]*?healthy_tile=true') {
            Write-Fail -Name 'browser-map-render' -Detail 'Healthy tile-layer status was not rendered.'
            return $false
        }

        if ($domDump -notmatch '(?is)data-testid=["'']observations-status["''][\s\S]*?loaded\s+[1-9][0-9]*\s+(?:county\s+)?records') {
            Write-Fail -Name 'browser-map-render' -Detail 'Loaded observation status was not rendered.'
            return $false
        }

        if ($domDump -notmatch '(?is)API distribution') {
            Write-Fail -Name 'browser-map-render' -Detail 'Distribution-backed legend was not rendered.'
            return $false
        }

        if ($domDump -notmatch '(?is)data-selected-dataset=["'']acs5["'']') {
            Write-Fail -Name 'browser-map-render' -Detail 'ACS5 was not the default county-map dataset.'
            return $false
        }

        if ($domDump -notmatch '(?is)data-selected-metric=["'']ACS:acs5:B01003_001["'']') {
            Write-Fail -Name 'browser-map-render' -Detail 'Canonical ACS5 population metric was not selected by default.'
            return $false
        }

        if ($domDump -notmatch '(?is)data-testid=["'']state-select["'']' -or $domDump -notmatch '(?is)data-testid=["'']county-select["'']') {
            Write-Fail -Name 'browser-map-render' -Detail 'State and county geography selectors were not rendered.'
            return $false
        }

        Write-Pass -Name 'browser-map-render' -Detail 'Headless browser rendered canonical ACS5 population, geography selectors, MapLibre, observations, and API bins.'
        return $true
    }
    catch {
        Write-Fail -Name 'browser-map-render' -Detail $_.Exception.Message
        return $false
    }
}

function Get-JsonItemsCount {
    param($Json)

    if ($Json -and $Json.items) {
        return @($Json.items).Count
    }
    return 0
}

function Invoke-ObservationSample {
    param([string]$MetricCode)

    $encodedMetricCode = [uri]::EscapeDataString($MetricCode)
    $uri = "http://localhost:3001/api/observations/latest?metric_code=$encodedMetricCode&geo_level=county&limit=5"
    return Invoke-JsonGet -Name "observations-latest-$MetricCode" -Uri $uri
}

function Invoke-GeoTileJoinAssert {
    if ($SkipGeoTileJoinCheck) {
        Write-Warn -Name 'geo-tile-join' -Detail 'Skipping API-to-geometry join validation by request.'
        return $true
    }

    try {
        $args = @(
            'scripts/check_mvp_geo_tile_join.py',
            '--env-file', $EnvFile,
            '--api-base-url', 'http://localhost:3001/api/',
            '--tiles-base-url', 'http://localhost:3001/tiles/',
            '--metric-code', $script:MetricCode,
            '--geo-level', 'COUNTY',
            '--layer-id', 'counties',
            '--limit', '100',
            '--minimum-join-ratio', '1.0'
        )

        & $PythonExecutable @args
        if ($LASTEXITCODE -ne 0) {
            Write-Fail -Name 'geo-tile-join' -Detail ("check_mvp_geo_tile_join.py failed with exit code $LASTEXITCODE")
            return $false
        }

        Write-Pass -Name 'geo-tile-join' -Detail 'County geometry, Martin tile metadata, and API observation joins validated.'
        return $true
    }
    catch {
        Write-Fail -Name 'geo-tile-join' -Detail $_.Exception.Message
        return $false
    }
}

try {
    $shouldStartServices = $StartServices

    if ($shouldStartServices) {
        Write-Host 'Starting external MVP services...'
        Invoke-ComposeUp
    }

    if (-not (Invoke-NextWebAssert -Uri 'http://localhost:3001/')) {
        exit 1
    }

    $healthResult = Invoke-JsonGet -Name 'api-health' -Uri 'http://localhost:3001/api/health'
    if ((-not $healthResult.Ok) -or $healthResult.Json.status -ne 'ok') {
        Write-Fail -Name 'api-health-payload' -Detail 'Expected JSON status=ok.'
        exit 1
    }

    $catalogResult = Invoke-JsonGet -Name 'catalog-metrics' -Uri 'http://localhost:3001/api/catalog/metrics?limit=5'
    if (-not $catalogResult.Ok) {
        exit 1
    }
    $catalogItemsCount = Get-JsonItemsCount -Json $catalogResult.Json
    if ($catalogItemsCount -eq 0) {
        Write-Fail -Name 'catalog-metrics-content' -Detail 'Metric catalog returned no items.'
        exit 1
    }
    Write-Pass -Name 'catalog-metrics-content' -Detail ("Metric catalog returned $catalogItemsCount sampled items.")

    $metricCode = 'population'
    $obsPrimary = Invoke-ObservationSample -MetricCode $metricCode
    $obsPrimaryCount = Get-JsonItemsCount -Json $obsPrimary.Json
    if ((-not $obsPrimary.Ok) -or $obsPrimaryCount -eq 0) {
        if ($obsPrimary.Ok -and $obsPrimaryCount -eq 0) {
            Write-Warn -Name 'observations-latest-population' -Detail 'Friendly metric_code=population returned zero county rows; trying canonical/fallback metrics.'
        }

        $catalogItems = @()
        if ($catalogResult.Json -and $catalogResult.Json.items) {
            $catalogItems = @($catalogResult.Json.items)
        }

        $candidateMetricCodes = @('ACS:acs5:B01003_001')
        if ($catalogItems.Count -gt 0) {
            $candidateMetricCodes += [string]$catalogItems[0].metric_code
        }

        $selectedFallback = $null
        foreach ($candidateMetricCode in $candidateMetricCodes) {
            if ([string]::IsNullOrWhiteSpace($candidateMetricCode)) {
                continue
            }

            $obsFallback = Invoke-ObservationSample -MetricCode $candidateMetricCode
            $obsFallbackCount = Get-JsonItemsCount -Json $obsFallback.Json
            if ($obsFallback.Ok -and $obsFallbackCount -gt 0) {
                $selectedFallback = $candidateMetricCode
                break
            }
        }

        if ([string]::IsNullOrWhiteSpace($selectedFallback)) {
            Write-Fail -Name 'observations-latest-fallback' -Detail 'No fallback metric returned county observation rows.'
            exit 1
        }

        $metricCode = $selectedFallback
        Write-Pass -Name 'observations-selected-metric' -Detail ("Using fallback metric_code=$metricCode")
    }
    else {
        Write-Pass -Name 'observations-selected-metric' -Detail ("Using metric_code=$metricCode with rows=$obsPrimaryCount")
    }
    $script:MetricCode = $metricCode

    $tileHealth = Invoke-JsonGet -Name 'tiles-health' -Uri 'http://localhost:3001/tiles/health'
    if (-not $tileHealth.Ok) {
        if (-not (Invoke-WebGet -Name 'tiles-root-fallback' -Uri 'http://localhost:3001/tiles/')) {
            exit 1
        }
    }

    if (-not (Invoke-GeoTileJoinAssert)) {
        exit 1
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
