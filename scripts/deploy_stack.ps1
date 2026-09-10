param(
    [ValidateSet('internal', 'external')]
    [string]$Mode = 'internal',

    [ValidateSet('init', 'up', 'down', 'all')]
    [string]$Action = 'all',

    [string]$EnvFile,

    [switch]$UseHostEnv,

    [switch]$WithLocalAirflow,

    # Escape hatch for the metadata/warehouse isolation guard below. Named for
    # what it permits rather than -Force, so it cannot be reached for casually
    # to get past an error whose whole point is that the target is production.
    [switch]$AllowAirflowMetadataInWarehouse
)

$ErrorActionPreference = 'Stop'

# The warehouse data-quality assessment stamps evidence with the deployed code
# commit; default it from the checked-out revision when the host does not set it.
if ([string]::IsNullOrWhiteSpace($env:DATA_QUALITY_COMMIT_SHA)) {
    $resolvedSha = (& git rev-parse HEAD 2>$null)
    if ($LASTEXITCODE -eq 0 -and $resolvedSha) {
        $env:DATA_QUALITY_COMMIT_SHA = $resolvedSha.Trim()
    }
}

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

# The guard below reads the same values compose will interpolate, so it has to
# read them the way compose does: the host environment wins over --env-file,
# and an unset key falls back to the ${VAR:-default} written into the compose
# file for this mode. Grading a value the stack will not actually use is worse
# than not grading at all -- it would pass a run that then migrates production.
function Read-EnvFile {
    param([string]$Path)

    $values = @{}
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -Path $Path)) {
        return $values
    }

    foreach ($line in Get-Content -Path $Path) {
        $trimmed = $line.Trim()
        if ($trimmed -eq '' -or $trimmed.StartsWith('#')) {
            continue
        }

        $separator = $trimmed.IndexOf('=')
        if ($separator -lt 1) {
            continue
        }

        $key = $trimmed.Substring(0, $separator).Trim()
        $value = $trimmed.Substring($separator + 1).Trim()
        if ($value.Length -ge 2) {
            $quoted = ($value.StartsWith('"') -and $value.EndsWith('"')) -or
                      ($value.StartsWith("'") -and $value.EndsWith("'"))
            if ($quoted) {
                $value = $value.Substring(1, $value.Length - 2)
            }
        }

        $values[$key] = $value
    }

    return $values
}

function Get-ComposeDefaults {
    # Only the ${VAR:-default} fallbacks the compose file for this mode
    # actually declares. External mode declares none for these keys on
    # purpose: it targets infrastructure this repository does not own, so an
    # invented default would be a guess about someone else's deployment.
    if ($Mode -eq 'external') {
        return @{}
    }

    return @{
        ANALYTICS_DB_HOST        = 'analytics_postgres'
        ANALYTICS_DB_PORT        = '5432'
        ANALYTICS_DB_NAME        = 'population_etl'
        AIRFLOW_METADATA_DB_HOST = 'service_postgres'
        AIRFLOW_METADATA_DB_PORT = '5432'
        AIRFLOW_METADATA_DB_NAME = 'airflow'
    }
}

function Resolve-EnvValue {
    param(
        [hashtable]$FileValues,
        [hashtable]$Defaults,
        [string[]]$Names
    )

    # $Names is a fallback chain, mirroring a nested compose expression such as
    # ${PUBLIC_DATA_DB_HOST:-${ANALYTICS_DB_HOST:-analytics_postgres}}.
    foreach ($name in $Names) {
        $hostValue = [Environment]::GetEnvironmentVariable($name)
        if (-not [string]::IsNullOrWhiteSpace($hostValue)) {
            return $hostValue
        }

        if ($FileValues.ContainsKey($name) -and -not [string]::IsNullOrWhiteSpace($FileValues[$name])) {
            return $FileValues[$name]
        }

        if ($Defaults.ContainsKey($name) -and -not [string]::IsNullOrWhiteSpace($Defaults[$name])) {
            return $Defaults[$name]
        }
    }

    return ''
}

function Get-DatabaseTarget {
    param(
        [string]$Label,
        [hashtable]$FileValues,
        [hashtable]$Defaults,
        [string[]]$HostNames,
        [string[]]$PortNames,
        [string[]]$NameNames
    )

    return [pscustomobject]@{
        Label    = $Label
        HostName = Resolve-EnvValue -FileValues $FileValues -Defaults $Defaults -Names $HostNames
        Port     = Resolve-EnvValue -FileValues $FileValues -Defaults $Defaults -Names $PortNames
        Name     = Resolve-EnvValue -FileValues $FileValues -Defaults $Defaults -Names $NameNames
    }
}

function Format-DatabaseTarget {
    param([pscustomobject]$Target)

    $port = if ([string]::IsNullOrWhiteSpace($Target.Port)) { '5432' } else { $Target.Port }
    return "$($Target.HostName):$port/$($Target.Name)"
}

function Test-SameDatabase {
    param(
        [pscustomobject]$Left,
        [pscustomobject]$Right
    )

    # A blank port means the compose default, so compare the effective value.
    $leftPort = if ([string]::IsNullOrWhiteSpace($Left.Port)) { '5432' } else { $Left.Port }
    $rightPort = if ([string]::IsNullOrWhiteSpace($Right.Port)) { '5432' } else { $Right.Port }

    # Host is compared as written: this cannot resolve DNS, so it will not
    # catch localhost spelled two ways. It is a guard against the documented
    # collision, not proof of isolation.
    return ($Left.HostName -ieq $Right.HostName) -and
           ($leftPort -eq $rightPort) -and
           ($Left.Name -ieq $Right.Name)
}

function Assert-AirflowMetadataIsolated {
    # airflow-init runs "airflow db migrate", which creates Airflow's metadata
    # schema in whatever database AIRFLOW_METADATA_DB_* names, and then resets
    # the public_data connection and every API pool. Aimed at the warehouse,
    # that is a one-way schema write into production data by an admin-capable
    # role. stack.external.env is exactly that shape today: metadata and
    # warehouse both read <host>/public_data.
    if ($AllowAirflowMetadataInWarehouse) {
        Write-Log 'Metadata isolation guard bypassed by -AllowAirflowMetadataInWarehouse'
        return
    }

    $envValues = @{}
    $envFileLabel = 'the host environment'
    if (-not $UseHostEnv) {
        $composeContext = Resolve-ComposeContext
        $envFileLabel = $composeContext.EnvFile
        $envValues = Read-EnvFile -Path $composeContext.EnvFile
    }

    $defaults = Get-ComposeDefaults

    $metadata = Get-DatabaseTarget -Label 'AIRFLOW_METADATA_DB_*' `
        -FileValues $envValues -Defaults $defaults `
        -HostNames @('AIRFLOW_METADATA_DB_HOST') `
        -PortNames @('AIRFLOW_METADATA_DB_PORT') `
        -NameNames @('AIRFLOW_METADATA_DB_NAME')

    if ([string]::IsNullOrWhiteSpace($metadata.HostName) -or [string]::IsNullOrWhiteSpace($metadata.Name)) {
        # Compose reports an unresolved required variable far better than a
        # half-informed guard can; let it.
        Write-Log 'Metadata isolation guard skipped: AIRFLOW_METADATA_DB_* is not fully resolved'
        return
    }

    $warehouses = @(
        (Get-DatabaseTarget -Label 'ANALYTICS_DB_* (API and Martin warehouse)' `
            -FileValues $envValues -Defaults $defaults `
            -HostNames @('ANALYTICS_DB_HOST') `
            -PortNames @('ANALYTICS_DB_PORT') `
            -NameNames @('ANALYTICS_DB_NAME')),
        (Get-DatabaseTarget -Label 'PUBLIC_DATA_DB_* (the public_data Airflow connection)' `
            -FileValues $envValues -Defaults $defaults `
            -HostNames @('PUBLIC_DATA_DB_HOST', 'ANALYTICS_DB_HOST') `
            -PortNames @('PUBLIC_DATA_DB_PORT', 'ANALYTICS_DB_PORT') `
            -NameNames @('PUBLIC_DATA_DB_NAME', 'ANALYTICS_DB_NAME'))
    )

    foreach ($warehouse in $warehouses) {
        if ([string]::IsNullOrWhiteSpace($warehouse.HostName) -or [string]::IsNullOrWhiteSpace($warehouse.Name)) {
            continue
        }

        if (-not (Test-SameDatabase -Left $metadata -Right $warehouse)) {
            continue
        }

        $message = @(
            "Refusing to run airflow-init: the Airflow metadata database and the warehouse are the same database.",
            "",
            "  metadata  ($($metadata.Label)): $(Format-DatabaseTarget -Target $metadata)",
            "  warehouse ($($warehouse.Label)): $(Format-DatabaseTarget -Target $warehouse)",
            "",
            "airflow-init would run 'airflow db migrate' against that database, creating Airflow's",
            "metadata schema inside the warehouse, then delete and recreate the public_data",
            "connection and reset every API pool to this repository's defaults.",
            "",
            "Fix one of:",
            "  - point AIRFLOW_METADATA_DB_NAME at a database of its own in '$envFileLabel'",
            "  - drop -WithLocalAirflow to start only redis/api/martin/web against existing Airflow",
            "  - pass -AllowAirflowMetadataInWarehouse if this really is intended"
        ) -join [Environment]::NewLine

        throw $message
    }
}

function Invoke-Init {
    if ($Mode -eq 'external' -and -not $WithLocalAirflow) {
        Write-Log 'External service-only init: starting redis/api/martin/web'
        $services = Get-ExternalServiceSet
        Invoke-Compose up '-d' @services
        return
    }

    Assert-AirflowMetadataIsolated
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

    # Internal compose leaves airflow-init in the default profile, so a bare
    # `up -d` runs it too; the guard belongs here as much as on init.
    Assert-AirflowMetadataIsolated
    Write-Log 'Starting stack in detached mode'
    Invoke-Compose up '-d'
}

function Invoke-Down {
    Write-Log 'Stopping stack'
    Invoke-Compose down
}

try {
    switch ($Action) {
        'init' { Invoke-Init }
        'up' { Invoke-Up }
        'down' { Invoke-Down }
        'all' {
            Invoke-Init
            Invoke-Up
        }
    }

    Write-Log 'Completed successfully'
    exit 0
}
catch {
    Write-Error $_
    exit 1
}
