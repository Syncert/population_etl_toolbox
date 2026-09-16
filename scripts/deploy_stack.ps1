param(
    [ValidateSet('internal', 'external')]
    [string]$Mode = 'internal',

    [ValidateSet('init', 'up', 'down', 'all')]
    [string]$Action = 'all',

    [string]$EnvFile,

    [switch]$UseHostEnv,

    [switch]$WithLocalAirflow,

    # Escape hatch for the metadata/warehouse isolation guard. Named for what
    # it permits rather than -Force, so it cannot be reached for casually to
    # get past an error whose whole point is that the target is production.
    [switch]$AllowAirflowMetadataInWarehouse
)

# This script is one of two entrypoints over one decision module. Every rule it
# used to carry in PowerShell -- env-file resolution, compose file and service
# selection per mode, database-target derivation, and the refusal to run
# airflow-init against the warehouse -- now lives in `tools/deployment.py`,
# where it is unit-tested and where the POSIX entrypoint reads it too. A second
# copy of those rules here would be a second place for them to drift, and this
# guard is the one that stands between `airflow db migrate` and production.
#
# The interface is unchanged: the same parameters, the same messages, the same
# exit codes. It now requires Python 3.11, which this repository already
# requires of an operator for `provision_app_api.py` and the test suites.

$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot

function Write-Log {
    param([string]$Message)
    Write-Host "[deploy:$Mode/$Action] $Message"
}

function Resolve-PythonCommand {
    foreach ($candidate in @('python', 'python3', 'py')) {
        $resolved = Get-Command -Name $candidate -ErrorAction SilentlyContinue
        if ($resolved) {
            return $candidate
        }
    }

    throw "No Python interpreter found on PATH. The deployment decisions live in tools/deployment.py; install Python 3.11 or run scripts/deploy_stack.py directly."
}

# The warehouse data-quality assessment stamps evidence with the deployed code
# commit; default it from the checked-out revision when the host does not set
# it, so the value is exported into the compose invocation below.
if ([string]::IsNullOrWhiteSpace($env:DATA_QUALITY_COMMIT_SHA)) {
    $resolvedSha = (& git rev-parse HEAD 2>$null)
    if ($LASTEXITCODE -eq 0 -and $resolvedSha) {
        $env:DATA_QUALITY_COMMIT_SHA = $resolvedSha.Trim()
    }
}

function Get-DeploymentPlan {
    $python = Resolve-PythonCommand
    $planArgs = @(
        (Join-Path $repositoryRoot 'scripts/deploy_stack.py'),
        '--mode', $Mode,
        '--action', $Action,
        '--emit-plan',
        # Ask for this script's own flag spellings, so a refusal suggests
        # -WithLocalAirflow rather than advice a PowerShell caller cannot take.
        '--powershell-flags'
    )

    if (-not [string]::IsNullOrWhiteSpace($EnvFile)) {
        $planArgs += @('--env-file', $EnvFile)
    }
    if ($UseHostEnv) { $planArgs += '--use-host-env' }
    if ($WithLocalAirflow) { $planArgs += '--with-local-airflow' }
    if ($AllowAirflowMetadataInWarehouse) { $planArgs += '--allow-airflow-metadata-in-warehouse' }

    $output = & $python @planArgs
    if (-not $output) {
        throw "tools/deployment.py produced no plan"
    }

    return ($output -join [Environment]::NewLine | ConvertFrom-Json)
}

function Invoke-ComposeStep {
    param([string[]]$ComposeArgs)

    Write-Log ("docker compose " + ($ComposeArgs -join ' '))
    & docker compose @ComposeArgs
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

try {
    $plan = Get-DeploymentPlan

    if (-not [string]::IsNullOrWhiteSpace($plan.error)) {
        throw $plan.error
    }

    switch ($plan.guard.status) {
        'refused' { throw $plan.guard.message }
        'bypassed' { Write-Log $plan.guard.message }
        'skipped' { Write-Log $plan.guard.message }
    }

    foreach ($step in $plan.steps) {
        Write-Log $step.description
        Invoke-ComposeStep -ComposeArgs $step.arguments
    }

    Write-Log 'Completed successfully'
    exit 0
}
catch {
    Write-Error $_
    exit 1
}
