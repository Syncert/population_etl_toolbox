<#
.SYNOPSIS
    Dispatch the plan backlog under docs/plans/ across parallel background
    Claude Code workers.

.DESCRIPTION
    The dispatcher is deliberately split in two. Every scheduling decision --
    plan discovery, metadata validation, dependency resolution, and selection --
    belongs to tools/plan_dispatcher, a Python package covered by the
    repository's normal pytest and Ruff gates. This script owns only process
    orchestration: the integration branch, one Git worktree and feature branch
    per plan, background Claude sessions, verification, and integration.

    Keeping the outer loop in PowerShell rather than in another model matters:
    concurrency, dependency order, retry ceilings, and termination stay
    deterministic and inspectable. Claude Code also refuses to launch a nested
    Claude Code session, so the fleet cannot be supervised from inside a Claude
    conversation.

    Each worker receives a completion-driven /goal prompt rather than a
    time-driven /loop, and that prompt carries an explicit stand-down clause so
    a worker that cannot progress documents a blocker instead of burning turns.

.PARAMETER Action
    run      Start or resume a dispatcher run and drive it to completion.
    plan     Show what the next tick would dispatch, then exit.
    status   Render the current run summary, then exit.
    stop     Stop every background session this run owns.
    clean    Remove the worktrees this run created.
    approve  Record human approval of the review gate named by -Gate.
    reject   Record human rejection of the review gate named by -Gate.
    reopen   Clear a recorded decision and reopen the gate named by -Gate.

.PARAMETER MaxConcurrency
    Maximum simultaneous plan workers. Parallel background agents multiply
    subscription usage roughly in proportion to their count, and integration
    cost rises faster than throughput, so the default is deliberately small.

.PARAMETER BaseBranch
    Branch the integration branch is cut from. Defaults to the branch currently
    checked out, because that is the branch holding the backlog the operator is
    looking at. Naming a branch whose docs/plans/ carries no dispatchable
    frontmatter fails the run rather than quietly finishing with nothing done.

.PARAMETER DryRun
    Print every Git and Claude command instead of executing it. The Python
    planner still runs, so a dry run exercises the real dependency graph.

.EXAMPLE
    ./tools/Invoke-ClaudePlans.ps1 -Action plan -DryRun

.EXAMPLE
    ./tools/Invoke-ClaudePlans.ps1 -Action run -MaxConcurrency 3

.EXAMPLE
    ./tools/Invoke-ClaudePlans.ps1 -Action approve -Gate three-source-review `
        -By 'syncert' -Note 'reviewed all three source diffs'
#>
[CmdletBinding()]
param(
    [ValidateSet('run', 'plan', 'status', 'stop', 'clean',
        'approve', 'reject', 'reopen')]
    [string]$Action = 'plan',

    [string]$Gate,

    [string]$By,

    [string]$Note,

    [ValidateRange(1, 8)]
    [int]$MaxConcurrency = 3,

    [string]$RunId,

    [string]$IntegrationBranch,

    [string]$BaseBranch,

    [string]$PlansRoot = 'docs/plans',

    [string]$StatePath = '.claude/plan-runner-state.json',

    [string]$WorktreeRoot = '.worktrees',

    [ValidateSet('auto', 'default', 'acceptEdits', 'bypassPermissions')]
    [string]$PermissionMode = 'auto',

    [ValidateRange(1, 10)]
    [int]$MaxAttempts = 2,

    [ValidateRange(5, 3600)]
    [int]$PollSeconds = 30,

    [switch]$DryRun,

    [switch]$Force
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$script:RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$script:PythonExecutable = $null

# The integration branch gets its own checkout. Merging in the operator's main
# working tree would land plan branches on whatever they happen to have checked
# out -- usually the base branch this design promises never to touch -- and
# would fight them for the working tree while the fleet runs.
$script:IntegrationWorktree = Join-Path $WorktreeRoot '_integration'
$script:PlansRootArgument = $PlansRoot

if ($DryRun) {
    # A dry run must not leave run state behind in the repository, so it plans
    # against a throwaway state file instead of the real one.
    $StatePath = Join-Path ([System.IO.Path]::GetTempPath()) (
        'plan-runner-dryrun-{0}.json' -f ([System.Guid]::NewGuid().ToString('N'))
    )
}

function Write-Log {
    param(
        [Parameter(Mandatory)][string]$Message,
        [ValidateSet('info', 'warn', 'error', 'ok')][string]$Level = 'info'
    )

    $colour = switch ($Level) {
        'warn' { 'Yellow' }
        'error' { 'Red' }
        'ok' { 'Green' }
        default { 'Cyan' }
    }
    Write-Host "[plans] $Message" -ForegroundColor $colour
}

function Get-PythonExecutable {
    if ($script:PythonExecutable) {
        return $script:PythonExecutable
    }

    foreach ($candidate in @('python', 'python3')) {
        $command = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($command) {
            $script:PythonExecutable = $command.Source
            return $script:PythonExecutable
        }
    }

    throw 'No python interpreter found on PATH; the plan dispatcher needs one.'
}

function Get-StateFullPath {
    <#
        .SYNOPSIS
            Resolve the run-state file, accepting repo-relative or absolute paths.
    #>
    if ([System.IO.Path]::IsPathRooted($StatePath)) {
        return $StatePath
    }
    return Join-Path $script:RepositoryRoot $StatePath
}

function Invoke-Planner {
    <#
        .SYNOPSIS
            Call the Python planner and return its parsed JSON result.
    #>
    param(
        [Parameter(Mandatory)][string[]]$PlannerArgs,
        [switch]$Raw
    )

    $arguments = @(
        '-m', 'tools.plan_dispatcher',
        '--plans-root', $script:PlansRootArgument,
        '--state-path', $StatePath
    ) + $PlannerArgs

    Push-Location $script:RepositoryRoot
    try {
        $output = & (Get-PythonExecutable) @arguments
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    if ($exitCode -ne 0) {
        throw "plan_dispatcher $($PlannerArgs -join ' ') failed with exit code $exitCode."
    }

    $text = ($output | Out-String)
    if ($Raw) {
        return $text
    }
    return $text | ConvertFrom-Json
}

function Invoke-Native {
    <#
        .SYNOPSIS
            Run an external command, honouring -DryRun and failing loudly.
    #>
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][string[]]$Arguments,
        [string]$WorkingDirectory = $script:RepositoryRoot,
        [switch]$AllowFailure
    )

    $rendered = "$FilePath $($Arguments -join ' ')"
    if ($DryRun) {
        Write-Log "DRYRUN $rendered"
        return [pscustomobject]@{ ExitCode = 0; Output = ''; DryRun = $true }
    }

    # Windows PowerShell turns a native command's stderr into NativeCommandError
    # records, and the script-scoped 'Stop' preference would make those
    # terminating -- escaping -AllowFailure entirely. Git routes ordinary
    # progress ('Preparing worktree ...') to stderr, so the first worktree add
    # would abort the whole run. Shadow the preference for the duration of the
    # call and judge success by the exit code, which is the only reliable
    # signal a native command gives.
    $ErrorActionPreference = 'Continue'

    Write-Verbose "exec: $rendered"
    Push-Location $WorkingDirectory
    try {
        $raw = & $FilePath @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    $output = (@($raw) | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.ToString() }
            else { $_ }
        }) -join [Environment]::NewLine

    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "Command failed ($exitCode): $rendered`n$output"
    }
    return [pscustomobject]@{
        ExitCode = $exitCode
        Output   = $output
        DryRun   = $false
    }
}

function Get-ShellInvocation {
    <#
        .SYNOPSIS
            Return the shell that runs a plan's verification command.
        .DESCRIPTION
            Verification commands are ordinary shell strings from plan
            frontmatter, and this repository's plans invoke tests/run.ps1.
            Running them in the PowerShell host that is already executing this
            dispatcher keeps one command string working on an operator's
            Windows machine and on a Linux runner alike; hard-coding bash would
            break every .ps1 runner, and hard-coding a name would miss Windows
            PowerShell.
    #>
    param([Parameter(Mandatory)][string]$Command)

    $host_ = (Get-Process -Id $PID).Path
    if (-not $host_) {
        $host_ = if ($IsWindows) { 'powershell' } else { 'pwsh' }
    }
    return @{
        FilePath  = $host_
        Arguments = @('-NoProfile', '-NonInteractive', '-Command', $Command)
    }
}

function Invoke-Git {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [string]$WorkingDirectory = $script:RepositoryRoot,
        [switch]$AllowFailure
    )

    return Invoke-Native -FilePath 'git' -Arguments $Arguments `
        -WorkingDirectory $WorkingDirectory -AllowFailure:$AllowFailure
}

function Test-GitRefExists {
    param([Parameter(Mandatory)][string]$Ref)

    if ($DryRun) { return $false }
    $result = Invoke-Git -Arguments @('rev-parse', '--verify', '--quiet', $Ref) -AllowFailure
    return $result.ExitCode -eq 0
}

function Assert-CleanWorkingTree {
    if ($DryRun -or $Force) { return }

    $result = Invoke-Git -Arguments @('status', '--porcelain')
    if (-not [string]::IsNullOrWhiteSpace($result.Output)) {
        throw ('The working tree has uncommitted changes. Commit or stash them ' +
            'first, or rerun with -Force to dispatch anyway.')
    }
}

function Resolve-BaseBranch {
    <#
        .SYNOPSIS
            Return the branch the integration branch is cut from.
        .DESCRIPTION
            The default is the checked-out branch rather than a hard-coded
            'main'. A backlog under active development lives on a feature
            branch, and cutting the run from a branch that does not carry that
            backlog produces an empty inventory and a run that does nothing.
    #>
    if ($BaseBranch) { return $BaseBranch }

    # Asked even under -DryRun: this is a read-only query, and a dry run that
    # reported a different base than the real run would be worthless.
    $ErrorActionPreference = 'Continue'
    Push-Location $script:RepositoryRoot
    try {
        $raw = & git rev-parse --abbrev-ref HEAD 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    $current = (@($raw) | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.ToString() }
            else { $_ }
        }) -join '' | ForEach-Object { $_.Trim() }
    if ($exitCode -ne 0 -or -not $current -or $current -eq 'HEAD') {
        throw ('Could not determine the current branch to use as the run base. ' +
            'Pass -BaseBranch <branch> explicitly.')
    }
    Write-Log "Base branch defaulted to the checked-out branch '$current'."
    return $current
}

function Assert-DispatchableInventory {
    <#
        .SYNOPSIS
            Fail loudly when the run would have no work to do.
        .DESCRIPTION
            The planner reads its backlog from the integration worktree. If that
            branch predates the plan workflow folders or the dispatch
            frontmatter, every plan parses as guidance and the inventory is
            empty -- which the scheduler correctly reports as 'nothing left to
            do'. That is indistinguishable from success, so it is caught here
            instead, where the cause can be named.
    #>
    param([Parameter(Mandatory)][string]$BaseBranchName)

    if ($DryRun) { return }

    $inventory = Invoke-Planner -PlannerArgs @('inventory')
    if (@($inventory.plans).Count -gt 0) { return }

    throw ("No dispatchable plans found under '$($inventory.plans_root)'. The " +
        "integration branch was cut from '$BaseBranchName', which does not " +
        'carry the plan workflow folders or their dispatch frontmatter. Pass ' +
        '-BaseBranch <branch> naming the branch that holds the backlog.')
}

function Resolve-RunIdentity {
    <#
        .SYNOPSIS
            Reuse an existing run's identity, or mint one for a new run.
    #>
    $statePath = Get-StateFullPath
    if (Test-Path -Path $statePath) {
        $existing = Get-Content -Path $statePath -Raw | ConvertFrom-Json
        return [pscustomobject]@{
            RunId             = $existing.run_id
            IntegrationBranch = $existing.integration_branch
            IsNew             = $false
        }
    }

    $stamp = Get-Date -Format 'yyyy-MM-dd-HHmmss'
    $resolvedRunId = if ($RunId) { $RunId } else { $stamp }
    $resolvedBranch = if ($IntegrationBranch) {
        $IntegrationBranch
    }
    else {
        "automation/plan-run-$resolvedRunId"
    }

    return [pscustomobject]@{
        RunId             = $resolvedRunId
        IntegrationBranch = $resolvedBranch
        IsNew             = $true
    }
}

function Initialize-Run {
    <#
        .SYNOPSIS
            Create run state and the integration branch that isolates the fleet.
        .DESCRIPTION
            Workers never touch the base branch. Every feature branch is cut
            from, and merged back into, a single integration branch, so an
            unattended run that goes wrong is discarded by deleting one branch.
    #>
    $identity = Resolve-RunIdentity
    if (-not $identity.IsNew) {
        Write-Log "Resuming run $($identity.RunId) on $($identity.IntegrationBranch)."
        Initialize-IntegrationWorktree -IntegrationBranch $identity.IntegrationBranch
        Assert-DispatchableInventory -BaseBranchName $identity.IntegrationBranch
        return $identity
    }

    $base = Resolve-BaseBranch
    Write-Log "Starting run $($identity.RunId) on $($identity.IntegrationBranch)."
    if (-not (Test-GitRefExists -Ref $identity.IntegrationBranch)) {
        Invoke-Git -Arguments @(
            'branch', $identity.IntegrationBranch, $base
        ) | Out-Null
    }
    Initialize-IntegrationWorktree -IntegrationBranch $identity.IntegrationBranch
    Assert-DispatchableInventory -BaseBranchName $base

    Invoke-Planner -PlannerArgs @(
        'init-run',
        '--run-id', $identity.RunId,
        '--integration-branch', $identity.IntegrationBranch,
        '--max-concurrency', "$MaxConcurrency"
    ) | Out-Null
    return $identity
}

function Initialize-IntegrationWorktree {
    <#
        .SYNOPSIS
            Check the integration branch out into its own worktree.
        .DESCRIPTION
            Once it exists, the planner reads the backlog from it, so a plan
            already merged into this run reads as satisfied and is never
            dispatched twice.

            The path is fixed, so a worktree left behind by an earlier run sits
            exactly where this one expects its own. Reusing it unchecked would
            read the previous run's backlog and, worse, send every merge to the
            previous run's integration branch. An existing checkout is therefore
            required to be on this run's branch, or the run stops.
    #>
    param([Parameter(Mandatory)][string]$IntegrationBranch)

    $absolutePath = Join-Path $script:RepositoryRoot $script:IntegrationWorktree
    if (Test-Path -Path $absolutePath) {
        $checkedOut = Get-CheckedOutBranch -WorktreeFullPath $absolutePath
        if ($checkedOut -ne $IntegrationBranch) {
            throw ("The integration worktree at '$($script:IntegrationWorktree)' " +
                "is on branch '$checkedOut', but this run integrates into " +
                "'$IntegrationBranch'. It belongs to an earlier run: finish or " +
                'discard that run, then remove it with ' +
                "'git worktree remove $($script:IntegrationWorktree)'.")
        }
    }
    else {
        Invoke-Git -Arguments @(
            'worktree', 'add', $script:IntegrationWorktree, $IntegrationBranch
        ) | Out-Null
    }
    if (Test-Path -Path (Join-Path $absolutePath $PlansRoot)) {
        $script:PlansRootArgument = Join-Path $absolutePath $PlansRoot
    }
}

function Get-CheckedOutBranch {
    <#
        .SYNOPSIS
            Return the branch name checked out in a worktree, or '' if detached.
    #>
    param([Parameter(Mandatory)][string]$WorktreeFullPath)

    $result = Invoke-Native -FilePath 'git' `
        -Arguments @('rev-parse', '--abbrev-ref', 'HEAD') `
        -WorkingDirectory $WorktreeFullPath -AllowFailure
    if ($result.ExitCode -ne 0) { return '' }
    return $result.Output.Trim()
}

function New-PlanWorktree {
    <#
        .SYNOPSIS
            Create an isolated checkout and feature branch for one plan.
        .DESCRIPTION
            Simultaneous workers must not share a working tree. The branch is
            created explicitly rather than through 'claude --worktree' so the
            run controls branch naming and the integration base.
    #>
    param(
        [Parameter(Mandatory)][pscustomobject]$Plan,
        [Parameter(Mandatory)][string]$IntegrationBranch
    )

    $relativePath = Join-Path $WorktreeRoot $Plan.id
    $absolutePath = Join-Path $script:RepositoryRoot $relativePath

    if (Test-Path -Path $absolutePath) {
        Write-Log "Reusing existing worktree for $($Plan.id)."
        return $relativePath
    }

    $arguments = if (Test-GitRefExists -Ref $Plan.branch) {
        @('worktree', 'add', $relativePath, $Plan.branch)
    }
    else {
        @('worktree', 'add', '-b', $Plan.branch, $relativePath, $IntegrationBranch)
    }

    Invoke-Git -Arguments $arguments | Out-Null
    return $relativePath
}

function Start-PlanWorker {
    <#
        .SYNOPSIS
            Launch one background Claude session for a plan.
        .DESCRIPTION
            'claude --bg' returns immediately and leaves the session under a
            local supervisor, so the fleet needs no terminal windows. Inspect it
            with 'claude agents', 'claude attach <id>', or 'claude stop <id>'.

            The session id is resolved from 'claude agents --json --cwd <tree>'
            rather than scraped from the launch output. Each plan owns its own
            worktree, so that listing identifies the worker exactly, and the id
            it returns is the same string the liveness poll later matches on.
    #>
    param(
        [Parameter(Mandatory)][pscustomobject]$Plan,
        [Parameter(Mandatory)][string]$WorktreePath
    )

    $prompt = Invoke-Planner -PlannerArgs @(
        'prompt', '--plan-id', $Plan.id, '--raw', '--display-root', $PlansRoot
    ) -Raw

    $arguments = @('--bg', '--permission-mode', $PermissionMode, $prompt)
    $workingDirectory = Join-Path $script:RepositoryRoot $WorktreePath
    if ($DryRun) {
        Write-Log "DRYRUN claude --bg (in $WorktreePath) for $($Plan.id)"
        return "dryrun-$($Plan.id)"
    }

    $result = Invoke-Native -FilePath 'claude' -Arguments $arguments `
        -WorkingDirectory $workingDirectory

    $session = Resolve-WorkerSession -WorktreeFullPath $workingDirectory
    if ($session) { return $session }

    # Fall back to the launch output only if the listing did not name the
    # session; an unresolvable id is reported rather than guessed at.
    $match = [regex]::Match(
        $result.Output, '(?<id>[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})'
    )
    if ($match.Success) { return $match.Groups['id'].Value }

    throw ("Launched a worker for $($Plan.id) but could not determine its " +
        "session id. Check 'claude agents --json' and stop any orphan.")
}

function Resolve-WorkerSession {
    <#
        .SYNOPSIS
            Return the background session id running in one worktree.
    #>
    param([Parameter(Mandatory)][string]$WorktreeFullPath)

    $result = Invoke-Native -FilePath 'claude' `
        -Arguments @('agents', '--json', '--cwd', $WorktreeFullPath) -AllowFailure
    if ($result.ExitCode -ne 0) { return $null }

    try { $sessions = $result.Output | ConvertFrom-Json }
    catch { return $null }

    $identified = @(@($sessions) |
        Where-Object { $_ -and $_.PSObject.Properties.Name -contains 'sessionId' })
    if ($identified.Count -eq 0) { return $null }
    return $identified[-1].sessionId
}

function Get-ActiveSessionIds {
    <#
        .SYNOPSIS
            Return the session ids Claude Code currently considers active.
        .DESCRIPTION
            'claude agents' without --json is an interactive TUI: it refuses to
            run when stdout is captured, which is exactly how this dispatcher
            invokes it. Only '--json' is a scripting contract, and it lists
            active sessions only -- completed background sessions appear solely
            under '--all'. Presence in that listing is therefore the liveness
            signal, and no prose parsing is involved.
        .OUTPUTS
            A string array of session ids, or $null when the listing could not
            be read. $null means 'do not know', which is not the same as 'none'.
    #>
    $result = Invoke-Native -FilePath 'claude' -Arguments @('agents', '--json') `
        -AllowFailure
    if ($result.ExitCode -ne 0) {
        Write-Log "Could not list background agents (exit $($result.ExitCode))." -Level warn
        return $null
    }

    try {
        $sessions = $result.Output | ConvertFrom-Json
    }
    catch {
        Write-Log "Background agent listing was not valid JSON." -Level warn
        return $null
    }

    return @(@($sessions) |
        Where-Object { $_ -and $_.PSObject.Properties.Name -contains 'sessionId' } |
        ForEach-Object { $_.sessionId })
}

function Get-WorkerState {
    <#
        .SYNOPSIS
            Report whether a background session is still working.
        .DESCRIPTION
            An unreadable listing reports 'unknown', and the caller leaves the
            worker alone rather than reaping it. Falsely declaring a live worker
            finished destroys its run; waiting a tick longer costs only time.
        .OUTPUTS
            'running', 'finished', or 'unknown'.
    #>
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string]$SessionId,
        [string[]]$ActiveSessionIds
    )

    if ($DryRun) { return 'finished' }
    if ([string]::IsNullOrWhiteSpace($SessionId)) { return 'finished' }
    if ($null -eq $ActiveSessionIds) { return 'unknown' }

    if ($ActiveSessionIds -contains $SessionId) { return 'running' }
    return 'finished'
}

function Test-PlanVerification {
    <#
        .SYNOPSIS
            Re-run the plan's own verification commands in its worktree.
        .DESCRIPTION
            A worker asserting success is a claim, not evidence. The dispatcher
            reruns the plan's declared commands itself, and treats a plan that
            declares none as unverifiable rather than as passing.
    #>
    param(
        [Parameter(Mandatory)][pscustomobject]$Plan,
        [Parameter(Mandatory)][string]$WorktreePath
    )

    if (-not $Plan.verify -or $Plan.verify.Count -eq 0) {
        return [pscustomobject]@{
            Passed = $false
            Detail = "Plan '$($Plan.id)' declares no verify commands."
        }
    }

    $workingDirectory = Join-Path $script:RepositoryRoot $WorktreePath
    foreach ($command in $Plan.verify) {
        Write-Log "Verifying $($Plan.id): $command"
        $shell = Get-ShellInvocation -Command $command
        $result = Invoke-Native -FilePath $shell.FilePath -Arguments $shell.Arguments `
            -WorkingDirectory $workingDirectory -AllowFailure
        if ($result.ExitCode -ne 0) {
            $tail = ($result.Output -split "`r?`n" | Select-Object -Last 20) -join "`n"
            return [pscustomobject]@{
                Passed = $false
                Detail = "'$command' failed with exit code $($result.ExitCode).`n$tail"
            }
        }
    }
    return [pscustomobject]@{ Passed = $true; Detail = 'All verify commands passed.' }
}

function Test-PlanHandedOff {
    <#
        .SYNOPSIS
            Confirm the worker moved its plan into needs_review/.
        .DESCRIPTION
            docs/plans/README.md makes the containing folder the authoritative
            workflow state, so the move is the worker's completion signal.
    #>
    param(
        [Parameter(Mandatory)][pscustomobject]$Plan,
        [Parameter(Mandatory)][string]$WorktreePath
    )

    if ($DryRun) { return $true }

    $planFile = Split-Path -Leaf $Plan.path
    $reviewPath = Join-Path (Join-Path $script:RepositoryRoot $WorktreePath) `
        (Join-Path $PlansRoot (Join-Path 'needs_review' $planFile))
    return Test-Path -Path $reviewPath
}

function Merge-PlanBranch {
    <#
        .SYNOPSIS
            Merge a verified feature branch into the integration branch.
    #>
    param(
        [Parameter(Mandatory)][pscustomobject]$Plan,
        [Parameter(Mandatory)][string]$IntegrationBranch
    )

    $message = "merge($($Plan.id)): integrate verified plan branch $($Plan.branch)"
    $integrationTree = Join-Path $script:RepositoryRoot $script:IntegrationWorktree
    $result = Invoke-Git -Arguments @(
        'merge', '--no-ff', '-m', $message, $Plan.branch
    ) -WorkingDirectory $integrationTree -AllowFailure

    if ($result.ExitCode -ne 0) {
        Invoke-Git -Arguments @('merge', '--abort') `
            -WorkingDirectory $integrationTree -AllowFailure | Out-Null
        return [pscustomobject]@{
            Merged = $false
            Detail = "Merge into $IntegrationBranch conflicted; resolve by hand."
        }
    }
    return [pscustomobject]@{ Merged = $true; Detail = "Merged into $IntegrationBranch." }
}

function Set-PlanStatus {
    param(
        [Parameter(Mandatory)][string]$PlanId,
        [Parameter(Mandatory)][string]$Status,
        [string]$Branch,
        [string]$Worktree,
        [string]$Session,
        [string]$Detail
    )

    $arguments = @('mark', '--plan-id', $PlanId, '--status', $Status)
    if ($Branch) { $arguments += @('--branch', $Branch) }
    if ($Worktree) { $arguments += @('--worktree', $Worktree) }
    if ($Session) { $arguments += @('--session', $Session) }
    if ($Detail) { $arguments += @('--detail', $Detail) }
    return Invoke-Planner -PlannerArgs $arguments
}

function Complete-PlanWorker {
    <#
        .SYNOPSIS
            Verify, integrate, and record the outcome of a finished worker.
    #>
    param(
        [Parameter(Mandatory)][pscustomobject]$Plan,
        [Parameter(Mandatory)][pscustomobject]$Record,
        [Parameter(Mandatory)][string]$IntegrationBranch
    )

    Set-PlanStatus -PlanId $Plan.id -Status 'verifying' | Out-Null

    if (-not (Test-PlanHandedOff -Plan $Plan -WorktreePath $Record.worktree)) {
        $detail = "Worker stopped without moving $($Plan.path) to needs_review/."
        Resolve-WorkerFailure -Plan $Plan -Record $Record -Detail $detail
        return
    }

    $verification = Test-PlanVerification -Plan $Plan -WorktreePath $Record.worktree
    if (-not $verification.Passed) {
        Resolve-WorkerFailure -Plan $Plan -Record $Record -Detail $verification.Detail
        return
    }

    $merge = Merge-PlanBranch -Plan $Plan -IntegrationBranch $IntegrationBranch
    if (-not $merge.Merged) {
        Set-PlanStatus -PlanId $Plan.id -Status 'blocked' -Detail $merge.Detail | Out-Null
        Write-Log "$($Plan.id) blocked: $($merge.Detail)" -Level warn
        return
    }

    Set-PlanStatus -PlanId $Plan.id -Status 'complete' -Detail $merge.Detail | Out-Null
    Write-Log "$($Plan.id) complete and integrated." -Level ok
}

function Resolve-WorkerFailure {
    <#
        .SYNOPSIS
            Retry a failed worker once, then stand it down as blocked.
        .DESCRIPTION
            The retry ceiling lives here, in deterministic code, rather than in
            a worker's own judgement about whether to keep trying.
    #>
    param(
        [Parameter(Mandatory)][pscustomobject]$Plan,
        [Parameter(Mandatory)][pscustomobject]$Record,
        [Parameter(Mandatory)][string]$Detail
    )

    if ($Record.attempts -ge $MaxAttempts) {
        Set-PlanStatus -PlanId $Plan.id -Status 'blocked' `
            -Detail "Gave up after $($Record.attempts) attempt(s). $Detail" | Out-Null
        Write-Log "$($Plan.id) blocked after $($Record.attempts) attempt(s)." -Level warn
        Write-Log $Detail -Level warn
        return
    }

    Set-PlanStatus -PlanId $Plan.id -Status 'pending' -Detail $Detail | Out-Null
    Write-Log "$($Plan.id) failed; will retry. $Detail" -Level warn
}

function Get-RunState {
    return Get-Content -Path (Get-StateFullPath) -Raw | ConvertFrom-Json
}

function Get-PlanRecord {
    param(
        [Parameter(Mandatory)][pscustomobject]$RunState,
        [Parameter(Mandatory)][string]$PlanId
    )

    if ($RunState.plans.PSObject.Properties.Name -contains $PlanId) {
        return $RunState.plans.$PlanId
    }
    return [pscustomobject]@{
        status = 'pending'; branch = ''; worktree = ''
        session = ''; attempts = 0; detail = ''
    }
}

function Get-PlanById {
    param(
        [Parameter(Mandatory)][string]$PlanId
    )

    $inventory = Invoke-Planner -PlannerArgs @('inventory')
    return $inventory.plans | Where-Object { $_.id -eq $PlanId } | Select-Object -First 1
}

function Invoke-DispatchTick {
    <#
        .SYNOPSIS
            Advance the run by one tick: reap finished workers, then dispatch.
        .OUTPUTS
            The planner's decision for this tick.
    #>
    param([Parameter(Mandatory)][string]$IntegrationBranch)

    $decision = Invoke-Planner -PlannerArgs @('plan')
    $runState = Get-RunState

    # One listing serves the whole tick: polling per worker would ask the same
    # question N times and could observe N different fleets.
    $activeSessions = if (@($decision.running).Count -gt 0 -and -not $DryRun) {
        Get-ActiveSessionIds
    }
    else {
        @()
    }

    foreach ($planId in @($decision.running)) {
        $record = Get-PlanRecord -RunState $runState -PlanId $planId
        $workerState = Get-WorkerState -SessionId $record.session `
            -ActiveSessionIds $activeSessions
        if ($workerState -eq 'running') {
            continue
        }
        if ($workerState -eq 'unknown') {
            Write-Log "$planId left running; its liveness could not be read." -Level warn
            continue
        }
        $plan = Get-PlanById -PlanId $planId
        Complete-PlanWorker -Plan $plan -Record $record -IntegrationBranch $IntegrationBranch
    }

    $decision = Invoke-Planner -PlannerArgs @('plan')
    foreach ($plan in @($decision.dispatch)) {
        $worktree = New-PlanWorktree -Plan $plan -IntegrationBranch $IntegrationBranch
        $session = Start-PlanWorker -Plan $plan -WorktreePath $worktree
        Set-PlanStatus -PlanId $plan.id -Status 'running' -Branch $plan.branch `
            -Worktree $worktree -Session $session -Detail '' | Out-Null
        Write-Log "Dispatched $($plan.id) on $($plan.branch) (session $session)." -Level ok
    }

    return Invoke-Planner -PlannerArgs @('plan')
}

function Invoke-Run {
    Assert-CleanWorkingTree
    $identity = Initialize-Run

    while ($true) {
        $decision = Invoke-DispatchTick -IntegrationBranch $identity.IntegrationBranch
        Write-Log $decision.reason

        if ($decision.done) {
            Write-Log "Run $($identity.RunId) finished." -Level ok
            Write-Host (Invoke-Planner -PlannerArgs @('status') -Raw)
            if (@($decision.blocked.PSObject.Properties).Count -eq 0) {
                return 0
            }
            return 1
        }
        if (@($decision.awaiting_review).Count -gt 0 -and
            @($decision.dispatch).Count -eq 0 -and
            @($decision.running).Count -eq 0) {
            Write-Log 'Paused for human review.' -Level warn
            foreach ($gateId in $decision.awaiting_review) {
                $gate = $decision.gates.$gateId
                Write-Log "  $gateId - $($gate.title)" -Level warn
                Write-Log "  review checklist: $PlansRoot/$($gate.path)" -Level warn
            }
            Write-Host (Invoke-Planner -PlannerArgs @('status') -Raw)
            Write-Log ("Approve with: ./tools/Invoke-ClaudePlans.ps1 -Action approve " +
                "-Gate <id> -By '<you>' -Note '<why>'")
            return 2
        }
        if ($decision.stalled) {
            Write-Log 'Run stalled; no plan can start.' -Level error
            Write-Host (Invoke-Planner -PlannerArgs @('status') -Raw)
            return 1
        }
        if ($DryRun) {
            Write-Log 'Dry run: stopping after one tick.'
            return 0
        }

        Start-Sleep -Seconds $PollSeconds
    }
}

function Invoke-GateDecision {
    <#
        .SYNOPSIS
            Record a human decision on a review gate.
        .DESCRIPTION
            Only a person runs this. Nothing the fleet does can clear a gate,
            which is what makes the checkpoint worth having.
    #>
    param([Parameter(Mandatory)][string]$Decision)

    if (-not $Gate) {
        throw "-Action $Decision requires -Gate <id>. Run -Action status to list gates."
    }

    $arguments = @($Decision, '--gate', $Gate)
    if ($By) { $arguments += @('--by', $By) }
    if ($Note) { $arguments += @('--note', $Note) }
    $result = Invoke-Planner -PlannerArgs $arguments

    Write-Log "Gate '$($result.id)' is now $($result.status)." -Level ok
    if ($result.status -eq 'approved') {
        Write-Log 'Rerun -Action run to continue the backlog.'
    }
    return 0
}

function Invoke-Stop {
    $runState = Get-RunState
    foreach ($property in $runState.plans.PSObject.Properties) {
        $record = $property.Value
        if ($record.status -ne 'running' -or -not $record.session) { continue }
        Write-Log "Stopping $($property.Name) (session $($record.session))."
        Invoke-Native -FilePath 'claude' -Arguments @('stop', $record.session) `
            -AllowFailure | Out-Null
        Set-PlanStatus -PlanId $property.Name -Status 'pending' `
            -Detail 'Stopped by operator.' | Out-Null
    }
    return 0
}

function Invoke-Clean {
    $runState = Get-RunState
    foreach ($property in $runState.plans.PSObject.Properties) {
        $record = $property.Value
        if (-not $record.worktree) { continue }
        Write-Log "Removing worktree $($record.worktree)."
        Invoke-Git -Arguments @(
            'worktree', 'remove', $record.worktree, '--force'
        ) -AllowFailure | Out-Null
    }
    Invoke-Git -Arguments @(
        'worktree', 'remove', $script:IntegrationWorktree, '--force'
    ) -AllowFailure | Out-Null
    Invoke-Git -Arguments @('worktree', 'prune') -AllowFailure | Out-Null
    return 0
}

try {
    $exitCode = switch ($Action) {
        'run' { Invoke-Run }
        'plan' {
            Initialize-Run | Out-Null
            Write-Host (Invoke-Planner -PlannerArgs @('status') -Raw)
            0
        }
        'status' {
            Write-Host (Invoke-Planner -PlannerArgs @('status') -Raw)
            0
        }
        'stop' { Invoke-Stop }
        'clean' { Invoke-Clean }
        'approve' { Invoke-GateDecision -Decision 'approve' }
        'reject' { Invoke-GateDecision -Decision 'reject' }
        'reopen' { Invoke-GateDecision -Decision 'reopen' }
    }
    exit $exitCode
}
catch {
    Write-Log $_.Exception.Message -Level error
    exit 1
}
