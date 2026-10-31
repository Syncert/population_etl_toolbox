# Register the weekly warehouse backup as a Windows scheduled task (OFFICE_LAB).
#
# The warehouse lives on a striped volume with no redundancy, so the dump goes
# to a drive outside the stripe. The task runs `backup_warehouse.py dump` and
# then `prune`, Sundays at 03:00, as the signed-in user (Docker Desktop runs in
# that user's session). A dump is skipped, not forced, while any statement has
# been active for more than 30 minutes: a load or a serving refresh is running,
# and competing with it for disk is how October's loads failed.
#
#   powershell -ExecutionPolicy Bypass -File scripts\register_backup_task.ps1 `
#       -Target E:\population_backups\warehouse
#
# Re-running replaces the task. Remove it with:
#   Unregister-ScheduledTask -TaskName PopulationWarehouseBackup -Confirm:$false

param(
    [Parameter(Mandatory = $true)][string]$Target,
    [int]$Keep = 2,
    [string]$Python = (Get-Command python).Source,
    [string]$At = "03:00"
)

$repository = Split-Path -Parent $PSScriptRoot
$script = Join-Path $repository "scripts\backup_warehouse.py"
$log = Join-Path $Target "backup.log"
$command = "& '$Python' '$script' dump --target '$Target' --skip-if-busy 30 *>> '$log'; " +
    "if (`$LASTEXITCODE -eq 0) { & '$Python' '$script' prune --target '$Target' --keep $Keep *>> '$log' }"

New-Item -ItemType Directory -Force $Target | Out-Null
$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -Command `"$command`"" `
    -WorkingDirectory $repository
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At $At
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun `
    -ExecutionTimeLimit (New-TimeSpan -Hours 12) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName "PopulationWarehouseBackup" -Action $action `
    -Trigger $trigger -Settings $settings -Force `
    -Description "Weekly pg_dump of the population warehouse to $Target (scripts/backup_warehouse.py)" | Out-Null
Write-Output "Registered PopulationWarehouseBackup: Sundays $At, dump to $Target, keep $Keep."
