$ErrorActionPreference = "Stop"

param(
  [string]$TaskName = "TTC_Pulse_Alerts_Sidecar"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$runner = Join-Path $scriptDir "run_sidecar_cycle.ps1"

if (-not (Test-Path $runner)) {
  throw "Runner script missing: $runner"
}

$taskCommand = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$runner`""

# Replace existing task if present.
schtasks /Delete /TN $TaskName /F *> $null 2>&1

schtasks /Create `
  /TN $TaskName `
  /SC MINUTE `
  /MO 30 `
  /TR $taskCommand `
  /F | Out-Null

schtasks /Run /TN $TaskName | Out-Null

Write-Host "Installed and started Windows Task Scheduler job: $TaskName"
