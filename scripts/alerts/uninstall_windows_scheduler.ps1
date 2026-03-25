$ErrorActionPreference = "Stop"

param(
  [string]$TaskName = "TTC_Pulse_Alerts_Sidecar"
)

schtasks /Delete /TN $TaskName /F | Out-Null
Write-Host "Uninstalled Windows Task Scheduler job: $TaskName"
