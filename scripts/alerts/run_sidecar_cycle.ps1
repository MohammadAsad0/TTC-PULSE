$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")

$pythonCandidates = @(
  (Join-Path $repoRoot "..\.venv-ttc\Scripts\python.exe"),
  (Join-Path $repoRoot ".venv\Scripts\python.exe"),
  "python"
)

$pythonExe = $null
foreach ($candidate in $pythonCandidates) {
  if ($candidate -eq "python") {
    try {
      & $candidate --version *> $null
      $pythonExe = $candidate
      break
    } catch {
    }
  } elseif (Test-Path $candidate) {
    $pythonExe = $candidate
    break
  }
}

if (-not $pythonExe) {
  throw "Python executable not found. Checked: $($pythonCandidates -join ', ')"
}

Push-Location $repoRoot
try {
  $env:PYTHONPATH = (Join-Path $repoRoot "src")
  & $pythonExe -m ttc_pulse.alerts.run_sidecar_cycle --allow-network
} finally {
  Pop-Location
}
