#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PYTHON_BIN="$REPO_ROOT/../.venv-ttc/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
fi
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="python3"
fi

cd "$REPO_ROOT"
export PYTHONPATH="$REPO_ROOT/src"
exec "$PYTHON_BIN" -m ttc_pulse.alerts.run_sidecar_cycle --allow-network
