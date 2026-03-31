#!/bin/zsh
set -euo pipefail

LABEL="${1:-com.ttcpulse.alerts.sidecar}"
PLIST_PATH="$HOME/Library/LaunchAgents/${LABEL}.plist"

launchctl bootout "gui/$UID/$LABEL" >/dev/null 2>&1 || true
if [ -f "$PLIST_PATH" ]; then
  rm -f "$PLIST_PATH"
fi

echo "Uninstalled launchd agent: $LABEL"
