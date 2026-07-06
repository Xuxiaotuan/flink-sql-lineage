#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BACKEND_PORT="${BACKEND_PORT:-8194}"
WEB_PORT="${WEB_PORT:-3001}"
PID_FILE="$ROOT_DIR/data/run/lineage-server.pid"

echo "Workspace: $ROOT_DIR"
echo

echo "Backend port $BACKEND_PORT:"
if lsof -nP -iTCP:"$BACKEND_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  lsof -nP -iTCP:"$BACKEND_PORT" -sTCP:LISTEN
else
  echo "  not listening"
fi

if [[ -f "$PID_FILE" ]]; then
  echo "Backend pidfile: $(cat "$PID_FILE")"
else
  echo "Backend pidfile: missing"
fi

echo
echo "Frontend port $WEB_PORT:"
if lsof -nP -iTCP:"$WEB_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  lsof -nP -iTCP:"$WEB_PORT" -sTCP:LISTEN
else
  echo "  not listening"
fi

echo
echo "Backend jar:"
ls -lh "$ROOT_DIR/lineage-server/lineage-server-start/target/lineage-server-1.0.0.jar" 2>/dev/null || echo "  missing"

echo
echo "Plugin directories:"
find "$ROOT_DIR/lineage-client/target/plugins" -maxdepth 2 -type d 2>/dev/null | sort || echo "  missing"

echo
echo "Screen sessions:"
screen -ls 2>/dev/null || true
