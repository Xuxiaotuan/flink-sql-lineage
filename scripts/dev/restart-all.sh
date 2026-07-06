#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BACKEND_MODE="${BACKEND_MODE:-mysql}"
WEB_PORT="${WEB_PORT:-3001}"

case "$BACKEND_MODE" in
  mysql)
    "$ROOT_DIR/scripts/dev/restart-backend-mysql.sh"
    ;;
  h2)
    "$ROOT_DIR/scripts/dev/restart-backend.sh"
    ;;
  *)
    echo "BACKEND_MODE must be mysql or h2." >&2
    exit 1
    ;;
esac

if ! lsof -tiTCP:"$WEB_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Frontend is not listening on port $WEB_PORT."
  echo "Start it with: scripts/dev/start-web.sh"
fi

"$ROOT_DIR/scripts/dev/status.sh"
