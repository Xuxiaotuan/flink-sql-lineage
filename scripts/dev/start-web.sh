#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${PORT:-3001}"
HOST="${HOST:-127.0.0.1}"
BACKEND_URL="${BACKEND_URL:-http://127.0.0.1:8194}"

cd "$ROOT_DIR/lineage-web"

if [[ ! -d node_modules ]]; then
  yarn install
fi

echo "Frontend starting on http://$HOST:$PORT"
echo "Proxy target: $BACKEND_URL"

PORT="$PORT" \
HOST="$HOST" \
BROWSER="${BROWSER:-none}" \
REACT_APP_PROXY_TARGET="$BACKEND_URL" \
yarn start
