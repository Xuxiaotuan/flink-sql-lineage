#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-3001}"
pids="$(lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)"

if [[ -z "$pids" ]]; then
  echo "No frontend process is listening on port $PORT"
  exit 0
fi

echo "$pids" | xargs kill >/dev/null 2>&1 || true
echo "Frontend stopped on port $PORT"
