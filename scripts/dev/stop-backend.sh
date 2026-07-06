#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${PORT:-8194}"
SESSION_NAME="${SESSION_NAME:-flink-lineage-backend}"
PID_FILE="${PID_FILE:-$ROOT_DIR/data/run/lineage-server.pid}"

if command -v screen >/dev/null 2>&1; then
  screen -S "$SESSION_NAME" -X quit >/dev/null 2>&1 || true
fi

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$PID_FILE"
fi

if [[ "${1:-}" == "--force-port" ]]; then
  pids="$(lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "$pids" ]]; then
    echo "$pids" | xargs kill >/dev/null 2>&1 || true
  fi
fi

for _ in $(seq 1 20); do
  if ! lsof -tiTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Backend stopped on port $PORT"
    exit 0
  fi
  sleep 1
done

echo "Backend is still listening on port $PORT" >&2
exit 1
