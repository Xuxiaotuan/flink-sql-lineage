#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LINES="${LINES:-120}"

for log in \
  "$ROOT_DIR/data/logs/lineage-server-dev.log" \
  "$ROOT_DIR/data/logs/lineage-server.log" \
  "$ROOT_DIR/data/logs/lineage-server-error.log"
do
  echo "==> $log"
  if [[ -f "$log" ]]; then
    tail -n "$LINES" "$log"
  else
    echo "missing"
  fi
  echo
done
