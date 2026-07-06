#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

"$ROOT_DIR/scripts/dev/stop-backend.sh" --force-port || true
REPLACE_PORT=1 "$ROOT_DIR/scripts/dev/start-backend-mysql.sh"
