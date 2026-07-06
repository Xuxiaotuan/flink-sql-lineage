#!/usr/bin/env bash
set -euo pipefail

FLINK_HOME="${FLINK_HOME:-$HOME/software/flink/flink-2.1.0}"

"$FLINK_HOME/bin/start-cluster.sh"
sleep 3
lsof -nP -iTCP:8081 -sTCP:LISTEN || true
