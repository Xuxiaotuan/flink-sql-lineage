#!/usr/bin/env bash
set -euo pipefail

FLINK_HOME="${FLINK_HOME:-$HOME/software/flink/flink-2.1.0}"
"$FLINK_HOME/bin/stop-cluster.sh"
