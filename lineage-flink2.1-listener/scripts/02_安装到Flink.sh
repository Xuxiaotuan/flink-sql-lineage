#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINK_HOME="${FLINK_HOME:-$HOME/software/flink/flink-2.1.0}"
PLUGIN_DIR="$FLINK_HOME/plugins/flink21-lineage-listener"

JAR_FILE="$("$SCRIPT_DIR/01_编译监听器.sh" | tail -n 1)"
mkdir -p "$PLUGIN_DIR"

cp "$JAR_FILE" "$PLUGIN_DIR/"
cp "$JAR_FILE" "$FLINK_HOME/lib/flink21-lineage-listener.jar"

echo "已安装监听器插件："
echo "$PLUGIN_DIR/$(basename "$JAR_FILE")"
echo "$FLINK_HOME/lib/flink21-lineage-listener.jar"
