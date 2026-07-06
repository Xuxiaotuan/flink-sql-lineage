#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FLINK_HOME="${FLINK_HOME:-$HOME/software/flink/flink-2.1.0}"
EVENT_FILE="${FLINK_LINEAGE_EVENT_FILE:-/tmp/flink21-lineage-events.jsonl}"

rm -f "$EVENT_FILE"

"$FLINK_HOME/bin/flink" run -d \
  -c com.hw.lineage.flink.listener.Flink21LineageDemoJob \
  "$MODULE_DIR/target/lineage-flink2.1-listener-1.0.0.jar"

sleep 5

echo "Listener 事件文件：$EVENT_FILE"
ls -l "$EVENT_FILE"
echo
echo "事件内容预览："
sed -n '1,3p' "$EVENT_FILE" | head -c 12000
echo
