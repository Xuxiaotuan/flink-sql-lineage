#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FLINK_HOME="${FLINK_HOME:-$HOME/software/flink/flink-2.1.0}"
CONFIG_FILE="$FLINK_HOME/conf/config.yaml"
LISTENER_FACTORY="com.hw.lineage.flink.listener.Flink21LineageListenerFactory"
EVENT_FILE="${FLINK_LINEAGE_EVENT_FILE:-/tmp/flink21-lineage-events.jsonl}"
COLLECTOR_URL="${LINEAGE_COLLECTOR_URL:-}"
COLLECTOR_USERNAME="${LINEAGE_COLLECTOR_USERNAME:-admin}"
COLLECTOR_PASSWORD="${LINEAGE_COLLECTOR_PASSWORD:-admin}"
COLLECTOR_CATALOG_ID="${LINEAGE_COLLECTOR_CATALOG_ID:-1}"
COLLECTOR_DATABASE="${LINEAGE_COLLECTOR_DATABASE:-default}"
COLLECTOR_SQL_FILE="${LINEAGE_COLLECTOR_SQL_FILE:-$MODULE_DIR/sql/listener-insert.sql}"
MARKER_BEGIN="# BEGIN flink21-lineage-listener-local-experiment"
MARKER_END="# END flink21-lineage-listener-local-experiment"

cp "$CONFIG_FILE" "$CONFIG_FILE.bak.$(date +%Y%m%d%H%M%S)"

python3 - "$CONFIG_FILE" "$LISTENER_FACTORY" "$EVENT_FILE" "$COLLECTOR_URL" "$COLLECTOR_USERNAME" \
  "$COLLECTOR_PASSWORD" "$COLLECTOR_CATALOG_ID" "$COLLECTOR_DATABASE" "$COLLECTOR_SQL_FILE" \
  "$MARKER_BEGIN" "$MARKER_END" <<'PY'
import sys
from pathlib import Path

(
    config_file,
    factory,
    event_file,
    collector_url,
    collector_username,
    collector_password,
    collector_catalog_id,
    collector_database,
    collector_sql_file,
    begin,
    end,
) = sys.argv[1:]
path = Path(config_file)
text = path.read_text()
while begin in text and end in text:
    start = text.index(begin)
    stop = text.index(end, start) + len(end)
    text = text[:start].rstrip() + "\n" + text[stop:].lstrip()

block = f"""
{begin}
execution.job-status-changed-listeners: {factory}
lineage.experiment.event-file: {event_file}
lineage.collector.url: {collector_url}
lineage.collector.username: {collector_username}
lineage.collector.password: {collector_password}
lineage.collector.catalog-id: {collector_catalog_id}
lineage.collector.database: {collector_database}
lineage.collector.sql-file: {collector_sql_file}
{end}
"""
path.write_text(text.rstrip() + "\n\n" + block)
PY

echo "已配置 Flink：$CONFIG_FILE"
echo "监听器工厂：$LISTENER_FACTORY"
echo "事件文件：$EVENT_FILE"
echo "Collector URL：${COLLECTOR_URL:-未启用，仅写 JSONL}"
