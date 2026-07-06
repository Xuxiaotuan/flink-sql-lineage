#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${MYSQL_ENV_FILE:-$ROOT_DIR/data/run/mysql.env}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_DATABASE="${MYSQL_DATABASE:-lineage}"
MYSQL_USERNAME="${MYSQL_USERNAME:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"

if [[ -z "$MYSQL_PASSWORD" ]]; then
  echo "MYSQL_PASSWORD is required. Put it in $ENV_FILE or export it before running." >&2
  exit 1
fi

if [[ ! "$MYSQL_DATABASE" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "MYSQL_DATABASE must contain only letters, numbers, and underscores." >&2
  exit 1
fi

MYSQL_PWD="$MYSQL_PASSWORD" mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USERNAME" -N -e "
SELECT CONCAT('mysql_version=', VERSION());
SELECT CONCAT('database=', SCHEMA_NAME) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$MYSQL_DATABASE';
SELECT CONCAT('tables=', COUNT(*)) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$MYSQL_DATABASE';
SELECT CONCAT('plugins=', COUNT(*), ':', COALESCE(GROUP_CONCAT(plugin_code ORDER BY plugin_id), '')) FROM \`$MYSQL_DATABASE\`.bas_plugin;
SELECT CONCAT('catalogs=', COUNT(*), ':', COALESCE(GROUP_CONCAT(catalog_name ORDER BY catalog_id), '')) FROM \`$MYSQL_DATABASE\`.bas_catalog;
SELECT CONCAT('tasks=', COUNT(*), ':', COALESCE(GROUP_CONCAT(task_name ORDER BY task_id), '')) FROM \`$MYSQL_DATABASE\`.bas_task;
"
