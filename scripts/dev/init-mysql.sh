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

if [[ "${RESET_DATABASE:-0}" != "1" ]]; then
  cat >&2 <<EOF
This will drop and recreate MySQL database '$MYSQL_DATABASE'.
It then runs scripts/mysql/1_schema.sql and imports Flink 2.1 seed data.

Set RESET_DATABASE=1 to confirm.
EOF
  exit 1
fi

tmp_data="$(mktemp)"
trap 'rm -f "$tmp_data"' EXIT

perl -pe "s/\\blineage\\./\`$MYSQL_DATABASE\`./g" "$ROOT_DIR/scripts/mysql/2_data.sql" > "$tmp_data"

MYSQL_PWD="$MYSQL_PASSWORD" mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USERNAME" -e \
  "DROP DATABASE IF EXISTS \`$MYSQL_DATABASE\`; CREATE DATABASE \`$MYSQL_DATABASE\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"

MYSQL_PWD="$MYSQL_PASSWORD" mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USERNAME" "$MYSQL_DATABASE" < "$ROOT_DIR/scripts/mysql/1_schema.sql"
MYSQL_PWD="$MYSQL_PASSWORD" mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USERNAME" < "$tmp_data"

"$ROOT_DIR/scripts/dev/check-mysql.sh"
