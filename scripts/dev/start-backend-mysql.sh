#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${MYSQL_ENV_FILE:-$ROOT_DIR/data/run/mysql.env}"
PORT="${PORT:-8194}"
LINEAGE_SECURITY_PERMIT_ALL_API="${LINEAGE_SECURITY_PERMIT_ALL_API:-true}"
SESSION_NAME="${SESSION_NAME:-flink-lineage-backend}"
LOG_DIR="$ROOT_DIR/data/logs"
RUN_DIR="$ROOT_DIR/data/run"
LOG_FILE="${LOG_FILE:-$LOG_DIR/lineage-server-dev.log}"
PID_FILE="${PID_FILE:-$RUN_DIR/lineage-server.pid}"
JAR_FILE="${JAR_FILE:-$ROOT_DIR/lineage-server/lineage-server-start/target/lineage-server-1.0.0.jar}"
JAVA_BIN="${JAVA_BIN:-}"

mkdir -p "$LOG_DIR" "$RUN_DIR"

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

if [[ -z "$JAVA_BIN" ]]; then
  if command -v /usr/libexec/java_home >/dev/null 2>&1; then
    JAVA_BIN="$(/usr/libexec/java_home -v 17)/bin/java"
  else
    JAVA_BIN="java"
  fi
fi

if [[ ! -f "$JAR_FILE" ]]; then
  echo "Backend jar not found: $JAR_FILE" >&2
  echo "Run Maven package first." >&2
  exit 1
fi

existing_pid="$(lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)"
if [[ -n "$existing_pid" ]]; then
  if [[ "${REPLACE_PORT:-0}" == "1" ]]; then
    "$ROOT_DIR/scripts/dev/stop-backend.sh" --force-port
  else
    echo "Port $PORT is already listening by PID(s): $existing_pid" >&2
    echo "Run REPLACE_PORT=1 scripts/dev/start-backend-mysql.sh, or scripts/dev/stop-backend.sh --force-port first." >&2
    exit 1
  fi
fi

JDBC_URL="jdbc:mysql://$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DATABASE?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai"

CMD=(
  env
  "SPRING_DATASOURCE_USERNAME=$MYSQL_USERNAME"
  "SPRING_DATASOURCE_PASSWORD=$MYSQL_PASSWORD"
  "$JAVA_BIN"
  -jar "$JAR_FILE"
  --spring.profiles.active=dev
  --profile.active=dev
  --server.port="$PORT"
  --spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
  "--spring.datasource.url=$JDBC_URL"
  --spring.servlet.multipart.max-file-size=20MB
  --spring.servlet.multipart.max-request-size=20MB
  --lineage.plugin.dir="$ROOT_DIR/lineage-client/target/plugins"
  --lineage.storage.dir="$ROOT_DIR/data/storage"
  --lineage.security.permit-all-api="$LINEAGE_SECURITY_PERMIT_ALL_API"
  --logging.file.path="$LOG_DIR"
)

cd "$ROOT_DIR"
: > "$LOG_FILE"

if command -v screen >/dev/null 2>&1 && [[ "${USE_SCREEN:-1}" != "0" ]]; then
  screen -S "$SESSION_NAME" -X quit >/dev/null 2>&1 || true
  printf -v root_q "%q" "$ROOT_DIR"
  printf -v log_q "%q" "$LOG_FILE"
  printf -v cmd_q "%q " "${CMD[@]}"
  screen -dmS "$SESSION_NAME" bash -lc "cd $root_q && exec $cmd_q >> $log_q 2>&1"
else
  nohup "${CMD[@]}" >> "$LOG_FILE" 2>&1 &
  echo "$!" > "$PID_FILE"
fi

for _ in $(seq 1 60); do
  if lsof -tiTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    lsof -tiTCP:"$PORT" -sTCP:LISTEN | head -n 1 > "$PID_FILE"
    echo "Backend started on http://127.0.0.1:$PORT"
    echo "Database: mysql://$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DATABASE"
    echo "Log: $LOG_FILE"
    echo "Login: admin / admin"
    exit 0
  fi
  sleep 1
done

echo "Backend did not start within 60 seconds. Last log lines:" >&2
tail -n 80 "$LOG_FILE" >&2 || true
exit 1
