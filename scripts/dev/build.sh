#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAVEN_BIN="${MAVEN_BIN:-/Users/xujiawei/software/apache-maven-3.8.8/bin/mvn}"

if [[ ! -x "$MAVEN_BIN" ]]; then
  MAVEN_BIN="$(command -v mvn)"
fi

cd "$ROOT_DIR"

JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || true)}" \
MAVEN_OPTS="${MAVEN_OPTS:-}" \
"$MAVEN_BIN" -pl lineage-flink2.1.x,lineage-client,lineage-server/lineage-server-start -am -DskipTests -Dprofile.active=test package
