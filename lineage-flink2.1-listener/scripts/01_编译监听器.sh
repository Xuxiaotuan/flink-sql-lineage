#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_DIR="$(cd "$MODULE_DIR/.." && pwd)"
MAVEN_BIN="${MAVEN_BIN:-mvn}"

if [[ -z "${JAVA_HOME:-}" && -x /usr/libexec/java_home ]]; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
fi

cd "$REPO_DIR"
"$MAVEN_BIN" -pl lineage-flink2.1-listener -am -DskipTests package

echo "$MODULE_DIR/target/lineage-flink2.1-listener-1.0.0.jar"
