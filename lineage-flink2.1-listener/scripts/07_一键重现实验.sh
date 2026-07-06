#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$SCRIPT_DIR/02_安装到Flink.sh"
"$SCRIPT_DIR/03_配置Flink.sh"
"$SCRIPT_DIR/04_启动Flink.sh"
"$SCRIPT_DIR/05_提交演示任务.sh"
"$SCRIPT_DIR/06_回放到血缘服务.py"

echo
echo "一键实验完成。打开前端查看最新任务："
echo "http://127.0.0.1:3001/#/job/list"
