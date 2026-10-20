#!/usr/bin/env bash
#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

DB_HOST=${DB_HOST:-mysql}
DB_PORT=${DB_PORT:-3306}

AIRFLOW_DB=${AIRFLOW_DB:-airflow_db}
DB_USER=${DB_USER:-airflow_user}
DB_SCHEME=${DB_SCHEME:-mysql+mysqldb}
DB_PASSWORD=${DB_PASSWORD:-airflow_pass}
DB_PROPERTIES=${DB_PROPERTIES:-""}

AIRFLOW_ADMIN_USER=${AIRFLOW_ADMIN_USER:-admin}
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD:-admin}

DB_USER_VAR=`echo "${DB_USER}" | python3 -c "import urllib.parse; encoded_user = urllib.parse.quote(input()); print(encoded_user)"`
DB_PASSWORD_VAR=`echo "${DB_PASSWORD}" | python3 -c "import urllib.parse; encoded_user = urllib.parse.quote(input()); print(encoded_user)"`

DB_CONN=`echo -n "${DB_SCHEME}://${DB_USER_VAR}:${DB_PASSWORD_VAR}@${DB_HOST}:${DB_PORT}/${AIRFLOW_DB}${DB_PROPERTIES}"`

# Airflow 3.x configuration
export AIRFLOW__API__AUTH_BACKENDS=${AIRFLOW__API__AUTH_BACKENDS:-"airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"}
export AIRFLOW__API__BASE_URL=${AIRFLOW__API__BASE_URL:-"http://localhost:8080"}

# Enable CSRF for API endpoints (required for production security)
export AIRFLOW__API__ENABLE_CSRF=${AIRFLOW__API__ENABLE_CSRF:-"True"}

# Configure SimpleAuthManager
echo "Configuring SimpleAuthManager (default in Airflow 3.x)"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="${AIRFLOW_ADMIN_USER}:admin"

AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE="${AIRFLOW_HOME}/simple_auth_manager_passwords.json"

# Airflow 3.x uses [database] section for SQL Alchemy connection
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-$DB_CONN}

airflow db migrate

# Create users for SimpleAuthManager
echo "SimpleAuthManager configured - user: ${AIRFLOW_ADMIN_USER}:admin"

AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}
PASSWORD_FILE="${AIRFLOW_HOME}/simple_auth_manager_passwords.json"

echo "Setting password for ${AIRFLOW_ADMIN_USER} in ${PASSWORD_FILE}..."
mkdir -p "${AIRFLOW_HOME}"

python3 -c "
import json
import os
from pathlib import Path

password_file = Path('${PASSWORD_FILE}')
passwords = {}

if password_file.exists():
    try:
        with open(password_file, 'r') as f:
            passwords = json.load(f)
    except:
        pass

passwords['${AIRFLOW_ADMIN_USER}'] = '${AIRFLOW_ADMIN_PASSWORD}'

with open(password_file, 'w') as f:
    json.dump(passwords, f, indent=2)

print(f'Password set for ${AIRFLOW_ADMIN_USER}')
"

echo "SimpleAuthManager user created with custom password"

rm -f /opt/airflow/airflow-webserver-monitor.pid

# Start Airflow 3.x components
#
# >>> TES-85 supervision block >>>
# 三进程同起 + `wait -n`:任一核心进程退出 → 打印「是谁 / 为什么 / 尾部 50 行日志」
# → 整容器退出 → 交给容器 `restart: unless-stopped` + scripts/docker_crashloop_watchdog.py
# 的 LOOP_STREAK 收口(3 次信号 → 告警 + docker stop)。
#
# 为什么要盯三个而不是只盯 scheduler:
#   api-server 提供 Task Execution API,LocalExecutor 的 worker 靠它领活 —— 它死了
#   所有 task 静默停摆;scheduler 心跳照常(这正是 TES-78 事故的形态)。
#   dag-processor 死了 DAG 不再解析,新 run 起不来。
#   此前只有 scheduler 是前台进程,另两个用 `nohup ... &` 起,死了没人知道:
#   容器仍 Up,容器引擎完全没机会介入(RestartPolicy 甚至还是 no)。
#   同样的问题在 data-dev/airflow/start.sh 里已经用 wait -n 解决,这里对齐同一口径。
#
# 两个可覆盖变量是为了让这段能被 scripts/test_supervision_tes85.py 用假 airflow 跑:
#   AIRFLOW_BIN(默认 airflow)、AIRFLOW_SUPERVISE_LOG_DIR(默认 /opt/airflow/logs)。
AIRFLOW_BIN=${AIRFLOW_BIN:-airflow}
SUPERVISE_LOG_DIR=${AIRFLOW_SUPERVISE_LOG_DIR:-/opt/airflow/logs}
mkdir -p "$SUPERVISE_LOG_DIR"
API_LOG="$SUPERVISE_LOG_DIR/api-server.log"
DP_LOG="$SUPERVISE_LOG_DIR/dag-processor.log"
SCHED_LOG="$SUPERVISE_LOG_DIR/scheduler.log"
: > "$API_LOG"; : > "$DP_LOG"; : > "$SCHED_LOG"

echo "Starting Airflow 3.x components (supervised: api-server / dag-processor / scheduler)..."

"$AIRFLOW_BIN" api-server --port 8080 >> "$API_LOG" 2>&1 &
API_PID=$!
"$AIRFLOW_BIN" dag-processor >> "$DP_LOG" 2>&1 &
DP_PID=$!
# scheduler 仍然要进容器 stdout(`docker logs` 是排障第一入口),所以 tee 一份到文件。
"$AIRFLOW_BIN" scheduler 2>&1 | tee -a "$SCHED_LOG" &
SCHED_PID=$!

echo "supervised pids: api-server=$API_PID dag-processor=$DP_PID scheduler=$SCHED_PID"
echo "supervised logs: $SUPERVISE_LOG_DIR"

supervise_shutdown() {
  local code=${1:-0}
  echo "supervisor: stopping all core processes (code=$code)" >&2
  kill -TERM "$API_PID" "$SCHED_PID" "$DP_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  exit "$code"
}
trap 'supervise_shutdown 0' TERM INT

wait -n "$API_PID" "$SCHED_PID" "$DP_PID"
CHILD_STATUS=$?

DEAD_NAME=""
DEAD_PID=""
DEAD_LOG=""
for spec in "api-server:$API_PID:$API_LOG" \
            "dag-processor:$DP_PID:$DP_LOG" \
            "scheduler:$SCHED_PID:$SCHED_LOG"; do
  name=${spec%%:*}
  rest=${spec#*:}
  pid=${rest%%:*}
  log=${rest#*:}
  if ! kill -0 "$pid" 2>/dev/null; then
    DEAD_NAME=$name; DEAD_PID=$pid; DEAD_LOG=$log
    break
  fi
done

{
  echo "=================================================================="
  echo "supervisor: a core process exited -> taking the container down"
  echo "  process : ${DEAD_NAME:-unknown}"
  echo "  pid     : ${DEAD_PID:-unknown}"
  echo "  status  : $CHILD_STATUS (137=SIGKILL/通常 OOM, 143=SIGTERM, 1=python 异常)"
  echo "  time    : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "  logs    : $DEAD_LOG (last 50 lines)"
  echo "------------------------------------------------------------------"
  tail -n 50 "$DEAD_LOG" 2>/dev/null || echo "  (log file missing: $DEAD_LOG)"
  echo "=================================================================="
} >&2

# 非零原样传出(137 能让 watchdog 的 R3「短命 + 非零退出」判据直接命中);
# 若是干净退出 0(不该发生)也按 1 报,否则崩溃会被记成正常终止。
if [ "$CHILD_STATUS" -eq 0 ] 2>/dev/null; then
  EXIT_CODE=1
else
  EXIT_CODE=$CHILD_STATUS
fi
supervise_shutdown "$EXIT_CODE"
# <<< TES-85 supervision block <<<
