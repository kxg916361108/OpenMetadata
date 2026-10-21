#!/usr/bin/env bash
# ============================================================================
# TES-137: 把 dag-processor / api-server 的日志重定向从 `>` 改成 `>>`
#
# 为什么必须改这一个字符
# ----------------------
# 镜像自带的那份结尾是 `nohup airflow dag-processor > /opt/airflow/logs/dag-processor.log`。
# `>` 让 bash 以 O_WRONLY|O_CREAT|O_TRUNC 打开文件 —— **不带 O_APPEND**。这种 fd 上做
# `truncate -s 0` 之后,进程的下一次写仍落在旧偏移上,文件头部变成一整段 NUL 空洞
# (实测:写 100 B → truncate → 再写 10 B ⇒ 文件 110 B,前 100 字节全是 0x00)。
# 也就是说「先归档再截断」这个唯一可行的轮转手法,在没有 O_APPEND 的文件上会把
# 活文件变成 sparse 二进制垃圾(grep 报 "Binary file matches",tail 只看到 NUL)。
#
# `>>` 用 O_APPEND 打开:每次写都由内核放到当前 EOF,截断后自动从 0 开始写,
# 再没有空洞。这是宿主侧 copytruncate 轮转能安全工作的**前提**。
#
# 与 TES-85 的关系
# ----------------
# 仓库里 ingestion/ingestion_dependency.sh 的 TES-85 监护版**已经是 `>>`**(见其
# `"$AIRFLOW_BIN" dag-processor >> "$DP_LOG"`)。本文件只是把**当前在跑的**旧镜像
# 脚本按同样语义补齐,让挂载+轮转先落地;TES-85 上线后本文件即可删除。
# 本文件相对在跑脚本的唯一差异 = 上面两行的 `>` → `>>` + 这行标记。
#
# 用法:由 docker/development/Dockerfile.tes137-logs COPY 进镜像的同名路径,
#       不是 bind mount(挂载会静默遮蔽镜像里的新脚本,TES-85 上线时会被悄悄跳过)。
# ============================================================================
echo "TES-137 entrypoint rev=1: dag-processor/api-server logs opened in append mode"
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
# TES-137 · 轮转守护(容器内)
# --------------------------
# 为什么轮转跑在容器里而不是宿主 cron:
#   1. 宿主是 macOS,没有 logrotate;newsyslog 是改名(mv),fd 仍指向旧 inode ⇒ 等于没轮转。
#   2. 本机 `crontab <file>` 会**静默挂死**(TES-104 记录过两次),新 cron 行装不进 live 表;
#      launchd 的 bootstrap 在受管会话里被禁。⇒ 宿主侧排不了班,「配了」等于「没生效」。
#   3. 截断(truncate)在 OrbStack 的 bind mount 上**必须在容器侧执行**才重置 append 偏移
#      (宿主 ftruncate 会让 dag-processor 继续写旧偏移,活文件变成 NUL 空洞 —— 实测过)。
#   脚本本体只存一份:宿主仓库 scripts/airflow_log_rotate.py,以只读 bind mount 挂进来。
#   循环体不写文件、不进 /dev/null:输出进容器 stdout ⇒ 由 TES-134 的 json-file 50m×60 兜底。
nohup bash -c 'while true; do sleep 600; python3 /opt/airflow/log_rotate.py --dir /opt/airflow/logs --container "" || echo "TES137 rotate rc=$?"; done' &

echo "Starting Airflow 3.x components..."
nohup airflow api-server --port 8080 >> /opt/airflow/logs/api-server.log 2>&1 &
nohup airflow dag-processor >> /opt/airflow/logs/dag-processor.log 2>&1 &
airflow scheduler
