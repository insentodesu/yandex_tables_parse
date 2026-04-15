#!/usr/bin/env bash
# Запускать из каталога репозитория: .../deploy/run-accounting-max.sh
# WorkingDirectory в systemd должен совпадать с корнем проекта (где .venv и run.py).
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$ROOT_DIR"
export PYTHONUNBUFFERED=1

GIT_REV=$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo "?")
echo "$(date -Is) accounting-max start cwd=$ROOT_DIR git=$GIT_REV" >&2

exec "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/run.py"
