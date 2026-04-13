#!/usr/bin/env bash
set -euo pipefail
cd /root/accounting_max_bot
export PYTHONUNBUFFERED=1

exec .venv/bin/python run.py
