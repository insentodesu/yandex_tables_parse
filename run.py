"""Entrypoint for accountant MAX bot."""

import asyncio
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent


if __name__ == "__main__":
    # В journalctl должна быть эта строка сразу при старте; если путь не тот каталог — systemd смотрит не туда.
    print(f"MAX bot: run.py directory = {_ROOT}", file=sys.stderr, flush=True)
    asyncio.run(run_scheduler_loop())
