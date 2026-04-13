"""Entrypoint for accountant MAX bot."""

import asyncio

from scheduler import run_scheduler_loop


if __name__ == "__main__":
    asyncio.run(run_scheduler_loop())
