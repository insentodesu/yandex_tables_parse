"""Configuration loaded from environment variables."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _as_bool(raw: str, default: bool = False) -> bool:
    value = (raw or "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


BASE_DIR = Path(__file__).resolve().parent

MAX_BOT_TOKEN: str = os.getenv("MAX_BOT_TOKEN", "")
MAX_CHAT_ID: int = int(os.getenv("MAX_CHAT_ID", "0") or "0")
MAX_SSL_VERIFY: bool = _as_bool(os.getenv("MAX_SSL_VERIFY", "true"), default=True)
SEND_MODE: str = os.getenv("SEND_MODE", "max").strip().lower()

POLL_INTERVAL_SECONDS: int = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))
RETRY_ATTEMPTS: int = int(os.getenv("RETRY_ATTEMPTS", "2"))
RETRY_DELAY_SECONDS: int = int(os.getenv("RETRY_DELAY_SECONDS", "2"))

# Backoff for TABLE_SOURCE HTTP fetches (Yandex Disk often returns 429 when polled too often).
TABLE_FETCH_MAX_RETRIES: int = max(1, int(os.getenv("TABLE_FETCH_MAX_RETRIES", "6")))
TABLE_FETCH_RETRY_BASE_SECONDS: float = float(os.getenv("TABLE_FETCH_RETRY_BASE_SECONDS", "2") or "2")
TABLE_FETCH_RETRY_MAX_SLEEP_SECONDS: float = float(
    os.getenv("TABLE_FETCH_RETRY_MAX_SLEEP_SECONDS", "120") or "120"
)
# Extra sleep after a polling cycle if the table fetch failed with HTTP 429 (0 disables).
RATE_LIMIT_COOLDOWN_SECONDS: int = int(os.getenv("RATE_LIMIT_COOLDOWN_SECONDS", "90") or "0")

DATABASE_PATH: str = os.getenv(
    "DATABASE_PATH",
    str(BASE_DIR / "data" / "accounting_max_bot.db"),
)

TABLE_SOURCE_TYPE: str = os.getenv("TABLE_SOURCE_TYPE", "csv_url").strip().lower()
TABLE_SOURCE: str = os.getenv("TABLE_SOURCE", "").strip()
TABLE_SHEET_NAME: str = os.getenv("TABLE_SHEET_NAME", "").strip()
TABLE_COMMAND_COLUMN: str = os.getenv("TABLE_COMMAND_COLUMN", "Бухгалтеру в чат").strip()

# Public Yandex Disk link (yandex_public_*): optional path inside a published folder and link password.
TABLE_YANDEX_PUBLIC_PATH: str = os.getenv("TABLE_YANDEX_PUBLIC_PATH", "").strip()
TABLE_YANDEX_PUBLIC_PASSWORD: str = os.getenv("TABLE_YANDEX_PUBLIC_PASSWORD", "").strip()

# OAuth access to your own Disk file (yandex_disk_*), when public API is blocked (e.g. download disabled).
YANDEX_DISK_TOKEN: str = os.getenv("YANDEX_DISK_TOKEN", "").strip()
TABLE_DISK_PATH: str = os.getenv("TABLE_DISK_PATH", "").strip()
MONTH_SHEET_NAMES: tuple[str, ...] = (
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
)

SOURCE_GOOGLE_CSV_URL: str = os.getenv(
    "SOURCE_GOOGLE_CSV_URL",
    (
        "https://docs.google.com/spreadsheets/d/"
        "16gaolLjUU7yCaHrKKfZ6wRHvAgDO0Be33-YUupGFFHw/export"
        "?format=csv&gid=1401714661"
    ),
).strip()

TEMPLATE_OUTPUT_PATH: str = os.getenv(
    "TEMPLATE_OUTPUT_PATH",
    str(BASE_DIR / "templates" / "yandex_accounting_template.xlsx"),
)

SOURCE_STRUCTURE_PATH: str = os.getenv(
    "SOURCE_STRUCTURE_PATH",
    str(BASE_DIR / "templates" / "source_sheet_structure.json"),
)

CHAT_OPTIONS: list[str] = [
    "Альфа, Счет, Маршрут",
    "Альфа, Счет, УПД, Маршрут",
    "Альфа, Счет",
    "Альфа, Счет, УПД",
    "Точка, Счет, Маршрут",
    "Точка, Счет, УПД, Маршрут",
    "Точка, Счет",
    "Точка, Счет, УПД",
    "ИП Точка, Счет, Маршрут",
    "ИП Точка, Счет, Акт, Маршрут",
    "УПД к Счету",
    "Точка Полная Инф",
]
