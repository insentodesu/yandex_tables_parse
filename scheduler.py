"""Polling loop for accountant notifications."""

from __future__ import annotations

import asyncio
import logging
import sys
import urllib.error

from aiohttp import TCPConnector
from maxapi import Bot
from maxapi.client.default import DefaultConnectionProperties
from maxapi.enums.parse_mode import ParseMode

import config
import dedup_store
from logging_config import setup_logging
from message_templates import build_message, canonicalize_command, resolve_command
from table_client import TableClient

setup_logging()
logger = logging.getLogger(__name__)


def _is_yandex_http_429(exc: BaseException) -> bool:
    cur: BaseException | None = exc
    for _ in range(8):
        if cur is None:
            break
        if isinstance(cur, urllib.error.HTTPError) and cur.code == 429:
            return True
        if isinstance(cur, RuntimeError):
            text = str(cur)
            if "HTTP 429" in text or " 429:" in text:
                return True
        cur = cur.__cause__
    return False


def create_bot() -> Bot:
    default_conn = None
    if not config.MAX_SSL_VERIFY:
        default_conn = DefaultConnectionProperties(connector=TCPConnector(ssl=False))
    return Bot(token=config.MAX_BOT_TOKEN, default_connection=default_conn)


async def send_accounting_message(bot: Bot | None, text: str) -> bool:
    if config.SEND_MODE == "console":
        logger.info("Тестовое сообщение в консоль:\n%s", text)
        print(text, flush=True)
        return True

    for attempt in range(config.RETRY_ATTEMPTS):
        try:
            if bot is None:
                raise RuntimeError("MAX bot is not initialized")
            await bot.send_message(
                chat_id=config.MAX_CHAT_ID,
                text=text,
                format=ParseMode.HTML,
            )
            return True
        except Exception as exc:
            logger.warning(
                "Ошибка отправки в MAX (попытка %s/%s): %s",
                attempt + 1,
                config.RETRY_ATTEMPTS,
                exc,
            )
            if attempt < config.RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY_SECONDS)
    return False


async def process_pending_rows(bot: Bot | None, client: TableClient | None = None) -> int:
    table_client = client or TableClient()
    sent_count = 0
    is_initialized = dedup_store.snapshot_initialized()
    previous_snapshot = dedup_store.load_snapshot()
    next_snapshot: list[dedup_store.SnapshotEntry] = []

    for row in await table_client.get_rows():
        row_key = dedup_store.build_row_key(row.sheet_name, row.row_number)
        previous_command = previous_snapshot.get(
            row_key,
            dedup_store.SnapshotEntry(
                row_key=row_key,
                sheet_name=row.sheet_name,
                row_number=row.row_number,
                command="",
            ),
        ).command
        raw_command = row.values.get(config.TABLE_COMMAND_COLUMN, "").strip()
        current_command = canonicalize_command(raw_command)

        if not current_command:
            continue

        if not is_initialized:
            next_snapshot.append(
                dedup_store.SnapshotEntry(
                    row_key=row_key,
                    sheet_name=row.sheet_name,
                    row_number=row.row_number,
                    command=current_command,
                )
            )
            continue

        if current_command == previous_command:
            next_snapshot.append(
                dedup_store.SnapshotEntry(
                    row_key=row_key,
                    sheet_name=row.sheet_name,
                    row_number=row.row_number,
                    command=current_command,
                )
            )
            continue

        resolved_command = resolve_command(raw_command)
        if resolved_command is None:
            logger.error("Строка %s пропущена: Unsupported command: %s", row.row_number, raw_command)
            next_snapshot.append(
                dedup_store.SnapshotEntry(
                    row_key=row_key,
                    sheet_name=row.sheet_name,
                    row_number=row.row_number,
                    command=current_command,
                )
            )
            continue

        text = build_message(raw_command, row.values)
        if not await send_accounting_message(bot, text):
            logger.error("Не удалось отправить строку %s листа %s", row.row_number, row.sheet_name)
            if previous_command:
                next_snapshot.append(
                    dedup_store.SnapshotEntry(
                        row_key=row_key,
                        sheet_name=row.sheet_name,
                        row_number=row.row_number,
                        command=previous_command,
                    )
                )
            continue

        next_snapshot.append(
            dedup_store.SnapshotEntry(
                row_key=row_key,
                sheet_name=row.sheet_name,
                row_number=row.row_number,
                command=current_command,
            )
        )
        sent_count += 1
        logger.info(
            "Отправлено уведомление sheet=%s row=%s command=%s",
            row.sheet_name,
            row.row_number,
            resolved_command,
        )

    dedup_store.replace_snapshot(next_snapshot)

    return sent_count


async def run_scheduler_loop() -> None:
    if config.SEND_MODE not in {"max", "console"}:
        logger.error("SEND_MODE должен быть max или console")
        sys.exit(1)
    if config.SEND_MODE == "max" and not config.MAX_BOT_TOKEN:
        logger.error("MAX_BOT_TOKEN не задан")
        sys.exit(1)
    if config.SEND_MODE == "max" and not config.MAX_CHAT_ID:
        logger.error("MAX_CHAT_ID не задан")
        sys.exit(1)
    if not config.TABLE_SOURCE:
        logger.error("TABLE_SOURCE не задан")
        sys.exit(1)

    dedup_store.init_db()
    bot = create_bot() if config.SEND_MODE == "max" else None
    client = TableClient()
    logger.info(
        "Планировщик запущен, mode=%s, интервал=%s сек, источник=%s (%s)",
        config.SEND_MODE,
        config.POLL_INTERVAL_SECONDS,
        config.TABLE_SOURCE_TYPE,
        config.TABLE_SOURCE,
    )

    while True:
        try:
            sent_count = await process_pending_rows(bot, client)
            if sent_count:
                logger.info("За цикл отправлено %s уведомлений", sent_count)
        except Exception as exc:
            logger.exception("Ошибка в polling-цикле")
            if config.RATE_LIMIT_COOLDOWN_SECONDS > 0 and _is_yandex_http_429(exc):
                logger.warning(
                    "Дополнительная пауза %s с после HTTP 429 (лимит Яндекс.Диска)",
                    config.RATE_LIMIT_COOLDOWN_SECONDS,
                )
                await asyncio.sleep(config.RATE_LIMIT_COOLDOWN_SECONDS)

        await asyncio.sleep(config.POLL_INTERVAL_SECONDS)
