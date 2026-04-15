"""Polling loop for accountant notifications."""

from __future__ import annotations

import asyncio
import logging
import sys
import urllib.error
from pathlib import Path

from aiohttp import TCPConnector
from maxapi import Bot
from maxapi.client.default import DefaultConnectionProperties
from maxapi.enums.parse_mode import ParseMode

import config
import dedup_store
from logging_config import setup_logging
from message_templates import build_message, canonicalize_command, resolve_command
from table_client import TableClient, normalize_header

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
    was_initialized = is_initialized
    previous_snapshot = dedup_store.load_snapshot()
    next_snapshot: list[dedup_store.SnapshotEntry] = []
    skipped_same_command = 0
    unsupported_command_rows = 0

    command_header_key = normalize_header(config.TABLE_COMMAND_COLUMN)

    logger.info("Загрузка таблицы из источника…")
    try:
        rows = await asyncio.wait_for(
            table_client.get_rows(),
            timeout=config.TABLE_LOAD_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.error(
            "Таймаут загрузки таблицы (%s с): зависание сети/Диска или очень большой XLSX. "
            "Увеличьте TABLE_LOAD_TIMEOUT_SECONDS в .env",
            config.TABLE_LOAD_TIMEOUT_SECONDS,
        )
        raise

    for row in rows:
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
        raw_command = row.values.get(command_header_key, "").strip()
        current_command = canonicalize_command(raw_command)

        if not current_command:
            continue

        if not is_initialized and not config.BOOTSTRAP_SEND_MAX:
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
            skipped_same_command += 1
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
            unsupported_command_rows += 1
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

        try:
            text = build_message(raw_command, row.values)
        except Exception:
            logger.exception(
                "Строка %s листа %s: ошибка сборки текста сообщения (проверьте данные строки)",
                row.row_number,
                row.sheet_name,
            )
            next_snapshot.append(
                dedup_store.SnapshotEntry(
                    row_key=row_key,
                    sheet_name=row.sheet_name,
                    row_number=row.row_number,
                    command=current_command,
                )
            )
            continue

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

    rows_with_command = sum(
        1
        for r in rows
        if canonicalize_command(str(r.values.get(command_header_key, "")).strip())
    )
    logger.info(
        "Опрос: строк в файле=%s, с заполненным «%s»=%s, отправлено в MAX=%s",
        len(rows),
        config.TABLE_COMMAND_COLUMN,
        rows_with_command,
        sent_count,
    )

    if unsupported_command_rows:
        logger.error(
            "Ни одного сообщения: в %s строках значение в «%s» не из списка бота "
            "(см. CHAT_OPTIONS / message_templates). Пример текста из ячейки смотрите выше (Unsupported command).",
            unsupported_command_rows,
            config.TABLE_COMMAND_COLUMN,
        )

    if (
        sent_count == 0
        and rows_with_command > 0
        and was_initialized
        and skipped_same_command > 0
        and unsupported_command_rows == 0
    ):
        logger.warning(
            "В канал ничего не отправлено: для %s строк выбор в «%s» совпадает с уже сохранённым "
            "в SQLite (бот не дублирует то же самое). Смените пункт в списке; либо после rm БД "
            "в .env поставьте BOOTSTRAP_SEND_MAX=1 на один рестарт (одна рассылка текущих строк). "
            "Файл БД: %s",
            skipped_same_command,
            config.TABLE_COMMAND_COLUMN,
            config.DATABASE_PATH,
        )

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
    src_type = config.TABLE_SOURCE_TYPE.strip().lower()
    disk_oauth = src_type in {"yandex_disk_xlsx", "yandex_disk_csv"}
    if not disk_oauth and not config.TABLE_SOURCE:
        logger.error("TABLE_SOURCE не задан")
        sys.exit(1)
    if disk_oauth:
        if not config.YANDEX_DISK_TOKEN.strip():
            logger.error("YANDEX_DISK_TOKEN не задан (нужен для %s)", src_type)
            sys.exit(1)
        if not config.TABLE_DISK_PATH.strip():
            logger.error("TABLE_DISK_PATH не задан")
            sys.exit(1)

    dedup_store.init_db()
    snap_init = dedup_store.snapshot_initialized()
    bot = create_bot() if config.SEND_MODE == "max" else None
    client = TableClient()
    log_src = config.TABLE_DISK_PATH if disk_oauth else config.TABLE_SOURCE
    # Одна строка: по ней в journalctl видно версию кода, .env (BOOTSTRAP_SEND_MAX) и был ли уже снимок в БД.
    logger.info(
        "Старт бота | mode=%s poll=%ss | BOOTSTRAP_SEND_MAX=%s | snapshot_initialized=%s | "
        "источник=%s (%s) | BASE_DIR=%s | scheduler=%s | БД=%s | колонка=%s",
        config.SEND_MODE,
        config.POLL_INTERVAL_SECONDS,
        config.BOOTSTRAP_SEND_MAX,
        snap_init,
        config.TABLE_SOURCE_TYPE,
        log_src,
        config.BASE_DIR,
        Path(__file__).resolve(),
        config.DATABASE_PATH,
        config.TABLE_COMMAND_COLUMN,
    )

    while True:
        try:
            await process_pending_rows(bot, client)
        except Exception as exc:
            logger.exception("Ошибка в polling-цикле")
            if config.RATE_LIMIT_COOLDOWN_SECONDS > 0 and _is_yandex_http_429(exc):
                logger.warning(
                    "Дополнительная пауза %s с после HTTP 429 (лимит Яндекс.Диска)",
                    config.RATE_LIMIT_COOLDOWN_SECONDS,
                )
                await asyncio.sleep(config.RATE_LIMIT_COOLDOWN_SECONDS)

        await asyncio.sleep(config.POLL_INTERVAL_SECONDS)
