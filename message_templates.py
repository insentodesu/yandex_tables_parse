"""Message formatting rules for accountant notifications."""

from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass
from html import escape
from typing import Any


def _normalize_key(value: str) -> str:
    return " ".join(str(value or "").replace("\n", " ").split()).strip()


def _normalize_value(value: Any) -> str:
    return " ".join(str(value or "").replace("\xa0", " ").replace("\n", " ").split()).strip()


def _normalize_command(value: str) -> str:
    normalized = _normalize_value(value).casefold()
    return normalized.replace(",", "")


def _row_map(row: dict[str, Any]) -> dict[str, str]:
    return {
        _normalize_key(key): _normalize_value(value)
        for key, value in row.items()
        if _normalize_key(key)
    }


def _get(row: dict[str, Any], *aliases: str) -> str:
    normalized = _row_map(row)
    for alias in aliases:
        value = normalized.get(_normalize_key(alias), "")
        if value:
            return value
    return ""


@dataclass(frozen=True, slots=True)
class FieldSpec:
    label: str
    aliases: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TemplateSpec:
    aliases: tuple[str, ...] = ()


# Поля в фиксированном порядке (только они попадают в MAX), не вся строка таблицы.
MESSAGE_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("Дата", ("Дата",)),
    FieldSpec("Заказчик", ("Клиент",)),
    FieldSpec("Менеджер", ("Менеджер",)),
    FieldSpec("Адрес доставки", ("Адрес доставки",)),
    FieldSpec("Услуга/товар", ("Услуга/товар", "Услуга товар")),
    FieldSpec("Транспорт", ("Транспорт", "Наименование услуги")),
    FieldSpec("Водитель", ("Водитель",)),
    FieldSpec("ед. изм.", ("ед. изм.",)),
    FieldSpec("Цена клиенту", ("Цена клиенту",)),
    FieldSpec("Кол-во", ("Кол-во",)),
    FieldSpec("Сумма клиенту", ("Сумма клиенту",)),
    FieldSpec("Своя Найм", ("Своя Найм", "Своя найм")),
    FieldSpec("Номер счета", ("Номер счета", "Номер счета ")),
)


TEMPLATE_SPECS: dict[str, TemplateSpec] = {
    "Альфа, Счет, Маршрут": TemplateSpec(),
    "Альфа, Счет, УПД, Маршрут": TemplateSpec(),
    "Альфа, Счет": TemplateSpec(),
    "Альфа, Счет, УПД": TemplateSpec(),
    "Точка, Счет, Маршрут": TemplateSpec(),
    "Точка, Счет, УПД, Маршрут": TemplateSpec(),
    "Точка, Счет": TemplateSpec(),
    "Точка, Счет, УПД": TemplateSpec(),
    "ИП Точка, Счет, Маршрут": TemplateSpec(),
    "ИП Точка, Счет, Акт, Маршрут": TemplateSpec(),
    "УПД к Счету": TemplateSpec(),
    "Точка Полная Инф": TemplateSpec(),
}

COMMAND_ALIASES: dict[str, str] = {
    _normalize_command(command): command for command in TEMPLATE_SPECS
}


def supported_commands() -> list[str]:
    return list(TEMPLATE_SPECS.keys())


def resolve_command(command: str) -> str | None:
    return COMMAND_ALIASES.get(_normalize_command(command))


def canonicalize_command(command: str) -> str:
    normalized_value = _normalize_value(command)
    return resolve_command(command) or normalized_value


def command_dedup_signature(raw_command: str) -> str:
    """Текст ячейки после нормализации пробелов — ключ дедупа в SQLite.

    Не путать с resolve_command/canonicalize_command: разные подписи в списке могут
    сопоставляться с одним шаблоном, но пользователь меняет именно текст в ячейке —
    при смене подписи уведомление должно уходить.
    """
    s = _normalize_value(raw_command.strip())
    return unicodedata.normalize("NFKC", s)


def stored_command_dedup_key(stored: str) -> str:
    """То же нормализованное представление, что и у command_dedup_signature, для сравнения с SQLite."""
    if not stored:
        return ""
    return unicodedata.normalize("NFKC", _normalize_value(stored.strip()))


def command_column_fingerprint(rows: list[Any], command_header_key: str) -> str:
    """Короткий хэш по всем непустым значениям колонки команды (для логов: меняется ли файл между опросами)."""
    parts: list[str] = []
    for r in rows:
        raw = str(r.values.get(command_header_key, "")).strip()
        v = command_dedup_signature(raw)
        if v:
            parts.append(f"{r.sheet_name}:{r.row_number}:{v}")
    parts.sort()
    body = "|".join(parts)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:12]


def _bold(value: str) -> str:
    return f"<b>{escape(value)}</b>"


def _field_line(label: str, value: str) -> str:
    return f"<b>{escape(label)}: </b>{escape(value)}"


def _build_notification_body(command: str, row: dict[str, Any]) -> str:
    lines = [_bold(_normalize_value(command))]
    for spec in MESSAGE_FIELDS:
        value = _get(row, *spec.aliases)
        if not value:
            continue
        lines.append(_field_line(spec.label, value))
    return "\n".join(lines)


def build_message(command: str, row: dict[str, Any]) -> str:
    command = command.strip()
    resolved_command = resolve_command(command)
    if resolved_command is None:
        raise ValueError(f"Unsupported command: {command}")
    return _build_notification_body(command, row)
