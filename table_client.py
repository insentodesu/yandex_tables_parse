"""Helpers for loading rows from exported spreadsheet sources."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from openpyxl import load_workbook
from python_calamine import load_workbook as load_calamine_workbook

import config


def normalize_header(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\n", " ").replace("\r", " ")
    return " ".join(text.split()).strip()


def normalize_cell(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ").replace("\r", " ").replace("\n", " ")
    return " ".join(text.split()).strip()


def _format_yandex_http_error(exc: urllib.error.HTTPError) -> str:
    try:
        raw = exc.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
        if isinstance(data, dict):
            err = str(data.get("error") or "").strip()
            msg = str(data.get("message") or data.get("description") or "").strip()
            parts = [p for p in (err, msg) if p]
            if parts:
                return f"Yandex Disk API HTTP {exc.code}: {' — '.join(parts)}"
    except Exception:
        pass
    reason = exc.reason if isinstance(exc.reason, str) else ""
    return f"Yandex Disk API HTTP {exc.code}: {reason or 'request failed'}"


@dataclass(slots=True)
class SpreadsheetRow:
    sheet_name: str
    row_number: int
    values: dict[str, str]


class TableClient:
    """Reads CSV/XLSX from a URL, file, public Yandex link, or OAuth path on Yandex Disk."""

    def __init__(
        self,
        source_type: str | None = None,
        source: str | None = None,
        sheet_name: str | None = None,
        command_column: str | None = None,
    ) -> None:
        self.source_type = (source_type or config.TABLE_SOURCE_TYPE).strip().lower()
        self.source = (source or config.TABLE_SOURCE).strip()
        self.sheet_name = (sheet_name or config.TABLE_SHEET_NAME).strip()
        self.command_column = normalize_header(command_column or config.TABLE_COMMAND_COLUMN)

    async def get_rows(self) -> list[SpreadsheetRow]:
        return await asyncio.to_thread(self._get_rows_sync)

    async def get_actionable_rows(self) -> list[SpreadsheetRow]:
        rows = await self.get_rows()
        return [
            row for row in rows
            if normalize_cell(row.values.get(self.command_column, ""))
        ]

    def _get_rows_sync(self) -> list[SpreadsheetRow]:
        if not self.source:
            raise ValueError("TABLE_SOURCE is not configured")

        if self.source_type == "csv_url":
            return self._load_csv(self._download_bytes(self.source).decode("utf-8-sig"))
        if self.source_type == "csv_file":
            return self._load_csv(Path(self.source).read_text(encoding="utf-8-sig"))
        if self.source_type == "xlsx_file":
            return self._load_xlsx(Path(self.source).read_bytes())
        if self.source_type == "xlsx_url":
            return self._load_xlsx(self._download_bytes(self.source))
        if self.source_type == "yandex_public_xlsx":
            return self._load_xlsx(self._download_yandex_public_bytes(self.source))
        if self.source_type == "yandex_public_csv":
            return self._load_csv(
                self._download_yandex_public_bytes(self.source).decode("utf-8-sig")
            )
        if self.source_type == "yandex_disk_xlsx":
            return self._load_xlsx(self._download_yandex_disk_oauth_bytes())
        if self.source_type == "yandex_disk_csv":
            return self._load_csv(self._download_yandex_disk_oauth_bytes().decode("utf-8-sig"))
        raise ValueError(f"Unsupported TABLE_SOURCE_TYPE: {self.source_type}")

    def _request_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"User-Agent": "Mozilla/5.0 accounting-max-bot"}
        if extra:
            headers.update(extra)
        return headers

    def _download_bytes(self, url: str, extra_headers: dict[str, str] | None = None) -> bytes:
        request = urllib.request.Request(url, headers=self._request_headers(extra_headers))
        context = ssl.create_default_context()
        if not config.MAX_SSL_VERIFY:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        try:
            with urllib.request.urlopen(request, timeout=30, context=context) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            raise RuntimeError(_format_yandex_http_error(exc)) from exc

    def _yandex_public_download_params(self, public_key: str) -> dict[str, str]:
        params: dict[str, str] = {"public_key": public_key}
        inner_path = config.TABLE_YANDEX_PUBLIC_PATH.strip()
        if inner_path:
            params["path"] = inner_path
        password = config.TABLE_YANDEX_PUBLIC_PASSWORD.strip()
        if password:
            params["password"] = password
        return params

    def _download_yandex_public_bytes(self, public_key: str) -> bytes:
        api_url = (
            "https://cloud-api.yandex.net/v1/disk/public/resources/download?"
            + urllib.parse.urlencode(self._yandex_public_download_params(public_key))
        )
        payload = self._fetch_json(api_url)
        download_url = normalize_cell(payload.get("href", ""))
        if not download_url:
            raise ValueError("Yandex public resource download URL is missing")
        return self._download_bytes(download_url)

    def _download_yandex_disk_oauth_bytes(self) -> bytes:
        token = config.YANDEX_DISK_TOKEN.strip()
        disk_path = config.TABLE_DISK_PATH.strip()
        if not token:
            raise ValueError("YANDEX_DISK_TOKEN is not configured")
        if not disk_path:
            raise ValueError("TABLE_DISK_PATH is not configured")
        auth_headers = {"Authorization": f"OAuth {token}"}
        api_url = (
            "https://cloud-api.yandex.net/v1/disk/resources/download?"
            + urllib.parse.urlencode({"path": disk_path})
        )
        payload = self._fetch_json(api_url, auth_headers)
        download_url = normalize_cell(payload.get("href", ""))
        if not download_url:
            raise ValueError("Yandex Disk OAuth download URL is missing")
        return self._download_bytes(download_url, auth_headers)

    def _fetch_json(self, url: str, extra_headers: dict[str, str] | None = None) -> dict[str, Any]:
        raw = self._download_bytes(url, extra_headers)
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Expected JSON object from Yandex public API")
        return data

    def _load_csv(self, content: str) -> list[SpreadsheetRow]:
        reader = csv.reader(io.StringIO(content))
        rows = list(reader)
        if not rows:
            return []

        headers = [normalize_header(header) for header in rows[0]]
        result: list[SpreadsheetRow] = []
        for row_number, row in enumerate(rows[1:], start=2):
            if not any(normalize_cell(cell) for cell in row):
                continue
            padded = list(row) + [""] * max(0, len(headers) - len(row))
            values = {
                headers[idx]: normalize_cell(padded[idx])
                for idx in range(len(headers))
                if headers[idx]
            }
            result.append(
                SpreadsheetRow(
                    sheet_name=self.sheet_name or "Sheet1",
                    row_number=row_number,
                    values=values,
                )
            )
        return result

    def _load_xlsx(self, content: bytes) -> list[SpreadsheetRow]:
        try:
            workbook = load_workbook(
                filename=io.BytesIO(content),
                data_only=True,
                read_only=True,
            )
            return self._load_xlsx_rows_with_openpyxl(workbook)
        except Exception:
            return self._load_xlsx_rows_with_calamine(content)

    def _load_xlsx_rows_with_openpyxl(self, workbook: Any) -> list[SpreadsheetRow]:
        result: list[SpreadsheetRow] = []
        for worksheet in self._select_openpyxl_worksheets(workbook):
            rows = list(worksheet.iter_rows(values_only=True))
            result.extend(self._build_spreadsheet_rows(worksheet.title, rows))
        return result

    def _load_xlsx_rows_with_calamine(self, content: bytes) -> list[SpreadsheetRow]:
        workbook = load_calamine_workbook(io.BytesIO(content))
        result: list[SpreadsheetRow] = []
        for worksheet_name in self._select_sheet_names(workbook.sheet_names):
            worksheet = workbook.get_sheet_by_name(worksheet_name)
            result.extend(self._build_spreadsheet_rows(worksheet_name, worksheet.to_python()))
        return result

    def _select_openpyxl_worksheets(self, workbook: Any) -> list[Any]:
        selected_names = self._select_sheet_names(tuple(workbook.sheetnames))
        if selected_names:
            return [workbook[name] for name in selected_names]
        return [workbook.active]

    def _select_sheet_names(self, sheet_names: tuple[str, ...] | list[str]) -> list[str]:
        if self.sheet_name:
            return [self.sheet_name]
        month_sheets = [name for name in config.MONTH_SHEET_NAMES if name in sheet_names]
        if month_sheets:
            return month_sheets
        if sheet_names:
            return [sheet_names[0]]
        return []

    def _build_spreadsheet_rows(
        self,
        sheet_name: str,
        rows: list[list[Any]] | list[tuple[Any, ...]],
    ) -> list[SpreadsheetRow]:
        if not rows:
            return []

        headers = [normalize_header(cell) for cell in rows[0]]
        result: list[SpreadsheetRow] = []
        for row_number, row in enumerate(rows[1:], start=2):
            if not any(normalize_cell(cell) for cell in row):
                continue
            padded = list(row) + [""] * max(0, len(headers) - len(row))
            values = {
                headers[idx]: normalize_cell(padded[idx])
                for idx in range(len(headers))
                if headers[idx]
            }
            result.append(
                SpreadsheetRow(
                    sheet_name=sheet_name,
                    row_number=row_number,
                    values=values,
                )
            )
        return result
