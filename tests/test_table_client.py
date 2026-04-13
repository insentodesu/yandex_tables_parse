"""Tests for spreadsheet readers."""

import asyncio
import urllib.error
from email.message import Message
from io import BytesIO
from unittest.mock import patch

import config
import table_client
from table_client import TableClient, _format_http_error_response


def test_public_password_param_variants_default():
    client = TableClient()
    with patch.object(config, "TABLE_YANDEX_PUBLIC_PASSWORD", "secret"):
        with patch.object(config, "TABLE_YANDEX_PUBLIC_PATH", ""):
            with patch.object(config, "TABLE_YANDEX_PUBLIC_PASSWORD_PARAM", ""):
                variants = client._yandex_public_download_params_variants("https://disk.yandex.ru/i/k")
    assert len(variants) == 4
    keys_used = []
    for v in variants:
        assert v["public_key"] == "https://disk.yandex.ru/i/k"
        for k in ("password", "pass", "pwd", "link_password"):
            if k in v:
                keys_used.append(k)
                assert v[k] == "secret"
                break
    assert keys_used == ["password", "pass", "pwd", "link_password"]


def test_public_password_param_fixed_key():
    client = TableClient()
    with patch.object(config, "TABLE_YANDEX_PUBLIC_PASSWORD", "x"):
        with patch.object(config, "TABLE_YANDEX_PUBLIC_PASSWORD_PARAM", "mypasskey"):
            variants = client._yandex_public_download_params_variants("https://a")
    assert variants == [{"public_key": "https://a", "mypasskey": "x"}]


def test_merge_url_query():
    u = table_client._merge_url_query("https://h/d?a=b", {"p": "1"})
    assert "a=b" in u and "p=1" in u


def test_format_http_error_403_includes_russian_hint():
    msg = _format_http_error_response(403, b"{}")
    assert "403" in msg
    assert "Проверьте" in msg


def test_download_bytes_retries_on_429(monkeypatch):
    calls = {"n": 0}

    class OkResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return b"ok-body"

    def fake_urlopen(req, timeout=None, context=None):
        calls["n"] += 1
        if calls["n"] < 3:
            msg = Message()
            msg["Retry-After"] = "0"
            raise urllib.error.HTTPError(
                "http://example.invalid",
                429,
                "Too Many Requests",
                msg,
                BytesIO(b'{"error":"too_many_requests"}'),
            )
        return OkResponse()

    monkeypatch.setattr(table_client.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(table_client.time, "sleep", lambda _s: None)

    client = TableClient()
    assert client._download_bytes("http://example.invalid") == b"ok-body"
    assert calls["n"] == 3


def test_csv_file_loading_and_filtering(tmp_path):
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(
        (
            "Дата,Клиент,Бухгалтеру в чат,Номер счета \n"
            "28.03.2026,ЭС,\"Альфа, Счет\",123\n"
            "29.03.2026,Без команды,,124\n"
        ),
        encoding="utf-8",
    )

    client = TableClient(
        source_type="csv_file",
        source=str(csv_path),
        command_column="Бухгалтеру в чат",
    )

    rows = asyncio.run(client.get_rows())
    actionable = asyncio.run(client.get_actionable_rows())

    assert len(rows) == 2
    assert len(actionable) == 1
    assert actionable[0].row_number == 2
    assert actionable[0].sheet_name == "Sheet1"
    assert actionable[0].values["Номер счета"] == "123"


def test_yandex_public_xlsx_resolves_public_link():
    client = TableClient(
        source_type="yandex_public_xlsx",
        source="https://disk.yandex.ru/i/public-key",
        command_column="Бухгалтеру в чат",
    )

    api_calls: list[str] = []

    def fake_fetch_json(url: str):
        api_calls.append(url)
        return {"href": "https://download.example.com/table.xlsx"}

    with patch.object(client, "_fetch_json", side_effect=fake_fetch_json):
        with patch.object(client, "_download_bytes", return_value=b"excel-bytes") as download:
            result = client._download_yandex_public_bytes(client.source)

    assert result == b"excel-bytes"
    assert api_calls == [
        "https://cloud-api.yandex.net/v1/disk/public/resources/download?"
        "public_key=https%3A%2F%2Fdisk.yandex.ru%2Fi%2Fpublic-key"
    ]
    download.assert_called_once_with(
        "https://download.example.com/table.xlsx",
        {"Referer": "https://disk.yandex.ru/i/public-key"},
    )


def test_yandex_public_xlsx_includes_password_and_inner_path():
    client = TableClient(
        source_type="yandex_public_xlsx",
        source="https://disk.yandex.ru/i/public-key",
        command_column="Бухгалтеру в чат",
    )

    api_calls: list[str] = []

    def fake_fetch_json(url: str):
        api_calls.append(url)
        return {"href": "https://download.example.com/table.xlsx"}

    with patch.object(config, "TABLE_YANDEX_PUBLIC_PATH", "/book.xlsx"):
        with patch.object(config, "TABLE_YANDEX_PUBLIC_PASSWORD", "4268"):
            with patch.object(client, "_fetch_json", side_effect=fake_fetch_json):
                with patch.object(client, "_download_bytes", return_value=b"x") as download:
                    client._download_yandex_public_bytes(client.source)

    download.assert_called_once_with(
        "https://download.example.com/table.xlsx",
        {"Referer": "https://disk.yandex.ru/i/public-key"},
    )
    assert api_calls == [
        "https://cloud-api.yandex.net/v1/disk/public/resources/download?"
        "public_key=https%3A%2F%2Fdisk.yandex.ru%2Fi%2Fpublic-key"
        "&path=%2Fbook.xlsx&password=4268"
    ]


def test_xlsx_falls_back_to_calamine():
    client = TableClient(
        source_type="xlsx_url",
        source="https://example.com/table.xlsx",
        command_column="Бухгалтеру в чат",
    )

    with patch("table_client.load_workbook", side_effect=ValueError("broken xlsx")):
        with patch.object(
            client,
            "_load_xlsx_rows_with_calamine",
            return_value=[
                TableClient(source_type="xlsx_url", source="https://example.com/table.xlsx")._build_spreadsheet_rows(
                    "Январь",
                    [
                        ["Дата", "Бухгалтеру в чат"],
                        ["28.03.2026", "Альфа, Счет"],
                    ],
                )[0]
            ],
        ):
            rows = client._load_xlsx(b"fake-bytes")

    assert len(rows) == 1
    assert rows[0].sheet_name == "Январь"
    assert rows[0].values["Бухгалтеру в чат"] == "Альфа, Счет"


def test_xlsx_reads_all_month_sheets():
    client = TableClient(
        source_type="xlsx_url",
        source="https://example.com/table.xlsx",
        command_column="Бухгалтеру в чат",
    )

    rows = client._build_spreadsheet_rows(
        "Январь",
        [
            ["Дата", "Бухгалтеру в чат"],
            ["28.03.2026", "Альфа, Счет"],
        ],
    ) + client._build_spreadsheet_rows(
        "Февраль",
        [
            ["Дата", "Бухгалтеру в чат"],
            ["29.03.2026", "Точка, Счет"],
        ],
    )

    assert [row.sheet_name for row in rows] == ["Январь", "Февраль"]
