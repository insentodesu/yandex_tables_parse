"""TABLE_DISK_PATH normalization for Yandex Disk API."""

from config import normalize_yandex_disk_path


def test_normalize_yandex_disk_path_strips_disk_prefix():
    assert normalize_yandex_disk_path("disk:/МаксТаблица.xlsx") == "/МаксТаблица.xlsx"


def test_normalize_yandex_disk_path_leading_slash():
    assert normalize_yandex_disk_path("МаксТаблица.xlsx") == "/МаксТаблица.xlsx"


def test_normalize_yandex_disk_path_unchanged_when_absolute():
    assert normalize_yandex_disk_path("/folder/a.xlsx") == "/folder/a.xlsx"


def test_normalize_yandex_disk_path_empty():
    assert normalize_yandex_disk_path("") == ""
