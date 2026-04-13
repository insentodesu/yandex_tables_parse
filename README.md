# Accounting MAX Bot

Python service that polls the working sheet every 15 seconds and sends accountant notifications to a MAX chat based on the value in `Бухгалтеру в чат`.

## What is included

- One-time template builder for migrating the source Google Sheet structure into Yandex Tables.
- Polling service that reads exported CSV/XLSX data from the migrated table.
- MAX sender built on top of `maxapi`.
- SQLite deduplication so filled cells stay untouched in the table.

## Supported commands

- `Альфа, Счет, Маршрут`
- `Альфа, Счет, УПД, Маршрут`
- `Альфа, Счет`
- `Альфа, Счет, УПД`
- `Точка, Счет, Маршрут`
- `Точка, Счет, УПД, Маршрут`
- `Точка, Счет`
- `Точка, Счет, УПД`
- `ИП Точка, Счет, Маршрут`
- `ИП Точка, Счет, Акт, Маршрут`
- `УПД к Счету`
- `Точка Полная Инф`

## One-time migration

1. Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2. Generate the import template from the source Google Sheet:

```bash
python prepare_yandex_template.py
```

3. Import `templates/yandex_accounting_template.xlsx` into Yandex Tables.
4. In Yandex Tables, verify the structure and keep the dropdown in `Бухгалтеру в чат`.
5. Decide how the bot will read the table after migration:
   - `TABLE_SOURCE_TYPE=yandex_public_xlsx` for a public Yandex Table link like `https://disk.yandex.ru/i/...`
   - `TABLE_SOURCE_TYPE=yandex_public_csv` for a public Yandex CSV resource
   - For a **password-protected** public link, set `TABLE_YANDEX_PUBLIC_PASSWORD` (and `TABLE_YANDEX_PUBLIC_PATH` if the link points to a folder and the file sits at a path inside it, e.g. `/book.xlsx`). The Disk REST docs do not name the password query key; the bot tries several (`password`, `pass`, …) on the public API and on the CDN URL. Override with `TABLE_YANDEX_PUBLIC_PASSWORD_PARAM` if you know the right key.
   - **Without OAuth:** if the share has **download disabled**, the public flow may still fail (403) — then either allow downloads on the link (password can stay), use **`xlsx_file`** / **`csv_file`** with a path to a copy synced to the server (e.g. Desktop app), or use OAuth (`yandex_disk_*`).
   - If the owner **disabled downloading** on the public link, the public API may stop working; use `TABLE_SOURCE_TYPE=yandex_disk_xlsx` or `yandex_disk_csv` with `YANDEX_DISK_TOKEN` and `TABLE_DISK_PATH` (path on your Disk, e.g. `disk:/folder/file.xlsx`). The bot requests a download URL over OAuth and then loads the file using the same token, as required by the Disk API.
   - `TABLE_SOURCE_TYPE=csv_url` for a published CSV export URL
   - `TABLE_SOURCE_TYPE=xlsx_url` for a published XLSX export URL
   - `TABLE_SOURCE_TYPE=csv_file` for a synced local CSV file
   - `TABLE_SOURCE_TYPE=xlsx_file` for a synced local XLSX file

## Bot setup

1. Copy `.env.example` to `.env` and fill in:
   - `MAX_BOT_TOKEN`
   - `MAX_CHAT_ID`
   - `TABLE_SOURCE_TYPE`
   - `TABLE_SOURCE`
   - For Yandex public links with a password: `TABLE_YANDEX_PUBLIC_PASSWORD`, and `TABLE_YANDEX_PUBLIC_PATH` when needed.
   - For OAuth Disk access: `YANDEX_DISK_TOKEN`, `TABLE_DISK_PATH`, and `TABLE_SOURCE_TYPE=yandex_disk_xlsx` or `yandex_disk_csv`. `TABLE_SOURCE` is not required in this mode. Get a token: create an app at [oauth.yandex.com/client/new](https://oauth.yandex.com/client/new/), enable **cloud_api:disk.read**, then open `https://oauth.yandex.ru/authorize?response_type=token&client_id=<ClientID>` and copy `access_token` from the URL fragment (see [Disk API quickstart](https://yandex.com/dev/disk-api/doc/en/concepts/quickstart)).
2. Start the bot:

```bash
. .venv/bin/activate
python run.py
```

## systemd

Deployment examples are in `deploy/run-accounting-max.sh` and `deploy/accounting-max.service`.

### Updating on the server

If `git pull` reports **divergent branches**, align with GitHub and redeploy:

```bash
cd ~/yandex_tables_parse
git fetch origin
git reset --hard origin/main
sudo systemctl restart accounting-max
```

After a successful update, errors from Yandex should appear as `RuntimeError: Yandex Disk API HTTP ...` in the log, not a bare `urllib.error.HTTPError`.

## Notes

- The bot does not clear the command cell after sending.
- Deduplication is based on a hash of the command and row payload.
- If row content changes, the hash changes too and the bot can resend the notification.
- Yandex Disk may return **HTTP 429** if the table is polled too often (each cycle does several requests). The client retries with backoff and honors `Retry-After`. Raise `POLL_INTERVAL_SECONDS` (for example 60–120) if limits persist. After a failed cycle with 429, the scheduler sleeps an extra `RATE_LIMIT_COOLDOWN_SECONDS` (default 90; set to `0` to disable).
- **HTTP 403** on a public link often means a wrong/missing `TABLE_YANDEX_PUBLIC_PASSWORD`, a revoked link, or **download disabled** on the share (the public API then refuses the file). Use `yandex_disk_xlsx` with `YANDEX_DISK_TOKEN` and `TABLE_DISK_PATH` as the owner. The bot sends a normal browser `User-Agent` and `Referer: TABLE_SOURCE` on the CDN download step to match what Yandex expects.
