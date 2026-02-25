# parser

Существует оркестрационный модуль и суб-модули парсеров.

Структура парсеров:
- `src/openinflation_parser/parsers/fixprice/`
- `src/openinflation_parser/parsers/chizhik/`

Структура оркестратора:
- `src/openinflation_parser/orchestration/cli.py` (CLI и запуск)
- `src/openinflation_parser/orchestration/server.py` (WS server и диспетчер)
- `src/openinflation_parser/orchestration/worker.py` (worker runtime)
- `src/openinflation_parser/orchestration/job_store.py` (история задач + sqlite)
- `src/openinflation_parser/orchestration/requests.py` (pydantic-валидация WS payload)

Парсеры имеют команды:
1. Собрать каталог категорий
2. Собрать товаров
3. Собрать информацию о магазине
4. Собрать информацию о городах

Пайплайн такой:
1. На основе доступной ОЗУ и на основе доступных прокси (или их отцуцтвия) оркестратор выбирает кол-во воркеров (управление через командную строку, важно - они НЕ демоны). Основное управление через вебсокет
2. Парсеры возвращают данные в шаблонизированном виде библиотеки openinflation-dataclass
3. После того как оркестратор получил всю нужную информацию об конкертном магазине, он сохраняет json на диск и запаковывает в .gz архив

## Реализовано

- `openinflation_parser.orchestrator`:
  - выбор числа воркеров по ОЗУ и прокси;
  - пул **non-daemon** воркеров на `multiprocessing.Process`;
  - очередь задач + сбор статусов;
  - pydantic-валидация входящих websocket-команд;
  - lifecycle history задач (TTL + max history) с опциональной sqlite-персистентностью;
  - websocket-управление задачами.
- `openinflation_parser.parsers.fixprice.FixPriceParser`:
  - работает на библиотеке `fixprice_api`;
  - маппит API-контракты в `openinflation_dataclass` (`Category`, `Card`, `AdministrativeUnit`, `RetailUnit`).
- `openinflation_parser.parsers.chizhik.ChizhikParser`:
  - работает на библиотеке `chizhik_api`;
  - маппит API-контракты в `openinflation_dataclass` (`Category`, `Card`, `AdministrativeUnit`, `RetailUnit`).
- `openinflation_parser.parsers.perekrestok.PerekrestokParser`:
  - работает на библиотеке `perekrestok_api`;
  - маппит API-контракты в `openinflation_dataclass` (`Category`, `Card`, `AdministrativeUnit`, `RetailUnit`).

## Установка

```bash
pip install -e .
```

Также можно установить зависимости через `requirements.txt`.

## Запуск оркестратора

```bash
python -m openinflation_parser.orchestrator \
  --host 127.0.0.1 \
  --port 8765 \
  --parser fixprice \
  --output-dir ./output \
  --auth-password changeme \
  --country-id 2 \
  --full-catalog \
  --max-pages-per-category 200 \
  --api-timeout-ms 120000 \
  --ram-per-worker-gb 1.5 \
  --log-level INFO \
  --proxy-file ./proxies.txt
```

Для подробного трассинга можно использовать `--log-level DEBUG`.

Важно: оркестратор в этом режиме только поднимает воркеры и ждёт задачи по WebSocket (`submit_store`).
Чтобы запустить парсинг сразу при старте, передайте `--bootstrap-store-code <PFM>`.
В режиме `--full-catalog` парсер обходит только leaf-категории (subcategories), а root-категория запрашивается только если у неё нет детей.

Полезные флаги:
- `--strict-validation` — включить строгую pydantic-валидацию mapped моделей (для CI/dev).
- `--jobs-max-history` — лимит terminal jobs в памяти/БД.
- `--jobs-retention-sec` — TTL terminal jobs.
- `--jobs-db-path` — путь к sqlite с состоянием задач (`""` чтобы отключить).
- `--auth-password` — статический пароль для websocket-команд (если задан, обязателен в каждом payload как `password`).
- `--download-host` / `--download-port` — где поднимать FastAPI endpoint загрузки.
- `--download-url-ttl-sec` — время жизни подписанной ссылки.
- `--download-secret` — HMAC secret для подписи ссылок (если не передан, генерируется при старте).

Пример для Чижика:
```bash
openinflation-orchestrator \
  --parser chizhik \
  --host 127.0.0.1 \
  --port 8765 \
  --output-dir ./output \
  --include-images \
  --max-pages-per-category 200 \
  --bootstrap-store-code moskva \
  --log-level DEBUG
```

Пример для Перекрёстка:
```bash
openinflation-orchestrator \
  --parser perekrestok \
  --host 127.0.0.1 \
  --port 8765 \
  --output-dir ./output \
  --city-id 81 \
  --include-images \
  --max-pages-per-category 200 \
  --bootstrap-store-code 1 \
  --log-level DEBUG
```

Пример для FixPrice:
```bash

openinflation-orchestrator \
  --parser fixprice \
  --host 127.0.0.1 \
  --port 8765 \
  --output-dir ./output \
  --city-id 3 \
  --include-images \
  --max-pages-per-category 200 \
  --bootstrap-store-code C001 \
  --log-level DEBUG
```

### Основные websocket actions

Если оркестратор запущен с `--auth-password`, добавляйте `"password":"<your-password>"` в каждый payload.

- `{"action":"ping","password":"<your-password>"}`
- `{"action":"submit_store","store_code":"C001","city_id":3,"password":"<your-password>"}`
- `{"action":"submit_store","store_code":"C001","city_id":3,"full_catalog":true,"max_pages_per_category":200,"products_per_page":27,"api_timeout_ms":120000,"password":"<your-password>"}`
- `{"action":"submit_store","parser":"chizhik","store_code":"moskva","full_catalog":true,"max_pages_per_category":200,"api_timeout_ms":120000,"password":"<your-password>"}`
- `{"action":"submit_store","parser":"perekrestok","store_code":"1","city_id":81,"full_catalog":true,"max_pages_per_category":200,"api_timeout_ms":120000,"password":"<your-password>"}`
- `{"action":"status","password":"<your-password>"}`
- `{"action":"status","job_id":"<id>","password":"<your-password>"}`
- `{"action":"jobs","password":"<your-password>"}`
- `{"action":"workers","password":"<your-password>"}`
- `{"action":"stream_job_log","job_id":"<id>","tail_lines":200,"password":"<your-password>"}`
- `{"action":"shutdown","password":"<your-password>"}`

`stream_job_log` работает как потоковый websocket-action: после запроса сервер отправляет события
`waiting` -> `snapshot` -> `append` -> `end` (или `error`). Параметр `tail_lines` задает, сколько
последних строк отдать в первом `snapshot` (по умолчанию 200, максимум 5000).

Для успешной задачи в `status/jobs` возвращаются:
- `output_json`, `output_gz`
- `download_url` (подписанный URL FastAPI `/download`)
- `download_sha256` (контрольная сумма архива `.tar.gz`)
- `download_expires_at` (UTC, до какого момента URL валиден)

После истечения `download_expires_at` оркестратор удаляет файлы `output_json/output_gz/output_worker_log`
и очищает download-поля у задачи, чтобы не накапливался файловый кэш.

Результат задачи сохраняется файлами `<store_code>_<timestamp>.json` и `<store_code>_<timestamp>.tar.gz` в `output_dir`.
Архив содержит `meta.json`, `worker.log` (лог процесса воркера по этой задаче) и каталог `images/`; поля изображений в `meta.json` содержат относительные пути до файлов внутри архива.
Материализация изображений в архив выполняется при запуске с `--include-images`.
Расширение файлов изображений определяется по сигнатуре содержимого (`jpg/png/webp/...`).
Если вместо изображения приходит HTML/ошибка WAF, файл не сохраняется, а в `meta.json` ставится `null`/пусто.
