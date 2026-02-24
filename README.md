# parser

Существует оркестрационный модуль и суб-модули парсеров.

Структура парсеров:
- `src/openinflation_parser/parsers/fixprice/`
- `src/openinflation_parser/parsers/chizhik/`

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
  - websocket-управление задачами.
- `openinflation_parser.parsers.fixprice.FixPriceParser`:
  - работает на библиотеке `fixprice_api`;
  - маппит API-контракты в `openinflation_dataclass` (`Category`, `Card`, `AdministrativeUnit`, `RetailUnit`).
- `openinflation_parser.parsers.chizhik.ChizhikParser`:
  - работает на библиотеке `chizhik_api`;
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
  --country-id 2 \
  --full-catalog \
  --max-pages-per-category 200 \
  --api-timeout-ms 120000 \
  --request-retries 5 \
  --request-retry-backoff-sec 2 \
  --ram-per-worker-gb 1.5 \
  --log-level INFO \
  --proxy-file ./proxies.txt
```

Для подробного трассинга можно использовать `--log-level DEBUG`.

Важно: оркестратор в этом режиме только поднимает воркеры и ждёт задачи по WebSocket (`submit_store`).
Чтобы запустить парсинг сразу при старте, передайте `--bootstrap-store-code <PFM>`.
В режиме `--full-catalog` парсер обходит только leaf-категории (subcategories), а root-категория запрашивается только если у неё нет детей.

Пример для Чижика:
```bash
openinflation-orchestrator \
  --parser chizhik \
  --host 127.0.0.1 \
  --port 8765 \
  --output-dir ./output \
  --full-catalog \
  --max-pages-per-category 200 \
  --bootstrap-store-code moskva \
  --log-level DEBUG
```

### Основные websocket actions

- `{"action":"ping"}`
- `{"action":"submit_store","store_code":"C001","city_id":3}`
- `{"action":"submit_store","store_code":"C001","city_id":3,"full_catalog":true,"max_pages_per_category":200,"products_per_page":27,"api_timeout_ms":120000,"request_retries":5}`
- `{"action":"submit_store","parser":"chizhik","store_code":"moskva","full_catalog":true,"max_pages_per_category":200,"api_timeout_ms":120000,"request_retries":5}`
- `{"action":"status"}`
- `{"action":"status","job_id":"<id>"}`
- `{"action":"jobs"}`
- `{"action":"workers"}`
- `{"action":"shutdown"}`

Результат успешной задачи: файлы `<store_code>_<timestamp>.json` и `<store_code>_<timestamp>.json.gz` в каталоге `output_dir`.
