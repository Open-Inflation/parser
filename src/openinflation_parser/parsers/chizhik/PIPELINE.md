# Chizhik Pipeline

Этот файл описывает локальный pipeline `ChizhikParser`.

По внешнему контракту он должен повторять lifecycle `FixPriceParser`, даже если внутри API требует дополнительные шаги.

## Входной контракт

В orchestration runtime парсер получает:

- внешний `store_code`
- флаги `include_images`, `use_product_info`, `strict_validation`

Снаружи в `submit_store` и в worker всегда передается только `store_code`.

## Идентификатор магазина

Для `ChizhikParser` внешний `store_code` теперь совпадает с `sap_id`.

Правило:

- в orchestration, dashboard и worker для `chizhik` хранится именно `sap_id`
- `delivery_*` эндпоинты получают тот же самый id без дополнительного резолва
- `RetailUnit.code` возвращает этот же `sap_id`

## Этапы

Порядок этапов должен быть таким же, как у `fixprice`:

1. `collect_categories()`
2. `build_catalog_queries(...)`
3. `collect_products_for_queries(...)`
4. `collect_store_info(...)`

`collect_store_info()` не является предварительным шагом для каталога.

## Categories

`collect_categories()`:

1. берет `store_code` из runtime config как готовый `sap_id`
2. вызывает `api.Catalog.delivery_tree(store_id=...)`
3. берет верхние категории
4. для каждой верхней категории добирает детей через `api.Catalog.delivery_tree_extended(...)`
5. маппит результат в `Category`

Для Chizhik подкатегории собираются отдельным методом, а не приходят полностью из одного дерева.

## Product Queries

`build_catalog_queries()`:

- работает уже на готовом дереве категорий
- при `full_catalog=True` предпочитает leaf-узлы
- использует строковые category ids
- убирает дубликаты

## Products

`collect_products_for_queries()`:

- итерирует подготовленные query
- отдает progress по query-уровню
- вызывает внутренний page collector
- дедуплицирует карточки по `plu`/`sku`
- сливает `categories_uid` для дублей

Сбор страницы каталога:

- идет через `api.Catalog.delivery_products_list(...)`
- использует `store_code`, который уже является `sap_id`
- опционально обогащает товар через `api.Catalog.Product.delivery_info(...)`
- маппит payload в `Card`

Правила маппинга товара:

- `available_count` берется из `stock_limit`
- `promo` использует `is_inout` как основной сигнал
- `package_quantity_net`, `package_weight_gross`, `package_unit` из payload Chizhik не заполняются
- package-логика для Chizhik не должна выдумываться по снапшотам `../chizhik_api/tests/__snapshots__`

Снапшот-контракты, на которые реально опирается parser:

- `../chizhik_api/tests/__snapshots__/ClassCatalog.delivery_tree.json`
  - top-level payload: `list`
  - parser берет первый dict-узел и читает у него `categories`
  - для root/child category используются `id`, `name`, опционально `slug`
- `../chizhik_api/tests/__snapshots__/ClassCatalog.delivery_tree_extended.json`
  - top-level payload: `dict`
  - parser читает `categories_tags`
  - дочерние категории в снапшоте имеют минимум `{id, name}`
- `../chizhik_api/tests/__snapshots__/ClassCatalog.delivery_products_list.json`
  - top-level payload: `dict`
  - parser читает `products`
  - product row в снапшоте содержит `plu`, `name`, `uom`, `stock_limit`, `prices`, `image_links`, `has_age_restriction`, `promo`, `rating`
  - в используемом снапшоте нет `categories_tree`, `images`, `meta_data`, поэтому на них нельзя опираться в list-response логике
- `../chizhik_api/tests/__snapshots__/ProductService.delivery_info.json`
  - top-level payload: `dict`
  - используется только для enrichment через `dict.update(...)`
  - parser/mapper читает из него `description`, `ingredients`, `attributes`, `image_links`, `stock_limit`, `prices`, `uom`
  - `prices` здесь приходят списком с `placement_type`, а не dict как в `delivery_products_list`
  - в используемом снапшоте нет `meta_data`, `slug`, `categories_tree`, поэтому эти поля нельзя считать delivery-contract для текущего parser

Важно:

- для логики текущего `ChizhikParser` смотреть нужно именно `ProductService.delivery_info`, а не `ProductService.info`
- `ClassCatalog.products_list.json`, `ProductService.info.json`, `ShopService.search.json`, `ClassGeolocation.cities_list.json` текущим parser не используются и не должны быть источником предположений о delivery-payload

## Store Info

`collect_store_info(...)`:

- вызывается после этапа каталога
- ищет магазин по `sap_id` в `Shop.all()`
- возвращает `RetailUnit`

Снапшот-контракт store info:

- `../chizhik_api/tests/__snapshots__/ShopService.all.json`
  - top-level payload: `list`
  - parser использует `sap_id`, `name`, `locality`, `lat`, `lon`, `average_rating`, `open_date`
  - `open_date` прокидывается только если это каноническая ISO-дата `YYYY-MM-DD`; статусные строки вроде `Скоро открытие!` parser не отправляет дальше
  - `locality` в используемом снапшоте приходит строкой, не объектом
  - `working_hours` в снапшоте есть, но текущий mapper не разворачивает его в `schedule_*`

## Cities

`collect_cities(...)`:

- сейчас возвращает пустой список
- `ClassGeolocation.cities_list.json` текущим parser не используется

## Что считается ошибкой

Неправильно, если:

- `collect_categories()` не работает без предварительного `collect_store_info()`
- worker перестраивается под `chizhik`-специфику вместо того, чтобы парсер адаптировался внутри себя
- `chizhik` снова вводит отдельный промежуточный id поверх `sap_id`
