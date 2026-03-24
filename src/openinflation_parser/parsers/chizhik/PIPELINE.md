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

## Store Info

`collect_store_info(...)`:

- вызывается после этапа каталога
- ищет магазин по `sap_id` в `Shop.all()`
- возвращает `RetailUnit`

## Что считается ошибкой

Неправильно, если:

- `collect_categories()` не работает без предварительного `collect_store_info()`
- worker перестраивается под `chizhik`-специфику вместо того, чтобы парсер адаптировался внутри себя
- `chizhik` снова вводит отдельный промежуточный id поверх `sap_id`
