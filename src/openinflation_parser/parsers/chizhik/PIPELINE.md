# Chizhik Pipeline

Этот файл описывает локальный pipeline `ChizhikParser`.

По внешнему контракту он должен повторять lifecycle `FixPriceParser`, даже если внутри API требует дополнительные шаги.

## Входной контракт

В orchestration runtime парсер получает:

- внешний `store_code`
- флаги `include_images`, `use_product_info`, `strict_validation`

Снаружи в `submit_store` и в worker всегда передается только `store_code`.

## Внутренний runtime context

Для delivery API Chizhik нужен внутренний `sap_id` / `store_id`.

Правило:

- этот id резолвится только внутри `ChizhikParser`
- наружу он не торчит как основной идентификатор магазина
- `RetailUnit.code` должен оставаться исходным внешним `store_code`

Рабочий паттерн:

- конфиг хранит внешний `store_code`
- `collect_categories()` или `collect_products_for_queries()` при необходимости вызывают внутренний `_ensure_store_id()`
- `_ensure_store_id()` резолвит `sap_id` и кэширует его

## Этапы

Порядок этапов должен быть таким же, как у `fixprice`:

1. `collect_categories()`
2. `build_catalog_queries(...)`
3. `collect_products_for_queries(...)`
4. `collect_store_info(...)`

`collect_store_info()` не является предварительным шагом для каталога.

## Categories

`collect_categories()`:

1. лениво резолвит внутренний `store_id` по внешнему `store_code`
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
- использует уже резолвленный внутренний `store_id`
- опционально обогащает товар через `api.Catalog.Product.delivery_info(...)`
- маппит payload в `Card`

## Store Info

`collect_store_info(...)`:

- вызывается после этапа каталога
- ищет магазин по внешнему `store_code`
- при необходимости использует fallback через город
- возвращает `RetailUnit`

Важно:

- внутренний `sap_id` можно сохранить в runtime cache
- но наружу код магазина не должен подменяться `sap_id`

## Что считается ошибкой

Неправильно, если:

- `collect_categories()` не работает без предварительного `collect_store_info()`
- worker перестраивается под `chizhik`-специфику вместо того, чтобы парсер адаптировался внутри себя
- наружу вместо `store_code` начинает возвращаться внутренний `sap_id`
