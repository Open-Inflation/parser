# FixPrice Pipeline

Этот файл описывает локальный pipeline `FixPriceParser`.

Эта реализация считается эталонной для остальных store-парсеров.

## Входной контракт

В orchestration runtime парсер получает:

- `country_id`
- внешний `store_code`
- флаги `include_images`, `use_product_info`, `strict_validation`

Внешний `store_code` не требует дополнительной подготовки перед началом парсинга категорий.

## Этапы

Порядок этапов должен быть таким:

1. `collect_categories()`
2. `build_catalog_queries(...)`
3. `collect_products_for_queries(...)`
4. `collect_store_info(...)`

Именно под этот порядок заточен worker progress reporting.

## Categories

`collect_categories()`:

- вызывает `api.Catalog.tree()`
- маппит дерево в `Category`
- не зависит от `collect_store_info()`

Если у категории есть дети, они уже приходят в основном дереве.

## Product Queries

`build_catalog_queries()`:

- при `full_catalog=False` берет только верхние категории в рамках `category_limit`
- при `full_catalog=True` предпочитает subcategories
- если у root-категории нет детей, добавляет query на саму root-категорию
- убирает дубликаты по `(category_alias, subcategory_alias)`

## Products

`collect_products_for_queries()`:

- итерирует уже подготовленные query
- отдает progress по query-уровню
- вызывает `collect_products(...)`
- дедуплицирует карточки по `sku`
- сливает `categories_uid`, если товар найден в нескольких query

`collect_products(...)`:

- читает страницу каталога через `api.Catalog.products_list(...)`
- опционально добирает карточку товара через `api.Catalog.Product.info(...)`
- опционально скачивает изображения
- маппит payload в `Card`

Правила маппинга товара:

- `available_count` берется напрямую из `inStock`
- `package_weight_gross` берется только из `variants.dimensions.weight` с fallback на `variants.weight`
- вес из payload переводится из граммов в килограммы
- `package_quantity_net` parser не вычисляет из `title` или `description`
- парсинг фасовки из заголовка и заполнение `package_quantity`/`package_unit` уже делает `../converter/converter/parsers/fixprice/title_parser.py`

Важно:

- `FixPriceMapper` не должен дублировать title-parsing из `converter`
- `strict_validation=True` в `Card` проверяет связку `package_quantity_net`/`package_weight_gross`/`package_unit`, поэтому gross-only payload из FixPrice валиден для обычного parser runtime, но не является strict-contract кейсом

## Store Info

`collect_store_info(...)`:

- вызывается после завершения этапа каталога
- ищет магазины через `api.Geolocation.Shop.search(...)`
- фильтрует их по внешнему `store_code`
- возвращает `RetailUnit`

Важно:

- `collect_store_info()` не подготавливает runtime context для категорий или товаров
- `RetailUnit.code` остается внешним магазинным кодом

## Cities

`collect_cities(...)`:

- получает список городов отдельно
- кешируется по `country_id`
- используется только для enrichment store info

## Правило для остальных парсеров

Если поведение нового парсера отличается от `FixPriceParser`, это должно считаться исключением и документироваться отдельно.
