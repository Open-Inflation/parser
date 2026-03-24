# Parser Pipeline Contract

Этот документ фиксирует рабочий контракт парсеров и ожидаемый порядок этапов в оркестраторе.

Эталон поведения: `FixPriceParser`.

## Цель

Все store-парсеры должны вести себя одинаково на уровне orchestration runtime:

1. Сначала собираются категории.
2. Затем собираются товары по ранее собранным категориям.
3. В конце собирается информация о магазине.

Такое разделение нужно, чтобы воркер мог корректно отчитываться о прогрессе парсинга:
- этап `categories`
- этап `products`
- этап `store_info`

Парсер не должен требовать предварительного вызова `collect_store_info()` для того, чтобы начать собирать категории или каталог.

## Общий контракт

Базовый интерфейс задается в [base.py](/home/admin1/Documents/GitHub/parser/src/openinflation_parser/parsers/base.py).

Каждый парсер обязан реализовать:

- `collect_categories() -> list[Category]`
- `collect_products(category_alias, subcategory_alias=None, page=1, limit=...) -> list[Card]`
- `collect_store_info(country_id=None, region_id=None, store_code=None) -> list[RetailUnit]`
- `collect_cities(country_id=None) -> list[AdministrativeUnit]`

Дополнительно parser runtime использует:

- `build_catalog_queries(...)`
- `collect_products_for_queries(...)`

## Оркестрационный порядок

Эталонный порядок в воркере:

1. `categories = await parser.collect_categories()`
2. `product_queries = parser.build_catalog_queries(categories, ...)`
3. `products = await parser.collect_products_for_queries(product_queries, ...)`
4. `stores = await parser.collect_store_info(store_code=job.store_code, ...)`
5. `store = stores[0].model_copy(update={"categories": selected_categories, "products": products})`

Следствия:

- `collect_categories()` должен работать без предварительного `collect_store_info()`.
- `collect_products_for_queries()` должен работать на основе того же входного контекста, что и `collect_categories()`.
- `collect_store_info()` не управляет жизненным циклом каталога, а только возвращает `RetailUnit`.

## Что считается входным идентификатором магазина

Снаружи оркестратор оперирует только `store_code`.

`store_code`:
- приходит в `submit_store`
- хранится в `WorkerJob.store_code`
- используется в имени выходных файлов и статусах задач
- должен оставаться внешним кодом магазина, а не внутренним API-идентификатором

Если API магазина требует внутренний идентификатор:

- парсер сам резолвит его внутри себя
- этот идентификатор считается внутренним runtime-состоянием
- он не подменяет внешний `store_code` в `RetailUnit.code` и в orchestration-моделях

## Внутреннее состояние парсера

Парсеру разрешено держать внутренний runtime context, если он нужен API-провайдеру:

- resolved `store_id`
- кэши product info
- кэши cities

Но это внутреннее состояние должно:

- вычисляться лениво
- быть доступным уже на этапе `collect_categories()`
- не требовать отдельного orchestration шага

Хороший паттерн:

- конфиг получает внешний `store_code`
- при первом запросе категорий/товаров парсер вызывает внутренний `_ensure_store_id()`
- `_ensure_store_id()` резолвит внутренний id и кэширует его

## Контракт `build_catalog_queries`

`build_catalog_queries()` работает только на уже собранном дереве категорий.

Ожидания:

- если `full_catalog=False`, выбираются только верхние категории в рамках `category_limit`
- если `full_catalog=True`, предпочтение отдается leaf-узлам
- дубликаты должны удаляться
- query должен содержать достаточно данных, чтобы затем корректно обновить `categories_uid`

Эталон здесь тоже `FixPriceParser`.

## Контракт `collect_products_for_queries`

Ожидания:

- принимает готовый список query
- обходит их по очереди
- отдает progress по query-уровню
- умеет дедуплицировать карточки
- умеет сливать `categories_uid`, если один и тот же товар найден в нескольких query

`collect_products_for_queries()` не должен сам повторно строить дерево категорий.

## Специфика Chizhik

Для `ChizhikParser` внешний контракт такой же, как у `FixPriceParser`, даже если API внутри устроен иначе.

Правило:

- наружу передается только `store_code`
- внутренний `sap_id`/`store_id` резолвится внутри парсера
- этапы остаются `categories -> products -> store_info`

Для дерева категорий у Chizhik:

- корневой список категорий берется из `delivery_tree`
- подкатегории для каждой верхней категории добираются отдельным вызовом `delivery_tree_extended`

Для каталога:

- листинг идет через `delivery_products_list`
- карточка товара опционально обогащается через `delivery_info`

`collect_store_info()` в Chizhik не должен быть предварительным шагом для каталога.

## Когда реализация считается неправильной

Неправильно, если:

- `collect_categories()` падает без вызванного ранее `collect_store_info()`
- парсер требует передать внутренний API-id вместо внешнего `store_code`
- `RetailUnit.code` подменяется внутренним `store_id`
- orchestration pipeline меняется под особенности одного магазина вместо того, чтобы магазин адаптировался к общему контракту

## Правило для будущих изменений

Если новый API магазина требует особую подготовку контекста:

1. Сначала проверяем, можно ли спрятать это внутри parser runtime.
2. Если можно, orchestration pipeline не меняем.
3. Если нельзя, нужно отдельно документировать, почему эталонный контракт недостаточен.

По умолчанию любое новое поведение должно копировать lifecycle `FixPriceParser`.
