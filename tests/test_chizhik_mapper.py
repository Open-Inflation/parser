from __future__ import annotations

import asyncio
from typing import Any

import pytest
from chizhik_api import ChizhikAPI
from openinflation_dataclass import AdministrativeUnit, Card

from openinflation_parser.parsers.chizhik import (
    CatalogProductsQuery,
    ChizhikMapper,
    ChizhikParser,
)


def _to_str(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return None


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _to_int(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def _attribute_value(product: dict[str, Any], name: str) -> str | None:
    attributes = product.get("attributes")
    if not isinstance(attributes, list):
        return None
    normalized = name.strip().lower()
    for item in attributes:
        if not isinstance(item, dict):
            continue
        item_name = item.get("name")
        item_value = item.get("value")
        if not isinstance(item_name, str) or not isinstance(item_value, str):
            continue
        if item_name.strip().lower() == normalized:
            return item_value
    return None


def _price_components(product: dict[str, Any]) -> tuple[float | None, float | None]:
    prices = product.get("prices")
    regular: float | None = None
    discount: float | None = None
    if isinstance(prices, dict):
        regular = _to_float(prices.get("regular"))
        discount = _to_float(prices.get("discount"))
    elif isinstance(prices, list):
        for item in prices:
            if not isinstance(item, dict):
                continue
            placement_type = item.get("placement_type")
            value = _to_float(item.get("value"))
            if value is None:
                continue
            if placement_type == "regular_secondary" and regular is None:
                regular = value
            elif placement_type == "promotional_primary" and discount is None:
                discount = value
        if discount is None and prices and isinstance(prices[0], dict):
            discount = _to_float(prices[0].get("value"))
    effective_price = discount if discount is not None else regular
    discount_price = regular if regular is not None and discount is not None and regular != discount else None
    return effective_price, discount_price


def _producer_country_from_attribute(value: Any) -> str | None:
    token = _to_str(value)
    if token is None:
        return None
    normalized = token.strip().lower()
    if normalized in {"россия", "russia", "rus"}:
        return "RUS"
    return None


def _expected_from_product_payload(product: dict[str, Any]) -> dict[str, Any]:
    rating = product.get("rating")
    rating_average = rating.get("rating_average") if isinstance(rating, dict) else rating
    reviews_count = rating.get("reviews_count") if isinstance(rating, dict) else product.get("reviews_count")
    price, discount_price = _price_components(product)
    return {
        "plu": _to_str(product.get("plu")),
        "source_page_url": None,
        "title": _to_str(product.get("name") or product.get("title")),
        "description": _to_str(product.get("description")),
        "adult": product.get("has_age_restriction"),
        "promo": discount_price is not None or product.get("promo") is not None,
        "rating": _to_float(rating_average),
        "reviews_count": _to_int(reviews_count),
        "price": price,
        "discount_price": discount_price,
        "unit_net": ChizhikMapper._unit_from_raw(product.get("uom") or product.get("base_unit")),
        "brand": _attribute_value(product, "Бренд"),
        "producer_name": _attribute_value(product, "Производитель"),
        "producer_country": _producer_country_from_attribute(
            _attribute_value(product, "Страна производства")
        ),
        "composition": _to_str(product.get("ingredients")),
    }


def _delivery_leaf_ids(categories: list[dict[str, Any]]) -> list[str]:
    leaf_ids: list[str] = []
    for category in categories:
        if not isinstance(category, dict):
            continue
        category_id = _to_str(category.get("id"))
        if category_id is not None:
            leaf_ids.append(category_id)
    return leaf_ids


def _collect_chizhik_live_payloads() -> dict[str, Any]:
    async def _collect() -> dict[str, Any]:
        async with ChizhikAPI(headless=True, timeout_ms=45000) as api:
            async def _request_json(
                *,
                operation: str,
                call: Any,
                retries: int = 3,
            ) -> Any:
                last_error: Exception | None = None
                for attempt in range(1, retries + 1):
                    try:
                        response = await call()
                        return response.json()
                    except Exception as exc:
                        last_error = exc
                        if attempt >= retries:
                            raise
                        await asyncio.sleep(float(1.5 * (2 ** (attempt - 1))))
                if last_error is not None:
                    raise last_error
                raise RuntimeError(f"Live request failed without error: {operation}")

            shops = await _request_json(
                operation="geo.shop.search",
                call=lambda: api.Geolocation.Shop.search(query="Москва"),
            )
            if not isinstance(shops, list) or not shops:
                raise RuntimeError("Chizhik Shop.search returned no stores.")
            first_shop = next((shop for shop in shops if isinstance(shop, dict)), None)
            if first_shop is None:
                raise RuntimeError("Chizhik Shop.search returned invalid payload.")
            store_id = _to_str(first_shop.get("sap_id"))
            if store_id is None:
                raise RuntimeError("Chizhik Shop.search did not return sap_id.")

            tree = await _request_json(
                operation=f"catalog.delivery_tree[{store_id}]",
                call=lambda: api.Catalog.delivery_tree(store_id=store_id),
            )
            if not isinstance(tree, list) or not tree:
                raise RuntimeError("Chizhik delivery_tree returned empty payload.")
            root = next((node for node in tree if isinstance(node, dict)), None)
            if root is None:
                raise RuntimeError("Chizhik delivery_tree returned invalid payload.")
            categories = root.get("categories")
            if not isinstance(categories, list) or not categories:
                raise RuntimeError("Chizhik delivery_tree returned empty categories.")
            first_node = next((node for node in categories if isinstance(node, dict)), None)
            if first_node is None:
                raise RuntimeError("Chizhik delivery_tree returned invalid category node.")

            extended = await _request_json(
                operation=f"catalog.delivery_tree_extended[{store_id}:{first_node['id']}]",
                call=lambda: api.Catalog.delivery_tree_extended(
                    store_id=store_id,
                    category_alias=str(first_node["id"]),
                ),
            )
            if not isinstance(extended, dict):
                raise RuntimeError("Chizhik delivery_tree_extended response is not an object.")
            children = extended.get("categories_tags")
            if not isinstance(children, list):
                children = []
            enriched_first_node = dict(first_node)
            enriched_first_node["children"] = children

            products_payload: dict[str, Any] | None = None
            product_row: dict[str, Any] | None = None
            used_category_id: str | None = None
            for category_id in _delivery_leaf_ids(children or categories):
                payload = await _request_json(
                    operation=f"catalog.delivery_products_list[{store_id}:{category_id}]",
                    call=lambda category_id=category_id: api.Catalog.delivery_products_list(
                        store_id=store_id,
                        category_alias=category_id,
                        offset=0,
                        limit=50,
                    ),
                )
                if not isinstance(payload, dict):
                    continue
                items = payload.get("products")
                if not isinstance(items, list) or not items:
                    continue
                first = items[0]
                if not isinstance(first, dict):
                    continue
                products_payload = payload
                product_row = first
                used_category_id = category_id
                break
            if product_row is None or used_category_id is None or products_payload is None:
                raise RuntimeError("Unable to fetch live Chizhik delivery product for tests.")

            product_id = _to_int(product_row.get("plu"))
            if product_id is None:
                raise RuntimeError("Chizhik delivery product plu is missing.")
            product_info = await _request_json(
                operation=f"catalog.product.delivery_info[{store_id}:{product_id}]",
                call=lambda: api.Catalog.Product.delivery_info(
                    store_id=store_id,
                    product_id=product_id,
                ),
            )
            if not isinstance(product_info, dict):
                raise RuntimeError("Chizhik Product.delivery_info response is not an object.")

            return {
                "store_id": store_id,
                "tree": categories,
                "first_node": enriched_first_node,
                "products_payload": products_payload,
                "product_row": product_row,
                "product_info": product_info,
                "used_category_id": used_category_id,
            }

    return asyncio.run(_collect())


@pytest.fixture(scope="module")
def chizhik_live_payloads() -> dict[str, Any]:
    return _collect_chizhik_live_payloads()


def test_map_category_node_from_live_response(chizhik_live_payloads: dict[str, Any]) -> None:
    node = chizhik_live_payloads["first_node"]
    mapped = ChizhikMapper.map_category_node(node)

    assert mapped.uid == str(node["id"])
    assert mapped.alias == str(node["id"])
    assert mapped.title == node["name"]
    assert isinstance(mapped.children, list)


def test_map_product_from_live_list_response(chizhik_live_payloads: dict[str, Any]) -> None:
    product = chizhik_live_payloads["product_row"]
    expected = _expected_from_product_payload(product)

    mapped = ChizhikMapper.map_product(
        product,
        main_image="images/00001/main.bin",
        gallery_images=["images/00001/gallery_001.bin"],
    )

    assert mapped.sku is None
    assert mapped.plu == expected["plu"]
    assert mapped.source_page_url == expected["source_page_url"]
    assert mapped.title == expected["title"]
    assert mapped.description == expected["description"]
    assert mapped.adult == expected["adult"]
    assert mapped.promo == expected["promo"]
    assert mapped.rating == expected["rating"]
    assert mapped.reviews_count == expected["reviews_count"]
    assert mapped.price == expected["price"]
    assert mapped.discount_price == expected["discount_price"]
    assert mapped.unit_net == expected["unit_net"]
    assert mapped.main_image == "images/00001/main.bin"
    assert mapped.images is not None
    assert mapped.images[0] == "images/00001/gallery_001.bin"


def test_map_product_from_live_info_response(chizhik_live_payloads: dict[str, Any]) -> None:
    product = chizhik_live_payloads["product_info"]
    expected = _expected_from_product_payload(product)

    mapped = ChizhikMapper.map_product(product)

    assert mapped.plu == expected["plu"]
    assert mapped.source_page_url == expected["source_page_url"]
    assert mapped.title == expected["title"]
    assert mapped.description == expected["description"]
    assert mapped.adult == expected["adult"]
    assert mapped.promo == expected["promo"]
    assert mapped.rating == expected["rating"]
    assert mapped.reviews_count == expected["reviews_count"]
    assert mapped.price == expected["price"]
    assert mapped.discount_price == expected["discount_price"]
    assert mapped.unit_net == expected["unit_net"]
    assert mapped.brand == expected["brand"]
    assert mapped.producer_name == expected["producer_name"]
    assert mapped.producer_country == expected["producer_country"]
    assert mapped.composition == expected["composition"]


def test_build_catalog_queries_full_mode_prefers_leaf_categories(
    chizhik_live_payloads: dict[str, Any],
) -> None:
    parser = ChizhikParser()
    categories = [
        ChizhikMapper.map_category_node(node)
        for node in chizhik_live_payloads["tree"]
        if isinstance(node, dict)
    ]

    queries = parser.build_catalog_queries(
        categories,
        full_catalog=True,
        category_limit=1,
    )

    roots_with_children = {
        category.uid
        for category in categories
        if category.uid is not None and category.children
    }
    query_ids = {query.category_id for query in queries}

    assert queries
    assert query_ids.isdisjoint(roots_with_children)


def test_map_product_source_page_url_strips_slug_slashes() -> None:
    mapped = ChizhikMapper.map_product(
        {
            "plu": "12345",
            "slug": "/test-product-12345/",
            "title": "Тестовый товар",
        }
    )

    assert mapped.source_page_url == "https://chizhik.club/product/test-product--12345/"


def test_map_product_source_page_url_does_not_duplicate_plu_suffix() -> None:
    mapped = ChizhikMapper.map_product(
        {
            "plu": "4303471",
            "slug": "voda-aldaya-essentuki-tselebnaya-mineralnaya-leche--4303471",
            "title": "Вода",
        }
    )

    assert (
        mapped.source_page_url
        == "https://chizhik.club/product/voda-aldaya-essentuki-tselebnaya-mineralnaya-leche--4303471/"
    )


def test_map_product_source_page_url_handles_slug_with_trailing_dash() -> None:
    mapped = ChizhikMapper.map_product(
        {
            "plu": "4303471",
            "slug": "voda-aldaya-essentuki-tselebnaya-mineralnaya-leche-",
            "title": "Вода",
        }
    )

    assert (
        mapped.source_page_url
        == "https://chizhik.club/product/voda-aldaya-essentuki-tselebnaya-mineralnaya-leche--4303471/"
    )


def test_collect_products_for_queries_merges_categories_uid_for_duplicate_key() -> None:
    parser = ChizhikParser()

    async def _fake_collect_products_page(
        *,
        query: CatalogProductsQuery,
        page: int,
    ) -> tuple[list[Card], int | None]:
        if page > 1:
            return [], 1
        return [Card.model_construct(sku=None, plu="PLU-1", categories_uid=["base"])], 1

    parser._collect_products_page = _fake_collect_products_page  # type: ignore[method-assign]
    queries = [
        CatalogProductsQuery(category_id="1", category_uid="1", category_slug="cat-1"),
        CatalogProductsQuery(category_id="2", category_uid="2", category_slug="cat-2"),
    ]

    cards = asyncio.run(
        parser.collect_products_for_queries(
            queries,
            page_limit=10,
            items_per_page=100,
        )
    )

    assert len(cards) == 1
    assert cards[0].plu == "PLU-1"
    assert cards[0].categories_uid == ["base", "1", "2"]


def test_collect_store_info_live_request() -> None:
    async def _collect() -> list[Any]:
        parser = ChizhikParser()
        async with parser:
            return await parser.collect_store_info(store_code="moskva")

    stores = asyncio.run(_collect())
    assert len(stores) == 1
    assert stores[0].code == "moskva"


def test_collect_store_info_without_store_code_returns_city_virtual_stores() -> None:
    parser = ChizhikParser()

    async def _fake_collect_cities(*, country_id: int | None = None) -> list[AdministrativeUnit]:
        del country_id
        return [
            AdministrativeUnit.model_construct(alias="moskva", name="Москва", latitude=55.75, longitude=37.61),
            AdministrativeUnit.model_construct(alias="spb", name="Санкт-Петербург", latitude=59.94, longitude=30.31),
        ]

    parser.collect_cities = _fake_collect_cities  # type: ignore[method-assign]
    stores = asyncio.run(parser.collect_store_info(store_code=None))
    assert len(stores) == 2
    assert {store.code for store in stores} == {"moskva", "spb"}
    assert stores[0].address is None


def test_collect_products_page_uses_product_info_with_cache(
    chizhik_live_payloads: dict[str, Any],
) -> None:
    parser = ChizhikParser()
    parser._effective_store_id = chizhik_live_payloads["store_id"]
    products_payload = chizhik_live_payloads["products_payload"]
    info_payload = chizhik_live_payloads["product_info"]

    class _Response:
        def __init__(self, payload: Any):
            self._payload = payload

        def json(self) -> Any:
            return self._payload

    calls: dict[str, int] = {"info": 0}
    list_item = dict(chizhik_live_payloads["product_row"])

    class _ProductService:
        async def delivery_info(self, store_id: str, product_id: int) -> _Response:
            assert store_id == chizhik_live_payloads["store_id"]
            assert product_id == info_payload["plu"]
            calls["info"] += 1
            return _Response(info_payload)

    class _Catalog:
        Product = _ProductService()

        async def delivery_products_list(
            self,
            *,
            store_id: str,
            category_alias: str,
            offset: int = 0,
            limit: int = 499,
        ) -> _Response:
            del category_alias
            assert store_id == chizhik_live_payloads["store_id"]
            assert limit == 499
            assert offset in {0, 499}
            return _Response({"products": [dict(list_item)] if offset in {0, 499} else []})

    class _Api:
        Catalog = _Catalog()

    parser._api = _Api()  # type: ignore[assignment]

    query = CatalogProductsQuery(category_id="88", category_uid="88", category_slug="shokolad")
    first_cards, total_pages = asyncio.run(parser._collect_products_page(query=query, page=1))
    second_cards, _ = asyncio.run(parser._collect_products_page(query=query, page=2))

    assert total_pages is not None
    assert len(first_cards) == 1
    assert len(second_cards) == 1
    assert first_cards[0].reviews_count == info_payload["rating"]["reviews_count"]
    assert first_cards[0].producer_name == _expected_from_product_payload(info_payload)["producer_name"]
    assert calls["info"] == 1


def test_collect_products_page_skips_product_info_when_disabled() -> None:
    parser = ChizhikParser()
    parser.config.use_product_info = False
    parser._effective_store_id = "HAOJ"

    class _Response:
        def __init__(self, payload: Any):
            self._payload = payload

        def json(self) -> Any:
            return self._payload

    calls: dict[str, int] = {"info": 0}
    list_item = {
        "plu": 12345,
        "name": "Demo product",
        "uom": "шт",
        "prices": {"regular": "111.0", "discount": None},
        "image_links": {"normal": ["https://example.test/image.jpeg"]},
    }

    class _ProductService:
        async def delivery_info(self, store_id: str, product_id: int) -> _Response:
            del store_id
            del product_id
            calls["info"] += 1
            return _Response({})

    class _Catalog:
        Product = _ProductService()

        async def delivery_products_list(
            self,
            *,
            store_id: str,
            category_alias: str,
            offset: int = 0,
            limit: int = 499,
        ) -> _Response:
            del store_id
            del category_alias
            del limit
            assert offset == 0
            return _Response({"products": [dict(list_item)]})

    class _Api:
        Catalog = _Catalog()

    parser._api = _Api()  # type: ignore[assignment]

    query = CatalogProductsQuery(category_id="88", category_uid="88", category_slug="demo")
    cards, total_pages = asyncio.run(parser._collect_products_page(query=query, page=1))

    assert total_pages is not None
    assert len(cards) == 1
    assert cards[0].plu == "12345"
    assert cards[0].reviews_count is None
    assert calls["info"] == 0
