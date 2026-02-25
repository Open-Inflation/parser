from __future__ import annotations

import asyncio
from typing import Any, Iterator

import pytest
from chizhik_api import ChizhikAPI
from openinflation_dataclass import Card

from openinflation_parser.parsers.chizhik import (
    CatalogProductsQuery,
    ChizhikMapper,
    ChizhikParser,
)


def _iter_leaf_category_ids(nodes: list[dict[str, Any]]) -> Iterator[int]:
    def walk(node: dict[str, Any]) -> Iterator[int]:
        node_id = node.get("id")
        raw_children = node.get("children")
        children = [child for child in raw_children if isinstance(child, dict)] if isinstance(raw_children, list) else []
        if isinstance(node_id, int) and not children:
            yield node_id
            return
        for child in children:
            yield from walk(child)

    for node in nodes:
        if isinstance(node, dict):
            yield from walk(node)


def _metadata_by_code(product: dict[str, Any]) -> dict[str, int | float | str]:
    raw_meta = product.get("meta_data")
    if not isinstance(raw_meta, list):
        return {}
    prepared: dict[str, int | float | str] = {}
    for item in raw_meta:
        if not isinstance(item, dict):
            continue
        code = item.get("code")
        value = item.get("value")
        if not isinstance(code, str):
            continue
        if isinstance(value, bool):
            continue
        if not isinstance(value, (int, float, str)):
            continue
        prepared.setdefault(code, value)
    return prepared


def _to_int(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _to_str(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    return None


def _producer_country_from_meta(value: Any) -> str | None:
    token = _to_str(value)
    if token is None:
        return None
    normalized = token.strip().lower()
    if normalized in {"россия", "russia", "rus"}:
        return "RUS"
    return None


def _expected_from_product_payload(product: dict[str, Any]) -> dict[str, Any]:
    meta = _metadata_by_code(product)
    return {
        "plu": _to_str(product.get("plu")),
        "title": _to_str(product.get("title")),
        "description": _to_str(product.get("description")),
        "adult": _to_bool(product.get("is_adults")),
        "promo": _to_bool(product.get("is_inout")),
        "rating": _to_float(product.get("rating")),
        "reviews_count": _to_int(product.get("reviews_count")),
        "price": _to_float(product.get("price")),
        "unit": ChizhikMapper._unit_from_raw(product.get("base_unit")),
        "brand": _to_str(meta.get("brand_name")),
        "producer_name": _to_str(meta.get("producer_name")),
        "producer_country": _producer_country_from_meta(meta.get("country")),
        "composition": _to_str(meta.get("composition")),
        "expiration_date_in_days": _to_int(meta.get("exp_date_days")),
    }


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

            tree = await _request_json(
                operation="catalog.tree",
                call=lambda: api.Catalog.tree(),
            )
            if not isinstance(tree, list):
                raise RuntimeError("Chizhik categories response is not a list.")
            top_nodes = [node for node in tree if isinstance(node, dict)]
            if not top_nodes:
                raise RuntimeError("Chizhik category tree is empty.")
            first_node = top_nodes[0]

            products_payload: dict[str, Any] | None = None
            product_row: dict[str, Any] | None = None
            used_category_id: int | None = None
            for category_id in _iter_leaf_category_ids(top_nodes):
                payload = await _request_json(
                    operation=f"catalog.products_list[{category_id}:1]",
                    call=lambda: api.Catalog.products_list(
                        page=1,
                        category_id=category_id,
                        city_id=None,
                    ),
                )
                if not isinstance(payload, dict):
                    continue
                items = payload.get("items")
                if not isinstance(items, list) or not items:
                    continue
                first = items[0]
                if not isinstance(first, dict):
                    continue
                products_payload = payload
                product_row = first
                used_category_id = category_id
                break
            if product_row is None or used_category_id is None:
                raise RuntimeError("Unable to fetch live Chizhik product for tests.")

            product_id = product_row.get("id")
            if not isinstance(product_id, int):
                raise RuntimeError("Chizhik product id is missing.")
            product_info = await _request_json(
                operation=f"catalog.product.info[{product_id}]",
                call=lambda: api.Catalog.Product.info(
                    product_id=product_id,
                    city_id=None,
                ),
            )
            if not isinstance(product_info, dict):
                raise RuntimeError("Chizhik Product.info response is not an object.")

            return {
                "tree": top_nodes,
                "first_node": first_node,
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
    assert mapped.alias == node["slug"]
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
    assert mapped.title == expected["title"]
    assert mapped.description == expected["description"]
    assert mapped.adult == expected["adult"]
    assert mapped.promo == expected["promo"]
    assert mapped.rating == expected["rating"]
    assert mapped.reviews_count == expected["reviews_count"]
    assert mapped.price == expected["price"]
    assert mapped.unit == expected["unit"]
    assert mapped.brand == expected["brand"]
    assert mapped.producer_name == expected["producer_name"]
    assert mapped.producer_country == expected["producer_country"]
    assert mapped.composition == expected["composition"]
    assert mapped.expiration_date_in_days == expected["expiration_date_in_days"]
    assert mapped.categories_uid is not None
    assert str(product["categories_tree"][0]["id"]) in mapped.categories_uid
    assert mapped.main_image == "images/00001/main.bin"
    assert mapped.images is not None
    assert mapped.images[0] == "images/00001/gallery_001.bin"


def test_map_product_from_live_info_response(chizhik_live_payloads: dict[str, Any]) -> None:
    product = chizhik_live_payloads["product_info"]
    expected = _expected_from_product_payload(product)

    mapped = ChizhikMapper.map_product(product)

    assert mapped.plu == expected["plu"]
    assert mapped.title == expected["title"]
    assert mapped.description == expected["description"]
    assert mapped.adult == expected["adult"]
    assert mapped.promo == expected["promo"]
    assert mapped.rating == expected["rating"]
    assert mapped.reviews_count == expected["reviews_count"]
    assert mapped.price == expected["price"]
    assert mapped.unit == expected["unit"]
    assert mapped.brand == expected["brand"]
    assert mapped.producer_name == expected["producer_name"]
    assert mapped.producer_country == expected["producer_country"]
    assert mapped.composition == expected["composition"]
    assert mapped.expiration_date_in_days == expected["expiration_date_in_days"]


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
        int(category.uid)
        for category in categories
        if category.uid is not None and category.uid.isdigit() and category.children
    }
    query_ids = {query.category_id for query in queries}

    assert queries
    assert query_ids.isdisjoint(roots_with_children)


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
        CatalogProductsQuery(category_id=1, category_uid="1", category_slug="cat-1"),
        CatalogProductsQuery(category_id=2, category_uid="2", category_slug="cat-2"),
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


def test_collect_products_page_uses_product_info_with_cache(
    chizhik_live_payloads: dict[str, Any],
) -> None:
    parser = ChizhikParser()
    products_payload = chizhik_live_payloads["products_payload"]
    info_payload = chizhik_live_payloads["product_info"]

    class _Response:
        def __init__(self, payload: Any):
            self._payload = payload

        def json(self) -> Any:
            return self._payload

    calls: dict[str, int] = {"info": 0}
    list_item = dict(chizhik_live_payloads["product_row"])
    list_item.pop("reviews_count", None)
    list_item.pop("is_adults", None)
    list_item.pop("meta_data", None)
    list_item.pop("base_unit", None)

    class _ProductService:
        async def info(self, product_id: int, city_id: str | None = None) -> _Response:
            assert product_id == info_payload["id"]
            assert city_id is None
            calls["info"] += 1
            return _Response(info_payload)

    class _Catalog:
        Product = _ProductService()

        async def products_list(
            self,
            page: int = 1,
            category_id: int | None = None,
            city_id: str | None = None,
            search: str | None = None,
        ) -> _Response:
            del category_id
            del city_id
            del search
            total_pages = products_payload.get("total_pages") if isinstance(products_payload, dict) else 2
            if not isinstance(total_pages, int):
                total_pages = 2
            assert page in {1, 2}
            return _Response({"total_pages": total_pages, "items": [dict(list_item)]})

    class _Api:
        Catalog = _Catalog()

    parser._api = _Api()  # type: ignore[assignment]

    query = CatalogProductsQuery(category_id=88, category_uid="88", category_slug="shokolad")
    first_cards, total_pages = asyncio.run(parser._collect_products_page(query=query, page=1))
    second_cards, _ = asyncio.run(parser._collect_products_page(query=query, page=2))

    assert total_pages is not None
    assert len(first_cards) == 1
    assert len(second_cards) == 1
    assert first_cards[0].reviews_count == info_payload["reviews_count"]
    assert first_cards[0].producer_name == _expected_from_product_payload(info_payload)["producer_name"]
    assert calls["info"] == 1
