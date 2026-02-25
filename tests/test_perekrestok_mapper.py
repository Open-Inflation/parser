from __future__ import annotations

import asyncio
from typing import Any, Iterator

import pytest

pytest.importorskip("perekrestok_api")
from perekrestok_api import PerekrestokAPI, abstraction
from openinflation_dataclass import Card

from openinflation_parser.parsers.perekrestok import (
    CatalogProductsQuery,
    PerekrestokMapper,
    PerekrestokParser,
)


def _category_filter(category_id: int) -> abstraction.CatalogFeedFilter:
    feed_filter = abstraction.CatalogFeedFilter()
    feed_filter.CATEGORY_ID = category_id
    return feed_filter


def _iter_leaf_category_ids(nodes: list[dict[str, Any]]) -> Iterator[int]:
    def walk(node: dict[str, Any]) -> Iterator[int]:
        category = node.get("category") if isinstance(node.get("category"), dict) else {}
        category_id = category.get("id")
        raw_children = node.get("children")
        children = [child for child in raw_children if isinstance(child, dict)] if isinstance(raw_children, list) else []

        if isinstance(category_id, int) and not children:
            yield category_id
            return
        for child in children:
            yield from walk(child)

    for node in nodes:
        if isinstance(node, dict):
            yield from walk(node)


def _feature_value(product: dict[str, Any], key: str) -> str | None:
    features = product.get("features")
    if not isinstance(features, list):
        return None
    for group in features:
        if not isinstance(group, dict):
            continue
        items = group.get("items")
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("key") != key:
                continue
            display = item.get("displayValues")
            if isinstance(display, list) and display:
                first = display[0]
                if isinstance(first, str):
                    return first
    return None


def _collect_perekrestok_live_payloads() -> dict[str, Any]:
    async def _collect() -> dict[str, Any]:
        async with PerekrestokAPI(headless=True, timeout_ms=45000) as api:
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

            current_payload = await _request_json(
                operation="geolocation.current",
                call=lambda: api.Geolocation.current(),
            )
            if not isinstance(current_payload, dict):
                raise RuntimeError("Geolocation.current response is not an object.")
            current_content = current_payload.get("content")
            if not isinstance(current_content, dict):
                raise RuntimeError("Geolocation.current content is missing.")
            current_city = current_content.get("city")
            if not isinstance(current_city, dict):
                raise RuntimeError("Geolocation.current city is missing.")

            tree_payload = await _request_json(
                operation="catalog.tree",
                call=lambda: api.Catalog.tree(),
            )
            if not isinstance(tree_payload, dict):
                raise RuntimeError("Catalog.tree response is not an object.")
            tree_content = tree_payload.get("content")
            if not isinstance(tree_content, dict):
                raise RuntimeError("Catalog.tree content is missing.")
            tree_items = tree_content.get("items")
            if not isinstance(tree_items, list):
                raise RuntimeError("Catalog.tree items are missing.")
            top_nodes = [node for node in tree_items if isinstance(node, dict)]
            if not top_nodes:
                raise RuntimeError("Catalog.tree is empty.")
            first_node = top_nodes[0]

            product_row: dict[str, Any] | None = None
            used_category_id: int | None = None
            attempts = 0
            for category_id in _iter_leaf_category_ids(top_nodes):
                attempts += 1
                if attempts > 80:
                    break
                feed_payload = await _request_json(
                    operation=f"catalog.feed[{category_id}:1]",
                    call=lambda: api.Catalog.feed(
                        filter=_category_filter(category_id),
                        page=1,
                        limit=24,
                    ),
                )
                if not isinstance(feed_payload, dict):
                    continue
                feed_content = feed_payload.get("content")
                if not isinstance(feed_content, dict):
                    continue
                items = feed_content.get("items")
                if not isinstance(items, list) or not items:
                    continue
                first_item = items[0]
                if not isinstance(first_item, dict):
                    continue
                product_row = first_item
                used_category_id = category_id
                break

            if product_row is None or used_category_id is None:
                raise RuntimeError("Unable to fetch live Perekrestok product for tests.")

            master_data = product_row.get("masterData")
            if not isinstance(master_data, dict):
                raise RuntimeError("Catalog.feed product.masterData is missing.")
            product_plu = master_data.get("plu")
            if not isinstance(product_plu, (int, str)):
                raise RuntimeError("Catalog.feed product PLU is missing.")

            product_info_payload = await _request_json(
                operation=f"catalog.product.info[{product_plu}]",
                call=lambda: api.Catalog.Product.info(product_plu=product_plu),
            )
            if not isinstance(product_info_payload, dict):
                raise RuntimeError("Product.info response is not an object.")
            product_info_content = product_info_payload.get("content")
            if not isinstance(product_info_content, dict):
                raise RuntimeError("Product.info content is missing.")

            shops_payload = await _request_json(
                operation="geolocation.shop.all",
                call=lambda: api.Geolocation.Shop.all(),
            )
            if not isinstance(shops_payload, dict):
                raise RuntimeError("Shop.all response is not an object.")
            shops_content = shops_payload.get("content")
            if not isinstance(shops_content, dict):
                raise RuntimeError("Shop.all content is missing.")
            shops = shops_content.get("items")
            if not isinstance(shops, list) or not shops:
                raise RuntimeError("Shop.all did not return shops.")
            shop_row = shops[0]
            if not isinstance(shop_row, dict):
                raise RuntimeError("Shop.all returned invalid shop row.")

            shop_id = shop_row.get("id") if isinstance(shop_row.get("id"), int) else None
            if shop_id is None:
                raise RuntimeError("Shop.on_map row does not include int id.")

            shop_info_payload = await _request_json(
                operation=f"geolocation.shop.info[{shop_id}]",
                call=lambda: api.Geolocation.Shop.info(shop_id=shop_id),
            )
            if not isinstance(shop_info_payload, dict):
                raise RuntimeError("Shop.info response is not an object.")
            shop_info = shop_info_payload.get("content")
            if not isinstance(shop_info, dict):
                raise RuntimeError("Shop.info content is missing.")

            return {
                "tree": top_nodes,
                "first_node": first_node,
                "current_city": current_city,
                "shop_row": shop_info,
                "product_row": product_row,
                "product_info": product_info_content,
                "used_category_id": used_category_id,
            }

    return asyncio.run(_collect())


@pytest.fixture(scope="module")
def perekrestok_live_payloads() -> dict[str, Any]:
    return _collect_perekrestok_live_payloads()


def test_map_category_node_from_live_response(perekrestok_live_payloads: dict[str, Any]) -> None:
    node = perekrestok_live_payloads["first_node"]
    category = node["category"]

    mapped = PerekrestokMapper.map_category_node(node)

    assert mapped.uid == str(category["id"])
    assert mapped.alias == category["slug"]
    assert mapped.title == category["title"]
    assert isinstance(mapped.children, list)


def test_map_city_from_live_response(perekrestok_live_payloads: dict[str, Any]) -> None:
    city = perekrestok_live_payloads["current_city"]

    mapped = PerekrestokMapper.map_city(city)

    assert mapped.name == city.get("name")
    assert mapped.alias == city.get("slug")
    assert mapped.country == "RUS"


def test_map_store_from_live_response(perekrestok_live_payloads: dict[str, Any]) -> None:
    store = perekrestok_live_payloads["shop_row"]
    city = store.get("city") if isinstance(store.get("city"), dict) else perekrestok_live_payloads["current_city"]
    administrative_unit = PerekrestokMapper.map_city(city)

    mapped = PerekrestokMapper.map_store(store, administrative_unit=administrative_unit)

    assert mapped.code == str(store.get("id"))
    assert mapped.address == store.get("address")
    assert mapped.administrative_unit.name == city.get("name")


def test_map_product_from_live_feed_response(perekrestok_live_payloads: dict[str, Any]) -> None:
    product = perekrestok_live_payloads["product_row"]
    category_id = perekrestok_live_payloads["used_category_id"]

    mapped = PerekrestokMapper.map_product(
        product,
        categories_uid=[str(category_id)],
        main_image="images/00001/main.bin",
        gallery_images=["images/00001/gallery_001.bin"],
    )

    master_data = product["masterData"]
    assert mapped.sku == str(product["id"])
    assert mapped.plu == str(master_data["plu"])
    assert mapped.title == product.get("title")
    assert mapped.price == product["priceTag"]["price"] / 100.0
    assert mapped.rating == PerekrestokMapper._rating_from_raw(product.get("rating"))
    assert mapped.data_matrix == (
        master_data.get("isMark")
        if isinstance(master_data.get("isMark"), bool)
        else None
    )
    assert mapped.price_unit == "RUB"
    assert mapped.categories_uid is not None
    assert str(category_id) in mapped.categories_uid
    assert mapped.main_image == "images/00001/main.bin"
    assert mapped.images is not None
    assert mapped.images[0] == "images/00001/gallery_001.bin"


def test_map_product_from_live_info_response(perekrestok_live_payloads: dict[str, Any]) -> None:
    product = perekrestok_live_payloads["product_info"]
    brand = _feature_value(product, "brand")
    producer_name = _feature_value(product, "proizvoditel")
    composition = _feature_value(product, "sostav")
    expiration_days_raw = _feature_value(product, "srok-hraneniya-maks")

    mapped = PerekrestokMapper.map_product(product)

    assert mapped.sku == str(product["id"])
    assert mapped.title == product.get("title")
    assert mapped.description == product.get("description")
    assert mapped.brand == brand
    assert mapped.producer_name == producer_name
    assert mapped.composition == composition
    assert mapped.expiration_date_in_days == PerekrestokMapper._expiration_days_from_raw(
        expiration_days_raw
    )


def test_build_catalog_queries_full_mode_prefers_leaf_categories(
    perekrestok_live_payloads: dict[str, Any],
) -> None:
    parser = PerekrestokParser()
    categories = [
        PerekrestokMapper.map_category_node(node)
        for node in perekrestok_live_payloads["tree"]
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
    parser = PerekrestokParser()

    async def _fake_collect_products_page(
        *,
        query: CatalogProductsQuery,
        page: int,
        limit: int,
    ) -> tuple[list[Card], bool]:
        del query
        del limit
        if page > 1:
            return [], False
        return [Card.model_construct(sku="SKU-1", categories_uid=["base"])], False

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
    assert cards[0].sku == "SKU-1"
    assert cards[0].categories_uid == ["base", "1", "2"]


def test_map_product_does_not_invent_missing_values() -> None:
    mapped = PerekrestokMapper.map_product(
        product={
            "id": None,
            "masterData": {},
            "priceTag": {},
        }
    )

    assert mapped.sku is None
    assert mapped.plu is None
    assert mapped.title is None
    assert mapped.price is None
    assert mapped.categories_uid is None
