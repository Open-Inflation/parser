from __future__ import annotations

import asyncio
from typing import Any, Iterator

import pytest
from fixprice_api import FixPriceAPI

from openinflation_parser.parsers.fixprice import FixPriceMapper


def _iter_children_nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
    raw_children = node.get("items")
    if isinstance(raw_children, dict):
        for child in raw_children.values():
            if isinstance(child, dict):
                yield child
    elif isinstance(raw_children, list):
        for child in raw_children:
            if isinstance(child, dict):
                yield child


def _iter_catalog_queries(tree: dict[str, Any]) -> Iterator[tuple[str, str | None, dict[str, Any]]]:
    for node in tree.values():
        if not isinstance(node, dict):
            continue
        category_alias = node.get("alias")
        if not isinstance(category_alias, str) or not category_alias:
            continue

        yielded_sub = False
        for child in _iter_children_nodes(node):
            sub_alias = child.get("alias")
            if isinstance(sub_alias, str) and sub_alias:
                yielded_sub = True
                yield category_alias, sub_alias, node
        if not yielded_sub:
            yield category_alias, None, node


@pytest.fixture(scope="module")
def fixprice_live_payloads() -> dict[str, Any]:
    async def _collect() -> dict[str, Any]:
        async with FixPriceAPI(headless=True, timeout_ms=45000) as api:
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
            assert isinstance(tree, dict) and tree
            first_category_node = next(
                node for node in tree.values() if isinstance(node, dict)
            )

            countries = await _request_json(
                operation="geolocation.countries_list",
                call=lambda: api.Geolocation.countries_list(),
            )
            assert isinstance(countries, list) and countries

            cities = await _request_json(
                operation="geolocation.cities_list[2]",
                call=lambda: api.Geolocation.cities_list(country_id=2),
            )
            assert isinstance(cities, list) and cities
            city_row = next(
                (
                    row
                    for row in cities
                    if isinstance(row, dict) and isinstance(row.get("id"), int) and row["id"] == 3
                ),
                None,
            )
            if city_row is None:
                city_row = next(
                    row
                    for row in cities
                    if isinstance(row, dict) and isinstance(row.get("id"), int)
                )
            city_id = int(city_row["id"])

            shops = await _request_json(
                operation=f"geolocation.shop.search[2:{city_id}]",
                call=lambda: api.Geolocation.Shop.search(country_id=2, city_id=city_id),
            )
            if not (isinstance(shops, list) and shops):
                shops = await _request_json(
                    operation="geolocation.shop.search[2]",
                    call=lambda: api.Geolocation.Shop.search(country_id=2),
                )
            assert isinstance(shops, list) and shops

            product: dict[str, Any] | None = None
            used_query: tuple[str, str | None] | None = None
            attempts = 0
            for category_alias, sub_alias, _node in _iter_catalog_queries(tree):
                attempts += 1
                if attempts > 60:
                    break
                products = await _request_json(
                    operation=f"catalog.products_list[{category_alias}:{sub_alias}:1]",
                    call=lambda: api.Catalog.products_list(
                        category_alias=category_alias,
                        subcategory_alias=sub_alias,
                        page=1,
                        limit=24,
                    ),
                )
                if isinstance(products, list) and products:
                    first = products[0]
                    if isinstance(first, dict):
                        product = first
                        used_query = (category_alias, sub_alias)
                        break
            if product is None:
                raise RuntimeError("Unable to fetch live FixPrice product for tests.")

            return {
                "tree": tree,
                "first_category_node": first_category_node,
                "countries": countries,
                "city_row": city_row,
                "shop_row": shops[0],
                "product_row": product,
                "used_query": used_query,
            }

    return asyncio.run(_collect())


def test_map_category_node_from_live_response(fixprice_live_payloads: dict[str, Any]) -> None:
    node = fixprice_live_payloads["first_category_node"]
    mapped = FixPriceMapper.map_category_node(node)

    assert mapped.uid == str(node["id"])
    assert mapped.alias == node["alias"]
    assert mapped.title == node["title"]
    assert isinstance(mapped.children, list)


def test_map_city_from_live_response(fixprice_live_payloads: dict[str, Any]) -> None:
    city = fixprice_live_payloads["city_row"]
    mapped = FixPriceMapper.map_city(city, country_id=2)

    assert mapped.name == city.get("title")
    assert mapped.country == "RUS"
    assert mapped.longitude == city.get("longitude")
    assert mapped.latitude == city.get("latitude")


def test_map_store_from_live_response(fixprice_live_payloads: dict[str, Any]) -> None:
    city = FixPriceMapper.map_city(fixprice_live_payloads["city_row"], country_id=2)
    shop = fixprice_live_payloads["shop_row"]
    mapped = FixPriceMapper.map_store(shop, administrative_unit=city)

    assert mapped.code == shop.get("pfm")
    assert mapped.address == shop.get("address")
    warehouse = shop.get("warehouse")
    if isinstance(warehouse, bool):
        assert mapped.retail_type == ("warehouse" if warehouse else "store")
    else:
        assert mapped.retail_type is None


def test_map_product_from_live_response(fixprice_live_payloads: dict[str, Any]) -> None:
    product = fixprice_live_payloads["product_row"]

    mapped = FixPriceMapper.map_product(
        product,
        main_image="images/00001/main.bin",
        gallery_images=["images/00001/gallery_001.bin"],
    )

    assert mapped.sku == product.get("sku")
    assert mapped.price is not None
    assert mapped.categories_uid
    assert mapped.main_image == "images/00001/main.bin"
    assert mapped.images is not None
    assert mapped.images[0] == "images/00001/gallery_001.bin"


def test_map_product_does_not_invent_missing_values() -> None:
    mapped = FixPriceMapper.map_product(
        product={
            "sku": None,
            "id": 123,
            "title": None,
            "price": None,
            "category": {},
        }
    )

    assert mapped.sku is None
    assert mapped.plu is None
    assert mapped.title is None
    assert mapped.price is None
    assert mapped.categories_uid is None


def test_map_store_without_warehouse_field_sets_type_none() -> None:
    city = FixPriceMapper.map_city(
        {"title": "X", "countryId": 2, "prefix": "г"},
        country_id=2,
    )
    mapped = FixPriceMapper.map_store(
        {
            "pfm": "X001",
            "address": "Addr",
            "scheduleWeekdays": None,
            "scheduleSaturday": None,
            "scheduleSunday": None,
            "temporarilyClosed": None,
            "longitude": None,
            "latitude": None,
        },
        administrative_unit=city,
    )
    assert mapped.retail_type is None


def test_country_mapping_covers_live_country_ids(fixprice_live_payloads: dict[str, Any]) -> None:
    countries = fixprice_live_payloads["countries"]
    ids = {
        row["id"]
        for row in countries
        if isinstance(row, dict) and isinstance(row.get("id"), int)
    }
    for country_id, code in FixPriceMapper.COUNTRY_ID_TO_CODE.items():
        if country_id in ids:
            assert FixPriceMapper.country_code_from_id(country_id) == code


def test_mapper_is_strict_about_contract_types() -> None:
    city = FixPriceMapper.map_city(
        {
            "title": "Test City",
            "prefix": 123,
            "countryId": "2",
            "longitude": "55.75",
            "latitude": "37.61",
        },
        country_id=None,
    )
    assert city.settlement_type is None
    assert city.country is None
    assert city.longitude is None
    assert city.latitude is None

    mapped = FixPriceMapper.map_product(
        product={
            "sku": "SKU-1",
            "price": "10.50",
            "unit": 1,
            "inStock": "7",
            "metaData": [
                {"name": "ok", "alias": "ok", "value": "yes"},
                {"name": "dict", "alias": "dict", "value": {"a": 1}},
                {"name": "bool", "alias": "bool", "value": True},
            ],
            "category": {"id": 99},
        }
    )
    assert mapped.unit is None
    assert mapped.available_count is None
    assert mapped.price == 10.5
    assert mapped.meta_data is not None
    assert len(mapped.meta_data) == 1
    assert mapped.meta_data[0].value == "yes"
