from __future__ import annotations

import asyncio
from typing import Any, Iterator

import pytest
from fixprice_api import FixPriceAPI

from openinflation_parser.parsers.fixprice import FixPriceMapper, FixPriceParser


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

            product_url = product.get("url")
            if not isinstance(product_url, str) or not product_url:
                raise RuntimeError("FixPrice product url is missing.")
            product_info = await _request_json(
                operation=f"catalog.product.info[{product_url}]",
                call=lambda: api.Catalog.Product.info(url=product_url),
            )
            if not isinstance(product_info, dict):
                raise RuntimeError("FixPrice Product.info response is not an object.")
            merged_product = dict(product)
            merged_product.update(product_info)

            return {
                "tree": tree,
                "first_category_node": first_category_node,
                "countries": countries,
                "city_row": city_row,
                "shop_row": shops[0],
                "product_row": product,
                "product_info": product_info,
                "merged_product": merged_product,
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
    product = fixprice_live_payloads["merged_product"]

    mapped = FixPriceMapper.map_product(
        product,
        price_unit=FixPriceMapper.currency_for_country_id(2),
        main_image="images/00001/main.bin",
        gallery_images=["images/00001/gallery_001.bin"],
    )

    assert mapped.sku == product.get("sku")
    assert mapped.description == product.get("description")
    assert mapped.price is not None
    assert mapped.categories_uid
    assert mapped.price_unit == "RUB"
    assert mapped.dimension_height == FixPriceMapper._numeric_from_raw(
        FixPriceMapper._first_variant(product).get("height")
    )
    assert mapped.dimension_width == FixPriceMapper._numeric_from_raw(
        FixPriceMapper._first_variant(product).get("width")
    )
    assert mapped.dimension_depth == FixPriceMapper._numeric_from_raw(
        FixPriceMapper._first_variant(product).get("length")
    )
    assert mapped.main_image == "images/00001/main.bin"
    assert mapped.images is not None
    assert mapped.images[0] == "images/00001/gallery_001.bin"


def test_map_product_from_live_info_response(fixprice_live_payloads: dict[str, Any]) -> None:
    product = fixprice_live_payloads["product_info"]
    mapped = FixPriceMapper.map_product(
        product,
        price_unit=FixPriceMapper.currency_for_country_id(2),
    )

    expected_producer_country: str | None = None
    properties = product.get("properties")
    if isinstance(properties, list):
        for row in properties:
            if not isinstance(row, dict):
                continue
            if row.get("alias") == "prodCountry":
                expected_producer_country = FixPriceMapper._producer_country_from_raw(row.get("value"))
                break

    assert mapped.sku == product.get("sku")
    assert mapped.description == product.get("description")
    assert mapped.producer_country == expected_producer_country
    assert mapped.price_unit == "RUB"
    assert mapped.meta_data is not None


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


def test_map_product_sets_package_quantity_gross_from_variant_weight() -> None:
    mapped = FixPriceMapper.map_product(
        product={
            "sku": "SKU-WEIGHT",
            "price": "10.00",
            "unitType": "кг",
            "category": {"id": 1},
            "variants": [
                {
                    "weight": 159.0,
                    "height": 10,
                    "width": 20,
                    "length": 30,
                }
            ],
        }
    )

    assert mapped.package_quantity_net is None
    assert mapped.package_quantity_gross == 159.0
    assert mapped.package_unit == "KGM"


def test_map_product_does_not_set_package_quantity_gross_for_non_weight_unit() -> None:
    mapped = FixPriceMapper.map_product(
        product={
            "sku": "SKU-NON-WEIGHT",
            "price": "10.00",
            "unitType": "шт",
            "category": {"id": 1},
            "variants": [{"weight": 159.0}],
        }
    )

    assert mapped.package_quantity_gross is None
    assert mapped.package_unit is None


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


def test_currency_from_country_payload_uses_snapshot_shape() -> None:
    assert (
        FixPriceMapper.currency_from_country_payload(
            {
                "id": 2,
                "alias": "RU",
                "currency": {"title": "Рубль", "symbol": "₽", "symbolFirst": False},
            }
        )
        == "RUB"
    )
    assert (
        FixPriceMapper.currency_from_country_payload(
            {
                "id": 8,
                "alias": "BY",
                "currency": {"title": "Белорусский рубль", "symbol": "руб", "symbolFirst": False},
            }
        )
        == "BYN"
    )
    assert (
        FixPriceMapper.currency_from_country_payload(
            {
                "id": 11,
                "alias": "RS",
                "currency": {"title": "Сербский динар", "symbol": "DIN", "symbolFirst": False},
            }
        )
        is None
    )


def test_producer_country_normalization_uses_russian_names_only() -> None:
    assert FixPriceMapper._producer_country_from_raw(" Россия ") == "RUS"
    assert (
        FixPriceMapper._producer_country_from_raw("Объединённые   Арабские, Эмираты.")
        == "ARE"
    )
    assert FixPriceMapper._producer_country_from_raw("China") is None


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


def test_collect_products_uses_product_info_with_cache(
    fixprice_live_payloads: dict[str, Any],
) -> None:
    parser = FixPriceParser()
    product_row = dict(fixprice_live_payloads["product_row"])
    info_payload = fixprice_live_payloads["product_info"]
    used_query = fixprice_live_payloads["used_query"]
    if used_query is None:
        raise RuntimeError("Used live query is missing.")
    expected_category_alias, expected_sub_alias = used_query

    class _Response:
        def __init__(self, payload: Any):
            self._payload = payload

        def json(self) -> Any:
            return self._payload

    calls: dict[str, int] = {"info": 0, "countries": 0}
    product_row.pop("description", None)
    product_row.pop("properties", None)

    class _ProductService:
        async def info(
            self,
            *,
            url: str | None = None,
            category: str | None = None,
            product_id: int | None = None,
            slug: str | None = None,
        ) -> _Response:
            del category
            del product_id
            del slug
            assert url == info_payload["url"]
            calls["info"] += 1
            return _Response(info_payload)

    class _Catalog:
        Product = _ProductService()

        async def products_list(
            self,
            category_alias: str,
            subcategory_alias: str | None = None,
            page: int = 1,
            limit: int = 24,
            sort: str = "popularity",
        ) -> _Response:
            del sort
            assert category_alias == expected_category_alias
            assert subcategory_alias == expected_sub_alias
            assert page in {1, 2}
            assert limit == 24
            return _Response([dict(product_row)])

    class _Geolocation:
        async def countries_list(self, alias: str | None = None) -> _Response:
            del alias
            calls["countries"] += 1
            return _Response(
                [
                    {
                        "id": 2,
                        "alias": "RU",
                        "currency": {"title": "Рубль", "symbol": "₽", "symbolFirst": False},
                    }
                ]
            )

    class _Api:
        Catalog = _Catalog()
        Geolocation = _Geolocation()

    parser._api = _Api()  # type: ignore[assignment]
    first_cards = asyncio.run(
        parser.collect_products(
            category_alias=expected_category_alias,
            subcategory_alias=expected_sub_alias,
            page=1,
            limit=24,
        )
    )
    second_cards = asyncio.run(
        parser.collect_products(
            category_alias=expected_category_alias,
            subcategory_alias=expected_sub_alias,
            page=2,
            limit=24,
        )
    )

    assert len(first_cards) == 1
    assert len(second_cards) == 1
    assert first_cards[0].description == info_payload["description"]
    assert first_cards[0].producer_country == FixPriceMapper._producer_country_from_raw(
        next(
            (
                row.get("value")
                for row in info_payload.get("properties", [])
                if isinstance(row, dict) and row.get("alias") == "prodCountry"
            ),
            None,
        )
    )
    assert first_cards[0].price_unit == "RUB"
    assert calls["info"] == 1
    assert calls["countries"] == 1
