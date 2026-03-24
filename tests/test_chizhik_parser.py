from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from openinflation_parser.parsers.chizhik.parser import ChizhikParser
from openinflation_parser.parsers.chizhik.types import ChizhikParserConfig


@dataclass
class _FakeResponse:
    payload: Any

    def json(self) -> Any:
        return self.payload


class _FakeCatalog:
    def __init__(self) -> None:
        self.delivery_tree_calls: list[str] = []
        self.delivery_tree_extended_calls: list[tuple[str, str]] = []

    async def delivery_tree(self, store_id: str) -> _FakeResponse:
        self.delivery_tree_calls.append(store_id)
        return _FakeResponse(
            [
                {
                    "id": "ROOT",
                    "name": "Основной каталог",
                    "categories": [
                        {
                            "id": "CAT-1",
                            "name": "Молочные продукты",
                        }
                    ],
                }
            ]
        )

    async def delivery_tree_extended(
        self,
        *,
        store_id: str,
        category_alias: str,
    ) -> _FakeResponse:
        self.delivery_tree_extended_calls.append((store_id, category_alias))
        return _FakeResponse(
            {
                "id": category_alias,
                "categories_tags": [
                    {"id": "SUB-1", "name": "Йогурты"},
                    {"id": "SUB-2", "name": "Молоко"},
                ],
            }
        )


class _FakeShopService:
    def __init__(self, payload: Any) -> None:
        self.payload = payload
        self.all_payload: Any = []
        self.search_calls: list[str] = []
        self.all_calls = 0

    async def search(self, query: str) -> _FakeResponse:
        self.search_calls.append(query)
        return _FakeResponse(self.payload)

    async def all(self) -> _FakeResponse:
        self.all_calls += 1
        return _FakeResponse(self.all_payload)


class _FakeGeolocation:
    def __init__(self, shop_payload: Any) -> None:
        self.Shop = _FakeShopService(shop_payload)

    async def cities_list(self, search_name: str, page: int = 1) -> _FakeResponse:
        del search_name
        del page
        return _FakeResponse(
            {
                "items": [
                    {
                        "name": "Москва",
                        "slug": "moskva",
                        "lat": 55.75,
                        "lon": 37.61,
                        "has_shop": True,
                    }
                ]
            }
        )


class _FakeApi:
    def __init__(self, *, shop_payload: Any) -> None:
        self.Catalog = _FakeCatalog()
        self.Geolocation = _FakeGeolocation(shop_payload)


def test_collect_store_info_resolves_delivery_store_id_from_shop_search() -> None:
    parser = ChizhikParser(ChizhikParserConfig(store_code="moskva"))
    parser._api = _FakeApi(
        shop_payload=[
            {
                "sap_id": "HAOJ",
                "slug": "moskva-800-letiia-11-34907",
                "locality": "Москва",
            }
        ]
    )

    stores = asyncio.run(parser.collect_store_info(store_code="moskva"))

    assert len(stores) == 1
    assert stores[0].code == "moskva"
    assert parser._effective_store_id == "HAOJ"


def test_collect_store_info_returns_empty_when_delivery_store_not_found() -> None:
    parser = ChizhikParser(ChizhikParserConfig(store_code="unknown-store"))
    parser._api = _FakeApi(shop_payload=[])

    stores = asyncio.run(parser.collect_store_info(store_code="unknown-store"))

    assert stores == []
    assert parser._effective_store_id is None


def test_collect_categories_uses_delivery_tree_and_extended_endpoints() -> None:
    parser = ChizhikParser(ChizhikParserConfig(store_code="moskva"))
    fake_api = _FakeApi(
        shop_payload=[
            {
                "sap_id": "HAOJ",
                "slug": "moskva-800-letiia-11-34907",
                "locality": "Москва",
            }
        ]
    )
    parser._api = fake_api

    categories = asyncio.run(parser.collect_categories())

    assert len(categories) == 1
    assert categories[0].uid == "CAT-1"
    assert categories[0].children is not None
    assert [child.uid for child in categories[0].children] == ["SUB-1", "SUB-2"]
    assert fake_api.Catalog.delivery_tree_calls == ["HAOJ"]
    assert fake_api.Catalog.delivery_tree_extended_calls == [("HAOJ", "CAT-1")]
    assert fake_api.Geolocation.Shop.search_calls == ["moskva"]


def test_collect_store_info_without_store_code_uses_real_shop_directory() -> None:
    parser = ChizhikParser()
    fake_api = _FakeApi(shop_payload=[])
    fake_api.Geolocation.Shop.all_payload = [
        {
            "sap_id": "HAOJ",
            "name": "Чижик, Москва, ул. Ленина, 1",
            "working_hours": "09:00 - 21:00",
            "lat": 55.75,
            "lon": 37.61,
            "locality": {
                "name": "Москва",
                "slug": "moskva",
                "lat": 55.75,
                "lon": 37.61,
            },
        },
        {
            "sap_id": "HAOJ",
            "name": "Дубликат",
            "working_hours": "10:00 - 22:00",
            "lat": 55.76,
            "lon": 37.62,
            "locality": "Москва",
        },
    ]
    parser._api = fake_api

    stores = asyncio.run(parser.collect_store_info(store_code=None))

    assert len(stores) == 1
    assert stores[0].code == "HAOJ"
    assert stores[0].retail_type == "store"
    assert stores[0].address == "Чижик, Москва, ул. Ленина, 1"
    assert stores[0].schedule_weekdays.open_from is None
    assert stores[0].administrative_unit.alias == "moskva"
    assert fake_api.Geolocation.Shop.all_calls == 1
