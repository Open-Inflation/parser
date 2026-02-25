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
    def __init__(self, *, with_city_payload: Any, without_city_payload: Any) -> None:
        self.with_city_payload = with_city_payload
        self.without_city_payload = without_city_payload
        self.tree_calls: list[str | None] = []

    async def tree(self, city_id: str | None = None) -> _FakeResponse:
        self.tree_calls.append(city_id)
        if city_id is None:
            return _FakeResponse(self.without_city_payload)
        return _FakeResponse(self.with_city_payload)


class _FakeApi:
    def __init__(self, catalog: _FakeCatalog) -> None:
        self.Catalog = catalog


def test_collect_categories_fallbacks_without_city_id_when_city_response_empty() -> None:
    config = ChizhikParserConfig(city_id="81")
    parser = ChizhikParser(config)

    category_node = {
        "id": 133,
        "name": "Основной каталог",
        "slug": "osnovnoi-katalog",
        "is_adults": False,
        "children": [],
    }
    fake_catalog = _FakeCatalog(
        with_city_payload=[],
        without_city_payload=[category_node],
    )
    parser._api = _FakeApi(fake_catalog)

    categories = asyncio.run(parser.collect_categories())

    assert len(categories) == 1
    assert fake_catalog.tree_calls == ["81", None]
    assert parser._effective_city_id is None


def test_collect_categories_uses_city_id_when_payload_is_valid() -> None:
    config = ChizhikParserConfig(city_id="fdc6b0ad-4096-43e7-a2e9-16404d2e1f68")
    parser = ChizhikParser(config)

    category_node = {
        "id": 133,
        "name": "Основной каталог",
        "slug": "osnovnoi-katalog",
        "is_adults": False,
        "children": [],
    }
    fake_catalog = _FakeCatalog(
        with_city_payload=[category_node],
        without_city_payload=[],
    )
    parser._api = _FakeApi(fake_catalog)

    categories = asyncio.run(parser.collect_categories())

    assert len(categories) == 1
    assert fake_catalog.tree_calls == ["fdc6b0ad-4096-43e7-a2e9-16404d2e1f68"]
    assert parser._effective_city_id == "fdc6b0ad-4096-43e7-a2e9-16404d2e1f68"
