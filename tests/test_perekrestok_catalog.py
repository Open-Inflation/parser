from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

from openinflation_parser.parsers.perekrestok.catalog import PerekrestokCatalogMixin
from openinflation_parser.parsers.perekrestok.types import CatalogProductsQuery
from openinflation_parser.parsers.runtime import ParserRuntimeMixin


class _DummyPerekrestokCatalog(PerekrestokCatalogMixin, ParserRuntimeMixin):
    def __init__(self, api: Any | None = None) -> None:
        self._api = api
        self._product_info_cache: dict[str, dict[str, Any]] = {}
        self.config = SimpleNamespace(
            include_images=False,
            image_limit_per_product=0,
            strict_validation=False,
        )

    def _require_api(self) -> Any:
        if self._api is None:
            raise RuntimeError("API is not configured for test parser")
        return self._api


def test_image_url_from_node_uses_width_height_from_contract() -> None:
    url = _DummyPerekrestokCatalog._image_url_from_node(
        {
            "cropUrlTemplate": (
                "https://tsx.x5static.net/i/%s/xdelivery/files/4e/21/"
                "2fca192f74d11d27f95310b5233b.jpg"
            ),
            "width": 898,
            "height": 898,
        }
    )
    assert (
        url
        == "https://tsx.x5static.net/i/898x898/xdelivery/files/4e/21/2fca192f74d11d27f95310b5233b.jpg"
    )


def test_image_url_from_node_without_placeholder_returns_template_as_is() -> None:
    url = _DummyPerekrestokCatalog._image_url_from_node(
        {
            "cropUrlTemplate": "https://tsx.x5static.net/static/image.jpg",
            "width": 100,
            "height": 100,
        }
    )
    assert url == "https://tsx.x5static.net/static/image.jpg"


def test_image_url_from_node_with_missing_dimensions_returns_none() -> None:
    url = _DummyPerekrestokCatalog._image_url_from_node(
        {
            "cropUrlTemplate": "https://tsx.x5static.net/i/%s/xdelivery/files/path.jpg",
            "width": None,
            "height": 200,
        }
    )
    assert url is None


def test_collect_products_page_handles_invalid_json_payload() -> None:
    class _BrokenJsonResponse:
        text = ""

        def json(self) -> Any:
            raise json.JSONDecodeError("Expecting value", self.text, 0)

    class _Catalog:
        async def feed(self, *, filter: Any, page: int, limit: int) -> _BrokenJsonResponse:
            del filter
            del page
            del limit
            return _BrokenJsonResponse()

    class _Api:
        Catalog = _Catalog()

    parser = _DummyPerekrestokCatalog(api=_Api())
    query = CatalogProductsQuery(category_id=42, category_uid="42", category_slug="bakery")

    cards, has_next_page = asyncio.run(
        parser._collect_products_page(
            query=query,
            page=1,
            limit=24,
        )
    )

    assert cards == []
    assert has_next_page is False
