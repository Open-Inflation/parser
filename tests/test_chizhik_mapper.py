from __future__ import annotations

import asyncio
import json
from io import BytesIO
from pathlib import Path

import pytest
from openinflation_dataclass import Card

from openinflation_parser.parsers.chizhik import (
    CatalogProductsQuery,
    ChizhikMapper,
    ChizhikParser,
)


SNAPSHOT_DIR = (
    Path(__file__).resolve().parents[2] / "chizhik_api" / "tests" / "__snapshots__"
)


def _load_snapshot(name: str):
    path = SNAPSHOT_DIR / name
    if not path.exists():
        pytest.skip(f"Snapshot file is missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def test_map_category_node_from_snapshot() -> None:
    tree = _load_snapshot("ClassCatalog.tree.json")
    node = tree[0]

    mapped = ChizhikMapper.map_category_node(node)

    assert mapped.uid == str(node["id"])
    assert mapped.alias == node["slug"]
    assert mapped.title == node["name"]
    assert isinstance(mapped.children, list)


def test_map_product_from_snapshot() -> None:
    products_payload = _load_snapshot("ClassCatalog.products_list.json")
    product = products_payload["items"][0]

    mapped = ChizhikMapper.map_product(
        product,
        main_image=BytesIO(b"main"),
        gallery_images=[BytesIO(b"extra")],
    )

    assert mapped.sku is None
    assert mapped.plu == product["plu"]
    assert mapped.price == product["price"]
    assert mapped.categories_uid is not None
    assert str(product["categories_tree"][0]["id"]) in mapped.categories_uid
    assert mapped.main_image.getvalue() == b"main"
    assert mapped.images[0].getvalue() == b"extra"


def test_build_catalog_queries_full_mode_prefers_leaf_categories() -> None:
    parser = ChizhikParser()
    tree = _load_snapshot("ClassCatalog.tree.json")
    categories = [ChizhikMapper.map_category_node(node) for node in tree if isinstance(node, dict)]

    queries = parser.build_catalog_queries(
        categories,
        full_catalog=True,
        category_limit=1,
    )

    query_ids = {query.category_id for query in queries}
    assert 133 not in query_ids  # Root category from snapshot has children and should be skipped.
    assert 165 in query_ids  # One of known leaf categories.


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


def test_collect_store_info_returns_virtual_store_without_live_api() -> None:
    parser = ChizhikParser()
    stores = asyncio.run(parser.collect_store_info(store_code="moskva"))
    assert len(stores) == 1
    assert stores[0].code == "moskva"
