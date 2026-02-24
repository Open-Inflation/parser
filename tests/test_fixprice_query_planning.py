from __future__ import annotations

import asyncio

from openinflation_dataclass import Card
from openinflation_dataclass import Category

from openinflation_parser.parsers.fixprice import CatalogProductsQuery, FixPriceParser


def _build_category_tree() -> list[Category]:
    sub_a1 = Category(uid="a1", alias="a1", title="A1", adult=False)
    sub_a2 = Category(uid="a2", alias="a2", title="A2", adult=False)
    root_a = Category(
        uid="a",
        alias="a",
        title="A",
        adult=False,
        children=[sub_a1, sub_a2],
    )
    root_b = Category(uid="b", alias="b", title="B", adult=False)
    return [root_a, root_b]


def test_build_catalog_queries_limited_mode_uses_top_categories_only() -> None:
    parser = FixPriceParser()
    categories = _build_category_tree()

    queries = parser.build_catalog_queries(
        categories,
        full_catalog=False,
        category_limit=1,
    )

    assert len(queries) == 1
    assert queries[0].category_alias == "a"
    assert queries[0].subcategory_alias is None
    assert queries[0].category_uid == "a"
    assert queries[0].subcategory_uid is None


def test_build_catalog_queries_full_mode_prefers_subcategories_over_root() -> None:
    parser = FixPriceParser()
    categories = _build_category_tree()

    queries = parser.build_catalog_queries(
        categories,
        full_catalog=True,
        category_limit=1,
    )

    prepared = {(query.category_alias, query.subcategory_alias) for query in queries}
    assert ("a", "a1") in prepared
    assert ("a", "a2") in prepared
    assert ("b", None) in prepared
    assert ("a", None) not in prepared

    by_pair = {
        (query.category_alias, query.subcategory_alias): (query.category_uid, query.subcategory_uid)
        for query in queries
    }
    assert by_pair[("a", "a1")] == ("a", "a1")
    assert by_pair[("a", "a2")] == ("a", "a2")
    assert by_pair[("b", None)] == ("b", None)


def test_collect_products_for_queries_merges_categories_uid_for_duplicate_sku() -> None:
    parser = FixPriceParser()

    async def _fake_collect_products(
        *,
        category_alias: str,
        subcategory_alias: str | None = None,
        page: int = 1,
        limit: int = 24,
    ) -> list[Card]:
        del limit
        if page > 1:
            return []
        if category_alias == "a" and subcategory_alias == "a1":
            return [Card.model_construct(sku="SKU-1", categories_uid=["root"])]
        if category_alias == "a" and subcategory_alias == "a2":
            return [Card.model_construct(sku="SKU-1", categories_uid=["root"])]
        return []

    parser.collect_products = _fake_collect_products  # type: ignore[method-assign]
    queries = [
        CatalogProductsQuery(
            category_alias="a",
            subcategory_alias="a1",
            category_uid="root",
            subcategory_uid="a1",
        ),
        CatalogProductsQuery(
            category_alias="a",
            subcategory_alias="a2",
            category_uid="root",
            subcategory_uid="a2",
        ),
    ]

    cards = asyncio.run(
        parser.collect_products_for_queries(
            queries,
            page_limit=10,
            items_per_page=27,
        )
    )
    assert len(cards) == 1
    assert cards[0].sku == "SKU-1"
    assert cards[0].categories_uid == ["root", "a1", "a2"]
