from __future__ import annotations

from openinflation_dataclass import Category

from openinflation_parser.parsers.fixprice import FixPriceParser


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


def test_build_catalog_queries_full_mode_includes_subcategories() -> None:
    parser = FixPriceParser()
    categories = _build_category_tree()

    queries = parser.build_catalog_queries(
        categories,
        full_catalog=True,
        category_limit=1,
    )

    prepared = {(query.category_alias, query.subcategory_alias) for query in queries}
    assert ("a", None) in prepared
    assert ("a", "a1") in prepared
    assert ("a", "a2") in prepared
    assert ("b", None) in prepared
