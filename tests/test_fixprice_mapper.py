from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path

import pytest

from openinflation_parser.parsers.fixprice import FixPriceMapper


SNAPSHOT_DIR = (
    Path(__file__).resolve().parents[2] / "fixprice_api" / "tests" / "__snapshots__"
)


def _load_snapshot(name: str):
    path = SNAPSHOT_DIR / name
    if not path.exists():
        pytest.skip(f"Snapshot file is missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def test_map_category_node_from_snapshot() -> None:
    tree = _load_snapshot("ClassCatalog.tree.json")
    first_node = next(iter(tree.values()))
    mapped = FixPriceMapper.map_category_node(first_node)

    assert mapped.uid == str(first_node["id"])
    assert mapped.alias == first_node["alias"]
    assert mapped.title == first_node["title"]
    assert isinstance(mapped.children, list)


def test_map_city_from_snapshot() -> None:
    cities = _load_snapshot("ClassGeolocation.cities_list.json")
    city = cities[0]

    mapped = FixPriceMapper.map_city(city, country_id=2)

    assert mapped.name == city["title"]
    assert mapped.country == "RUS"
    assert mapped.longitude == city["longitude"]
    assert mapped.latitude == city["latitude"]


def test_map_store_from_snapshot() -> None:
    cities = _load_snapshot("ClassGeolocation.cities_list.json")
    shops = _load_snapshot("ShopService.search.json")

    city = FixPriceMapper.map_city(cities[0], country_id=2)
    mapped = FixPriceMapper.map_store(shops[0], administrative_unit=city)

    assert mapped.code == shops[0]["pfm"]
    assert mapped.address == shops[0]["address"]
    assert mapped.retail_type == "warehouse"
    assert mapped.schedule_weekdays.open_from == "09:00"
    assert mapped.schedule_weekdays.closed_from == "20:00"


def test_map_product_from_snapshot() -> None:
    products = _load_snapshot("ClassCatalog.products_list.json")
    product = products[0]

    mapped = FixPriceMapper.map_product(
        product,
        main_image=BytesIO(b"main"),
        gallery_images=[BytesIO(b"extra")],
    )

    assert mapped.sku == product["sku"]
    assert mapped.price == 124.0
    assert mapped.price_unit is None
    assert mapped.plu is None
    assert mapped.categories_uid
    assert mapped.main_image.getvalue() == b"main"
    assert mapped.images[0].getvalue() == b"extra"


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
            # no "warehouse" key
        },
        administrative_unit=city,
    )
    assert mapped.retail_type is None


def test_country_mapping_covers_snapshot_country_ids() -> None:
    countries = _load_snapshot("ClassGeolocation.countries_list.json")

    expected = {
        2: "RUS",
        3: "KAZ",
        4: "GEO",
        5: "KGZ",
        6: "UZB",
        7: "LVA",
        8: "BLR",
        9: "MNG",
        10: "ARE",
        11: "SRB",
    }

    ids = {row["id"] for row in countries}
    assert set(expected).issubset(ids)

    for country_id, code in expected.items():
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
