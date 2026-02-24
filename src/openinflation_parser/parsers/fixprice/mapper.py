from __future__ import annotations

import re
from io import BytesIO
from typing import Any, Literal

from openinflation_dataclass import (
    AdministrativeUnit,
    Card,
    Category,
    MetaData,
    RetailUnit,
    Schedule,
)

from .types import CountryCode, CurrencyCode, ProducerCountryCode


class FixPriceMapper:
    """Mappers from FixPrice API contracts to openinflation dataclasses."""

    COUNTRY_ID_TO_CODE: dict[int, CountryCode] = {
        7: "LVA",
        2: "RUS",
        4: "GEO",
        5: "KGZ",
        6: "UZB",
        3: "KAZ",
        8: "BLR",
        9: "MNG",
        10: "ARE",
        11: "SRB",
    }
    COUNTRY_TO_CURRENCY: dict[CountryCode, CurrencyCode] = {
        "RUS": "RUB",
        "BLR": "BYN",
        "ARE": "AED",
        "USA": "USD",
        "LVA": "EUR",
        "GEO": "GEL",
        "KGZ": "KGS",
        "UZB": "UZS",
        "MNG": "MNT",
        "SRB": "RSD",
        "KAZ": "KZT",
    }
    HHMM_PATTERN = re.compile(r"(?:[01]\d|2[0-3]):[0-5]\d")
    DECIMAL_PATTERN = re.compile(r"^-?\d+(?:\.\d+)?$")
    CITY_PREFIXES = {"г", "г.", "город", "city"}
    VILLAGE_PREFIXES = {
        "п",
        "п.",
        "пос",
        "пос.",
        "поселок",
        "посёлок",
        "с",
        "с.",
        "село",
        "д",
        "д.",
        "деревня",
        "village",
    }

    @classmethod
    def country_code_from_id(cls, country_id: int | None) -> CountryCode | None:
        if country_id is None:
            return None
        return cls.COUNTRY_ID_TO_CODE.get(country_id)

    @classmethod
    def producer_country_from_country_id(
        cls,
        country_id: int | None,
    ) -> ProducerCountryCode | None:
        return cls.country_code_from_id(country_id)

    @classmethod
    def currency_for_country_id(cls, country_id: int | None) -> CurrencyCode | None:
        country = cls.country_code_from_id(country_id)
        if country is None:
            return None
        return cls.COUNTRY_TO_CURRENCY.get(country)

    @staticmethod
    def _safe_str(value: Any) -> str | None:
        return value if isinstance(value, str) else None

    @staticmethod
    def _safe_bool(value: Any) -> bool | None:
        return value if isinstance(value, bool) else None

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        return None

    @classmethod
    def _safe_int(cls, value: Any) -> int | None:
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    @staticmethod
    def _id_to_str(value: Any) -> str | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return str(value)
        if isinstance(value, str):
            return value
        return None

    @classmethod
    def _price_str_to_float(cls, value: Any) -> float | None:
        if not isinstance(value, str):
            return None
        if not cls.DECIMAL_PATTERN.fullmatch(value):
            return None
        try:
            return float(value)
        except ValueError:
            return None

    @classmethod
    def _settlement_type(cls, prefix: Any) -> Literal["village", "city"] | None:
        if not isinstance(prefix, str):
            return None
        value = prefix.strip().lower()
        if not value:
            return None
        if value in cls.CITY_PREFIXES:
            return "city"
        if value in cls.VILLAGE_PREFIXES:
            return "village"
        return None

    @classmethod
    def _schedule_from_raw(cls, value: Any) -> Schedule:
        if not isinstance(value, str) or "-" not in value:
            return Schedule.model_construct(open_from=None, closed_from=None)
        opened, closed = [part.strip() for part in value.split("-", 1)]
        if not cls.HHMM_PATTERN.fullmatch(opened):
            return Schedule.model_construct(open_from=None, closed_from=None)
        if not cls.HHMM_PATTERN.fullmatch(closed):
            return Schedule.model_construct(open_from=None, closed_from=None)
        return Schedule.model_construct(open_from=opened, closed_from=closed)

    @classmethod
    def _unit_from_raw(cls, value: Any) -> Literal["PCE", "KGM", "LTR"] | None:
        if not isinstance(value, str):
            return None
        token = value.strip().lower()
        if not token:
            return None
        if token in {"кг", "kg", "kgm", "килограмм", "килограммы"}:
            return "KGM"
        if token in {"л", "ltr", "liter", "литр", "литры"}:
            return "LTR"
        if token in {"шт", "pce", "piece", "pcs"}:
            return "PCE"
        return None

    @classmethod
    def map_category_node(cls, node: dict[str, Any]) -> Category:
        uid = cls._id_to_str(node.get("id"))
        alias = cls._safe_str(node.get("alias"))
        title = cls._safe_str(node.get("title"))

        raw_children = node.get("items")
        if isinstance(raw_children, dict):
            child_nodes = raw_children.values()
        elif isinstance(raw_children, list):
            child_nodes = raw_children
        else:
            child_nodes = []

        children: list[Category] = []
        for child in child_nodes:
            if isinstance(child, dict):
                children.append(cls.map_category_node(child))

        return Category.model_construct(
            uid=uid,
            alias=alias,
            title=title,
            adult=cls._safe_bool(node.get("adult")),
            children=children,
        )

    @classmethod
    def map_city(cls, city: dict[str, Any], *, country_id: int | None = None) -> AdministrativeUnit:
        effective_country_id = cls._safe_int(city.get("countryId"))
        if effective_country_id is None:
            effective_country_id = country_id
        country = cls.country_code_from_id(effective_country_id)

        city_name = cls._safe_str(city.get("title"))
        if city_name is None:
            city_name = cls._safe_str(city.get("name"))

        return AdministrativeUnit.model_construct(
            settlement_type=cls._settlement_type(city.get("prefix")),
            name=city_name,
            alias=cls._safe_str(city.get("alias")),
            country=country,
            region=cls._safe_str(city.get("regionTitle")),
            longitude=cls._safe_float(city.get("longitude")),
            latitude=cls._safe_float(city.get("latitude")),
        )

    @classmethod
    def fallback_administrative_unit(
        cls,
        *,
        country_id: int | None,
        city_id: int | None,
    ) -> AdministrativeUnit:
        del country_id
        del city_id
        return AdministrativeUnit.model_construct(
            settlement_type=None,
            name=None,
            alias=None,
            country=None,
            region=None,
            longitude=None,
            latitude=None,
        )

    @classmethod
    def map_store(
        cls,
        store: dict[str, Any],
        *,
        administrative_unit: AdministrativeUnit,
    ) -> RetailUnit:
        warehouse = cls._safe_bool(store.get("warehouse"))
        if warehouse is True:
            retail_type: Literal["pickup_point", "store", "warehouse"] | None = "warehouse"
        elif warehouse is False:
            retail_type = "store"
        else:
            retail_type = None

        return RetailUnit.model_construct(
            retail_type=retail_type,
            code=cls._safe_str(store.get("pfm")),
            address=cls._safe_str(store.get("address")),
            schedule_weekdays=cls._schedule_from_raw(store.get("scheduleWeekdays")),
            schedule_saturday=cls._schedule_from_raw(store.get("scheduleSaturday")),
            schedule_sunday=cls._schedule_from_raw(store.get("scheduleSunday")),
            temporarily_closed=cls._safe_bool(store.get("temporarilyClosed")),
            longitude=cls._safe_float(store.get("longitude")),
            latitude=cls._safe_float(store.get("latitude")),
            administrative_unit=administrative_unit,
            categories=None,
            products=None,
        )

    @classmethod
    def map_product(
        cls,
        product: dict[str, Any],
        *,
        main_image: BytesIO | None = None,
        gallery_images: list[BytesIO] | None = None,
    ) -> Card:
        sku = cls._safe_str(product.get("sku"))
        category = product.get("category") if isinstance(product.get("category"), dict) else {}
        category_id = cls._id_to_str(category.get("id"))
        categories_uid = [category_id] if category_id is not None else None

        source_slug = cls._safe_str(product.get("url"))
        source_page_url: str | None = None
        if source_slug:
            source_page_url = f"https://fix-price.com/catalog/{source_slug.lstrip('/')}"

        unit = cls._unit_from_raw(product.get("unit"))
        available_raw = product.get("inStock")
        available_count = cls._safe_int(available_raw) if unit == "PCE" else cls._safe_float(available_raw)

        raw_meta = product.get("metaData")
        metadata: list[MetaData] | None = None
        if isinstance(raw_meta, list):
            prepared_meta: list[MetaData] = []
            for item in raw_meta:
                if not isinstance(item, dict):
                    continue
                name = cls._safe_str(item.get("name"))
                alias = cls._safe_str(item.get("alias"))
                value = item.get("value")
                if name is None or alias is None or value is None:
                    continue
                if isinstance(value, bool):
                    continue
                if not isinstance(value, (int, float, str)):
                    continue
                prepared_meta.append(
                    MetaData.model_construct(name=name, alias=alias, value=value)
                )
            metadata = prepared_meta or None

        brand_block = product.get("brand") if isinstance(product.get("brand"), dict) else {}
        card_payload: dict[str, Any] = {
            "sku": sku,
            "plu": cls._safe_str(product.get("plu")),
            "source_page_url": source_page_url,
            "title": cls._safe_str(product.get("title")),
            "description": cls._safe_str(product.get("description")),
            "adult": cls._safe_bool(product.get("adult")),
            "new": cls._safe_bool(product.get("isNew")),
            "promo": cls._safe_bool(product.get("isPromo")),
            "season": cls._safe_bool(product.get("isSeason")),
            "hit": cls._safe_bool(product.get("isHit")),
            "data_matrix": cls._safe_bool(product.get("isQRMark")),
            "brand": cls._safe_str(brand_block.get("title")),
            "producer_name": cls._safe_str(product.get("producerName")),
            "producer_country": cls._safe_str(product.get("producerCountry")),
            "composition": cls._safe_str(product.get("composition")),
            "meta_data": metadata,
            "expiration_date_in_days": cls._safe_int(product.get("expirationDateInDays")),
            "rating": cls._safe_float(product.get("rating")),
            "reviews_count": cls._safe_int(product.get("reviewsCount")),
            "price": cls._price_str_to_float(product.get("price")),
            "discount_price": (
                cls._price_str_to_float(product.get("specialPrice"))
                if product.get("specialPrice") is not None
                else None
            ),
            "loyal_price": cls._price_str_to_float(product.get("loyalPrice")),
            "wholesale_price": None,
            "price_unit": cls._safe_str(product.get("priceUnit")),
            "unit": unit,
            "available_count": available_count,
            "package_quantity": cls._safe_float(product.get("packageQuantity")),
            "package_unit": cls._safe_str(product.get("packageUnit")),
            "categories_uid": categories_uid,
            "images": gallery_images if gallery_images else None,
        }
        if main_image is not None:
            card_payload["main_image"] = main_image
        return Card.model_construct(**card_payload)
