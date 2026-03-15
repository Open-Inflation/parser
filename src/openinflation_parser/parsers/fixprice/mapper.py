from __future__ import annotations

import re
from typing import Any, Literal

from openinflation_dataclass import (
    AdministrativeUnit,
    Card,
    Category,
    MetaData,
    RetailUnit,
    Schedule,
)

from ..model_builder import build_model
from .types import CountryCode, CurrencyCode, ProducerCountryCode


class FixPriceMapper:
    """Mappers from FixPrice API contracts to openinflation dataclasses."""

    COUNTRY_ID_TO_CODE: dict[int, CountryCode] = {
        2: "RUS",
        8: "BLR",
        10: "ARE",
    }
    COUNTRY_TO_CURRENCY: dict[CountryCode, CurrencyCode] = {
        "RUS": "RUB",
        "BLR": "BYN",
        "ARE": "AED",
        "USA": "USD",
    }
    COUNTRY_ALIAS_TO_CURRENCY: dict[str, CurrencyCode] = {
        "RU": "RUB",
        "BY": "BYN",
        "AE": "AED",
        "LV": "EUR",
        "US": "USD",
    }
    PRODUCER_COUNTRY_NAMES: dict[str, ProducerCountryCode] = {
        "беларусь": "BLR",
        "белоруссия": "BLR",
        "республика беларусь": "BLR",
        "россия": "RUS",
        "российская федерация": "RUS",
        "сша": "USA",
        "соединенные штаты": "USA",
        "соединенные штаты америки": "USA",
        "оаэ": "ARE",
        "объединенные арабские эмираты": "ARE",
        "китай": "CHN",
        "кнр": "CHN",
        "китайская народная республика": "CHN",
    }
    PRICE_UNIT_ALIASES: dict[str, CurrencyCode] = {
        "BYN": "BYN",
        "RUB": "RUB",
        "USD": "USD",
        "EUR": "EUR",
        "AED": "AED",
    }
    HHMM_PATTERN = re.compile(r"(?:[01]\d|2[0-3]):[0-5]\d")
    DECIMAL_PATTERN = re.compile(r"^-?\d+(?:\.\d+)?$")
    COUNTRY_NAME_CLEAN_PATTERN = re.compile(r"[^а-я0-9]+")
    MULTISPACE_PATTERN = re.compile(r"\s+")
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
    def _build(
        cls,
        model_cls: Any,
        payload: dict[str, Any],
        *,
        strict_validation: bool,
    ) -> Any:
        return build_model(
            model_cls,
            payload,
            strict_validation=strict_validation,
        )

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

    @classmethod
    def currency_from_country_payload(cls, country: dict[str, Any]) -> CurrencyCode | None:
        country_alias = cls._safe_str(country.get("alias"))
        if country_alias is None:
            return None
        return cls.COUNTRY_ALIAS_TO_CURRENCY.get(country_alias.strip().upper())

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

    @classmethod
    def _numeric_from_raw(cls, value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            token = value.strip().replace(",", ".")
            if not token:
                return None
            if cls.DECIMAL_PATTERN.fullmatch(token) is None:
                return None
            try:
                return float(token)
            except ValueError:
                return None
        return None

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
        return cls._numeric_from_raw(value)

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
    def _schedule_from_raw(
        cls,
        value: Any,
        *,
        strict_validation: bool,
    ) -> Schedule:
        payload: dict[str, Any]
        if not isinstance(value, str) or "-" not in value:
            payload = {"open_from": None, "closed_from": None}
            return cls._build(
                Schedule,
                payload,
                strict_validation=strict_validation,
            )
        opened, closed = [part.strip() for part in value.split("-", 1)]
        if not cls.HHMM_PATTERN.fullmatch(opened):
            payload = {"open_from": None, "closed_from": None}
            return cls._build(
                Schedule,
                payload,
                strict_validation=strict_validation,
            )
        if not cls.HHMM_PATTERN.fullmatch(closed):
            payload = {"open_from": None, "closed_from": None}
            return cls._build(
                Schedule,
                payload,
                strict_validation=strict_validation,
            )
        payload = {"open_from": opened, "closed_from": closed}
        return cls._build(
            Schedule,
            payload,
            strict_validation=strict_validation,
        )

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
    def _normalize_country_name(cls, value: str) -> str:
        normalized = value.strip().lower().replace("ё", "е")
        normalized = cls.COUNTRY_NAME_CLEAN_PATTERN.sub(" ", normalized)
        normalized = cls.MULTISPACE_PATTERN.sub(" ", normalized).strip()
        return normalized

    @classmethod
    def _producer_country_from_raw(cls, value: Any) -> ProducerCountryCode | None:
        token = cls._safe_str(value)
        if token is None:
            return None
        normalized = cls._normalize_country_name(token)
        if not normalized:
            return None
        return cls.PRODUCER_COUNTRY_NAMES.get(normalized)

    @classmethod
    def _price_unit_from_raw(cls, value: Any) -> CurrencyCode | None:
        token = cls._safe_str(value)
        if token is None:
            return None
        normalized = token.strip().upper()
        if not normalized:
            return None
        return cls.PRICE_UNIT_ALIASES.get(normalized)

    @classmethod
    def _metadata_from_product(
        cls,
        product: dict[str, Any],
        *,
        strict_validation: bool,
    ) -> list[MetaData] | None:
        prepared_meta: list[MetaData] = []
        seen: set[tuple[str, str, int | float | str]] = set()

        def append_metadata(*, raw_name: Any, raw_alias: Any, raw_value: Any) -> None:
            name = cls._safe_str(raw_name)
            alias = cls._safe_str(raw_alias)
            if name is None or alias is None:
                return
            if isinstance(raw_value, bool):
                return
            if not isinstance(raw_value, (int, float, str)):
                return

            key = (name, alias, raw_value)
            if key in seen:
                return
            seen.add(key)
            prepared_meta.append(
                cls._build(
                    MetaData,
                    {"name": name, "alias": alias, "value": raw_value},
                    strict_validation=strict_validation,
                )
            )

        raw_meta = product.get("metaData")
        if isinstance(raw_meta, list):
            for item in raw_meta:
                if not isinstance(item, dict):
                    continue
                append_metadata(
                    raw_name=item.get("name"),
                    raw_alias=item.get("alias"),
                    raw_value=item.get("value"),
                )

        for block_key in ("properties", "extraDescriptions"):
            block = product.get(block_key)
            if not isinstance(block, list):
                continue
            for item in block:
                if not isinstance(item, dict):
                    continue
                append_metadata(
                    raw_name=item.get("title"),
                    raw_alias=item.get("alias"),
                    raw_value=item.get("value"),
                )

        return prepared_meta or None

    @classmethod
    def _first_property_value(cls, product: dict[str, Any], *, alias: str) -> Any:
        properties = product.get("properties")
        if not isinstance(properties, list):
            return None
        for item in properties:
            if not isinstance(item, dict):
                continue
            if item.get("alias") == alias:
                return item.get("value")
        return None

    @classmethod
    def _first_extra_description_value(cls, product: dict[str, Any], *, alias: str) -> Any:
        extra_descriptions = product.get("extraDescriptions")
        if not isinstance(extra_descriptions, list):
            return None
        for item in extra_descriptions:
            if not isinstance(item, dict):
                continue
            if item.get("alias") == alias:
                return item.get("value")
        return None

    @classmethod
    def _category_uid_from_product(cls, product: dict[str, Any]) -> str | None:
        category = product.get("category")
        if isinstance(category, dict):
            return cls._id_to_str(category.get("id"))
        return cls._id_to_str(category)

    @classmethod
    def _first_variant(cls, product: dict[str, Any]) -> dict[str, Any]:
        variants = product.get("variants")
        if not isinstance(variants, list):
            return {}
        for item in variants:
            if isinstance(item, dict):
                return item
        return {}

    @classmethod
    def map_category_node(
        cls,
        node: dict[str, Any],
        *,
        strict_validation: bool = False,
    ) -> Category:
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
                children.append(
                    cls.map_category_node(
                        child,
                        strict_validation=strict_validation,
                    )
                )

        return cls._build(
            Category,
            {
                "uid": uid,
                "alias": alias,
                "title": title,
                "adult": cls._safe_bool(node.get("adult")),
                "children": children,
            },
            strict_validation=strict_validation,
        )

    @classmethod
    def map_city(
        cls,
        city: dict[str, Any],
        *,
        country_id: int | None = None,
        strict_validation: bool = False,
    ) -> AdministrativeUnit:
        effective_country_id = cls._safe_int(city.get("countryId"))
        if effective_country_id is None:
            effective_country_id = country_id
        country = cls.country_code_from_id(effective_country_id)

        city_name = cls._safe_str(city.get("title"))
        if city_name is None:
            city_name = cls._safe_str(city.get("name"))

        return cls._build(
            AdministrativeUnit,
            {
                "settlement_type": cls._settlement_type(city.get("prefix")),
                "name": city_name,
                "alias": cls._safe_str(city.get("alias")),
                "country": country,
                "region": cls._safe_str(city.get("regionTitle")),
                "longitude": cls._safe_float(city.get("longitude")),
                "latitude": cls._safe_float(city.get("latitude")),
            },
            strict_validation=strict_validation,
        )

    @classmethod
    def fallback_administrative_unit(
        cls,
        *,
        country_id: int | None,
        city_id: int | None,
        strict_validation: bool = False,
    ) -> AdministrativeUnit:
        del country_id
        del city_id
        return cls._build(
            AdministrativeUnit,
            {
                "settlement_type": None,
                "name": None,
                "alias": None,
                "country": None,
                "region": None,
                "longitude": None,
                "latitude": None,
            },
            strict_validation=strict_validation,
        )

    @classmethod
    def map_store(
        cls,
        store: dict[str, Any],
        *,
        administrative_unit: AdministrativeUnit,
        strict_validation: bool = False,
    ) -> RetailUnit:
        warehouse = cls._safe_bool(store.get("warehouse"))
        if warehouse is True:
            retail_type: Literal["pickup_point", "store", "warehouse"] | None = "warehouse"
        elif warehouse is False:
            retail_type = "store"
        else:
            retail_type = None

        return cls._build(
            RetailUnit,
            {
                "retail_type": retail_type,
                "code": cls._safe_str(store.get("pfm")),
                "address": cls._safe_str(store.get("address")),
                "schedule_weekdays": cls._schedule_from_raw(
                    store.get("scheduleWeekdays"),
                    strict_validation=strict_validation,
                ),
                "schedule_saturday": cls._schedule_from_raw(
                    store.get("scheduleSaturday"),
                    strict_validation=strict_validation,
                ),
                "schedule_sunday": cls._schedule_from_raw(
                    store.get("scheduleSunday"),
                    strict_validation=strict_validation,
                ),
                "temporarily_closed": cls._safe_bool(store.get("temporarilyClosed")),
                "longitude": cls._safe_float(store.get("longitude")),
                "latitude": cls._safe_float(store.get("latitude")),
                "administrative_unit": administrative_unit,
                "categories": None,
                "products": None,
            },
            strict_validation=strict_validation,
        )

    @classmethod
    def map_product(
        cls,
        product: dict[str, Any],
        *,
        price_unit: CurrencyCode | None = None,
        main_image: str | None = None,
        gallery_images: list[str] | None = None,
        strict_validation: bool = False,
    ) -> Card:
        sku = cls._safe_str(product.get("sku"))
        category_id = cls._category_uid_from_product(product)
        categories_uid = [category_id] if category_id is not None else None

        source_slug = cls._safe_str(product.get("url"))
        source_page_url: str | None = None
        if source_slug:
            source_page_url = f"https://fix-price.com/catalog/{source_slug.lstrip('/')}"

        unit = cls._unit_from_raw(product.get("unit"))
        if unit is None:
            unit = cls._unit_from_raw(product.get("unitType"))
        available_raw = product.get("inStock")
        available_count = (
            cls._safe_int(available_raw)
            if unit == "PCE"
            else cls._safe_float(available_raw)
        )

        metadata = cls._metadata_from_product(
            product,
            strict_validation=strict_validation,
        )

        brand_block = product.get("brand")
        brand: str | None = None
        if isinstance(brand_block, dict):
            brand = cls._safe_str(brand_block.get("title"))
        elif isinstance(brand_block, str):
            brand = brand_block

        producer_name = cls._safe_str(
            cls._first_property_value(product, alias="manufacturer")
        )
        producer_country = cls._producer_country_from_raw(
            cls._first_property_value(product, alias="prodCountry")
        )
        composition = cls._safe_str(
            cls._first_extra_description_value(product, alias="composition")
        )

        variant = cls._first_variant(product)
        dimension_height = cls._numeric_from_raw(variant.get("height"))
        dimension_width = cls._numeric_from_raw(variant.get("width"))
        dimension_depth = cls._numeric_from_raw(variant.get("length"))

        package_quantity_net = None
        package_quantity_gross = None
        package_unit = None

        effective_price_unit = cls._price_unit_from_raw(product.get("priceUnit"))
        if effective_price_unit is None:
            effective_price_unit = price_unit

        price = cls._price_str_to_float(product.get("price"))
        if price is None:
            price = cls._price_str_to_float(variant.get("price"))
        loyal_price = (
            cls._price_str_to_float(product.get("specialPrice", {}).get("price"))
            if isinstance(product.get("specialPrice"), dict)
            else None
        )
        discount_price = None

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
            "brand": brand,
            "producer_name": producer_name,
            "producer_country": producer_country,
            "composition": composition,
            "meta_data": metadata,
            "expiration_date_in_days": cls._safe_int(product.get("expirationDateInDays")),
            "rating": cls._numeric_from_raw(product.get("rating")),
            "reviews_count": cls._safe_int(product.get("reviewsCount")),
            "price": price,
            "discount_price": discount_price,
            "loyal_price": loyal_price,
            "wholesale_price": None,
            "price_unit": effective_price_unit,
            "unit": unit,
            "available_count": available_count,
            "package_quantity_net": package_quantity_net,
            "package_quantity_gross": package_quantity_gross,
            "package_unit": package_unit,
            "dimension_height": dimension_height,
            "dimension_width": dimension_width,
            "dimension_depth": dimension_depth,
            "categories_uid": categories_uid,
            "main_image": main_image,
            "images": gallery_images if gallery_images else None,
        }
        return cls._build(
            Card,
            card_payload,
            strict_validation=strict_validation,
        )
