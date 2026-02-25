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


class PerekrestokMapper:
    """Mappers from Perekrestok API contracts to openinflation dataclasses."""

    COUNTRY_ID_TO_CODE: dict[int, CountryCode] = {
        2: "RUS",
    }
    COUNTRY_TO_CURRENCY: dict[CountryCode, CurrencyCode] = {
        "RUS": "RUB",
    }
    COUNTRY_NAME_TO_CODE: dict[str, ProducerCountryCode] = {
        "россия": "RUS",
        "рф": "RUS",
        "russia": "RUS",
        "rus": "RUS",
        "китай": "CHN",
        "кнр": "CHN",
        "china": "CHN",
        "chn": "CHN",
        "беларусь": "BLR",
        "белоруссия": "BLR",
        "belarus": "BLR",
        "blr": "BLR",
        "сша": "USA",
        "usa": "USA",
        "соединенные штаты": "USA",
        "united states": "USA",
        "оаэ": "ARE",
        "uae": "ARE",
        "объединенные арабские эмираты": "ARE",
    }
    HHMM_RANGE_PATTERN = re.compile(
        r"^\s*(\d{1,2}:\d{2})(?::\d{2})?\s*-\s*(\d{1,2}:\d{2})(?::\d{2})?\s*$"
    )
    EXPIRATION_DAYS_PATTERN = re.compile(
        r"^\s*(\d+)\s*(?:дн(?:\.|ей|я|ь)?|день|дня|дней|day|days)?\s*$",
        flags=re.IGNORECASE,
    )

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

    @staticmethod
    def _safe_int(value: Any) -> int | None:
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

    @staticmethod
    def _first_scalar(values: Any) -> int | float | str | None:
        if isinstance(values, bool):
            return None
        if isinstance(values, (int, float, str)):
            return values
        if not isinstance(values, list):
            return None
        for item in values:
            if isinstance(item, bool):
                continue
            if isinstance(item, (int, float, str)):
                return item
        return None

    @classmethod
    def _feature_value_map(
        cls,
        product: dict[str, Any],
    ) -> tuple[dict[str, int | float | str], list[tuple[str, str, int | float | str]]]:
        features_raw = product.get("features")
        values: dict[str, int | float | str] = {}
        metadata_rows: list[tuple[str, str, int | float | str]] = []
        if not isinstance(features_raw, list):
            return values, metadata_rows

        for group in features_raw:
            if not isinstance(group, dict):
                continue
            items = group.get("items")
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                alias = cls._safe_str(item.get("key"))
                name = cls._safe_str(item.get("title"))
                value = cls._first_scalar(item.get("displayValues"))
                if value is None:
                    value = cls._first_scalar(item.get("values"))
                if value is None:
                    value = cls._first_scalar(item.get("value"))
                if alias is None or name is None or value is None:
                    continue
                values.setdefault(alias, value)
                metadata_rows.append((name, alias, value))
        return values, metadata_rows

    @classmethod
    def _producer_country_from_raw(cls, value: Any) -> ProducerCountryCode | None:
        token = cls._safe_str(value)
        if token is None:
            return None
        normalized = token.strip().lower()
        if not normalized:
            return None
        return cls.COUNTRY_NAME_TO_CODE.get(normalized)

    @staticmethod
    def _kopecks_to_rub(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value) / 100.0
        return None

    @classmethod
    def _rating_from_raw(cls, value: Any) -> float | None:
        base = cls._safe_float(value)
        if base is None:
            return None
        return base * 0.01

    @classmethod
    def _expiration_days_from_raw(cls, value: Any) -> int | None:
        base_int = cls._safe_int(value)
        if base_int is not None:
            return base_int
        token = cls._safe_str(value)
        if token is None:
            return None
        matched = cls.EXPIRATION_DAYS_PATTERN.fullmatch(token)
        if matched is None:
            return None
        try:
            return int(matched.group(1))
        except ValueError:
            return None

    @staticmethod
    def _normalize_hhmm(value: str) -> str | None:
        parts = value.split(":")
        if len(parts) != 2:
            return None
        hour_raw, minute_raw = parts
        if not (hour_raw.isdigit() and minute_raw.isdigit()):
            return None
        hour = int(hour_raw)
        minute = int(minute_raw)
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return None
        return f"{hour:02d}:{minute:02d}"

    @classmethod
    def _empty_schedule(cls, *, strict_validation: bool) -> Schedule:
        return cls._build(
            Schedule,
            {
                "open_from": None,
                "closed_from": None,
            },
            strict_validation=strict_validation,
        )

    @classmethod
    def _schedule_from_raw(
        cls,
        value: Any,
        *,
        strict_validation: bool,
    ) -> Schedule:
        if not isinstance(value, str):
            return cls._empty_schedule(strict_validation=strict_validation)

        matched = cls.HHMM_RANGE_PATTERN.fullmatch(value)
        if matched is None:
            return cls._empty_schedule(strict_validation=strict_validation)

        open_from = cls._normalize_hhmm(matched.group(1))
        closed_from = cls._normalize_hhmm(matched.group(2))
        if open_from is None or closed_from is None:
            return cls._empty_schedule(strict_validation=strict_validation)

        return cls._build(
            Schedule,
            {
                "open_from": open_from,
                "closed_from": closed_from,
            },
            strict_validation=strict_validation,
        )

    @classmethod
    def _schedule_from_pickup_operation_time(
        cls,
        value: Any,
        *,
        strict_validation: bool,
    ) -> Schedule:
        if not isinstance(value, dict):
            return cls._empty_schedule(strict_validation=strict_validation)
        open_from = cls._safe_str(value.get("from"))
        closed_from = cls._safe_str(value.get("to"))
        if open_from is None or closed_from is None:
            return cls._empty_schedule(strict_validation=strict_validation)

        open_normalized = cls._normalize_hhmm(open_from)
        closed_normalized = cls._normalize_hhmm(closed_from)
        if open_normalized is None or closed_normalized is None:
            return cls._empty_schedule(strict_validation=strict_validation)

        return cls._build(
            Schedule,
            {
                "open_from": open_normalized,
                "closed_from": closed_normalized,
            },
            strict_validation=strict_validation,
        )

    @staticmethod
    def _coordinates_from_location(location: Any) -> tuple[float | None, float | None]:
        if not isinstance(location, dict):
            return None, None
        coordinates = location.get("coordinates")
        if not isinstance(coordinates, list) or len(coordinates) < 2:
            return None, None

        lon_raw = coordinates[0]
        lat_raw = coordinates[1]
        if isinstance(lon_raw, bool) or isinstance(lat_raw, bool):
            return None, None
        if not isinstance(lon_raw, (int, float)):
            return None, None
        if not isinstance(lat_raw, (int, float)):
            return None, None
        return float(lon_raw), float(lat_raw)

    @classmethod
    def _unit_from_raw(cls, value: Any) -> Literal["PCE", "KGM", "LTR"] | None:
        token = cls._safe_str(value)
        if token is None:
            return None
        normalized = token.strip().lower()
        if not normalized:
            return None
        if normalized in {"шт", "pce", "piece", "pcs"}:
            return "PCE"
        if normalized in {"кг", "kg", "kgm", "килограмм"}:
            return "KGM"
        if normalized in {"л", "ltr", "liter", "литр"}:
            return "LTR"
        return None

    @classmethod
    def _category_from_node(
        cls,
        node: dict[str, Any],
        *,
        strict_validation: bool,
    ) -> Category:
        category = node.get("category") if isinstance(node.get("category"), dict) else {}
        raw_children = node.get("children")
        children: list[Category] = []
        if isinstance(raw_children, list):
            for child in raw_children:
                if not isinstance(child, dict):
                    continue
                children.append(
                    cls._category_from_node(
                        child,
                        strict_validation=strict_validation,
                    )
                )

        return cls._build(
            Category,
            {
                "uid": cls._id_to_str(category.get("id")),
                "alias": cls._safe_str(category.get("slug")),
                "title": cls._safe_str(category.get("title")),
                "adult": cls._safe_bool(category.get("isAdultContent")),
                "children": children,
            },
            strict_validation=strict_validation,
        )

    @classmethod
    def map_category_node(
        cls,
        node: dict[str, Any],
        *,
        strict_validation: bool = False,
    ) -> Category:
        return cls._category_from_node(
            node,
            strict_validation=strict_validation,
        )

    @classmethod
    def map_city(
        cls,
        city: dict[str, Any],
        *,
        country_id: int | None = 2,
        strict_validation: bool = False,
    ) -> AdministrativeUnit:
        longitude, latitude = cls._coordinates_from_location(city.get("location"))
        return cls._build(
            AdministrativeUnit,
            {
                "settlement_type": "city",
                "name": cls._safe_str(city.get("name")),
                "alias": cls._safe_str(city.get("slug")) or cls._safe_str(city.get("key")),
                "country": cls.country_code_from_id(country_id),
                "region": cls._safe_str(city.get("area")),
                "longitude": longitude,
                "latitude": latitude,
            },
            strict_validation=strict_validation,
        )

    @classmethod
    def fallback_administrative_unit(
        cls,
        *,
        strict_validation: bool = False,
    ) -> AdministrativeUnit:
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
        is_catalog_available = cls._safe_bool(store.get("isCatalogAvailable"))
        is_pickup_available = cls._safe_bool(store.get("isPickupAvailable"))

        retail_type: Literal["pickup_point", "store", "warehouse"] | None
        if is_catalog_available is True:
            retail_type = "store"
        elif is_catalog_available is False and is_pickup_available is True:
            retail_type = "pickup_point"
        else:
            retail_type = None

        longitude, latitude = cls._coordinates_from_location(store.get("location"))

        weekdays = cls._schedule_from_raw(
            store.get("schedule"),
            strict_validation=strict_validation,
        )
        if weekdays.open_from is None and weekdays.closed_from is None:
            weekdays = cls._schedule_from_pickup_operation_time(
                store.get("pickupOperationTime"),
                strict_validation=strict_validation,
            )

        return cls._build(
            RetailUnit,
            {
                "retail_type": retail_type,
                "code": cls._id_to_str(store.get("id")),
                "address": cls._safe_str(store.get("address")),
                "schedule_weekdays": weekdays,
                "schedule_saturday": cls._empty_schedule(
                    strict_validation=strict_validation
                ),
                "schedule_sunday": cls._empty_schedule(
                    strict_validation=strict_validation
                ),
                "temporarily_closed": None,
                "longitude": longitude,
                "latitude": latitude,
                "administrative_unit": administrative_unit,
                "categories": None,
                "products": None,
            },
            strict_validation=strict_validation,
        )

    @classmethod
    def _merge_categories_uid(
        cls,
        *groups: list[str] | None,
    ) -> list[str] | None:
        prepared: list[str] = []
        seen: set[str] = set()
        for group in groups:
            if not isinstance(group, list):
                continue
            for item in group:
                token = cls._safe_str(item)
                if token is None or token in seen:
                    continue
                seen.add(token)
                prepared.append(token)
        return prepared or None

    @classmethod
    def _categories_uid_from_product(cls, product: dict[str, Any]) -> list[str] | None:
        category_ids: list[str] = []

        primary_category = (
            product.get("primaryCategory")
            if isinstance(product.get("primaryCategory"), dict)
            else {}
        )
        primary_id = cls._id_to_str(primary_category.get("id"))
        if primary_id is not None:
            category_ids.append(primary_id)

        catalog_primary_category = (
            product.get("catalogPrimaryCategory")
            if isinstance(product.get("catalogPrimaryCategory"), dict)
            else {}
        )
        catalog_primary_id = cls._id_to_str(catalog_primary_category.get("id"))
        if catalog_primary_id is not None:
            category_ids.append(catalog_primary_id)

        return category_ids or None

    @classmethod
    def map_product(
        cls,
        product: dict[str, Any],
        *,
        categories_uid: list[str] | None = None,
        main_image: str | None = None,
        gallery_images: list[str] | None = None,
        strict_validation: bool = False,
    ) -> Card:
        master_data = (
            product.get("masterData")
            if isinstance(product.get("masterData"), dict)
            else {}
        )
        price_tag = (
            product.get("priceTag") if isinstance(product.get("priceTag"), dict) else {}
        )

        feature_values, metadata_rows = cls._feature_value_map(product)
        metadata: list[MetaData] | None = None
        if metadata_rows:
            metadata = [
                cls._build(
                    MetaData,
                    {
                        "name": name,
                        "alias": alias,
                        "value": value,
                    },
                    strict_validation=strict_validation,
                )
                for name, alias, value in metadata_rows
            ]

        primary_category = (
            product.get("primaryCategory")
            if isinstance(product.get("primaryCategory"), dict)
            else {}
        )
        catalog_primary_category = (
            product.get("catalogPrimaryCategory")
            if isinstance(product.get("catalogPrimaryCategory"), dict)
            else {}
        )

        product_categories_uid = cls._categories_uid_from_product(product)
        merged_categories_uid = cls._merge_categories_uid(
            categories_uid,
            product_categories_uid,
        )

        payload: dict[str, Any] = {
            "sku": cls._id_to_str(product.get("id")),
            "plu": cls._safe_str(master_data.get("plu")) or cls._safe_str(product.get("plu")),
            "source_page_url": None,
            "title": cls._safe_str(product.get("title")),
            "description": cls._safe_str(product.get("description")),
            "adult": cls._safe_bool(primary_category.get("isAdultContent"))
            if cls._safe_bool(primary_category.get("isAdultContent")) is not None
            else cls._safe_bool(catalog_primary_category.get("isAdultContent")),
            "new": None,
            "promo": None,
            "season": None,
            "hit": None,
            "data_matrix": cls._safe_bool(master_data.get("isMark")),
            "brand": cls._safe_str(feature_values.get("brand")),
            "producer_name": cls._safe_str(feature_values.get("proizvoditel")),
            "producer_country": cls._producer_country_from_raw(feature_values.get("strana")),
            "composition": cls._safe_str(feature_values.get("sostav")),
            "meta_data": metadata,
            "expiration_date_in_days": cls._expiration_days_from_raw(
                feature_values.get("srok-hraneniya-maks")
            ),
            "rating": cls._rating_from_raw(product.get("rating")),
            "reviews_count": cls._safe_int(product.get("reviewCount")),
            "price": cls._kopecks_to_rub(price_tag.get("price")),
            "discount_price": None,
            "loyal_price": None,
            "wholesale_price": None,
            "price_unit": cls.currency_for_country_id(2),
            "unit": cls._unit_from_raw(master_data.get("unitName")),
            "available_count": None,
            "package_quantity": None,
            "package_unit": None,
            "categories_uid": merged_categories_uid,
            "main_image": main_image,
            "images": gallery_images if gallery_images else None,
        }

        return cls._build(
            Card,
            payload,
            strict_validation=strict_validation,
        )
