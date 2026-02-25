from __future__ import annotations

from typing import Any

from openinflation_dataclass import (
    AdministrativeUnit,
    Card,
    Category,
    MetaData,
    RetailUnit,
    Schedule,
)

from ..model_builder import build_model


class ChizhikMapper:
    """Mappers from Chizhik API contracts to openinflation dataclasses."""

    COUNTRY_NAME_TO_CODE: dict[str, str] = {
        "россия": "RUS",
        "russia": "RUS",
        "rus": "RUS",
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

    @classmethod
    def _producer_country_from_raw(cls, value: Any) -> str | None:
        token = cls._safe_str(value)
        if token is None:
            return None
        normalized = token.strip().lower()
        if not normalized:
            return None
        return cls.COUNTRY_NAME_TO_CODE.get(normalized)

    @staticmethod
    def _empty_schedule(*, strict_validation: bool) -> Schedule:
        return build_model(
            Schedule,
            {"open_from": None, "closed_from": None},
            strict_validation=strict_validation,
        )

    @classmethod
    def _unit_from_raw(cls, value: Any) -> str | None:
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
    def _flatten_category_uids(cls, nodes: Any) -> list[str] | None:
        if not isinstance(nodes, list):
            return None
        prepared: list[str] = []
        seen: set[str] = set()

        def walk(node: Any) -> None:
            if not isinstance(node, dict):
                return
            uid = cls._id_to_str(node.get("id"))
            if uid is not None and uid not in seen:
                seen.add(uid)
                prepared.append(uid)
            raw_children = node.get("children")
            if isinstance(raw_children, list):
                for child in raw_children:
                    walk(child)

        for node in nodes:
            walk(node)
        return prepared or None

    @classmethod
    def map_category_node(
        cls,
        node: dict[str, Any],
        *,
        strict_validation: bool = False,
    ) -> Category:
        raw_children = node.get("children")
        children: list[Category] = []
        if isinstance(raw_children, list):
            for child in raw_children:
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
                "uid": cls._id_to_str(node.get("id")),
                "alias": cls._safe_str(node.get("slug")),
                "title": cls._safe_str(node.get("name")),
                "adult": cls._safe_bool(node.get("is_adults")),
                "children": children,
            },
            strict_validation=strict_validation,
        )

    @classmethod
    def map_city(
        cls,
        city: dict[str, Any],
        *,
        strict_validation: bool = False,
    ) -> AdministrativeUnit:
        return cls._build(
            AdministrativeUnit,
            {
                "settlement_type": "city",
                "name": cls._safe_str(city.get("name")),
                "alias": cls._safe_str(city.get("slug")),
                "country": None,
                "region": None,
                "longitude": cls._safe_float(city.get("lon")),
                "latitude": cls._safe_float(city.get("lat")),
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
    def map_virtual_store(
        cls,
        *,
        store_code: str | None,
        administrative_unit: AdministrativeUnit,
        strict_validation: bool = False,
    ) -> RetailUnit:
        return cls._build(
            RetailUnit,
            {
                "retail_type": None,
                "code": cls._safe_str(store_code),
                "address": None,
                "schedule_weekdays": cls._empty_schedule(
                    strict_validation=strict_validation
                ),
                "schedule_saturday": cls._empty_schedule(
                    strict_validation=strict_validation
                ),
                "schedule_sunday": cls._empty_schedule(
                    strict_validation=strict_validation
                ),
                "temporarily_closed": None,
                "longitude": administrative_unit.longitude,
                "latitude": administrative_unit.latitude,
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
        main_image: str | None = None,
        gallery_images: list[str] | None = None,
        strict_validation: bool = False,
    ) -> Card:
        raw_meta = product.get("meta_data")
        metadata: list[MetaData] | None = None
        meta_by_code: dict[str, int | float | str] = {}
        if isinstance(raw_meta, list):
            prepared: list[MetaData] = []
            for item in raw_meta:
                if not isinstance(item, dict):
                    continue
                name = cls._safe_str(item.get("name"))
                alias = cls._safe_str(item.get("code"))
                value = item.get("value")
                if name is None or alias is None or value is None:
                    continue
                if isinstance(value, bool):
                    continue
                if not isinstance(value, (int, float, str)):
                    continue
                meta_by_code.setdefault(alias, value)
                prepared.append(
                    cls._build(
                        MetaData,
                        {
                            "name": name,
                            "alias": alias,
                            "value": value,
                        },
                        strict_validation=strict_validation,
                    )
                )
            metadata = prepared or None

        payload: dict[str, Any] = {
            "sku": None,
            "plu": cls._safe_str(product.get("plu")),
            "source_page_url": None,
            "title": cls._safe_str(product.get("title")),
            "description": cls._safe_str(product.get("description")),
            "adult": cls._safe_bool(product.get("is_adults")),
            "new": None,
            "promo": cls._safe_bool(product.get("is_inout")),
            "season": None,
            "hit": None,
            "data_matrix": None,
            "brand": cls._safe_str(meta_by_code.get("brand_name")),
            "producer_name": cls._safe_str(meta_by_code.get("producer_name")),
            "producer_country": cls._producer_country_from_raw(meta_by_code.get("country")),
            "composition": cls._safe_str(meta_by_code.get("composition")),
            "meta_data": metadata,
            "expiration_date_in_days": cls._safe_int(meta_by_code.get("exp_date_days")),
            "rating": cls._safe_float(product.get("rating")),
            "reviews_count": cls._safe_int(product.get("reviews_count")),
            "price": cls._safe_float(product.get("price")),
            "discount_price": None,
            "loyal_price": None,
            "wholesale_price": None,
            "price_unit": None,
            "unit": cls._unit_from_raw(product.get("base_unit")),
            "available_count": None,
            "package_quantity": None,
            "package_unit": None,
            "categories_uid": cls._flatten_category_uids(product.get("categories_tree")),
            "main_image": main_image,
            "images": gallery_images if gallery_images else None,
        }
        return cls._build(
            Card,
            payload,
            strict_validation=strict_validation,
        )
