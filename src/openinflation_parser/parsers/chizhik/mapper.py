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

    PRODUCT_PAGE_BASE_URL = "https://chizhik.club/product"
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

    @classmethod
    def _safe_text(cls, *values: Any) -> str | None:
        for value in values:
            token = cls._safe_str(value)
            if token is not None:
                return token
        return None

    @staticmethod
    def _safe_bool(value: Any) -> bool | None:
        return value if isinstance(value, bool) else None

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            token = value.strip().replace(",", ".")
            if not token:
                return None
            try:
                return float(token)
            except ValueError:
                return None
        return None

    @classmethod
    def _safe_int(cls, value: Any) -> int | None:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str):
            parsed = cls._safe_float(value)
            if parsed is not None and parsed.is_integer():
                return int(parsed)
        return None

    @classmethod
    def _available_count_from_raw(
        cls,
        value: Any,
        *,
        unit_net: str | None,
    ) -> int | float | None:
        parsed = cls._safe_float(value)
        if parsed is None:
            return None
        if unit_net == "PCE":
            if parsed.is_integer():
                return int(parsed)
            return None
        return parsed

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

    @staticmethod
    def _children_from_node(node: dict[str, Any]) -> list[Any] | None:
        for key in ("children", "categories", "categories_tags"):
            raw_children = node.get(key)
            if isinstance(raw_children, list):
                return raw_children
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
            raw_children = cls._children_from_node(node)
            if isinstance(raw_children, list):
                for child in raw_children:
                    walk(child)

        for node in nodes:
            walk(node)
        return prepared or None

    @classmethod
    def _source_page_url_from_product(cls, product: dict[str, Any]) -> str | None:
        slug = cls._safe_str(product.get("slug"))
        if slug is None:
            return None
        normalized = slug.strip().strip("/")
        if not normalized:
            return None
        plu = cls._safe_str(product.get("plu"))
        if plu is not None:
            normalized_plu = plu.strip()
            if normalized_plu:
                if normalized.endswith(f"--{normalized_plu}"):
                    slug_with_plu = normalized
                elif normalized.endswith(f"-{normalized_plu}"):
                    slug_with_plu = f"{normalized[: -len(normalized_plu) - 1]}--{normalized_plu}"
                else:
                    slug_base = normalized.rstrip("-")
                    slug_with_plu = f"{slug_base}--{normalized_plu}"
                return f"{cls.PRODUCT_PAGE_BASE_URL}/{slug_with_plu}/"
        return f"{cls.PRODUCT_PAGE_BASE_URL}/{normalized}/"

    @classmethod
    def _meta_from_attributes(
        cls,
        attributes: Any,
        *,
        strict_validation: bool,
    ) -> tuple[list[MetaData] | None, dict[str, str]]:
        if not isinstance(attributes, list):
            return None, {}

        prepared: list[MetaData] = []
        named: dict[str, str] = {}
        for item in attributes:
            if not isinstance(item, dict):
                continue
            name = cls._safe_str(item.get("name"))
            value = cls._safe_text(item.get("value"))
            uom = cls._safe_str(item.get("uom"))
            if name is None or value is None:
                continue
            alias = name.strip().lower()
            named.setdefault(alias, value)
            if uom:
                rendered = f"{value} {uom}".strip()
            else:
                rendered = value
            prepared.append(
                cls._build(
                    MetaData,
                    {
                        "name": name,
                        "alias": alias,
                        "value": rendered,
                    },
                    strict_validation=strict_validation,
                )
            )
        return prepared or None, named

    @classmethod
    def _price_components(
        cls,
        product: dict[str, Any],
    ) -> tuple[float | None, float | None]:
        prices = product.get("prices")
        regular: float | None = None
        discount: float | None = None
        if isinstance(prices, dict):
            regular = cls._safe_float(prices.get("regular"))
            discount = cls._safe_float(prices.get("discount"))
            if discount is None:
                discount = cls._safe_float(prices.get("cpd_promo_price"))
        elif isinstance(prices, list):
            for item in prices:
                if not isinstance(item, dict):
                    continue
                placement_type = cls._safe_str(item.get("placement_type"))
                value = cls._safe_float(item.get("value"))
                if value is None:
                    continue
                if placement_type == "regular_secondary" and regular is None:
                    regular = value
                elif placement_type == "promotional_primary" and discount is None:
                    discount = value
            if discount is None and prices:
                discount = cls._safe_float(prices[0].get("value")) if isinstance(prices[0], dict) else None
        effective_price = discount if discount is not None else regular
        discount_price = regular if discount is not None and regular != discount else None
        return effective_price, discount_price

    @classmethod
    def _rating_value(cls, product: dict[str, Any]) -> float | None:
        rating = product.get("rating")
        if isinstance(rating, dict):
            return cls._safe_float(rating.get("rating_average"))
        return cls._safe_float(rating)

    @classmethod
    def _reviews_count(cls, product: dict[str, Any]) -> int | None:
        rating = product.get("rating")
        if isinstance(rating, dict):
            return cls._safe_int(rating.get("reviews_count"))
        return cls._safe_int(product.get("reviews_count"))

    @classmethod
    def map_category_node(
        cls,
        node: dict[str, Any],
        *,
        strict_validation: bool = False,
    ) -> Category:
        raw_children = cls._children_from_node(node)
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
                "alias": cls._safe_text(node.get("slug"), cls._id_to_str(node.get("id"))),
                "title": cls._safe_text(node.get("title"), node.get("name")),
                "adult": cls._safe_bool(node.get("is_adults")) or False,
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
    def map_store(
        cls,
        store: dict[str, Any],
        *,
        administrative_unit: AdministrativeUnit,
        strict_validation: bool = False,
    ) -> RetailUnit:
        return cls._build(
            RetailUnit,
            {
                "retail_type": "store",
                "code": cls._safe_text(store.get("sap_id")),
                "address": cls._safe_text(store.get("name")),
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
                "rating": cls._safe_float(store.get("average_rating")),
                "reviews_count": None,
                "open_date": cls._safe_str(store.get("open_date")),
                "longitude": cls._safe_float(store.get("lon")),
                "latitude": cls._safe_float(store.get("lat")),
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
        metadata, named_attributes = cls._meta_from_attributes(
            product.get("attributes"),
            strict_validation=strict_validation,
        )
        raw_meta = product.get("meta_data")
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
            if metadata is None:
                metadata = prepared or None
            else:
                metadata = [*metadata, *prepared]

        price, discount_price = cls._price_components(product)
        composition = cls._safe_text(
            meta_by_code.get("composition"),
            product.get("ingredients"),
            named_attributes.get("состав"),
        )
        producer_name = cls._safe_text(
            meta_by_code.get("producer_name"),
            named_attributes.get("производитель"),
        )
        producer_country = cls._producer_country_from_raw(
            meta_by_code.get("country") or named_attributes.get("страна производства")
        )
        brand = cls._safe_text(
            meta_by_code.get("brand_name"),
            named_attributes.get("бренд"),
        )
        adult = cls._safe_bool(product.get("is_adults"))
        if adult is None:
            adult = cls._safe_bool(product.get("has_age_restriction"))
        promo = cls._safe_bool(product.get("is_inout"))
        if promo is None:
            promo = discount_price is not None or product.get("promo") is not None
        unit_net = cls._unit_from_raw(product.get("base_unit") or product.get("uom"))
        available_count = cls._available_count_from_raw(
            product.get("stock_limit"),
            unit_net=unit_net,
        )

        payload: dict[str, Any] = {
            "sku": None,
            "plu": cls._id_to_str(product.get("plu")),
            "source_page_url": cls._source_page_url_from_product(product),
            "title": cls._safe_text(product.get("title"), product.get("name")),
            "description": cls._safe_str(product.get("description")),
            "adult": adult,
            "new": None,
            "promo": promo,
            "season": None,
            "hit": None,
            "data_matrix": None,
            "brand": brand,
            "producer_name": producer_name,
            "producer_country": producer_country,
            "composition": composition,
            "meta_data": metadata,
            "expiration_date_in_days": cls._safe_int(
                meta_by_code.get("exp_date_days") or named_attributes.get("срок хранения")
            ),
            "rating": cls._rating_value(product),
            "reviews_count": cls._reviews_count(product),
            "price": price if price is not None else cls._safe_float(product.get("price")),
            "discount_price": discount_price,
            "loyal_price": None,
            "wholesale_price": None,
            "price_unit": None,
            "unit_net": unit_net,
            "available_count": available_count,
            "package_quantity_net": None,
            "package_weight_gross": None,
            "package_unit": None,
            "package_count": None,
            "categories_uid": cls._flatten_category_uids(product.get("categories_tree")),
            "main_image": main_image,
            "images": gallery_images if gallery_images else None,
        }
        return cls._build(
            Card,
            payload,
            strict_validation=strict_validation,
        )
