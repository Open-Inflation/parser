from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Literal

from .base import StoreParser

from openinflation_dataclass import (
    AdministrativeUnit,
    Card,
    Category,
    MetaData,
    RetailUnit,
    Schedule,
)


CountryCode = Literal["BLR", "RUS", "USA", "ARE"]
ProducerCountryCode = Literal["BLR", "RUS", "USA", "ARE", "CHN"]
CurrencyCode = Literal["BYN", "RUB", "USD", "EUR", "AED"]
LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class FixPriceParserConfig:
    country_id: int = 2
    city_id: int | None = None
    proxy: str | dict[str, Any] | None = None
    headless: bool = True
    include_images: bool = False
    image_limit_per_product: int = 1


@dataclass(frozen=True, slots=True)
class CatalogProductsQuery:
    category_alias: str
    subcategory_alias: str | None = None


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
    HHMM_PATTERN = re.compile(r"(?:[01]\d|2[0-3]):[0-5]\d")

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
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized:
            return None
        return normalized

    @staticmethod
    def _safe_bool(value: Any) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "no", "n", "off"}:
                return False
        return None

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.strip().replace(",", ".")
            if not cleaned:
                return None
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    @classmethod
    def _safe_int(cls, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return None
            if cleaned.isdigit():
                return int(cleaned)
            parsed = cls._safe_float(cleaned)
            if parsed is None:
                return None
            return int(parsed)
        return None

    @classmethod
    def _settlement_type(cls, prefix: Any) -> Literal["village", "city"] | None:
        value = str(prefix or "").strip().lower()
        if value in {"г", "г.", "город", "city"}:
            return "city"
        if value:
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
        token = str(value or "").strip().lower()
        if token in {"кг", "kg", "kgm", "килограмм", "килограммы"}:
            return "KGM"
        if token in {"л", "ltr", "liter", "литр", "литры"}:
            return "LTR"
        if token in {"шт", "pce", "piece", "pcs"}:
            return "PCE"
        return None

    @classmethod
    def map_category_node(cls, node: dict[str, Any]) -> Category:
        uid = cls._safe_str(node.get("id"))
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
        _ = country_id
        _ = city_id
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
        category_id = cls._safe_str(category.get("id"))
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
                if not isinstance(value, (int, float, str)):
                    value = str(value)
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
            "price": cls._safe_float(product.get("price")),
            "discount_price": (
                cls._safe_float(product.get("specialPrice"))
                if product.get("specialPrice") is not None
                else None
            ),
            "loyal_price": cls._safe_float(product.get("loyalPrice")),
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


class FixPriceParser(StoreParser):
    """First parser implementation based on fixprice_api."""

    def __init__(self, config: FixPriceParserConfig | None = None):
        self.config = config or FixPriceParserConfig()
        self._api: Any = None
        self._city_cache_by_country: dict[int, dict[int, AdministrativeUnit]] = {}

    async def __aenter__(self) -> "FixPriceParser":
        from fixprice_api import FixPriceAPI

        LOGGER.info(
            "Initializing FixPrice API client: country_id=%s city_id=%s include_images=%s",
            self.config.country_id,
            self.config.city_id,
            self.config.include_images,
        )
        self._api = FixPriceAPI(
            headless=self.config.headless,
            proxy=self.config.proxy,
        )
        await self._api.__aenter__()

        if self.config.city_id is not None:
            self._api.city_id = self.config.city_id
        LOGGER.info("FixPrice API session warmed up")
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._api is not None:
            await self._api.__aexit__(*exc_info)
            self._api = None
        LOGGER.info("FixPrice API session closed")

    def _require_api(self) -> Any:
        if self._api is None:
            raise RuntimeError("FixPriceParser must be used inside 'async with'.")
        return self._api

    def build_catalog_queries(
        self,
        categories: list[Category],
        *,
        full_catalog: bool,
        category_limit: int,
    ) -> list[CatalogProductsQuery]:
        if not categories:
            return []

        if not full_catalog:
            selected = categories[: max(1, category_limit)]
            return [
                CatalogProductsQuery(category_alias=category.alias)
                for category in selected
                if category.alias
            ]

        queries: list[CatalogProductsQuery] = []
        for category in categories:
            if not category.alias:
                continue
            # Root category request.
            queries.append(CatalogProductsQuery(category_alias=category.alias))

            # Subcategory aliases are queried under the root alias.
            for child in category.children:
                if not child.alias:
                    continue
                queries.append(
                    CatalogProductsQuery(
                        category_alias=category.alias,
                        subcategory_alias=child.alias,
                    )
                )

        deduplicated: list[CatalogProductsQuery] = []
        seen: set[tuple[str, str | None]] = set()
        for query in queries:
            key = (query.category_alias, query.subcategory_alias)
            if key in seen:
                continue
            seen.add(key)
            deduplicated.append(query)
        return deduplicated

    async def collect_products_for_queries(
        self,
        queries: list[CatalogProductsQuery],
        *,
        page_limit: int,
        items_per_page: int = 24,
    ) -> list[Card]:
        safe_page_limit = max(1, page_limit)
        safe_items_per_page = max(1, min(27, items_per_page))

        all_products: list[Card] = []
        seen_skus: set[str] = set()

        for query in queries:
            for page in range(1, safe_page_limit + 1):
                page_products = await self.collect_products(
                    category_alias=query.category_alias,
                    subcategory_alias=query.subcategory_alias,
                    page=page,
                    limit=safe_items_per_page,
                )
                if not page_products:
                    break

                for card in page_products:
                    if card.sku is not None and card.sku in seen_skus:
                        continue
                    if card.sku is not None:
                        seen_skus.add(card.sku)
                    all_products.append(card)
        LOGGER.info(
            "Collected products for queries: queries=%s unique_products=%s",
            len(queries),
            len(all_products),
        )
        return all_products

    async def collect_categories(self) -> list[Category]:
        api = self._require_api()
        LOGGER.info("Collecting category tree")
        response = await api.Catalog.tree()
        raw_tree = response.json()

        categories: list[Category] = []
        if not isinstance(raw_tree, dict):
            return categories

        for node in raw_tree.values():
            if isinstance(node, dict):
                categories.append(FixPriceMapper.map_category_node(node))
        LOGGER.info("Collected categories: %s", len(categories))
        return categories

    async def _download_image(self, url: str) -> BytesIO | None:
        api = self._require_api()
        if not self.config.include_images:
            return None
        try:
            stream = await api.General.download_image(url=url)
        except Exception:
            LOGGER.exception("Image download failed: %s", url)
            return None
        return BytesIO(stream.getvalue())

    async def _collect_product_images(
        self,
        product: dict[str, Any],
    ) -> tuple[BytesIO | None, list[BytesIO] | None]:
        if not self.config.include_images:
            return None, None

        image_nodes = product.get("images")
        if not isinstance(image_nodes, list):
            return None, None

        urls: list[str] = []
        for image_node in image_nodes:
            if not isinstance(image_node, dict):
                continue
            src = image_node.get("src")
            if isinstance(src, str) and src:
                urls.append(src)
        if not urls:
            return None, None

        main = await self._download_image(urls[0])
        gallery: list[BytesIO] = []

        limit = max(0, self.config.image_limit_per_product)
        for url in urls[1 : 1 + limit]:
            image = await self._download_image(url)
            if image is not None:
                gallery.append(image)
        return main, (gallery or None)

    async def collect_products(
        self,
        category_alias: str,
        *,
        subcategory_alias: str | None = None,
        page: int = 1,
        limit: int = 24,
    ) -> list[Card]:
        api = self._require_api()
        LOGGER.info(
            "Collecting products: category=%s subcategory=%s page=%s limit=%s",
            category_alias,
            subcategory_alias,
            page,
            limit,
        )
        response = await api.Catalog.products_list(
            category_alias=category_alias,
            subcategory_alias=subcategory_alias,
            page=page,
            limit=limit,
        )
        raw_products = response.json()

        if not isinstance(raw_products, list):
            return []

        cards: list[Card] = []
        for raw_product in raw_products:
            if not isinstance(raw_product, dict):
                continue
            main_image, gallery_images = await self._collect_product_images(raw_product)
            cards.append(
                FixPriceMapper.map_product(
                    raw_product,
                    main_image=main_image,
                    gallery_images=gallery_images,
                )
            )
        LOGGER.info(
            "Collected products page: category=%s page=%s count=%s",
            category_alias,
            page,
            len(cards),
        )
        return cards

    async def collect_cities(self, *, country_id: int | None = None) -> list[AdministrativeUnit]:
        api = self._require_api()
        target_country_id = country_id or self.config.country_id

        if target_country_id in self._city_cache_by_country:
            LOGGER.debug("Cities cache hit: country_id=%s", target_country_id)
            return list(self._city_cache_by_country[target_country_id].values())

        LOGGER.info("Collecting cities: country_id=%s", target_country_id)
        response = await api.Geolocation.cities_list(country_id=target_country_id)
        raw_cities = response.json()
        if not isinstance(raw_cities, list):
            return []

        city_map: dict[int, AdministrativeUnit] = {}
        for raw_city in raw_cities:
            if not isinstance(raw_city, dict):
                continue
            city_id = FixPriceMapper._safe_int(raw_city.get("id"))
            if city_id is None:
                continue
            city_map[city_id] = FixPriceMapper.map_city(
                raw_city,
                country_id=target_country_id,
            )

        self._city_cache_by_country[target_country_id] = city_map
        LOGGER.info("Collected cities: country_id=%s count=%s", target_country_id, len(city_map))
        return list(city_map.values())

    async def _get_city_map(
        self,
        *,
        country_id: int,
        city_id: int | None,
    ) -> dict[int, AdministrativeUnit]:
        await self.collect_cities(country_id=country_id)
        cached = dict(self._city_cache_by_country.get(country_id, {}))
        if city_id is not None and city_id not in cached:
            api = self._require_api()
            LOGGER.info("City cache miss, requesting city_info: city_id=%s", city_id)
            response = await api.Geolocation.city_info(city_id=city_id)
            city = response.json()
            if isinstance(city, dict):
                cached[city_id] = FixPriceMapper.map_city(city, country_id=country_id)
                self._city_cache_by_country[country_id] = cached
        return cached

    async def collect_store_info(
        self,
        *,
        country_id: int | None = None,
        region_id: int | None = None,
        city_id: int | None = None,
        store_code: str | None = None,
    ) -> list[RetailUnit]:
        api = self._require_api()
        target_country_id = country_id or self.config.country_id

        LOGGER.info(
            "Collecting stores: country_id=%s region_id=%s city_id=%s store_code=%s",
            target_country_id,
            region_id,
            city_id,
            store_code,
        )
        response = await api.Geolocation.Shop.search(
            country_id=target_country_id,
            region_id=region_id,
            city_id=city_id,
        )
        raw_shops = response.json()
        if not isinstance(raw_shops, list):
            return []

        city_map = await self._get_city_map(country_id=target_country_id, city_id=city_id)
        code_filter = str(store_code).lower() if store_code is not None else None

        stores: list[RetailUnit] = []
        for raw_shop in raw_shops:
            if not isinstance(raw_shop, dict):
                continue
            code = str(raw_shop.get("pfm") or raw_shop.get("id") or "").lower()
            if code_filter and code != code_filter:
                continue

            mapped_city_id = FixPriceMapper._safe_int(raw_shop.get("cityId"))
            administrative_unit = city_map.get(mapped_city_id)
            if administrative_unit is None:
                administrative_unit = FixPriceMapper.fallback_administrative_unit(
                    country_id=target_country_id,
                    city_id=mapped_city_id,
                )

            stores.append(
                FixPriceMapper.map_store(
                    raw_shop,
                    administrative_unit=administrative_unit,
                )
            )
        LOGGER.info("Collected stores: matched=%s", len(stores))
        return stores
