from __future__ import annotations

import logging
from typing import Any

from openinflation_dataclass import AdministrativeUnit, Card, Category, RetailUnit

from ..base import StoreParser
from ..runtime import ParserRuntimeMixin
from .mapper import FixPriceMapper
from .types import CatalogProductsQuery, FixPriceParserConfig


LOGGER = logging.getLogger(__name__)


class FixPriceParser(ParserRuntimeMixin, StoreParser):
    """First parser implementation based on fixprice_api."""

    def __init__(self, config: FixPriceParserConfig | None = None):
        self.config = config or FixPriceParserConfig()
        self._api: Any = None
        self._city_cache_by_country: dict[int, dict[int, AdministrativeUnit]] = {}

    async def __aenter__(self) -> "FixPriceParser":
        from fixprice_api import FixPriceAPI

        LOGGER.info(
            "Initializing FixPrice API client: country_id=%s city_id=%s include_images=%s timeout_ms=%s",
            self.config.country_id,
            self.config.city_id,
            self.config.include_images,
            self.config.timeout_ms,
        )
        self._api = FixPriceAPI(
            headless=self.config.headless,
            proxy=self.config.proxy,
            timeout_ms=self.config.timeout_ms,
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

    @classmethod
    def _query_categories_uid(cls, query: CatalogProductsQuery) -> list[str] | None:
        category_uid = [query.category_uid] if query.category_uid is not None else None
        subcategory_uid = (
            [query.subcategory_uid] if query.subcategory_uid is not None else None
        )
        return cls._merge_categories_uid(category_uid, subcategory_uid)

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
            queries: list[CatalogProductsQuery] = []
            for category in selected:
                category_alias = self._safe_non_empty_str(category.alias)
                if category_alias is None:
                    continue
                queries.append(
                    CatalogProductsQuery(
                        category_alias=category_alias,
                        category_uid=self._safe_non_empty_str(category.uid),
                    )
                )
            return queries

        queries: list[CatalogProductsQuery] = []
        for category in categories:
            category_alias = self._safe_non_empty_str(category.alias)
            if category_alias is None:
                continue
            category_uid = self._safe_non_empty_str(category.uid)

            subqueries: list[CatalogProductsQuery] = []
            for child in category.children:
                child_alias = self._safe_non_empty_str(child.alias)
                if child_alias is None:
                    continue
                subqueries.append(
                    CatalogProductsQuery(
                        category_alias=category_alias,
                        subcategory_alias=child_alias,
                        category_uid=category_uid,
                        subcategory_uid=self._safe_non_empty_str(child.uid),
                    )
                )
            if subqueries:
                queries.extend(subqueries)
                continue

            # If no subcategories exist, query the root category directly.
            queries.append(
                CatalogProductsQuery(
                    category_alias=category_alias,
                    category_uid=category_uid,
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
        sku_to_index: dict[str, int] = {}

        for query in queries:
            query_categories_uid = self._query_categories_uid(query)
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
                    enriched_card = card
                    card_categories_uid = getattr(card, "categories_uid", None)
                    merged_categories_uid = self._merge_categories_uid(
                        card_categories_uid,
                        query_categories_uid,
                    )
                    if merged_categories_uid != card_categories_uid:
                        enriched_card = card.model_copy(
                            update={"categories_uid": merged_categories_uid}
                        )

                    sku = enriched_card.sku
                    if sku is not None and sku in sku_to_index:
                        current_index = sku_to_index[sku]
                        current_card = all_products[current_index]
                        current_categories_uid = getattr(current_card, "categories_uid", None)
                        updated_categories_uid = self._merge_categories_uid(
                            current_categories_uid,
                            getattr(enriched_card, "categories_uid", None),
                        )
                        if updated_categories_uid != current_categories_uid:
                            all_products[current_index] = current_card.model_copy(
                                update={"categories_uid": updated_categories_uid}
                            )
                        continue

                    if sku is not None:
                        sku_to_index[sku] = len(all_products)
                    all_products.append(enriched_card)
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
                categories.append(
                    FixPriceMapper.map_category_node(
                        node,
                        strict_validation=self.config.strict_validation,
                    )
                )
        LOGGER.info("Collected categories: %s", len(categories))
        return categories

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
            main_image, gallery_images = await self._collect_product_images(
                api=api,
                product=raw_product,
                include_images=self.config.include_images,
                images_field="images",
                image_url_field="src",
                image_limit=self.config.image_limit_per_product,
            )
            cards.append(
                FixPriceMapper.map_product(
                    raw_product,
                    main_image=main_image,
                    gallery_images=gallery_images,
                    strict_validation=self.config.strict_validation,
                )
            )
        LOGGER.info(
            "Collected products page: category=%s subcategory=%s page=%s count=%s",
            category_alias,
            subcategory_alias,
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
                strict_validation=self.config.strict_validation,
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
                cached[city_id] = FixPriceMapper.map_city(
                    city,
                    country_id=country_id,
                    strict_validation=self.config.strict_validation,
                )
                self._city_cache_by_country[country_id] = cached
        return cached

    async def collect_store_info(
        self,
        *,
        country_id: int | None = None,
        region_id: int | None = None,
        city_id: int | str | None = None,
        store_code: str | None = None,
    ) -> list[RetailUnit]:
        api = self._require_api()
        target_country_id = country_id or self.config.country_id
        target_city_id: int | None
        if isinstance(city_id, int):
            target_city_id = city_id
        elif isinstance(city_id, str):
            try:
                target_city_id = int(city_id)
            except ValueError:
                target_city_id = None
        else:
            target_city_id = None

        LOGGER.info(
            "Collecting stores: country_id=%s region_id=%s city_id=%s store_code=%s",
            target_country_id,
            region_id,
            target_city_id,
            store_code,
        )
        response = await api.Geolocation.Shop.search(
            country_id=target_country_id,
            region_id=region_id,
            city_id=target_city_id,
        )
        raw_shops = response.json()
        if not isinstance(raw_shops, list):
            return []

        city_map = await self._get_city_map(
            country_id=target_country_id,
            city_id=target_city_id,
        )
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
                    strict_validation=self.config.strict_validation,
                )

            stores.append(
                FixPriceMapper.map_store(
                    raw_shop,
                    administrative_unit=administrative_unit,
                    strict_validation=self.config.strict_validation,
                )
            )
        LOGGER.info("Collected stores: matched=%s", len(stores))
        return stores
