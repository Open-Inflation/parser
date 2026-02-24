from __future__ import annotations

import logging
from typing import Any

from openinflation_dataclass import AdministrativeUnit, Card, Category, RetailUnit

from ..base import StoreParser
from ..runtime import ParserRuntimeMixin
from .mapper import ChizhikMapper
from .types import CatalogProductsQuery, ChizhikParserConfig


LOGGER = logging.getLogger(__name__)


class ChizhikParser(ParserRuntimeMixin, StoreParser):
    """Parser implementation based on chizhik_api."""

    def __init__(self, config: ChizhikParserConfig | None = None):
        self.config = config or ChizhikParserConfig()
        self._api: Any = None
        self._city_cache: dict[str, AdministrativeUnit] = {}
        self._product_info_cache: dict[int, dict[str, Any]] = {}

    async def __aenter__(self) -> "ChizhikParser":
        from chizhik_api import ChizhikAPI

        LOGGER.info(
            "Initializing Chizhik API client: city_id=%s include_images=%s timeout_ms=%s",
            self.config.city_id,
            self.config.include_images,
            self.config.timeout_ms,
        )
        self._api = ChizhikAPI(
            headless=self.config.headless,
            proxy=self.config.proxy,
            timeout_ms=self.config.timeout_ms,
        )
        await self._api.__aenter__()
        LOGGER.info("Chizhik API session warmed up")
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._api is not None:
            await self._api.__aexit__(*exc_info)
            self._api = None
        LOGGER.info("Chizhik API session closed")

    def _require_api(self) -> Any:
        if self._api is None:
            raise RuntimeError("ChizhikParser must be used inside 'async with'.")
        return self._api

    @classmethod
    def _category_id_from_uid(cls, uid: Any) -> int | None:
        if isinstance(uid, int) and not isinstance(uid, bool):
            return uid
        token = cls._safe_non_empty_str(uid)
        if token is None:
            return None
        try:
            return int(token)
        except ValueError:
            return None

    @staticmethod
    def _iter_leaf_categories(category: Category) -> list[Category]:
        if not category.children:
            return [category]
        leaves: list[Category] = []
        for child in category.children:
            leaves.extend(ChizhikParser._iter_leaf_categories(child))
        return leaves

    @staticmethod
    def _card_key(card: Card) -> str | None:
        if card.sku is not None:
            return card.sku
        return card.plu

    @classmethod
    def _product_id_from_raw(cls, value: Any) -> int | None:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        token = cls._safe_non_empty_str(value)
        if token is None:
            return None
        try:
            return int(token)
        except ValueError:
            return None

    async def _collect_product_info(self, *, product_id: int) -> dict[str, Any] | None:
        cached = self._product_info_cache.get(product_id)
        if cached is not None:
            return cached

        api = self._require_api()
        response = await api.Catalog.Product.info(
            product_id=product_id,
            city_id=self.config.city_id,
        )
        payload = response.json()
        if not isinstance(payload, dict):
            return None

        self._product_info_cache[product_id] = payload
        return payload

    def build_catalog_queries(
        self,
        categories: list[Category],
        *,
        full_catalog: bool,
        category_limit: int,
    ) -> list[CatalogProductsQuery]:
        if not categories:
            return []

        queries: list[CatalogProductsQuery] = []
        if not full_catalog:
            selected = categories[: max(1, category_limit)]
            for category in selected:
                category_uid = self._safe_non_empty_str(category.uid)
                category_id = self._category_id_from_uid(category_uid)
                if category_id is None:
                    continue
                queries.append(
                    CatalogProductsQuery(
                        category_id=category_id,
                        category_uid=category_uid,
                        category_slug=self._safe_non_empty_str(category.alias),
                    )
                )
            return queries

        for category in categories:
            leaves = self._iter_leaf_categories(category)
            if not leaves:
                leaves = [category]
            for leaf in leaves:
                category_uid = self._safe_non_empty_str(leaf.uid)
                category_id = self._category_id_from_uid(category_uid)
                if category_id is None:
                    continue
                queries.append(
                    CatalogProductsQuery(
                        category_id=category_id,
                        category_uid=category_uid,
                        category_slug=self._safe_non_empty_str(leaf.alias),
                    )
                )

        deduplicated: list[CatalogProductsQuery] = []
        seen: set[int] = set()
        for query in queries:
            if query.category_id in seen:
                continue
            seen.add(query.category_id)
            deduplicated.append(query)
        return deduplicated

    async def _collect_products_page(
        self,
        *,
        query: CatalogProductsQuery,
        page: int,
    ) -> tuple[list[Card], int | None]:
        api = self._require_api()
        LOGGER.info(
            "Collecting products: category_id=%s slug=%s page=%s",
            query.category_id,
            query.category_slug,
            page,
        )
        response = await api.Catalog.products_list(
            page=page,
            category_id=query.category_id,
            city_id=self.config.city_id,
        )
        payload = response.json()
        if not isinstance(payload, dict):
            return [], None

        total_pages_raw = payload.get("total_pages")
        total_pages = total_pages_raw if isinstance(total_pages_raw, int) else None
        items = payload.get("items")
        if not isinstance(items, list):
            return [], total_pages

        cards: list[Card] = []
        enriched_count = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            mapped_payload = item
            product_id = self._product_id_from_raw(item.get("id"))
            if product_id is not None:
                try:
                    product_info = await self._collect_product_info(product_id=product_id)
                    if product_info is not None:
                        merged_payload = dict(item)
                        merged_payload.update(product_info)
                        mapped_payload = merged_payload
                        enriched_count += 1
                except Exception as exc:
                    LOGGER.warning(
                        "Failed to collect product info: product_id=%s error=%s",
                        product_id,
                        exc,
                    )

            main_image, gallery_images = await self._collect_product_images(
                api=api,
                product=mapped_payload,
                include_images=self.config.include_images,
                images_field="images",
                image_url_field="image",
                image_limit=self.config.image_limit_per_product,
            )
            cards.append(
                ChizhikMapper.map_product(
                    mapped_payload,
                    main_image=main_image,
                    gallery_images=gallery_images,
                    strict_validation=self.config.strict_validation,
                )
            )

        LOGGER.info(
            "Collected products page: category_id=%s slug=%s page=%s count=%s enriched=%s total_pages=%s",
            query.category_id,
            query.category_slug,
            page,
            len(cards),
            enriched_count,
            total_pages,
        )
        return cards, total_pages

    async def collect_products_for_queries(
        self,
        queries: list[CatalogProductsQuery],
        *,
        page_limit: int,
        items_per_page: int = 100,
    ) -> list[Card]:
        del items_per_page  # Chizhik API uses fixed server page size.
        safe_page_limit = max(1, page_limit)

        all_products: list[Card] = []
        key_to_index: dict[str, int] = {}

        for query in queries:
            query_categories_uid = (
                [query.category_uid] if query.category_uid is not None else None
            )
            for page in range(1, safe_page_limit + 1):
                page_products, total_pages = await self._collect_products_page(
                    query=query,
                    page=page,
                )
                if not page_products:
                    break

                for card in page_products:
                    merged_categories_uid = self._merge_categories_uid(
                        card.categories_uid,
                        query_categories_uid,
                    )
                    enriched = card
                    if merged_categories_uid != card.categories_uid:
                        enriched = card.model_copy(
                            update={"categories_uid": merged_categories_uid}
                        )

                    key = self._card_key(enriched)
                    if key is not None and key in key_to_index:
                        current_index = key_to_index[key]
                        current_card = all_products[current_index]
                        updated_categories_uid = self._merge_categories_uid(
                            current_card.categories_uid,
                            enriched.categories_uid,
                        )
                        if updated_categories_uid != current_card.categories_uid:
                            all_products[current_index] = current_card.model_copy(
                                update={"categories_uid": updated_categories_uid}
                            )
                        continue

                    if key is not None:
                        key_to_index[key] = len(all_products)
                    all_products.append(enriched)

                if total_pages is not None and page >= total_pages:
                    break

        LOGGER.info(
            "Collected products for queries: queries=%s unique_products=%s",
            len(queries),
            len(all_products),
        )
        return all_products

    async def collect_categories(self) -> list[Category]:
        api = self._require_api()
        LOGGER.info("Collecting Chizhik category tree")
        response = await api.Catalog.tree(city_id=self.config.city_id)
        raw_tree = response.json()
        if not isinstance(raw_tree, list):
            return []

        categories: list[Category] = []
        for node in raw_tree:
            if isinstance(node, dict):
                categories.append(
                    ChizhikMapper.map_category_node(
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
        limit: int = 100,
    ) -> list[Card]:
        del subcategory_alias
        del limit

        category_id = self._category_id_from_uid(category_alias)
        if category_id is None:
            return []

        products, _ = await self._collect_products_page(
            query=CatalogProductsQuery(
                category_id=category_id,
                category_uid=str(category_id),
                category_slug=None,
            ),
            page=max(1, page),
        )
        return products

    async def collect_cities(self, *, country_id: int | None = None) -> list[AdministrativeUnit]:
        del country_id
        if self._city_cache:
            return list(self._city_cache.values())

        api = self._require_api()
        search = self._safe_non_empty_str(self.config.city_search) or "а"
        max_pages = max(1, self.config.max_city_pages)

        for page in range(1, max_pages + 1):
            LOGGER.info("Collecting cities: search=%s page=%s", search, page)
            response = await api.Geolocation.cities_list(search_name=search, page=page)
            payload = response.json()
            if not isinstance(payload, dict):
                break
            items = payload.get("items")
            if not isinstance(items, list) or not items:
                break

            for item in items:
                if not isinstance(item, dict):
                    continue
                city = ChizhikMapper.map_city(
                    item,
                    strict_validation=self.config.strict_validation,
                )
                key = city.alias or city.name
                if key is None:
                    continue
                self._city_cache[key] = city

            total_pages_raw = payload.get("total_pages")
            total_pages = total_pages_raw if isinstance(total_pages_raw, int) else None
            if total_pages is not None and page >= total_pages:
                break

        return list(self._city_cache.values())

    async def _city_for_store_code(self, store_code: str) -> AdministrativeUnit | None:
        api = self._require_api()
        response = await api.Geolocation.cities_list(search_name=store_code, page=1)
        payload = response.json()
        if not isinstance(payload, dict):
            return None
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            return None

        normalized = store_code.strip().lower()
        exact: dict[str, Any] | None = None
        with_shop: dict[str, Any] | None = None
        for item in items:
            if not isinstance(item, dict):
                continue
            slug = self._safe_non_empty_str(item.get("slug"))
            name = self._safe_non_empty_str(item.get("name"))
            has_shop = item.get("has_shop") is True
            if slug is not None and slug.lower() == normalized:
                exact = item
                break
            if name is not None and name.lower() == normalized:
                exact = item
                break
            if has_shop and with_shop is None:
                with_shop = item

        selected = exact or with_shop
        if selected is None and isinstance(items[0], dict):
            selected = items[0]
        if selected is None:
            return None
        return ChizhikMapper.map_city(
            selected,
            strict_validation=self.config.strict_validation,
        )

    async def collect_store_info(
        self,
        *,
        country_id: int | None = None,
        region_id: int | None = None,
        city_id: int | str | None = None,
        store_code: str | None = None,
    ) -> list[RetailUnit]:
        del country_id
        del region_id
        del city_id

        if store_code is None:
            return []

        administrative_unit = ChizhikMapper.fallback_administrative_unit(
            strict_validation=self.config.strict_validation
        )
        try:
            matched = await self._city_for_store_code(store_code)
            if matched is not None:
                administrative_unit = matched
        except Exception:
            LOGGER.exception("Failed to resolve city by store code: %s", store_code)

        return [
            ChizhikMapper.map_virtual_store(
                store_code=store_code,
                administrative_unit=administrative_unit,
                strict_validation=self.config.strict_validation,
            )
        ]
