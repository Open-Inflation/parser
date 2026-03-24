from __future__ import annotations

from collections.abc import Callable
import logging
from typing import Any

from openinflation_dataclass import AdministrativeUnit, Card, Category, RetailUnit

from ..base import StoreParser
from ..runtime import ParserRuntimeMixin
from .mapper import ChizhikMapper
from .types import CatalogProductsQuery, ChizhikParserConfig


LOGGER = logging.getLogger(__name__)
PAGE_PROGRESS_LOG_EVERY = 10


class ChizhikParser(ParserRuntimeMixin, StoreParser):
    """Parser implementation based on chizhik_api."""

    def __init__(self, config: ChizhikParserConfig | None = None):
        self.config = config or ChizhikParserConfig()
        self._api: Any = None
        self._effective_store_id: str | None = None
        self._city_cache: dict[str, AdministrativeUnit] = {}
        self._product_info_cache: dict[int, dict[str, Any]] = {}

    async def __aenter__(self) -> "ChizhikParser":
        from chizhik_api import ChizhikAPI

        LOGGER.info(
            "Initializing Chizhik API client: include_images=%s use_product_info=%s timeout_ms=%s",
            self.config.include_images,
            self.config.use_product_info,
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
    def _category_id_from_uid(cls, uid: Any) -> str | None:
        if isinstance(uid, int) and not isinstance(uid, bool):
            return str(uid)
        return cls._safe_non_empty_str(uid)

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

    def _require_store_id(self) -> str:
        store_id = self._safe_non_empty_str(self._effective_store_id)
        if store_id is None:
            raise RuntimeError(
                "ChizhikParser requires a resolved store_id before collecting categories or products."
            )
        return store_id

    async def _ensure_store_id(self) -> str:
        store_id = self._safe_non_empty_str(self._effective_store_id)
        if store_id is not None:
            return store_id

        store_code = self._safe_non_empty_str(self.config.store_code)
        if store_code is None:
            raise RuntimeError(
                "ChizhikParser requires store_code in config before collecting categories or products."
            )
        matched_shop = await self._resolve_store(store_code)
        if matched_shop is None:
            raise ValueError(f"Store code {store_code!r} not found.")
        resolved_store_id = self._safe_non_empty_str(matched_shop.get("sap_id"))
        if resolved_store_id is None:
            raise ValueError(f"Store code {store_code!r} did not resolve to sap_id.")
        self._effective_store_id = resolved_store_id
        return resolved_store_id

    async def _collect_product_info(self, *, product_id: int) -> dict[str, Any] | None:
        cached = self._product_info_cache.get(product_id)
        if cached is not None:
            return cached

        api = self._require_api()
        response = await api.Catalog.Product.delivery_info(
            store_id=await self._ensure_store_id(),
            product_id=product_id,
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
        seen: set[str] = set()
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
        store_id = await self._ensure_store_id()
        LOGGER.info(
            "Collecting delivery products: store_id=%s category_id=%s slug=%s page=%s",
            store_id,
            query.category_id,
            query.category_slug,
            page,
        )
        limit = 499
        response = await api.Catalog.delivery_products_list(
            store_id=store_id,
            category_alias=query.category_id,
            offset=max(0, page - 1) * limit,
            limit=limit,
        )
        payload = response.json()
        if not isinstance(payload, dict):
            return [], None

        items = payload.get("products")
        if not isinstance(items, list):
            return [], None
        total_pages = page + 1 if len(items) >= limit else page

        total_items = len(items)
        cards: list[Card] = []
        enriched_count = 0
        candidate_image_products = 0
        candidate_image_urls = 0
        downloaded_main_images = 0
        downloaded_gallery_images = 0
        cards_with_downloaded_images = 0
        no_image_samples_logged = 0
        for item_index, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                if item_index % PAGE_PROGRESS_LOG_EVERY == 0 or item_index == total_items:
                    LOGGER.info(
                        "Collecting products progress: category_id=%s slug=%s page=%s inspected=%s/%s mapped=%s enriched=%s image_candidates_products=%s downloaded_main=%s downloaded_gallery=%s",
                        query.category_id,
                        query.category_slug,
                        page,
                        item_index,
                        total_items,
                        len(cards),
                        enriched_count,
                        candidate_image_products,
                        downloaded_main_images,
                        downloaded_gallery_images,
                    )
                continue
            mapped_payload = item
            product_id = self._product_id_from_raw(item.get("plu") or item.get("id"))
            if self.config.use_product_info and product_id is not None:
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

            image_urls = self._image_urls_from_product(mapped_payload)
            if image_urls:
                candidate_image_products += 1
                candidate_image_urls += len(image_urls)
            elif self.config.include_images and no_image_samples_logged < 3:
                no_image_samples_logged += 1
                LOGGER.debug(
                    "Product has no image candidates: product_id=%s category_id=%s page=%s keys=%s",
                    product_id,
                    query.category_id,
                    page,
                    sorted(mapped_payload.keys()),
                )

            main_image, gallery_images = await self._collect_product_images(
                api=api,
                product=mapped_payload,
                include_images=self.config.include_images,
                images_field="images",
                image_url_field="image",
                image_limit=self.config.image_limit_per_product,
                image_urls=image_urls,
            )
            gallery_count = len(gallery_images) if isinstance(gallery_images, list) else 0
            if main_image is not None:
                downloaded_main_images += 1
            downloaded_gallery_images += gallery_count
            if main_image is not None or gallery_count > 0:
                cards_with_downloaded_images += 1
            cards.append(
                ChizhikMapper.map_product(
                    mapped_payload,
                    main_image=main_image,
                    gallery_images=gallery_images,
                    strict_validation=self.config.strict_validation,
                )
            )
            if item_index % PAGE_PROGRESS_LOG_EVERY == 0 or item_index == total_items:
                LOGGER.info(
                    "Collecting products progress: category_id=%s slug=%s page=%s inspected=%s/%s mapped=%s enriched=%s image_candidates_products=%s downloaded_main=%s downloaded_gallery=%s",
                    query.category_id,
                    query.category_slug,
                    page,
                    item_index,
                    total_items,
                    len(cards),
                    enriched_count,
                    candidate_image_products,
                    downloaded_main_images,
                    downloaded_gallery_images,
                )

        LOGGER.info(
            "Collected products page: category_id=%s slug=%s page=%s count=%s enriched=%s total_pages=%s include_images=%s image_candidates_products=%s image_candidates_urls=%s downloaded_main=%s downloaded_gallery=%s cards_with_downloaded_images=%s",
            query.category_id,
            query.category_slug,
            page,
            len(cards),
            enriched_count,
            total_pages,
            self.config.include_images,
            candidate_image_products,
            candidate_image_urls,
            downloaded_main_images,
            downloaded_gallery_images,
            cards_with_downloaded_images,
        )
        return cards, total_pages

    @classmethod
    def _image_urls_from_product(cls, product: dict[str, Any]) -> list[str]:
        image_links = product.get("image_links")
        urls: list[str] = []
        if isinstance(image_links, dict):
            for key in ("normal", "small"):
                raw_urls = image_links.get(key)
                if not isinstance(raw_urls, list):
                    continue
                for value in raw_urls:
                    token = cls._safe_non_empty_str(value)
                    if token is not None and token not in urls:
                        urls.append(token)
        if urls:
            return urls
        return cls._extract_image_urls(
            product=product,
            images_field="images",
            image_url_field="image",
        )

    async def collect_products_for_queries(
        self,
        queries: list[CatalogProductsQuery],
        *,
        page_limit: int,
        items_per_page: int = 100,
        progress_callback: Callable[[int, int, str | None], None] | None = None,
    ) -> list[Card]:
        del items_per_page  # Chizhik API uses fixed server page size.
        safe_page_limit = max(1, page_limit)

        all_products: list[Card] = []
        key_to_index: dict[str, int] = {}

        total_queries = len(queries)
        for query_index, query in enumerate(queries, start=1):
            if progress_callback is not None:
                current_alias = query.category_slug or query.category_uid or str(query.category_id)
                progress_callback(total_queries, query_index - 1, current_alias)
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

        if progress_callback is not None and total_queries > 0:
            progress_callback(total_queries, total_queries, None)
        LOGGER.info(
            "Collected products for queries: queries=%s unique_products=%s",
            len(queries),
            len(all_products),
        )
        return all_products

    async def collect_categories(self) -> list[Category]:
        api = self._require_api()
        store_id = await self._ensure_store_id()
        LOGGER.info("Collecting Chizhik delivery category tree: store_id=%s", store_id)
        response = await api.Catalog.delivery_tree(store_id=store_id)
        raw_tree = response.json()

        if not isinstance(raw_tree, list):
            LOGGER.warning(
                "Chizhik delivery category tree has invalid payload type: type=%s store_id=%s",
                type(raw_tree).__name__,
                store_id,
            )
            return []

        tree_nodes = [node for node in raw_tree if isinstance(node, dict)]
        if not tree_nodes:
            return []
        root = tree_nodes[0]
        category_nodes = root.get("categories")
        if not isinstance(category_nodes, list):
            category_nodes = tree_nodes

        categories: list[Category] = []
        for node in category_nodes:
            if not isinstance(node, dict):
                continue
            enriched_node = dict(node)
            category_id = self._safe_non_empty_str(node.get("id"))
            if category_id is not None:
                try:
                    extended_response = await api.Catalog.delivery_tree_extended(
                        store_id=store_id,
                        category_alias=category_id,
                    )
                    extended_payload = extended_response.json()
                    if isinstance(extended_payload, dict):
                        children = extended_payload.get("categories_tags")
                        if isinstance(children, list):
                            enriched_node["children"] = children
                except Exception as exc:
                    LOGGER.warning(
                        "Failed to collect category extension: store_id=%s category_id=%s error=%s",
                        store_id,
                        category_id,
                        exc,
                    )
            categories.append(
                ChizhikMapper.map_category_node(
                    enriched_node,
                    strict_validation=self.config.strict_validation,
                )
            )

        LOGGER.info(
            "Collected delivery categories: %s (store_id=%s)",
            len(categories),
            store_id,
        )
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
                category_uid=category_id,
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
        normalized = store_code.strip().lower()
        for city in await self.collect_cities():
            alias = self._safe_non_empty_str(city.alias)
            name = self._safe_non_empty_str(city.name)
            if alias is not None and alias.lower() == normalized:
                return city
            if name is not None and name.lower() == normalized:
                return city

        response = await api.Geolocation.cities_list(search_name=store_code, page=1)
        payload = response.json()
        if not isinstance(payload, dict):
            return None
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            return None

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
        store_code: str | None = None,
    ) -> list[RetailUnit]:
        del country_id
        del region_id

        if store_code is None:
            stores: list[RetailUnit] = []
            seen_codes: set[str] = set()
            for city in await self.collect_cities():
                code = self._safe_non_empty_str(city.alias) or self._safe_non_empty_str(city.name)
                if code is None:
                    continue
                normalized_code = code.lower()
                if normalized_code in seen_codes:
                    continue
                seen_codes.add(normalized_code)
                stores.append(
                    ChizhikMapper.map_virtual_store(
                        store_code=code,
                        administrative_unit=city,
                        strict_validation=self.config.strict_validation,
                    )
                )
            LOGGER.info("Collected city-based virtual stores: matched=%s", len(stores))
            return stores

        administrative_unit = ChizhikMapper.fallback_administrative_unit(
            strict_validation=self.config.strict_validation
        )
        try:
            requested_store_code = self._safe_non_empty_str(store_code)
            if requested_store_code is not None:
                self.config.store_code = requested_store_code
            matched_shop = await self._resolve_store(store_code)
            if matched_shop is not None:
                resolved_store_id = self._safe_non_empty_str(matched_shop.get("sap_id"))
                if resolved_store_id is not None:
                    self._effective_store_id = resolved_store_id
                locality = matched_shop.get("locality")
                if isinstance(locality, dict):
                    administrative_unit = ChizhikMapper.map_city(
                        locality,
                        strict_validation=self.config.strict_validation,
                    )
                elif isinstance(locality, str):
                    matched = await self._city_for_store_code(locality)
                    if matched is not None:
                        administrative_unit = matched
                else:
                    matched = await self._city_for_store_code(store_code)
                    if matched is not None:
                        administrative_unit = matched
            else:
                matched = await self._city_for_store_code(store_code)
                if matched is not None:
                    administrative_unit = matched
        except Exception:
            LOGGER.exception("Failed to resolve city by store code: %s", store_code)

        if self._safe_non_empty_str(self._effective_store_id) is None:
            return []

        return [
            ChizhikMapper.map_virtual_store(
                store_code=store_code,
                administrative_unit=administrative_unit,
                strict_validation=self.config.strict_validation,
            )
        ]

    async def _resolve_store(self, store_code: str) -> dict[str, Any] | None:
        api = self._require_api()
        normalized = store_code.strip().lower()

        async def _payload_items(call: Any) -> list[dict[str, Any]]:
            response = await call()
            payload = response.json()
            if not isinstance(payload, list):
                return []
            return [item for item in payload if isinstance(item, dict)]

        def _pick(items: list[dict[str, Any]]) -> dict[str, Any] | None:
            exact_sap: dict[str, Any] | None = None
            exact_slug: dict[str, Any] | None = None
            locality_match: dict[str, Any] | None = None
            for item in items:
                sap_id = self._safe_non_empty_str(item.get("sap_id"))
                slug = self._safe_non_empty_str(item.get("slug"))
                locality = item.get("locality")
                locality_slug = None
                locality_name = None
                if isinstance(locality, dict):
                    locality_slug = self._safe_non_empty_str(locality.get("slug"))
                    locality_name = self._safe_non_empty_str(locality.get("name"))
                elif isinstance(locality, str):
                    locality_name = self._safe_non_empty_str(locality)
                if sap_id is not None and sap_id.lower() == normalized:
                    exact_sap = item
                    break
                if slug is not None and slug.lower() == normalized:
                    exact_slug = item
                if slug is not None and slug.lower().startswith(f"{normalized}-"):
                    exact_slug = item
                if locality_slug is not None and locality_slug.lower() == normalized:
                    locality_match = item
                if locality_name is not None and locality_name.lower() == normalized:
                    locality_match = item
            return exact_sap or exact_slug or locality_match or next(iter(items), None)

        payload = await _payload_items(lambda: api.Geolocation.Shop.search(query=store_code))
        selected = _pick(payload)
        if selected is not None:
            return selected

        if not payload:
            city = await self._city_for_store_code(store_code)
            city_name = self._safe_non_empty_str(city.name) if city is not None else None
            if city_name is not None and city_name.lower() != normalized:
                payload = await _payload_items(lambda: api.Geolocation.Shop.search(query=city_name))
                selected = _pick(payload)
                if selected is not None:
                    return selected

        payload = await _payload_items(api.Geolocation.Shop.all)
        return _pick(payload)
