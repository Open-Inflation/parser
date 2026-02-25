from __future__ import annotations

import logging
from typing import Any

from openinflation_dataclass import Card, Category

from .mapper import PerekrestokMapper
from .types import CatalogProductsQuery


LOGGER = logging.getLogger(__name__)


class PerekrestokCatalogMixin:
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
            leaves.extend(PerekrestokCatalogMixin._iter_leaf_categories(child))
        return leaves

    @staticmethod
    def _card_key(card: Card) -> str | None:
        if card.sku is not None:
            return card.sku
        return card.plu

    def _build_feed_filter(self, *, category_id: int) -> Any:
        from perekrestok_api import abstraction

        feed_filter = abstraction.CatalogFeedFilter()
        feed_filter.CATEGORY_ID = category_id
        return feed_filter

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

    @classmethod
    def _product_plu_from_payload(cls, product: dict[str, Any]) -> str | None:
        master_data = (
            product.get("masterData")
            if isinstance(product.get("masterData"), dict)
            else {}
        )
        plu_raw = master_data.get("plu")
        if isinstance(plu_raw, int) and not isinstance(plu_raw, bool):
            return str(plu_raw)
        if isinstance(plu_raw, str):
            token = plu_raw.strip()
            return token or None

        top_plu = product.get("plu")
        if isinstance(top_plu, int) and not isinstance(top_plu, bool):
            return str(top_plu)
        if isinstance(top_plu, str):
            token = top_plu.strip()
            return token or None
        return None

    async def _collect_product_info(self, *, product_plu: str) -> dict[str, Any] | None:
        cached = self._product_info_cache.get(product_plu)
        if cached is not None:
            return cached

        api = self._require_api()
        response = await api.Catalog.Product.info(product_plu=product_plu)
        payload = response.json()
        if not isinstance(payload, dict):
            return None

        content = payload.get("content")
        if not isinstance(content, dict):
            return None

        self._product_info_cache[product_plu] = content
        return content

    @classmethod
    def _image_url_from_node(cls, node: Any) -> str | None:
        if not isinstance(node, dict):
            return None
        template = cls._safe_non_empty_str(node.get("cropUrlTemplate"))
        if template is None:
            return None
        if "%s" not in template:
            return template

        width = node.get("width")
        height = node.get("height")
        if (
            isinstance(width, int)
            and not isinstance(width, bool)
            and width > 0
            and isinstance(height, int)
            and not isinstance(height, bool)
            and height > 0
        ):
            return template.replace("%s", f"{width}x{height}")

        return None

    async def _collect_product_images_for_payload(
        self,
        *,
        product: dict[str, Any],
    ) -> tuple[Any, list[Any] | None]:
        if not self.config.include_images:
            return None, None

        image_nodes = product.get("images")
        if not isinstance(image_nodes, list):
            image_nodes = []

        urls: list[str] = []
        for node in image_nodes:
            url = self._image_url_from_node(node)
            if url is not None:
                urls.append(url)

        if not urls:
            single = self._image_url_from_node(product.get("image"))
            if single is not None:
                urls.append(single)

        if not urls:
            return None, None

        main = await self._download_image_if_needed(
            api=self._require_api(),
            url=urls[0],
            include_images=self.config.include_images,
        )
        gallery: list[Any] = []
        safe_limit = max(0, int(self.config.image_limit_per_product))
        for url in urls[1 : 1 + safe_limit]:
            image = await self._download_image_if_needed(
                api=self._require_api(),
                url=url,
                include_images=self.config.include_images,
            )
            if image is not None:
                gallery.append(image)
        return main, (gallery or None)

    async def _collect_products_page(
        self,
        *,
        query: CatalogProductsQuery,
        page: int,
        limit: int,
    ) -> tuple[list[Card], bool]:
        api = self._require_api()
        safe_limit = max(1, min(100, limit))

        LOGGER.info(
            "Collecting products: category_id=%s slug=%s page=%s limit=%s",
            query.category_id,
            query.category_slug,
            page,
            safe_limit,
        )
        response = await api.Catalog.feed(
            filter=self._build_feed_filter(category_id=query.category_id),
            page=page,
            limit=safe_limit,
        )
        payload = response.json()
        if not isinstance(payload, dict):
            return [], False
        content = payload.get("content")
        if not isinstance(content, dict):
            return [], False

        raw_items = content.get("items")
        if not isinstance(raw_items, list):
            return [], False

        cards: list[Card] = []
        enriched_count = 0
        query_categories_uid = (
            [query.category_uid] if query.category_uid is not None else None
        )

        for item in raw_items:
            if not isinstance(item, dict):
                continue

            mapped_payload = item
            product_plu = self._product_plu_from_payload(item)
            if product_plu is not None:
                try:
                    info = await self._collect_product_info(product_plu=product_plu)
                    if info is not None:
                        merged = dict(item)
                        merged.update(info)
                        mapped_payload = merged
                        enriched_count += 1
                except Exception as exc:
                    LOGGER.warning(
                        "Failed to collect product info: plu=%s error=%s",
                        product_plu,
                        exc,
                    )

            main_image, gallery_images = await self._collect_product_images_for_payload(
                product=mapped_payload
            )
            cards.append(
                PerekrestokMapper.map_product(
                    mapped_payload,
                    categories_uid=query_categories_uid,
                    main_image=main_image,
                    gallery_images=gallery_images,
                    strict_validation=self.config.strict_validation,
                )
            )

        paginator = content.get("paginator")
        next_page_exists = False
        if isinstance(paginator, dict):
            next_raw = paginator.get("nextPageExists")
            if isinstance(next_raw, bool):
                next_page_exists = next_raw

        LOGGER.info(
            "Collected products page: category_id=%s slug=%s page=%s count=%s enriched=%s next_page=%s",
            query.category_id,
            query.category_slug,
            page,
            len(cards),
            enriched_count,
            next_page_exists,
        )
        return cards, next_page_exists

    async def collect_products_for_queries(
        self,
        queries: list[CatalogProductsQuery],
        *,
        page_limit: int,
        items_per_page: int = 100,
    ) -> list[Card]:
        safe_page_limit = max(1, page_limit)
        safe_items_per_page = max(1, min(100, items_per_page))

        all_products: list[Card] = []
        key_to_index: dict[str, int] = {}

        for query in queries:
            query_categories_uid = (
                [query.category_uid] if query.category_uid is not None else None
            )
            for page in range(1, safe_page_limit + 1):
                page_products, has_next_page = await self._collect_products_page(
                    query=query,
                    page=page,
                    limit=safe_items_per_page,
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

                if not has_next_page:
                    break

        LOGGER.info(
            "Collected products for queries: queries=%s unique_products=%s",
            len(queries),
            len(all_products),
        )
        return all_products

    async def collect_categories(self) -> list[Category]:
        api = self._require_api()
        LOGGER.info("Collecting Perekrestok category tree")
        response = await api.Catalog.tree()
        payload = response.json()
        if not isinstance(payload, dict):
            return []
        content = payload.get("content")
        if not isinstance(content, dict):
            return []
        raw_items = content.get("items")
        if not isinstance(raw_items, list):
            return []

        categories: list[Category] = []
        for node in raw_items:
            if not isinstance(node, dict):
                continue
            categories.append(
                PerekrestokMapper.map_category_node(
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
            limit=max(1, min(100, limit)),
        )
        return products
