from __future__ import annotations

from abc import ABC, abstractmethod

from openinflation_dataclass import AdministrativeUnit, Card, Category, RetailUnit


class StoreParser(ABC):
    """Parser contract used by orchestration workers."""

    async def __aenter__(self) -> "StoreParser":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    @abstractmethod
    async def collect_categories(self) -> list[Category]:
        """Collect category tree."""

    @abstractmethod
    async def collect_products(
        self,
        category_alias: str,
        *,
        subcategory_alias: str | None = None,
        page: int = 1,
        limit: int = 24,
    ) -> list[Card]:
        """Collect products for a category page."""

    async def collect_products_for_categories(
        self,
        category_aliases: list[str],
        *,
        page_limit: int = 1,
        items_per_page: int = 24,
    ) -> list[Card]:
        """Collect products for multiple categories and pages."""
        products: list[Card] = []
        safe_page_limit = max(1, page_limit)
        safe_items_per_page = max(1, items_per_page)

        for category_alias in category_aliases:
            if not category_alias:
                continue
            for page in range(1, safe_page_limit + 1):
                page_products = await self.collect_products(
                    category_alias=category_alias,
                    page=page,
                    limit=safe_items_per_page,
                )
                if not page_products:
                    break
                products.extend(page_products)
        return products

    @abstractmethod
    async def collect_store_info(
        self,
        *,
        country_id: int | None = None,
        region_id: int | None = None,
        city_id: int | None = None,
        store_code: str | None = None,
    ) -> list[RetailUnit]:
        """Collect store information."""

    @abstractmethod
    async def collect_cities(self, *, country_id: int | None = None) -> list[AdministrativeUnit]:
        """Collect available cities."""
