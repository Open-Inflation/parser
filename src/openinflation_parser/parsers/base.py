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

    @abstractmethod
    async def collect_store_info(
        self,
        *,
        country_id: int | None = None,
        region_id: int | None = None,
        store_code: str | None = None,
    ) -> list[RetailUnit]:
        """Collect store information."""

    @abstractmethod
    async def collect_cities(self, *, country_id: int | None = None) -> list[AdministrativeUnit]:
        """Collect available cities."""
