from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


CountryCode = Literal["RUS"]
ProducerCountryCode = Literal["BLR", "RUS", "USA", "ARE", "CHN"]
CurrencyCode = Literal["RUB"]


@dataclass(slots=True)
class PerekrestokParserConfig:
    country_id: int = 2
    city_id: int | None = None
    proxy: str | dict[str, Any] | None = None
    headless: bool = True
    timeout_ms: float = 90000.0
    include_images: bool = False
    image_limit_per_product: int = 1
    strict_validation: bool = False
    image_cache_dir: str | None = None
    city_search: str = "а"
    city_search_limit: int = 200
    shops_page_size: int = 100
    max_shops_pages: int = 30


@dataclass(frozen=True, slots=True)
class CatalogProductsQuery:
    category_id: int
    category_uid: str | None = None
    category_slug: str | None = None
