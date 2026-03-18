from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


CountryCode = Literal["BLR", "RUS", "USA", "ARE"]
ProducerCountryCode = Literal["BLR", "RUS", "USA", "ARE", "CHN"]
CurrencyCode = Literal["BYN", "RUB", "USD", "EUR", "AED"]


@dataclass(slots=True)
class FixPriceParserConfig:
    country_id: int = 2
    city_id: int | None = None
    proxy: str | dict[str, Any] | None = None
    headless: bool = True
    timeout_ms: float = 90000.0
    include_images: bool = False
    use_product_info: bool = True
    image_limit_per_product: int = 1
    strict_validation: bool = False
    image_cache_dir: str | None = None


@dataclass(frozen=True, slots=True)
class CatalogProductsQuery:
    category_alias: str
    subcategory_alias: str | None = None
    category_uid: str | None = None
    subcategory_uid: str | None = None
