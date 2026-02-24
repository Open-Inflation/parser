from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


CountryCode = Literal["RUS"]
ProducerCountryCode = Literal["RUS"]
CurrencyCode = Literal["RUB"]


@dataclass(slots=True)
class ChizhikParserConfig:
    country_id: int = 2
    city_id: str | None = None
    proxy: str | dict[str, Any] | None = None
    headless: bool = True
    timeout_ms: float = 90000.0
    request_retries: int = 3
    request_retry_backoff_sec: float = 1.5
    include_images: bool = False
    image_limit_per_product: int = 1
    strict_validation: bool = False
    city_search: str = "а"
    max_city_pages: int = 10


@dataclass(frozen=True, slots=True)
class CatalogProductsQuery:
    category_id: int
    category_uid: str | None = None
    category_slug: str | None = None
