from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


CountryCode = Literal["BLR", "RUS", "USA", "ARE", "LVA", "GEO", "KGZ", "UZB", "MNG", "SRB", "KAZ"]
ProducerCountryCode = Literal[
    "BLR",
    "RUS",
    "USA",
    "ARE",
    "LVA",
    "GEO",
    "KGZ",
    "UZB",
    "MNG",
    "SRB",
    "KAZ",
    "CHN",
]
CurrencyCode = Literal["BYN", "RUB", "USD", "EUR", "AED", "GEL", "KGS", "UZS", "MNT", "RSD", "KZT"]


@dataclass(slots=True)
class FixPriceParserConfig:
    country_id: int = 2
    city_id: int | None = None
    proxy: str | dict[str, Any] | None = None
    headless: bool = True
    timeout_ms: float = 90000.0
    request_retries: int = 3
    request_retry_backoff_sec: float = 1.5
    include_images: bool = False
    image_limit_per_product: int = 1


@dataclass(frozen=True, slots=True)
class CatalogProductsQuery:
    category_alias: str
    subcategory_alias: str | None = None
    category_uid: str | None = None
    subcategory_uid: str | None = None
