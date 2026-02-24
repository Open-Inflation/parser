from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", ""}:
            return False
    return bool(value)


def normalize_city_id(value: Any) -> int | str | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return str(value)
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        try:
            return int(token)
        except ValueError:
            return token
    return str(value)


@dataclass(slots=True)
class WorkerJob:
    job_id: str
    parser_name: str
    store_code: str
    output_dir: str
    country_id: int = 2
    city_id: int | str | None = None
    api_timeout_ms: float = 90000.0
    request_retries: int = 3
    request_retry_backoff_sec: float = 1.5
    category_limit: int = 1
    pages_per_category: int = 1
    max_pages_per_category: int = 200
    products_per_page: int = 24
    full_catalog: bool = False
    include_images: bool = False
    strict_validation: bool = False

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "WorkerJob":
        city_id = normalize_city_id(payload.get("city_id"))
        return cls(
            job_id=str(payload["job_id"]),
            parser_name=str(payload["parser_name"]),
            store_code=str(payload["store_code"]),
            output_dir=str(payload["output_dir"]),
            country_id=int(payload.get("country_id", 2)),
            city_id=city_id,
            api_timeout_ms=float(payload.get("api_timeout_ms", 90000.0)),
            request_retries=int(payload.get("request_retries", 3)),
            request_retry_backoff_sec=float(
                payload.get("request_retry_backoff_sec", 1.5)
            ),
            category_limit=int(payload.get("category_limit", 1)),
            pages_per_category=int(payload.get("pages_per_category", 1)),
            max_pages_per_category=int(payload.get("max_pages_per_category", 200)),
            products_per_page=int(payload.get("products_per_page", 24)),
            full_catalog=coerce_bool(payload.get("full_catalog", False)),
            include_images=coerce_bool(payload.get("include_images", False)),
            strict_validation=coerce_bool(payload.get("strict_validation", False)),
        )

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class JobDefaults:
    parser_name: str
    output_dir: str
    country_id: int
    city_id: int | str | None
    api_timeout_ms: float
    request_retries: int
    request_retry_backoff_sec: float
    category_limit: int
    pages_per_category: int
    max_pages_per_category: int
    products_per_page: int
    full_catalog: bool
    include_images: bool
    strict_validation: bool
