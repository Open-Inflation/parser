from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .base import StoreParser
from .chizhik import ChizhikParser, ChizhikParserConfig
from .fixprice import FixPriceParser, FixPriceParserConfig
from .perekrestok import PerekrestokParser, PerekrestokParserConfig


@dataclass(frozen=True, slots=True)
class ParserRunSettings:
    country_id: int
    city_id: int | str | None
    timeout_ms: float
    request_retries: int
    request_retry_backoff_sec: float
    include_images: bool
    strict_validation: bool


class ParserAdapter(Protocol):
    name: str

    def create_parser(
        self,
        *,
        settings: ParserRunSettings,
        proxy: str | None,
    ) -> StoreParser: ...

    def city_id_for_store_info(self, city_id: int | str | None) -> int | str | None: ...


class FixPriceAdapter:
    name = "fixprice"

    @staticmethod
    def _fixprice_city_id(city_id: int | str | None) -> int | None:
        if isinstance(city_id, int):
            return city_id
        if isinstance(city_id, str):
            try:
                return int(city_id)
            except ValueError:
                return None
        return None

    def create_parser(
        self,
        *,
        settings: ParserRunSettings,
        proxy: str | None,
    ) -> StoreParser:
        config = FixPriceParserConfig(
            country_id=settings.country_id,
            city_id=self._fixprice_city_id(settings.city_id),
            proxy=proxy,
            timeout_ms=settings.timeout_ms,
            request_retries=settings.request_retries,
            request_retry_backoff_sec=settings.request_retry_backoff_sec,
            include_images=settings.include_images,
            strict_validation=settings.strict_validation,
        )
        return FixPriceParser(config)

    def city_id_for_store_info(self, city_id: int | str | None) -> int | None:
        return self._fixprice_city_id(city_id)


class ChizhikAdapter:
    name = "chizhik"

    def create_parser(
        self,
        *,
        settings: ParserRunSettings,
        proxy: str | None,
    ) -> StoreParser:
        chizhik_city_id = None if settings.city_id is None else str(settings.city_id)
        config = ChizhikParserConfig(
            country_id=settings.country_id,
            city_id=chizhik_city_id,
            proxy=proxy,
            timeout_ms=settings.timeout_ms,
            request_retries=settings.request_retries,
            request_retry_backoff_sec=settings.request_retry_backoff_sec,
            include_images=settings.include_images,
            strict_validation=settings.strict_validation,
        )
        return ChizhikParser(config)

    def city_id_for_store_info(self, city_id: int | str | None) -> int | str | None:
        return city_id


class PerekrestokAdapter:
    name = "perekrestok"

    @staticmethod
    def _perekrestok_city_id(city_id: int | str | None) -> int | None:
        if isinstance(city_id, int):
            return city_id
        if isinstance(city_id, str):
            token = city_id.strip()
            if not token:
                return None
            try:
                return int(token)
            except ValueError:
                return None
        return None

    def create_parser(
        self,
        *,
        settings: ParserRunSettings,
        proxy: str | None,
    ) -> StoreParser:
        config = PerekrestokParserConfig(
            country_id=settings.country_id,
            city_id=self._perekrestok_city_id(settings.city_id),
            proxy=proxy,
            timeout_ms=settings.timeout_ms,
            request_retries=settings.request_retries,
            request_retry_backoff_sec=settings.request_retry_backoff_sec,
            include_images=settings.include_images,
            strict_validation=settings.strict_validation,
        )
        return PerekrestokParser(config)

    def city_id_for_store_info(self, city_id: int | str | None) -> int | None:
        return self._perekrestok_city_id(city_id)


PARSER_ADAPTERS: dict[str, ParserAdapter] = {
    "fixprice": FixPriceAdapter(),
    "chizhik": ChizhikAdapter(),
    "perekrestok": PerekrestokAdapter(),
}


def get_parser_adapter(parser_name: str) -> ParserAdapter:
    normalized = parser_name.strip().lower()
    if normalized not in PARSER_ADAPTERS:
        supported = ", ".join(sorted(PARSER_ADAPTERS))
        raise ValueError(
            f"Unsupported parser adapter {parser_name!r}. Supported: {supported}"
        )
    return PARSER_ADAPTERS[normalized]
