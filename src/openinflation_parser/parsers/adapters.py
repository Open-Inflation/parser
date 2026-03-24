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
    store_code: str | None
    timeout_ms: float
    include_images: bool
    use_product_info: bool
    strict_validation: bool
    image_cache_dir: str | None = None


class ParserAdapter(Protocol):
    name: str

    def create_parser(
        self,
        *,
        settings: ParserRunSettings,
        proxy: str | None,
    ) -> StoreParser: ...


class FixPriceAdapter:
    name = "fixprice"

    def create_parser(
        self,
        *,
        settings: ParserRunSettings,
        proxy: str | None,
    ) -> StoreParser:
        config = FixPriceParserConfig(
            country_id=settings.country_id,
            proxy=proxy,
            timeout_ms=settings.timeout_ms,
            include_images=settings.include_images,
            use_product_info=settings.use_product_info,
            strict_validation=settings.strict_validation,
            image_cache_dir=settings.image_cache_dir,
        )
        return FixPriceParser(config)

class ChizhikAdapter:
    name = "chizhik"

    def create_parser(
        self,
        *,
        settings: ParserRunSettings,
        proxy: str | None,
    ) -> StoreParser:
        config = ChizhikParserConfig(
            country_id=settings.country_id,
            store_code=settings.store_code,
            proxy=proxy,
            timeout_ms=settings.timeout_ms,
            include_images=settings.include_images,
            use_product_info=settings.use_product_info,
            strict_validation=settings.strict_validation,
            image_cache_dir=settings.image_cache_dir,
        )
        return ChizhikParser(config)

class PerekrestokAdapter:
    name = "perekrestok"

    def create_parser(
        self,
        *,
        settings: ParserRunSettings,
        proxy: str | None,
    ) -> StoreParser:
        config = PerekrestokParserConfig(
            country_id=settings.country_id,
            proxy=proxy,
            timeout_ms=settings.timeout_ms,
            include_images=settings.include_images,
            strict_validation=settings.strict_validation,
            image_cache_dir=settings.image_cache_dir,
        )
        return PerekrestokParser(config)


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
