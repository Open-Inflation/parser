from __future__ import annotations

from .adapters import ParserRunSettings, get_parser_adapter
from .base import StoreParser
from .chizhik import ChizhikParser, ChizhikParserConfig
from .fixprice import FixPriceParser, FixPriceParserConfig

PARSER_REGISTRY: dict[str, type[StoreParser]] = {
    "chizhik": ChizhikParser,
    "fixprice": FixPriceParser,
}


def get_parser(parser_name: str) -> type[StoreParser]:
    normalized = parser_name.strip().lower()
    if normalized not in PARSER_REGISTRY:
        supported = ", ".join(sorted(PARSER_REGISTRY))
        raise ValueError(f"Unsupported parser {parser_name!r}. Supported: {supported}")
    return PARSER_REGISTRY[normalized]


__all__ = [
    "ChizhikParser",
    "ChizhikParserConfig",
    "FixPriceParser",
    "FixPriceParserConfig",
    "ParserRunSettings",
    "StoreParser",
    "get_parser_adapter",
    "get_parser",
]
