from __future__ import annotations

import logging
from typing import Any

from openinflation_dataclass import AdministrativeUnit

from ..base import StoreParser
from ..runtime import ParserRuntimeMixin
from .catalog import PerekrestokCatalogMixin
from .geolocation import PerekrestokGeolocationMixin
from .types import PerekrestokParserConfig


LOGGER = logging.getLogger(__name__)


class PerekrestokParser(
    PerekrestokCatalogMixin,
    PerekrestokGeolocationMixin,
    ParserRuntimeMixin,
    StoreParser,
):
    """Parser implementation based on perekrestok_api."""

    def __init__(self, config: PerekrestokParserConfig | None = None):
        self.config = config or PerekrestokParserConfig()
        self._api: Any = None
        self._city_cache_by_id: dict[int, AdministrativeUnit] = {}
        self._product_info_cache: dict[str, dict[str, Any]] = {}

    async def __aenter__(self) -> "PerekrestokParser":
        from perekrestok_api import PerekrestokAPI

        LOGGER.info(
            "Initializing Perekrestok API client: city_id=%s include_images=%s timeout_ms=%s",
            self.config.city_id,
            self.config.include_images,
            self.config.timeout_ms,
        )
        self._api = PerekrestokAPI(
            headless=self.config.headless,
            proxy=self.config.proxy,
            timeout_ms=self.config.timeout_ms,
        )
        await self._api.__aenter__()

        if self.config.city_id is not None:
            try:
                await self._set_city_context(city_id=self.config.city_id)
            except Exception as exc:
                LOGGER.warning(
                    "Failed to set city context: city_id=%s error=%s",
                    self.config.city_id,
                    exc,
                )

        LOGGER.info("Perekrestok API session warmed up")
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._api is not None:
            await self._api.__aexit__(*exc_info)
            self._api = None
        LOGGER.info("Perekrestok API session closed")

    def _require_api(self) -> Any:
        if self._api is None:
            raise RuntimeError("PerekrestokParser must be used inside 'async with'.")
        return self._api
