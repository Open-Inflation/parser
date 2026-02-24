from __future__ import annotations

import logging
from typing import Any

from openinflation_dataclass import AdministrativeUnit, RetailUnit

from .mapper import PerekrestokMapper


LOGGER = logging.getLogger(__name__)


class PerekrestokGeolocationMixin:
    @classmethod
    def _store_id_from_code(cls, value: Any) -> int | None:
        token = cls._safe_non_empty_str(value)
        if token is None:
            return None
        try:
            return int(token)
        except ValueError:
            return None

    @staticmethod
    def _city_id_from_raw(value: Any) -> int | None:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str):
            token = value.strip()
            if not token:
                return None
            try:
                return int(token)
            except ValueError:
                return None
        return None

    async def _set_city_context(self, *, city_id: int) -> None:
        api = self._require_api()
        response = await self._with_retry(
            operation=f"geolocation.shop.on_map[{city_id}:1]",
            call=lambda: api.Geolocation.Shop.on_map(
                city_id=city_id,
                page=1,
                limit=1,
            ),
        )
        payload = response.json()
        if not isinstance(payload, dict):
            return
        content = payload.get("content")
        if not isinstance(content, dict):
            return
        items = content.get("items")
        if not isinstance(items, list) or not items:
            return
        first_item = items[0]
        if not isinstance(first_item, dict):
            return
        shop_id = self._store_id_from_code(first_item.get("id"))
        if shop_id is None:
            return

        await self._with_retry(
            operation=f"geolocation.selection.shop_point[{shop_id}]",
            call=lambda: api.Geolocation.Selection.shop_point(shop_id=shop_id),
        )
        LOGGER.info(
            "Selected session city context: city_id=%s shop_id=%s",
            city_id,
            shop_id,
        )

    async def collect_cities(self, *, country_id: int | None = None) -> list[AdministrativeUnit]:
        if self._city_cache_by_id:
            return list(self._city_cache_by_id.values())

        api = self._require_api()
        target_country_id = country_id or self.config.country_id
        search = self._safe_non_empty_str(self.config.city_search) or "а"
        limit = max(1, self.config.city_search_limit)

        LOGGER.info("Collecting cities: query=%s limit=%s", search, limit)
        response = await self._with_retry(
            operation=f"geolocation.search[{search}:{limit}]",
            call=lambda: api.Geolocation.search(search=search, limit=limit),
        )
        payload = response.json()
        if isinstance(payload, dict):
            content = payload.get("content")
            if isinstance(content, dict):
                items = content.get("items")
                if isinstance(items, list):
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        city_id = self._city_id_from_raw(item.get("id"))
                        if city_id is None:
                            continue
                        self._city_cache_by_id[city_id] = PerekrestokMapper.map_city(
                            item,
                            country_id=target_country_id,
                            strict_validation=self.config.strict_validation,
                        )

        if not self._city_cache_by_id:
            fallback = await self._with_retry(
                operation="geolocation.current",
                call=lambda: api.Geolocation.current(),
            )
            fallback_payload = fallback.json()
            if isinstance(fallback_payload, dict):
                content = fallback_payload.get("content")
                if isinstance(content, dict) and isinstance(content.get("city"), dict):
                    city = content["city"]
                    city_id = self._city_id_from_raw(city.get("id"))
                    if city_id is not None:
                        self._city_cache_by_id[city_id] = PerekrestokMapper.map_city(
                            city,
                            country_id=target_country_id,
                            strict_validation=self.config.strict_validation,
                        )

        LOGGER.info("Collected cities: %s", len(self._city_cache_by_id))
        return list(self._city_cache_by_id.values())

    async def _collect_shops_on_map(self, *, city_id: int | None) -> list[dict[str, Any]]:
        api = self._require_api()
        safe_page_size = max(1, min(200, self.config.shops_page_size))
        safe_max_pages = max(1, self.config.max_shops_pages)

        shops: list[dict[str, Any]] = []
        seen_ids: set[int] = set()

        for page in range(1, safe_max_pages + 1):
            response = await self._with_retry(
                operation=f"geolocation.shop.on_map[{city_id}:{page}:{safe_page_size}]",
                call=lambda: api.Geolocation.Shop.on_map(
                    page=page,
                    limit=safe_page_size,
                    city_id=city_id,
                ),
            )
            payload = response.json()
            if not isinstance(payload, dict):
                break
            content = payload.get("content")
            if not isinstance(content, dict):
                break
            items = content.get("items")
            if not isinstance(items, list) or not items:
                break

            for shop in items:
                if not isinstance(shop, dict):
                    continue
                shop_id = self._store_id_from_code(shop.get("id"))
                if shop_id is not None and shop_id in seen_ids:
                    continue
                if shop_id is not None:
                    seen_ids.add(shop_id)
                shops.append(shop)

            paginator = content.get("paginator")
            next_exists = False
            if isinstance(paginator, dict):
                next_raw = paginator.get("nextPageExists")
                if isinstance(next_raw, bool):
                    next_exists = next_raw
            if not next_exists:
                break

        return shops

    async def _collect_shop_info(self, *, shop_id: int) -> dict[str, Any] | None:
        api = self._require_api()
        response = await self._with_retry(
            operation=f"geolocation.shop.info[{shop_id}]",
            call=lambda: api.Geolocation.Shop.info(shop_id=shop_id),
        )
        payload = response.json()
        if not isinstance(payload, dict):
            return None
        content = payload.get("content")
        if isinstance(content, dict):
            return content
        return None

    @classmethod
    def _matches_store_code(cls, *, store: dict[str, Any], store_code: str) -> bool:
        normalized = store_code.strip().lower()
        if not normalized:
            return False

        shop_id = cls._store_id_from_code(store.get("id"))
        if shop_id is not None and str(shop_id) == normalized:
            return True

        for field in ("title", "address"):
            token = cls._safe_non_empty_str(store.get(field))
            if token is not None and token.lower() == normalized:
                return True

        city = store.get("city") if isinstance(store.get("city"), dict) else {}
        for field in ("slug", "name", "key"):
            token = cls._safe_non_empty_str(city.get(field))
            if token is not None and token.lower() == normalized:
                return True

        return False

    async def collect_store_info(
        self,
        *,
        country_id: int | None = None,
        region_id: int | None = None,
        city_id: int | str | None = None,
        store_code: str | None = None,
    ) -> list[RetailUnit]:
        del region_id

        target_country_id = country_id or self.config.country_id
        target_city_id: int | None
        if isinstance(city_id, int):
            target_city_id = city_id
        elif isinstance(city_id, str):
            try:
                target_city_id = int(city_id)
            except ValueError:
                target_city_id = None
        else:
            target_city_id = self.config.city_id

        LOGGER.info(
            "Collecting stores: country_id=%s city_id=%s store_code=%s",
            target_country_id,
            target_city_id,
            store_code,
        )

        matched_shops: list[dict[str, Any]] = []
        if target_city_id is not None:
            shops = await self._collect_shops_on_map(city_id=target_city_id)
            if store_code is None:
                matched_shops = shops
            else:
                matched_shops = [
                    shop
                    for shop in shops
                    if self._matches_store_code(store=shop, store_code=store_code)
                ]
        elif store_code is not None:
            shop_id = self._store_id_from_code(store_code)
            if shop_id is not None:
                info = await self._collect_shop_info(shop_id=shop_id)
                if info is not None:
                    matched_shops = [info]
            else:
                shops = await self._collect_shops_on_map(city_id=None)
                matched_shops = [
                    shop
                    for shop in shops
                    if self._matches_store_code(store=shop, store_code=store_code)
                ]

        if not matched_shops and store_code is not None:
            shop_id = self._store_id_from_code(store_code)
            if shop_id is not None:
                info = await self._collect_shop_info(shop_id=shop_id)
                if info is not None:
                    matched_shops = [info]

        stores: list[RetailUnit] = []
        for shop in matched_shops:
            if not isinstance(shop, dict):
                continue
            raw_city = shop.get("city") if isinstance(shop.get("city"), dict) else None

            administrative_unit: AdministrativeUnit
            if isinstance(raw_city, dict):
                administrative_unit = PerekrestokMapper.map_city(
                    raw_city,
                    country_id=target_country_id,
                    strict_validation=self.config.strict_validation,
                )
                raw_city_id = self._city_id_from_raw(raw_city.get("id"))
                if raw_city_id is not None:
                    self._city_cache_by_id[raw_city_id] = administrative_unit
            elif target_city_id is not None and target_city_id in self._city_cache_by_id:
                administrative_unit = self._city_cache_by_id[target_city_id]
            else:
                administrative_unit = PerekrestokMapper.fallback_administrative_unit(
                    strict_validation=self.config.strict_validation,
                )

            stores.append(
                PerekrestokMapper.map_store(
                    shop,
                    administrative_unit=administrative_unit,
                    strict_validation=self.config.strict_validation,
                )
            )

        LOGGER.info("Collected stores: matched=%s", len(stores))
        return stores
