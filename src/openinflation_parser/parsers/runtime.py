from __future__ import annotations

import logging
from io import BytesIO
from typing import Any


LOGGER = logging.getLogger(__name__)


class ParserRuntimeMixin:
    """Shared API-call/image/string helpers for parser implementations."""

    config: Any

    @staticmethod
    def _safe_non_empty_str(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        token = value.strip()
        return token or None

    @classmethod
    def _merge_categories_uid(cls, *groups: list[str] | None) -> list[str] | None:
        prepared: list[str] = []
        seen: set[str] = set()
        for group in groups:
            if not isinstance(group, list):
                continue
            for item in group:
                token = cls._safe_non_empty_str(item)
                if token is None or token in seen:
                    continue
                seen.add(token)
                prepared.append(token)
        return prepared or None

    async def _download_image_if_needed(
        self,
        *,
        api: Any,
        url: str,
        include_images: bool,
    ) -> BytesIO | None:
        if not include_images:
            return None
        try:
            stream = await api.General.download_image(url=url)
        except Exception:
            LOGGER.exception("Image download failed: %s", url)
            return None
        return BytesIO(stream.getvalue())

    async def _collect_product_images(
        self,
        *,
        api: Any,
        product: dict[str, Any],
        include_images: bool,
        images_field: str,
        image_url_field: str,
        image_limit: int,
    ) -> tuple[BytesIO | None, list[BytesIO] | None]:
        if not include_images:
            return None, None

        image_nodes = product.get(images_field)
        if not isinstance(image_nodes, list):
            return None, None

        urls: list[str] = []
        for node in image_nodes:
            if not isinstance(node, dict):
                continue
            value = self._safe_non_empty_str(node.get(image_url_field))
            if value is not None:
                urls.append(value)
        if not urls:
            return None, None

        main = await self._download_image_if_needed(
            api=api,
            url=urls[0],
            include_images=include_images,
        )
        gallery: list[BytesIO] = []
        safe_limit = max(0, image_limit)
        for url in urls[1 : 1 + safe_limit]:
            image = await self._download_image_if_needed(
                api=api,
                url=url,
                include_images=include_images,
            )
            if image is not None:
                gallery.append(image)
        return main, (gallery or None)
