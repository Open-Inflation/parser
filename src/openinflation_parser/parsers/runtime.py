from __future__ import annotations

import base64
import logging
from typing import Any


LOGGER = logging.getLogger(__name__)
INLINE_IMAGE_TOKEN_PREFIX = "__oi_inline_image_b64__:"


class ParserRuntimeMixin:
    """Shared API-call/image/string helpers for parser implementations."""

    config: Any

    @staticmethod
    def _safe_non_empty_str(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        token = value.strip()
        return token or None

    @staticmethod
    def _detect_image_extension(payload: bytes) -> str | None:
        if payload.startswith(b"\xff\xd8\xff"):
            return "jpg"
        if payload.startswith(b"\x89PNG\r\n\x1a\n"):
            return "png"
        if payload.startswith((b"GIF87a", b"GIF89a")):
            return "gif"
        if payload.startswith(b"BM"):
            return "bmp"
        if payload.startswith(b"RIFF") and payload[8:12] == b"WEBP":
            return "webp"
        if len(payload) >= 12 and payload[4:12] in {b"ftypavif", b"ftypavis"}:
            return "avif"
        return None

    @staticmethod
    def _non_image_reason(payload: bytes) -> str:
        probe = payload[:512].strip().lower()
        if not probe:
            return "empty-payload"
        if probe.startswith((b"<!doctype html", b"<html", b"<?xml")):
            return "html-response"
        if b"request rejected" in probe or b"access denied" in probe:
            return "waf-rejection"
        if b"<body" in probe or b"<head" in probe:
            return "html-fragment"
        return "unknown-content"

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
    ) -> str | None:
        if not include_images:
            return None
        try:
            stream = await api.General.download_image(url=url)
        except Exception:
            LOGGER.exception("Image download failed: %s", url)
            return None
        payload = stream.getvalue()
        if not payload:
            LOGGER.warning("Image payload is empty: url=%s", url)
            return None

        extension = self._detect_image_extension(payload)
        if extension is None:
            LOGGER.warning(
                "Skipping non-image payload: url=%s reason=%s bytes=%s",
                url,
                self._non_image_reason(payload),
                len(payload),
            )
            return None

        encoded = base64.b64encode(payload).decode("ascii")
        return f"{INLINE_IMAGE_TOKEN_PREFIX}{extension}:{encoded}"

    @classmethod
    def _extract_image_urls(
        cls,
        *,
        product: dict[str, Any],
        images_field: str,
        image_url_field: str,
    ) -> list[str]:
        image_nodes = product.get(images_field)
        urls: list[str] = []
        if isinstance(image_nodes, list):
            for node in image_nodes:
                if isinstance(node, dict):
                    value = cls._safe_non_empty_str(node.get(image_url_field))
                    if value is not None:
                        urls.append(value)
                    continue
                value = cls._safe_non_empty_str(node)
                if value is not None:
                    urls.append(value)
            return urls

        # Some APIs return a direct top-level URL instead of a list node.
        top_level_url = cls._safe_non_empty_str(product.get(image_url_field))
        if top_level_url is not None:
            urls.append(top_level_url)
        return urls

    async def _collect_product_images(
        self,
        *,
        api: Any,
        product: dict[str, Any],
        include_images: bool,
        images_field: str,
        image_url_field: str,
        image_limit: int,
        image_urls: list[str] | None = None,
    ) -> tuple[str | None, list[str] | None]:
        if not include_images:
            return None, None

        if image_urls is not None:
            urls: list[str] = []
            for value in image_urls:
                token = self._safe_non_empty_str(value)
                if token is not None:
                    urls.append(token)
        else:
            urls = self._extract_image_urls(
                product=product,
                images_field=images_field,
                image_url_field=image_url_field,
            )
        if not urls:
            return None, None

        main = await self._download_image_if_needed(
            api=api,
            url=urls[0],
            include_images=include_images,
        )
        gallery: list[str] = []
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
