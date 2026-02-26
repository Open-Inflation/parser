from __future__ import annotations

import hashlib
import logging
from pathlib import Path
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

    @staticmethod
    def _safe_path_token(value: Any, *, fallback: str = "item") -> str:
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8", errors="ignore")
            except Exception:
                value = ""
        token = str(value).strip().lower() if value is not None else ""
        if not token:
            return fallback
        prepared = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in token)
        prepared = prepared.strip("_")
        if not prepared:
            return fallback
        return prepared[:64]

    def _image_cache_root(self) -> Path | None:
        image_cache_dir = self._safe_non_empty_str(getattr(self.config, "image_cache_dir", None))
        if image_cache_dir is None:
            return None
        root = Path(image_cache_dir).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root

    @classmethod
    def _image_subject_token(cls, *, product: dict[str, Any]) -> str:
        for key in ("sku", "plu", "id", "uid", "code", "slug", "alias"):
            token = cls._safe_non_empty_str(product.get(key))
            if token is not None:
                return cls._safe_path_token(token, fallback="product")
            raw = product.get(key)
            if isinstance(raw, int) and not isinstance(raw, bool):
                return cls._safe_path_token(raw, fallback="product")
        return "product"

    def _persist_image_payload(
        self,
        *,
        product: dict[str, Any],
        source_url: str,
        payload: bytes,
        extension: str,
        role: str,
        ordinal: int,
    ) -> str | None:
        root = self._image_cache_root()
        if root is None:
            LOGGER.warning(
                "Image cache dir is not configured, dropping image payload: role=%s url=%s",
                role,
                source_url,
            )
            return None

        role_token = self._safe_path_token(role, fallback="image")
        subject_token = self._image_subject_token(product=product)
        source_hash = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:16]
        relative = Path("images") / subject_token / (
            f"{role_token}_{max(1, ordinal):03d}_{source_hash}.{extension}"
        )
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        return relative.as_posix()

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
    ) -> tuple[bytes, str] | None:
        if not include_images:
            return None
        try:
            stream = await api.General.download_image(url=url)
        except Exception as exc:
            LOGGER.warning(
                "Image download failed: url=%s error=%s detail=%s",
                url,
                type(exc).__name__,
                exc,
            )
            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug("Image download traceback: url=%s", url, exc_info=True)
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
        return payload, extension

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
        main_path: str | None = None
        if main is not None:
            main_payload, main_ext = main
            main_path = self._persist_image_payload(
                product=product,
                source_url=urls[0],
                payload=main_payload,
                extension=main_ext,
                role="main",
                ordinal=1,
            )
        gallery: list[str] = []
        safe_limit = max(0, image_limit)
        for index, url in enumerate(urls[1 : 1 + safe_limit], start=1):
            image = await self._download_image_if_needed(
                api=api,
                url=url,
                include_images=include_images,
            )
            if image is not None:
                payload, extension = image
                image_path = self._persist_image_payload(
                    product=product,
                    source_url=url,
                    payload=payload,
                    extension=extension,
                    role="gallery",
                    ordinal=index,
                )
                if image_path is not None:
                    gallery.append(image_path)
        return main_path, (gallery or None)
