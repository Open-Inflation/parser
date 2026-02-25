from __future__ import annotations

import base64
import binascii
import gzip
import logging
import os
import re
import shutil
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from openinflation_dataclass import to_json

from ..parsers.runtime import INLINE_IMAGE_TOKEN_PREFIX


LOGGER = logging.getLogger(__name__)
_INLINE_IMAGE_EXT_RE = re.compile(r"^[a-z0-9]{2,8}$")


def setup_logging(level: str) -> int:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(processName)s | %(name)s | %(message)s",
    )
    return int(numeric_level)


def require_websockets_module() -> Any:
    try:
        import websockets
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Package 'websockets' is required to run orchestrator server. "
            "Install project dependencies first."
        ) from exc
    return websockets


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def detect_available_ram_gb() -> float:
    """Detect available RAM in GiB."""
    try:
        import psutil  # type: ignore

        return psutil.virtual_memory().available / (1024**3)
    except Exception:
        pass

    if hasattr(os, "sysconf"):
        try:
            page_size = os.sysconf("SC_PAGE_SIZE")
            available_pages = os.sysconf("SC_AVPHYS_PAGES")
            if page_size > 0 and available_pages > 0:
                return (page_size * available_pages) / (1024**3)
        except (ValueError, OSError):
            pass

    return 1.0


def choose_worker_count(
    *,
    available_ram_gb: float,
    ram_per_worker_gb: float,
    proxies_count: int,
    max_workers: int | None,
    proxy_reuse_factor: int = 1,
) -> int:
    """Select worker count based on RAM and proxy availability."""
    safe_ram_per_worker = ram_per_worker_gb if ram_per_worker_gb > 0 else 1.0
    by_ram = int(available_ram_gb // safe_ram_per_worker)
    if by_ram < 1:
        by_ram = 1

    safe_proxy_reuse_factor = max(1, proxy_reuse_factor)
    by_proxy = (
        proxies_count * safe_proxy_reuse_factor
        if proxies_count > 0
        else by_ram
    )
    workers = min(by_ram, by_proxy)

    if max_workers is not None:
        workers = min(workers, max(1, max_workers))
    return max(1, workers)


def load_proxy_list(explicit: list[str], proxy_file: str | None) -> list[str]:
    proxies = [proxy.strip() for proxy in explicit if proxy and proxy.strip()]

    if proxy_file:
        file_path = Path(proxy_file).expanduser()
        if file_path.exists():
            for line in file_path.read_text(encoding="utf-8").splitlines():
                candidate = line.strip()
                if not candidate or candidate.startswith("#"):
                    continue
                proxies.append(candidate)

    # Keep insertion order while de-duplicating.
    unique_proxies = list(dict.fromkeys(proxies))
    return unique_proxies


def safe_store_code(value: str) -> str:
    token = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value.strip())
    token = token.strip("_")
    return token or "store"


def write_payload(payload: str, *, output_dir: str, store_code: str) -> tuple[str, str]:
    out_dir = Path(output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = f"{safe_store_code(store_code)}_{timestamp}"

    json_path = out_dir / f"{base}.json"
    json_gz_path = out_dir / f"{base}.json.gz"

    json_path.write_text(payload, encoding="utf-8")
    with gzip.open(json_gz_path, "wt", encoding="utf-8") as stream:
        stream.write(payload)

    LOGGER.info("Store payload saved: json=%s gz=%s", json_path, json_gz_path)
    return str(json_path), str(json_gz_path)


def _decode_inline_image_token(value: str) -> tuple[bytes, str] | None:
    if not value.startswith(INLINE_IMAGE_TOKEN_PREFIX):
        return None
    encoded_with_meta = value[len(INLINE_IMAGE_TOKEN_PREFIX) :]
    if not encoded_with_meta:
        return None
    extension = "bin"
    encoded = encoded_with_meta
    if ":" in encoded_with_meta:
        maybe_ext, maybe_encoded = encoded_with_meta.split(":", 1)
        normalized_ext = maybe_ext.strip().lower()
        if _INLINE_IMAGE_EXT_RE.fullmatch(normalized_ext):
            extension = normalized_ext
            encoded = maybe_encoded
    if not encoded:
        return None
    try:
        payload = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        return None
    if not payload:
        return None
    return payload, extension


def _card_file_stem(card: Any, *, index: int) -> str:
    sku = getattr(card, "sku", None)
    if isinstance(sku, str) and sku.strip():
        return safe_store_code(sku)
    plu = getattr(card, "plu", None)
    if isinstance(plu, str) and plu.strip():
        return safe_store_code(plu)
    return f"product_{index:05d}"


def _materialize_card_images(
    card: Any,
    *,
    card_index: int,
    bundle_dir: Path,
) -> int:
    images_written = 0
    stem = _card_file_stem(card, index=card_index)
    card_dir = bundle_dir / "images" / f"{card_index:05d}_{stem}"

    main_image_value = getattr(card, "main_image", None)
    if isinstance(main_image_value, str):
        decoded_main = _decode_inline_image_token(main_image_value)
        if decoded_main is not None:
            main_payload, main_ext = decoded_main
            main_path = card_dir / f"main.{main_ext}"
            main_path.parent.mkdir(parents=True, exist_ok=True)
            main_path.write_bytes(main_payload)
            setattr(card, "main_image", main_path.relative_to(bundle_dir).as_posix())
            images_written += 1
        elif main_image_value.startswith(INLINE_IMAGE_TOKEN_PREFIX):
            LOGGER.warning(
                "Dropping invalid inline main image token: card_index=%s",
                card_index,
            )
            setattr(card, "main_image", None)
    elif main_image_value is not None:
        setattr(card, "main_image", None)

    images_value = getattr(card, "images", None)
    if isinstance(images_value, list):
        materialized_gallery: list[str] = []
        for image_index, image_value in enumerate(images_value, start=1):
            if not isinstance(image_value, str):
                continue
            decoded_gallery = _decode_inline_image_token(image_value)
            if decoded_gallery is None:
                if image_value.startswith(INLINE_IMAGE_TOKEN_PREFIX):
                    LOGGER.warning(
                        "Dropping invalid inline gallery image token: card_index=%s image_index=%s",
                        card_index,
                        image_index,
                    )
                    continue
                materialized_gallery.append(image_value)
                continue
            payload, extension = decoded_gallery
            image_path = card_dir / f"gallery_{image_index:03d}.{extension}"
            image_path.parent.mkdir(parents=True, exist_ok=True)
            image_path.write_bytes(payload)
            materialized_gallery.append(image_path.relative_to(bundle_dir).as_posix())
            images_written += 1
        setattr(card, "images", materialized_gallery or None)
    elif images_value is not None:
        setattr(card, "images", None)

    return images_written


def write_store_bundle(
    store: Any,
    *,
    output_dir: str,
    store_code: str,
    worker_log_path: str | None = None,
) -> tuple[str, str]:
    out_dir = Path(output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = f"{safe_store_code(store_code)}_{timestamp}"

    json_path = out_dir / f"{base}.json"
    archive_path = out_dir / f"{base}.tar.gz"
    bundle_dir = out_dir / f".{base}_bundle"

    if bundle_dir.exists():
        shutil.rmtree(bundle_dir, ignore_errors=True)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    images_written = 0
    worker_log_included = False
    bundled_worker_log_path = bundle_dir / "worker.log"
    products = getattr(store, "products", None)
    if isinstance(products, list):
        for index, card in enumerate(products, start=1):
            images_written += _materialize_card_images(
                card,
                card_index=index,
                bundle_dir=bundle_dir,
            )

    if worker_log_path is not None:
        source_worker_log = Path(worker_log_path).expanduser()
        if source_worker_log.is_file():
            shutil.copy2(source_worker_log, bundled_worker_log_path)
            worker_log_included = True
        else:
            LOGGER.warning("Worker log file is missing, skipping archive attach: %s", source_worker_log)

    payload = to_json(store)
    meta_path = bundle_dir / "meta.json"
    meta_path.write_text(payload, encoding="utf-8")
    json_path.write_text(payload, encoding="utf-8")

    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(meta_path, arcname="meta.json")
        if worker_log_included:
            archive.add(bundled_worker_log_path, arcname="worker.log")
        images_dir = bundle_dir / "images"
        if images_dir.exists():
            for path in sorted(images_dir.rglob("*")):
                if path.is_file():
                    archive.add(path, arcname=path.relative_to(bundle_dir).as_posix())

    shutil.rmtree(bundle_dir, ignore_errors=True)
    LOGGER.info(
        "Store payload saved: json=%s archive=%s images=%s worker_log=%s",
        json_path,
        archive_path,
        images_written,
        worker_log_included,
    )
    return str(json_path), str(archive_path)
