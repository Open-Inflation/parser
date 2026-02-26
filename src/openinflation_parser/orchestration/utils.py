from __future__ import annotations

import atexit
import base64
import binascii
import gzip
import logging
import os
import re
import shutil
import tarfile
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from openinflation_dataclass import to_json

from .. import __version__
from ..parsers.runtime import INLINE_IMAGE_TOKEN_PREFIX

try:
    from opentelemetry import trace as otel_trace  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    otel_trace = None


LOGGER = logging.getLogger(__name__)
_INLINE_IMAGE_EXT_RE = re.compile(r"^[a-z0-9]{2,8}$")
LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | svc=%(service_name)s | role=%(entity_type)s "
    "| pid=%(process)d(%(processName)s) | worker=%(worker_id)s | job=%(job_id)s "
    "| store=%(store_code)s | parser=%(parser_name)s | trace=%(otel_trace_id)s "
    "span=%(otel_span_id)s | %(name)s | %(message)s"
)
DEFAULT_ORCHESTRATOR_SERVICE_NAME = "openinflation-orchestrator"
DEFAULT_WORKER_SERVICE_NAME = "openinflation-worker"
_LOG_CONTEXT: ContextVar[dict[str, str]] = ContextVar("openinflation_log_context", default={})
_UPTRACE_IS_CONFIGURED = False


class _ObservabilityContextFilter(logging.Filter):
    def __init__(self, *, service_name: str, entity_type: str, worker_id: int | None):
        super().__init__()
        self._service_name = service_name
        self._entity_type = entity_type
        self._worker_id = "-" if worker_id is None else str(worker_id)

    @staticmethod
    def _trace_attrs() -> tuple[str, str]:
        if otel_trace is None:
            return "-", "-"
        span = otel_trace.get_current_span()
        if span is None:
            return "-", "-"
        span_context = span.get_span_context()
        if span_context is None or not span_context.is_valid:
            return "-", "-"
        return f"{span_context.trace_id:032x}", f"{span_context.span_id:016x}"

    def filter(self, record: logging.LogRecord) -> bool:
        context = _LOG_CONTEXT.get()
        record.service_name = context.get("service_name", self._service_name)
        record.entity_type = context.get("entity_type", self._entity_type)
        record.worker_id = context.get("worker_id", self._worker_id)
        record.job_id = context.get("job_id", "-")
        record.store_code = context.get("store_code", "-")
        record.parser_name = context.get("parser_name", "-")
        trace_id, span_id = self._trace_attrs()
        record.otel_trace_id = trace_id
        record.otel_span_id = span_id
        return True


def set_log_context(**values: Any) -> Token[dict[str, str]]:
    context = dict(_LOG_CONTEXT.get())
    for key, value in values.items():
        if value is None:
            context.pop(key, None)
        else:
            context[key] = str(value)
    return _LOG_CONTEXT.set(context)


def reset_log_context(token: Token[dict[str, str]]) -> None:
    _LOG_CONTEXT.reset(token)


def _mask_uptrace_dsn(dsn: str) -> str:
    if "@" not in dsn:
        return "***"
    prefix, suffix = dsn.split("@", 1)
    visible_prefix = prefix[: min(6, len(prefix))]
    return f"{visible_prefix}***@{suffix}"


def setup_logging(
    level: str,
    *,
    service_name: str = DEFAULT_ORCHESTRATOR_SERVICE_NAME,
    entity_type: str = "orchestrator",
    worker_id: int | None = None,
) -> int:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format=LOG_FORMAT,
        force=True,
    )
    root_logger = logging.getLogger()
    for existing_filter in list(root_logger.filters):
        if isinstance(existing_filter, _ObservabilityContextFilter):
            root_logger.removeFilter(existing_filter)
    root_logger.addFilter(
        _ObservabilityContextFilter(
            service_name=service_name,
            entity_type=entity_type,
            worker_id=worker_id,
        )
    )
    # Keep third-party retry internals from flooding DEBUG logs for every request.
    third_party_level = max(logging.INFO, numeric_level)
    logging.getLogger("aiohttp_retry").setLevel(third_party_level)
    return int(numeric_level)


def setup_uptrace(
    *,
    service_name: str,
    entity_type: str,
    dsn: str | None = None,
    deployment_environment: str | None = None,
    worker_id: int | None = None,
) -> bool:
    global _UPTRACE_IS_CONFIGURED
    if _UPTRACE_IS_CONFIGURED:
        return True

    resolved_dsn = (dsn or os.environ.get("UPTRACE_DSN") or "").strip()
    if not resolved_dsn:
        LOGGER.debug("Uptrace DSN is not configured, telemetry disabled for service=%s", service_name)
        return False

    try:
        import uptrace
    except ModuleNotFoundError:
        LOGGER.warning(
            "UPTRACE_DSN is set, but package 'uptrace' is missing. "
            "Install dependencies to enable telemetry for service=%s",
            service_name,
        )
        return False

    resolved_environment = (
        deployment_environment
        or os.environ.get("UPTRACE_DEPLOYMENT_ENV")
        or os.environ.get("ENVIRONMENT")
        or "dev"
    )
    resource_attributes: dict[str, Any] = {"app.entity_type": entity_type}
    if worker_id is not None:
        resource_attributes["app.worker_id"] = str(worker_id)

    base_configure_kwargs: dict[str, Any] = {
        "dsn": resolved_dsn,
        "service_name": service_name,
        "service_version": __version__,
        "deployment_environment": resolved_environment,
        "resource_attributes": resource_attributes,
    }
    configure_attempts = [
        dict(base_configure_kwargs),
        {
            key: value
            for key, value in base_configure_kwargs.items()
            if key != "resource_attributes"
        },
        {
            key: value
            for key, value in base_configure_kwargs.items()
            if key not in {"resource_attributes", "deployment_environment"}
        },
        {"dsn": resolved_dsn, "service_name": service_name},
    ]
    last_type_error: TypeError | None = None
    for configure_kwargs in configure_attempts:
        try:
            uptrace.configure_opentelemetry(**configure_kwargs)
            _UPTRACE_IS_CONFIGURED = True
            break
        except TypeError as exc:
            last_type_error = exc
        except Exception:
            LOGGER.exception("Uptrace setup failed for service=%s", service_name)
            return False
    if not _UPTRACE_IS_CONFIGURED:
        if last_type_error is not None:
            LOGGER.warning(
                "Uptrace setup failed for service=%s because of unsupported API signature: %s",
                service_name,
                last_type_error,
            )
        return False

    if hasattr(uptrace, "shutdown"):
        atexit.register(uptrace.shutdown)
    LOGGER.info(
        "Uptrace enabled: service=%s entity=%s env=%s dsn=%s",
        service_name,
        entity_type,
        resolved_environment,
        _mask_uptrace_dsn(resolved_dsn),
    )
    return True


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
    prepared_images_dir: str | None = None,
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
    prepared_images_root: Path | None = None
    if prepared_images_dir is not None:
        candidate = Path(prepared_images_dir).expanduser().resolve()
        if candidate.is_dir():
            prepared_images_root = candidate
        else:
            LOGGER.warning(
                "Prepared images dir is missing or invalid, fallback to inline token materialization: %s",
                candidate,
            )

    if prepared_images_root is not None:
        images_dir = bundle_dir / "images"
        shutil.copytree(prepared_images_root, images_dir, dirs_exist_ok=True)
        images_written = sum(1 for path in images_dir.rglob("*") if path.is_file())
    else:
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
