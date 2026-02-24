from __future__ import annotations

import gzip
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LOGGER = logging.getLogger(__name__)


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
