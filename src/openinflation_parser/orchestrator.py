from __future__ import annotations

import argparse
import asyncio
import contextlib
import gzip
import json
import logging
import multiprocessing as mp
import os
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .parsers import (
    ChizhikParser,
    ChizhikParserConfig,
    FixPriceParser,
    FixPriceParserConfig,
    get_parser,
)

from openinflation_dataclass import to_json


LOGGER = logging.getLogger(__name__)


def _setup_logging(level: str) -> int:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(processName)s | %(name)s | %(message)s",
    )
    return int(numeric_level)


def _require_websockets_module() -> Any:
    try:
        import websockets
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Package 'websockets' is required to run orchestrator server. "
            "Install project dependencies first."
        ) from exc
    return websockets


def _utc_now_iso() -> str:
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
) -> int:
    """Select worker count based on RAM and proxy availability."""
    safe_ram_per_worker = ram_per_worker_gb if ram_per_worker_gb > 0 else 1.0
    by_ram = int(available_ram_gb // safe_ram_per_worker)
    if by_ram < 1:
        by_ram = 1

    by_proxy = proxies_count if proxies_count > 0 else by_ram
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


def _safe_store_code(value: str) -> str:
    token = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value.strip())
    token = token.strip("_")
    return token or "store"


def _write_payload(payload: str, *, output_dir: str, store_code: str) -> tuple[str, str]:
    out_dir = Path(output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = f"{_safe_store_code(store_code)}_{timestamp}"

    json_path = out_dir / f"{base}.json"
    json_gz_path = out_dir / f"{base}.json.gz"

    json_path.write_text(payload, encoding="utf-8")
    with gzip.open(json_gz_path, "wt", encoding="utf-8") as stream:
        stream.write(payload)

    LOGGER.info("Store payload saved: json=%s gz=%s", json_path, json_gz_path)
    return str(json_path), str(json_gz_path)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", ""}:
            return False
    return bool(value)


def _normalize_city_id(value: Any) -> int | str | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return str(value)
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        try:
            return int(token)
        except ValueError:
            return token
    return str(value)


@dataclass(slots=True)
class WorkerJob:
    job_id: str
    parser_name: str
    store_code: str
    output_dir: str
    country_id: int = 2
    city_id: int | str | None = None
    api_timeout_ms: float = 90000.0
    request_retries: int = 3
    request_retry_backoff_sec: float = 1.5
    category_limit: int = 1
    pages_per_category: int = 1
    max_pages_per_category: int = 200
    products_per_page: int = 24
    full_catalog: bool = False
    include_images: bool = False

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "WorkerJob":
        city_id = _normalize_city_id(payload.get("city_id"))

        return cls(
            job_id=str(payload["job_id"]),
            parser_name=str(payload["parser_name"]),
            store_code=str(payload["store_code"]),
            output_dir=str(payload["output_dir"]),
            country_id=int(payload.get("country_id", 2)),
            city_id=city_id,
            api_timeout_ms=float(payload.get("api_timeout_ms", 90000.0)),
            request_retries=int(payload.get("request_retries", 3)),
            request_retry_backoff_sec=float(
                payload.get("request_retry_backoff_sec", 1.5)
            ),
            category_limit=int(payload.get("category_limit", 1)),
            pages_per_category=int(payload.get("pages_per_category", 1)),
            max_pages_per_category=int(payload.get("max_pages_per_category", 200)),
            products_per_page=int(payload.get("products_per_page", 24)),
            full_catalog=_coerce_bool(payload.get("full_catalog", False)),
            include_images=_coerce_bool(payload.get("include_images", False)),
        )

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class JobDefaults:
    parser_name: str
    output_dir: str
    country_id: int
    city_id: int | str | None
    api_timeout_ms: float
    request_retries: int
    request_retry_backoff_sec: float
    category_limit: int
    pages_per_category: int
    max_pages_per_category: int
    products_per_page: int
    full_catalog: bool
    include_images: bool


async def _execute_store_job(
    job: WorkerJob,
    proxy: str | None,
    *,
    worker_id: int,
) -> tuple[str, str]:
    LOGGER.info(
        "Worker %s started job %s for store=%s parser=%s city_id=%s full_catalog=%s timeout_ms=%s retries=%s",
        worker_id,
        job.job_id,
        job.store_code,
        job.parser_name,
        job.city_id,
        job.full_catalog,
        job.api_timeout_ms,
        job.request_retries,
    )

    if job.parser_name == "fixprice":
        fixprice_city_id: int | None
        if isinstance(job.city_id, int):
            fixprice_city_id = job.city_id
        elif isinstance(job.city_id, str):
            try:
                fixprice_city_id = int(job.city_id)
            except ValueError:
                fixprice_city_id = None
        else:
            fixprice_city_id = None

        config = FixPriceParserConfig(
            country_id=job.country_id,
            city_id=fixprice_city_id,
            proxy=proxy,
            timeout_ms=job.api_timeout_ms,
            request_retries=job.request_retries,
            request_retry_backoff_sec=job.request_retry_backoff_sec,
            include_images=job.include_images,
        )
        parser = FixPriceParser(config)
        store_city_id: int | str | None = fixprice_city_id
    elif job.parser_name == "chizhik":
        chizhik_city_id: str | None
        if job.city_id is None:
            chizhik_city_id = None
        else:
            chizhik_city_id = str(job.city_id)
        config = ChizhikParserConfig(
            country_id=job.country_id,
            city_id=chizhik_city_id,
            proxy=proxy,
            timeout_ms=job.api_timeout_ms,
            request_retries=job.request_retries,
            request_retry_backoff_sec=job.request_retry_backoff_sec,
            include_images=job.include_images,
        )
        parser = ChizhikParser(config)
        store_city_id = job.city_id
    else:
        parser_cls = get_parser(job.parser_name)
        raise ValueError(
            f"Parser {job.parser_name!r} is registered as {parser_cls.__name__}, "
            "but worker configuration cannot build parser config for it."
        )

    async with parser:
        categories = await parser.collect_categories()
        selected_categories = (
            categories
            if job.full_catalog
            else categories[: max(1, job.category_limit)]
        )
        product_queries = parser.build_catalog_queries(
            categories,
            full_catalog=job.full_catalog,
            category_limit=job.category_limit,
        )
        LOGGER.info(
            "Worker %s job %s selected categories: full_catalog=%s requested=%s available=%s selected=%s queries=%s",
            worker_id,
            job.job_id,
            job.full_catalog,
            job.category_limit,
            len(categories),
            len(selected_categories),
            len(product_queries),
        )

        if not product_queries:
            raise ValueError("No categories available to collect products.")

        page_limit = (
            max(1, job.max_pages_per_category)
            if job.full_catalog
            else max(1, job.pages_per_category)
        )

        products = await parser.collect_products_for_queries(
            product_queries,
            page_limit=page_limit,
            items_per_page=job.products_per_page,
        )
        LOGGER.info(
            "Worker %s job %s collected products=%s page_limit=%s",
            worker_id,
            job.job_id,
            len(products),
            page_limit,
        )

        stores = await parser.collect_store_info(
            country_id=job.country_id,
            city_id=store_city_id,
            store_code=job.store_code,
        )
        LOGGER.info(
            "Worker %s job %s store search result count=%s",
            worker_id,
            job.job_id,
            len(stores),
        )
        if not stores:
            raise ValueError(
                f"Store code {job.store_code!r} not found. "
                "Use a valid PFM value and provide city_id when possible."
            )

        store = stores[0].model_copy(update={"categories": selected_categories, "products": products})
        payload = to_json(store)
        json_path, json_gz_path = _write_payload(
            payload,
            output_dir=job.output_dir,
            store_code=job.store_code,
        )
        LOGGER.info(
            "Worker %s finished job %s successfully: json=%s gz=%s",
            worker_id,
            job.job_id,
            json_path,
            json_gz_path,
        )
        return json_path, json_gz_path


def worker_process_loop(
    worker_id: int,
    proxy: str | None,
    log_level: str,
    job_queue: Any,
    result_queue: Any,
) -> None:
    _setup_logging(log_level)
    LOGGER.info("Worker %s booted (proxy=%s)", worker_id, proxy or "none")

    while True:
        payload = job_queue.get()
        if payload is None:
            LOGGER.info("Worker %s received stop signal", worker_id)
            break

        try:
            job = WorkerJob.from_payload(payload)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("Worker %s received invalid job payload", worker_id)
            result_queue.put(
                {
                    "event": "finished",
                    "status": "error",
                    "worker_id": worker_id,
                    "job_id": str(payload.get("job_id", "unknown")),
                    "timestamp": _utc_now_iso(),
                    "message": f"Invalid job payload: {exc}",
                }
            )
            continue

        LOGGER.info(
            "Worker %s picked job %s (store=%s)",
            worker_id,
            job.job_id,
            job.store_code,
        )
        result_queue.put(
            {
                "event": "started",
                "status": "running",
                "worker_id": worker_id,
                "job_id": job.job_id,
                "timestamp": _utc_now_iso(),
            }
        )

        try:
            json_path, json_gz_path = asyncio.run(
                _execute_store_job(job, proxy=proxy, worker_id=worker_id)
            )
            result_queue.put(
                {
                    "event": "finished",
                    "status": "success",
                    "worker_id": worker_id,
                    "job_id": job.job_id,
                    "timestamp": _utc_now_iso(),
                    "output_json": json_path,
                    "output_gz": json_gz_path,
                }
            )
        except Exception as exc:
            LOGGER.exception("Worker %s failed job %s: %s", worker_id, job.job_id, exc)
            result_queue.put(
                {
                    "event": "finished",
                    "status": "error",
                    "worker_id": worker_id,
                    "job_id": job.job_id,
                    "timestamp": _utc_now_iso(),
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )


class OrchestratorServer:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        worker_count: int,
        proxies: list[str],
        defaults: JobDefaults,
        log_level: str = "INFO",
    ):
        self.host = host
        self.port = port
        self.worker_count = max(1, worker_count)
        self.proxies = proxies
        self.defaults = defaults
        self.log_level = log_level

        self._ctx = mp.get_context("spawn")
        self._job_queue = self._ctx.Queue()
        self._result_queue = self._ctx.Queue()
        self._workers: list[mp.Process] = []
        self._jobs: dict[str, dict[str, Any]] = {}
        self._stop_event = asyncio.Event()
        self._collector_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._is_stopped = False

    def _worker_proxy(self, index: int) -> str | None:
        if not self.proxies:
            return None
        return self.proxies[index % len(self.proxies)]

    def start_workers(self) -> None:
        LOGGER.info("Starting %s workers", self.worker_count)
        for index in range(self.worker_count):
            process = self._ctx.Process(
                target=worker_process_loop,
                args=(
                    index + 1,
                    self._worker_proxy(index),
                    self.log_level,
                    self._job_queue,
                    self._result_queue,
                ),
                daemon=False,
                name=f"orchestrator-worker-{index + 1}",
            )
            process.start()
            LOGGER.info(
                "Worker started: index=%s pid=%s proxy=%s",
                index + 1,
                process.pid,
                self._worker_proxy(index) or "none",
            )
            self._workers.append(process)

    async def _collect_results(self) -> None:
        while True:
            event = await asyncio.to_thread(self._result_queue.get)
            if event is None:
                LOGGER.info("Result collector received stop signal")
                break
            if not isinstance(event, dict):
                LOGGER.warning("Result collector got non-dict event: %r", event)
                continue
            job_id = str(event.get("job_id", ""))
            if not job_id:
                LOGGER.debug("Result collector skipped event without job_id: %s", event)
                continue
            if job_id not in self._jobs:
                LOGGER.debug("Result collector skipped unknown job_id=%s", job_id)
                continue

            job_state = self._jobs[job_id]
            event_name = event.get("event")

            if event_name == "started":
                job_state["status"] = "running"
                job_state["started_at"] = event.get("timestamp")
                job_state["worker_id"] = event.get("worker_id")
                LOGGER.info(
                    "Job %s started on worker %s",
                    job_id,
                    event.get("worker_id"),
                )
                continue

            if event_name == "finished":
                job_state["status"] = event.get("status", "error")
                job_state["finished_at"] = event.get("timestamp")
                job_state["worker_id"] = event.get("worker_id")
                if "message" in event:
                    job_state["message"] = event["message"]
                if "traceback" in event:
                    job_state["traceback"] = event["traceback"]
                if "output_json" in event:
                    job_state["output_json"] = event["output_json"]
                if "output_gz" in event:
                    job_state["output_gz"] = event["output_gz"]
                LOGGER.info(
                    "Job %s finished: status=%s worker=%s",
                    job_id,
                    job_state["status"],
                    event.get("worker_id"),
                )
                continue

            LOGGER.warning("Unknown result event for job %s: %s", job_id, event_name)

    async def _log_heartbeat(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(15.0)
            summary = self._global_status()
            LOGGER.debug(
                "Heartbeat: workers=%s jobs_total=%s jobs_by_status=%s",
                summary["workers_total"],
                summary["jobs_total"],
                summary["jobs_by_status"],
            )

    async def _enqueue_job(self, request: dict[str, Any]) -> dict[str, Any]:
        store_code = str(request.get("store_code", "")).strip()
        if not store_code:
            raise ValueError("Field 'store_code' is required for action 'submit_store'.")

        parser_name = str(request.get("parser", self.defaults.parser_name)).lower().strip()
        get_parser(parser_name)

        city_id = _normalize_city_id(request.get("city_id", self.defaults.city_id))

        job = WorkerJob(
            job_id=uuid4().hex,
            parser_name=parser_name,
            store_code=store_code,
            output_dir=str(request.get("output_dir", self.defaults.output_dir)),
            country_id=int(request.get("country_id", self.defaults.country_id)),
            city_id=city_id,
            api_timeout_ms=float(request.get("api_timeout_ms", self.defaults.api_timeout_ms)),
            request_retries=max(
                0,
                int(request.get("request_retries", self.defaults.request_retries)),
            ),
            request_retry_backoff_sec=max(
                0.1,
                float(
                    request.get(
                        "request_retry_backoff_sec",
                        self.defaults.request_retry_backoff_sec,
                    )
                ),
            ),
            category_limit=max(1, int(request.get("category_limit", self.defaults.category_limit))),
            pages_per_category=max(
                1, int(request.get("pages_per_category", self.defaults.pages_per_category))
            ),
            max_pages_per_category=max(
                1,
                int(
                    request.get(
                        "max_pages_per_category",
                        self.defaults.max_pages_per_category,
                    )
                ),
            ),
            products_per_page=max(
                1, int(request.get("products_per_page", self.defaults.products_per_page))
            ),
            full_catalog=_coerce_bool(request.get("full_catalog", self.defaults.full_catalog)),
            include_images=_coerce_bool(
                request.get("include_images", self.defaults.include_images)
            ),
        )

        self._jobs[job.job_id] = {
            "job_id": job.job_id,
            "status": "queued",
            "created_at": _utc_now_iso(),
            "store_code": job.store_code,
            "parser": job.parser_name,
            "country_id": job.country_id,
            "city_id": job.city_id,
            "api_timeout_ms": job.api_timeout_ms,
            "request_retries": job.request_retries,
            "request_retry_backoff_sec": job.request_retry_backoff_sec,
            "category_limit": job.category_limit,
            "pages_per_category": job.pages_per_category,
            "max_pages_per_category": job.max_pages_per_category,
            "products_per_page": job.products_per_page,
            "full_catalog": job.full_catalog,
            "include_images": job.include_images,
            "output_dir": job.output_dir,
        }

        await asyncio.to_thread(self._job_queue.put, job.to_payload())
        LOGGER.info(
            "Job enqueued: id=%s store=%s parser=%s city_id=%s full_catalog=%s timeout_ms=%s retries=%s category_limit=%s pages=%s max_pages=%s per_page=%s include_images=%s",
            job.job_id,
            job.store_code,
            job.parser_name,
            job.city_id,
            job.full_catalog,
            job.api_timeout_ms,
            job.request_retries,
            job.category_limit,
            job.pages_per_category,
            job.max_pages_per_category,
            job.products_per_page,
            job.include_images,
        )
        return {"job_id": job.job_id, "status": "queued"}

    def _global_status(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for item in self._jobs.values():
            status = str(item.get("status", "unknown"))
            counts[status] = counts.get(status, 0) + 1
        return {
            "workers_total": len(self._workers),
            "jobs_total": len(self._jobs),
            "jobs_by_status": counts,
        }

    def _workers_status(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for idx, process in enumerate(self._workers):
            rows.append(
                {
                    "index": idx + 1,
                    "pid": process.pid,
                    "alive": process.is_alive(),
                    "proxy": self._worker_proxy(idx),
                }
            )
        return rows

    async def _dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        action = str(request.get("action", "")).strip().lower()
        if not action:
            return {"ok": False, "error": "Field 'action' is required."}

        LOGGER.debug("Dispatch action=%s request=%s", action, request)
        try:
            if action == "ping":
                return {"ok": True, "action": "pong", "timestamp": _utc_now_iso()}

            if action == "submit_store":
                payload = await self._enqueue_job(request)
                return {"ok": True, "action": action} | payload

            if action == "status":
                job_id = request.get("job_id")
                if job_id:
                    job = self._jobs.get(str(job_id))
                    if not job:
                        return {"ok": False, "action": action, "error": "Job not found."}
                    return {"ok": True, "action": action, "job": job}
                return {"ok": True, "action": action, "summary": self._global_status()}

            if action == "jobs":
                jobs = sorted(self._jobs.values(), key=lambda row: row["created_at"])
                return {"ok": True, "action": action, "jobs": jobs}

            if action == "workers":
                return {"ok": True, "action": action, "workers": self._workers_status()}

            if action == "shutdown":
                self._stop_event.set()
                return {"ok": True, "action": action, "message": "Shutdown scheduled."}

            if action == "help":
                return {
                    "ok": True,
                    "action": action,
                    "actions": [
                        "ping",
                        "submit_store",
                        "status",
                        "jobs",
                        "workers",
                        "shutdown",
                    ],
                }

            return {"ok": False, "action": action, "error": "Unknown action."}
        except Exception as exc:
            LOGGER.exception("Dispatch failed: action=%s error=%s", action, exc)
            return {"ok": False, "action": action, "error": str(exc)}

    async def _handle_client(self, websocket: Any) -> None:
        LOGGER.info("WebSocket client connected: %s", getattr(websocket, "remote_address", None))
        async for message in websocket:
            try:
                request = json.loads(message)
                if not isinstance(request, dict):
                    raise ValueError("Request must be a JSON object.")
            except Exception as exc:
                LOGGER.warning("Invalid client message: %s", exc)
                response = {"ok": False, "error": f"Invalid JSON payload: {exc}"}
                await websocket.send(json.dumps(response, ensure_ascii=False))
                continue

            response = await self._dispatch(request)
            await websocket.send(json.dumps(response, ensure_ascii=False))
        LOGGER.info("WebSocket client disconnected: %s", getattr(websocket, "remote_address", None))

    async def run(self, *, bootstrap_store_code: str | None = None) -> None:
        websockets = _require_websockets_module()
        self.start_workers()
        self._collector_task = asyncio.create_task(self._collect_results())
        self._heartbeat_task = asyncio.create_task(self._log_heartbeat())

        if bootstrap_store_code:
            await self._enqueue_job({"store_code": bootstrap_store_code})
            LOGGER.info("Bootstrap job submitted for store_code=%s", bootstrap_store_code)
        else:
            LOGGER.info(
                "No bootstrap store configured. Waiting for WebSocket action 'submit_store'."
            )
            LOGGER.info(
                "Example: {\"action\":\"submit_store\",\"store_code\":\"C001\",\"city_id\":%s}",
                self.defaults.city_id,
            )

        try:
            LOGGER.info("WebSocket server listening on ws://%s:%s", self.host, self.port)
            async with websockets.serve(self._handle_client, self.host, self.port):
                await self._stop_event.wait()
        finally:
            LOGGER.info("Server stop requested")
            await self.stop()

    async def stop(self) -> None:
        if self._is_stopped:
            return
        self._is_stopped = True

        LOGGER.info("Stopping orchestrator workers")
        for _ in self._workers:
            await asyncio.to_thread(self._job_queue.put, None)

        for process in self._workers:
            await asyncio.to_thread(process.join, 10.0)
            if process.is_alive():
                LOGGER.warning("Worker pid=%s did not exit gracefully, terminating", process.pid)
                process.terminate()
                await asyncio.to_thread(process.join, 2.0)
            LOGGER.info("Worker stopped: pid=%s alive=%s", process.pid, process.is_alive())

        await asyncio.to_thread(self._result_queue.put, None)
        if self._collector_task is not None:
            await self._collector_task
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task

        self._job_queue.close()
        self._result_queue.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OpenInflation orchestration server")
    parser.add_argument("--host", default="127.0.0.1", help="WebSocket host")
    parser.add_argument("--port", type=int, default=8765, help="WebSocket port")

    parser.add_argument(
        "--parser",
        default="fixprice",
        choices=["fixprice", "chizhik"],
        help="Parser name",
    )
    parser.add_argument("--output-dir", default="./output", help="Output directory for payload files")
    parser.add_argument("--country-id", type=int, default=2, help="Default country id")
    parser.add_argument(
        "--city-id",
        type=str,
        default=None,
        help="Default city id (int for fixprice, string/int for chizhik).",
    )
    parser.add_argument(
        "--api-timeout-ms",
        type=float,
        default=90000.0,
        help="Parser API request timeout in milliseconds.",
    )
    parser.add_argument(
        "--request-retries",
        type=int,
        default=3,
        help="Retry count for retryable API errors (timeouts).",
    )
    parser.add_argument(
        "--request-retry-backoff-sec",
        type=float,
        default=1.5,
        help="Base backoff (seconds) for retries (exponential).",
    )

    parser.add_argument(
        "--category-limit",
        type=int,
        default=1,
        help="Categories per store job (used when --full-catalog is off).",
    )
    parser.add_argument(
        "--pages-per-category",
        type=int,
        default=1,
        help="Pages per category (used when --full-catalog is off).",
    )
    parser.add_argument(
        "--max-pages-per-category",
        type=int,
        default=200,
        help="Safety page cap per category query for --full-catalog mode.",
    )
    parser.add_argument("--products-per-page", type=int, default=24, help="Items per page (1..27)")
    parser.add_argument(
        "--full-catalog",
        action="store_true",
        help="Traverse all categories/subcategories and paginate each query until empty page (capped by --max-pages-per-category).",
    )
    parser.add_argument("--include-images", action="store_true", help="Download product images")

    parser.add_argument(
        "--proxy",
        action="append",
        default=[],
        help="Proxy URL. Can be used multiple times.",
    )
    parser.add_argument(
        "--proxy-file",
        default=None,
        help="Path to file with proxies (one per line).",
    )

    parser.add_argument(
        "--ram-per-worker-gb",
        type=float,
        default=1.5,
        help="Expected RAM footprint per worker in GiB.",
    )
    parser.add_argument(
        "--available-ram-gb",
        type=float,
        default=None,
        help="Override available RAM detection (GiB).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Optional hard cap for worker count.",
    )
    parser.add_argument(
        "--bootstrap-store-code",
        default=None,
        help="Submit this store code immediately after startup.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Application log level.",
    )
    return parser


async def _run_orchestrator(args: argparse.Namespace) -> None:
    _setup_logging(args.log_level)
    proxies = load_proxy_list(args.proxy, args.proxy_file)
    available_ram_gb = args.available_ram_gb
    if available_ram_gb is None:
        available_ram_gb = detect_available_ram_gb()

    worker_count = choose_worker_count(
        available_ram_gb=available_ram_gb,
        ram_per_worker_gb=args.ram_per_worker_gb,
        proxies_count=len(proxies),
        max_workers=args.max_workers,
    )

    defaults = JobDefaults(
        parser_name=args.parser,
        output_dir=args.output_dir,
        country_id=args.country_id,
        city_id=args.city_id,
        api_timeout_ms=max(1000.0, args.api_timeout_ms),
        request_retries=max(0, args.request_retries),
        request_retry_backoff_sec=max(0.1, args.request_retry_backoff_sec),
        category_limit=max(1, args.category_limit),
        pages_per_category=max(1, args.pages_per_category),
        max_pages_per_category=max(1, args.max_pages_per_category),
        products_per_page=max(1, min(27, args.products_per_page)),
        full_catalog=args.full_catalog,
        include_images=args.include_images,
    )

    LOGGER.info(
        "Starting orchestrator config: %s",
        json.dumps(
            {
                "host": args.host,
                "port": args.port,
                "parser": args.parser,
                "workers": worker_count,
                "available_ram_gb": round(available_ram_gb, 2),
                "ram_per_worker_gb": args.ram_per_worker_gb,
                "proxies": len(proxies),
                "output_dir": str(Path(args.output_dir).expanduser().resolve()),
                "log_level": args.log_level,
                "full_catalog": args.full_catalog,
                "max_pages_per_category": max(1, args.max_pages_per_category),
                "api_timeout_ms": max(1000.0, args.api_timeout_ms),
                "request_retries": max(0, args.request_retries),
                "request_retry_backoff_sec": max(0.1, args.request_retry_backoff_sec),
            },
            ensure_ascii=False,
        ),
    )
    LOGGER.info("WebSocket endpoint: ws://%s:%s", args.host, args.port)

    server = OrchestratorServer(
        host=args.host,
        port=args.port,
        worker_count=worker_count,
        proxies=proxies,
        defaults=defaults,
        log_level=args.log_level,
    )
    await server.run(bootstrap_store_code=args.bootstrap_store_code)


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        asyncio.run(_run_orchestrator(args))
    except KeyboardInterrupt:
        LOGGER.info("Orchestrator interrupted by user.")


if __name__ == "__main__":
    main()
