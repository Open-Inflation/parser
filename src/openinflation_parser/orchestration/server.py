from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import multiprocessing as mp
import secrets
from typing import Any
from uuid import uuid4

from ..parsers import get_parser, get_parser_adapter
from .job_store import JobStore
from .models import JobDefaults, WorkerJob, coerce_bool
from .server_downloads import OrchestratorDownloadsMixin
from .server_requests import OrchestratorRequestsMixin
from .server_workers import OrchestratorWorkersMixin
from .utils import require_websockets_module, utc_now_iso

try:
    from opentelemetry import trace as otel_trace  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    otel_trace = None


LOGGER = logging.getLogger(__name__)


class OrchestratorServer(
    OrchestratorWorkersMixin,
    OrchestratorDownloadsMixin,
    OrchestratorRequestsMixin,
):
    def __init__(
        self,
        *,
        host: str,
        port: int,
        worker_count: int,
        proxies: list[str],
        defaults: JobDefaults,
        log_level: str = "INFO",
        max_jobs_per_worker: int = 1,
        jobs_max_history: int = 1000,
        jobs_retention_sec: int = 86400,
        jobs_db_path: str | None = None,
        auth_password: str | None = None,
        download_host: str | None = None,
        download_port: int | None = None,
        download_url_ttl_sec: int = 3600,
        download_secret: str | None = None,
        uptrace_dsn: str | None = None,
        uptrace_environment: str | None = None,
        uptrace_worker_service_name: str = "openinflation-worker",
    ):
        self.host = host
        self.port = port
        self.download_host = (download_host or host).strip() or host
        self.download_port = int(download_port if download_port is not None else (port + 1))
        self.download_url_ttl_sec = max(30, int(download_url_ttl_sec))
        self._download_secret = (download_secret or secrets.token_hex(32)).encode("utf-8")
        self._uptrace_dsn = uptrace_dsn
        self._uptrace_environment = uptrace_environment
        self._uptrace_worker_service_name = uptrace_worker_service_name
        if auth_password == "":
            raise ValueError("auth_password must be non-empty when provided.")
        self._auth_password = auth_password
        self.worker_count = max(1, worker_count)
        self.proxies = proxies
        self.defaults = defaults
        self.log_level = log_level
        self.max_jobs_per_worker = max(1, int(max_jobs_per_worker))

        self._ctx = mp.get_context("spawn")
        self._result_queue = self._ctx.Queue()
        self._workers: list[mp.Process] = []
        self._worker_queues: dict[int, Any] = {}
        self._worker_busy: dict[int, bool] = {}
        self._worker_current_job: dict[int, str | None] = {}
        self._workers_pending_recycle: set[int] = set()
        self._pending_jobs: list[WorkerJob] = []
        self._active_proxy_parser_pairs: set[tuple[str, str]] = set()
        self._job_proxy_pair: dict[str, tuple[str, str]] = {}
        self._job_store = JobStore(
            max_history=jobs_max_history,
            retention_seconds=jobs_retention_sec,
            sqlite_path=jobs_db_path,
        )
        self._stop_event = asyncio.Event()
        self._collector_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._download_server: Any = None
        self._download_task: asyncio.Task[None] | None = None
        self._is_stopped = False

    def _worker_proxy(self, index: int) -> str | None:
        if not self.proxies:
            return None
        return self.proxies[index % len(self.proxies)]

    @staticmethod
    def _span_context(name: str, *, attributes: dict[str, Any] | None = None) -> Any:
        if otel_trace is None:
            return contextlib.nullcontext()
        tracer = otel_trace.get_tracer(__name__)
        return tracer.start_as_current_span(name, attributes=attributes or {})

    async def _log_heartbeat(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(15.0)
            with self._span_context(
                "orchestrator.heartbeat",
                attributes={
                    "app.entity_type": "orchestrator",
                    "app.workers_total": len(self._workers),
                },
            ):
                reconciled_jobs = self._reconcile_orphaned_running_jobs()
                normalized_slots, restarted_workers = self._reconcile_worker_slots()
                if reconciled_jobs > 0 or normalized_slots > 0 or restarted_workers > 0:
                    await self._try_dispatch_jobs()
                cleaned = self._cleanup_expired_download_artifacts()
                pruned = self._job_store.prune()
                summary = self._job_store.summary()
                LOGGER.debug(
                    "Heartbeat: workers=%s jobs_total=%s jobs_by_status=%s reconciled_jobs=%s normalized_slots=%s restarted_workers=%s cleaned=%s pruned=%s",
                    len(self._workers),
                    summary["jobs_total"],
                    summary["jobs_by_status"],
                    reconciled_jobs,
                    normalized_slots,
                    restarted_workers,
                    cleaned,
                    pruned,
                )

    async def _enqueue_job(self, request: dict[str, Any]) -> dict[str, Any]:
        store_code = str(request.get("store_code", "")).strip()
        if not store_code:
            raise ValueError("Field 'store_code' is required for action 'submit_store'.")

        parser_name = str(request.get("parser", self.defaults.parser_name)).lower().strip()
        get_parser(parser_name)
        get_parser_adapter(parser_name)

        job = WorkerJob(
            job_id=uuid4().hex,
            parser_name=parser_name,
            store_code=store_code,
            output_dir=str(request.get("output_dir", self.defaults.output_dir)),
            country_id=int(request.get("country_id", self.defaults.country_id)),
            api_timeout_ms=float(request.get("api_timeout_ms", self.defaults.api_timeout_ms)),
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
            full_catalog=coerce_bool(request.get("full_catalog", self.defaults.full_catalog)),
            include_images=coerce_bool(
                request.get("include_images", self.defaults.include_images)
            ),
            use_product_info=coerce_bool(
                request.get("use_product_info", self.defaults.use_product_info)
            ),
            strict_validation=coerce_bool(
                request.get("strict_validation", self.defaults.strict_validation)
            ),
        )

        state = {
            "job_id": job.job_id,
            "status": "queued",
            "created_at": utc_now_iso(),
            "store_code": job.store_code,
            "parser": job.parser_name,
            "country_id": job.country_id,
            "api_timeout_ms": job.api_timeout_ms,
            "category_limit": job.category_limit,
            "pages_per_category": job.pages_per_category,
            "max_pages_per_category": job.max_pages_per_category,
            "products_per_page": job.products_per_page,
            "full_catalog": job.full_catalog,
            "include_images": job.include_images,
            "use_product_info": job.use_product_info,
            "strict_validation": job.strict_validation,
            "output_dir": job.output_dir,
        }
        self._job_store.upsert(state)
        self._job_store.prune()
        self._pending_jobs.append(job)
        dispatched = await self._try_dispatch_jobs()
        LOGGER.info(
            "Job enqueued: id=%s store=%s parser=%s full_catalog=%s timeout_ms=%s category_limit=%s pages=%s max_pages=%s per_page=%s include_images=%s use_product_info=%s strict_validation=%s pending=%s dispatched_now=%s",
            job.job_id,
            job.store_code,
            job.parser_name,
            job.full_catalog,
            job.api_timeout_ms,
            job.category_limit,
            job.pages_per_category,
            job.max_pages_per_category,
            job.products_per_page,
            job.include_images,
            job.use_product_info,
            job.strict_validation,
            len(self._pending_jobs),
            dispatched,
        )
        return {"job_id": job.job_id, "status": "queued"}

    async def run(self, *, bootstrap_store_code: str | None = None) -> None:
        websockets = require_websockets_module()
        try:
            await self._start_download_server()
            self.start_workers()
            self._collector_task = asyncio.create_task(self._collect_results())
            self._heartbeat_task = asyncio.create_task(self._log_heartbeat())
            startup_reconciled = self._reconcile_orphaned_running_jobs()
            if startup_reconciled > 0:
                LOGGER.warning(
                    "Startup reconcile marked orphaned jobs as error: count=%s",
                    startup_reconciled,
                )
                await self._try_dispatch_jobs()

            if bootstrap_store_code:
                await self._enqueue_job(
                    {
                        "store_code": bootstrap_store_code,
                        "parser": self.defaults.parser_name,
                    }
                )
                LOGGER.info("Bootstrap job submitted for store_code=%s", bootstrap_store_code)
            else:
                LOGGER.info(
                    "No bootstrap store configured. Waiting for WebSocket action 'submit_store'."
                )
                example_payload: dict[str, Any] = {
                    "action": "submit_store",
                    "store_code": "C001",
                }
                if self._auth_password is not None:
                    example_payload["password"] = "<your-password>"
                LOGGER.info("Example: %s", json.dumps(example_payload, ensure_ascii=False))

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

        if self._download_server is not None:
            LOGGER.info("Stopping download API")
            self._download_server.should_exit = True
        if self._download_task is not None:
            try:
                await self._download_task
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("Download API task exited with error")
            finally:
                self._download_task = None
                self._download_server = None

        LOGGER.info("Stopping orchestrator workers")
        for worker_id, queue in self._worker_queues.items():
            await asyncio.to_thread(queue.put, None)
            LOGGER.debug("Stop signal sent to worker queue: worker=%s", worker_id)

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

        for queue in self._worker_queues.values():
            queue.close()
        self._result_queue.close()
