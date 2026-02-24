from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import multiprocessing as mp
from dataclasses import asdict
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from ..parsers import get_parser, get_parser_adapter
from .job_store import JobStore
from .models import JobDefaults, WorkerJob, coerce_bool, normalize_city_id
from .requests import (
    HelpRequest,
    JobsRequest,
    ParsedRequest,
    PingRequest,
    ShutdownRequest,
    StatusRequest,
    SubmitStoreRequest,
    UnknownRequest,
    WorkersRequest,
    parse_request,
)
from .utils import require_websockets_module, utc_now_iso
from .worker import worker_process_loop


LOGGER = logging.getLogger(__name__)


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
        jobs_max_history: int = 1000,
        jobs_retention_sec: int = 86400,
        jobs_db_path: str | None = None,
    ):
        self.host = host
        self.port = port
        self.worker_count = max(1, worker_count)
        self.proxies = proxies
        self.defaults = defaults
        self.log_level = log_level

        self._ctx = mp.get_context("spawn")
        self._result_queue = self._ctx.Queue()
        self._workers: list[mp.Process] = []
        self._worker_queues: dict[int, Any] = {}
        self._worker_busy: dict[int, bool] = {}
        self._worker_current_job: dict[int, str | None] = {}
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
        self._is_stopped = False

    def _worker_proxy(self, index: int) -> str | None:
        if not self.proxies:
            return None
        return self.proxies[index % len(self.proxies)]

    def start_workers(self) -> None:
        LOGGER.info("Starting %s workers", self.worker_count)
        for index in range(self.worker_count):
            worker_id = index + 1
            worker_queue = self._ctx.Queue()
            process = self._ctx.Process(
                target=worker_process_loop,
                args=(
                    worker_id,
                    self._worker_proxy(index),
                    self.log_level,
                    worker_queue,
                    self._result_queue,
                ),
                daemon=False,
                name=f"orchestrator-worker-{worker_id}",
            )
            process.start()
            LOGGER.info(
                "Worker started: index=%s pid=%s proxy=%s",
                worker_id,
                process.pid,
                self._worker_proxy(index) or "none",
            )
            self._workers.append(process)
            self._worker_queues[worker_id] = worker_queue
            self._worker_busy[worker_id] = False
            self._worker_current_job[worker_id] = None

    @staticmethod
    def _worker_id_from_value(value: Any) -> int | None:
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

    def _pair_for_job_and_worker(
        self,
        *,
        job: WorkerJob,
        worker_id: int,
    ) -> tuple[str, str] | None:
        proxy = self._worker_proxy(worker_id - 1)
        if proxy is None:
            return None
        return (job.parser_name, proxy)

    def _reserve_worker_slot_for_job(self, *, worker_id: int, job: WorkerJob) -> None:
        self._worker_busy[worker_id] = True
        self._worker_current_job[worker_id] = job.job_id
        pair = self._pair_for_job_and_worker(job=job, worker_id=worker_id)
        if pair is not None:
            self._active_proxy_parser_pairs.add(pair)
            self._job_proxy_pair[job.job_id] = pair

    def _release_worker_slot_for_job(self, *, job_id: str, worker_id: int | None) -> None:
        pair = self._job_proxy_pair.pop(job_id, None)
        if pair is not None:
            self._active_proxy_parser_pairs.discard(pair)
        if worker_id is not None:
            self._worker_busy[worker_id] = False
            if self._worker_current_job.get(worker_id) == job_id:
                self._worker_current_job[worker_id] = None
            return

        for candidate_worker_id, current_job_id in self._worker_current_job.items():
            if current_job_id == job_id:
                self._worker_busy[candidate_worker_id] = False
                self._worker_current_job[candidate_worker_id] = None
                break

    def _can_dispatch_job_to_worker(self, *, job: WorkerJob, worker_id: int) -> bool:
        if self._worker_busy.get(worker_id, False):
            return False
        process = self._workers[worker_id - 1]
        if not process.is_alive():
            return False
        pair = self._pair_for_job_and_worker(job=job, worker_id=worker_id)
        if pair is None:
            return True
        return pair not in self._active_proxy_parser_pairs

    async def _try_dispatch_jobs(self) -> int:
        if not self._pending_jobs:
            return 0

        dispatched = 0
        for worker_id in range(1, len(self._workers) + 1):
            if not self._pending_jobs:
                break
            if self._worker_busy.get(worker_id, False):
                continue

            selected_index: int | None = None
            for index, job in enumerate(self._pending_jobs):
                if self._can_dispatch_job_to_worker(job=job, worker_id=worker_id):
                    selected_index = index
                    break
            if selected_index is None:
                continue

            job = self._pending_jobs.pop(selected_index)
            self._reserve_worker_slot_for_job(worker_id=worker_id, job=job)
            queue = self._worker_queues[worker_id]
            await asyncio.to_thread(queue.put, asdict(job))

            job_state = self._job_store.get(job.job_id)
            if job_state is not None:
                job_state["worker_id"] = worker_id
                self._job_store.upsert(job_state)
            dispatched += 1
            LOGGER.info(
                "Job dispatched: id=%s worker=%s parser=%s proxy=%s pending=%s",
                job.job_id,
                worker_id,
                job.parser_name,
                self._worker_proxy(worker_id - 1) or "none",
                len(self._pending_jobs),
            )
        return dispatched

    def _reconcile_orphaned_running_jobs(self) -> int:
        worker_alive = {idx + 1: process.is_alive() for idx, process in enumerate(self._workers)}
        reconciled = 0
        for job_state in self._job_store.values():
            status = str(job_state.get("status", ""))
            if status not in {"running", "queued"}:
                continue

            worker_id = self._worker_id_from_value(job_state.get("worker_id"))
            if worker_id is None or worker_alive.get(worker_id, False):
                continue

            job_id = str(job_state.get("job_id", "unknown"))
            worker_label = str(worker_id) if worker_id is not None else "unknown"
            job_state["status"] = "error"
            job_state["finished_at"] = utc_now_iso()
            job_state["message"] = (
                "Worker process stopped before reporting job completion "
                f"(worker_id={worker_label})."
            )
            self._job_store.upsert(job_state)
            self._release_worker_slot_for_job(job_id=job_id, worker_id=worker_id)
            reconciled += 1
            LOGGER.warning(
                "Job %s marked as error due to missing worker heartbeat: worker_id=%s",
                job_id,
                worker_label,
            )
        return reconciled

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
            job_state = self._job_store.get(job_id)
            if job_state is None:
                self._release_worker_slot_for_job(
                    job_id=job_id,
                    worker_id=self._worker_id_from_value(event.get("worker_id")),
                )
                await self._try_dispatch_jobs()
                LOGGER.debug("Result collector skipped unknown job_id=%s", job_id)
                continue

            event_name = event.get("event")

            if event_name == "started":
                job_state["status"] = "running"
                job_state["started_at"] = event.get("timestamp")
                event_worker_id = self._worker_id_from_value(event.get("worker_id"))
                if event_worker_id is not None:
                    job_state["worker_id"] = event_worker_id
                self._job_store.upsert(job_state)
                LOGGER.info(
                    "Job %s started on worker %s",
                    job_id,
                    event.get("worker_id"),
                )
                continue

            if event_name == "finished":
                finished_worker_id = self._worker_id_from_value(event.get("worker_id"))
                job_state["status"] = event.get("status", "error")
                job_state["finished_at"] = event.get("timestamp")
                if finished_worker_id is not None:
                    job_state["worker_id"] = finished_worker_id
                if "message" in event:
                    job_state["message"] = event["message"]
                if "traceback" in event:
                    job_state["traceback"] = event["traceback"]
                if "output_json" in event:
                    job_state["output_json"] = event["output_json"]
                if "output_gz" in event:
                    job_state["output_gz"] = event["output_gz"]
                self._job_store.upsert(job_state)
                LOGGER.info(
                    "Job %s finished: status=%s worker=%s",
                    job_id,
                    job_state["status"],
                    event.get("worker_id"),
                )
                self._release_worker_slot_for_job(job_id=job_id, worker_id=finished_worker_id)
                await self._try_dispatch_jobs()
                continue

            LOGGER.warning("Unknown result event for job %s: %s", job_id, event_name)

    async def _log_heartbeat(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(15.0)
            reconciled = self._reconcile_orphaned_running_jobs()
            if reconciled > 0:
                await self._try_dispatch_jobs()
            pruned = self._job_store.prune()
            summary = self._job_store.summary()
            LOGGER.debug(
                "Heartbeat: workers=%s jobs_total=%s jobs_by_status=%s reconciled=%s pruned=%s",
                len(self._workers),
                summary["jobs_total"],
                summary["jobs_by_status"],
                reconciled,
                pruned,
            )

    async def _enqueue_job(self, request: dict[str, Any]) -> dict[str, Any]:
        store_code = str(request.get("store_code", "")).strip()
        if not store_code:
            raise ValueError("Field 'store_code' is required for action 'submit_store'.")

        parser_name = str(request.get("parser", self.defaults.parser_name)).lower().strip()
        get_parser(parser_name)
        get_parser_adapter(parser_name)

        city_id = normalize_city_id(request.get("city_id", self.defaults.city_id))

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
            full_catalog=coerce_bool(request.get("full_catalog", self.defaults.full_catalog)),
            include_images=coerce_bool(
                request.get("include_images", self.defaults.include_images)
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
            "strict_validation": job.strict_validation,
            "output_dir": job.output_dir,
        }
        self._job_store.upsert(state)
        self._job_store.prune()
        self._pending_jobs.append(job)
        dispatched = await self._try_dispatch_jobs()
        LOGGER.info(
            "Job enqueued: id=%s store=%s parser=%s city_id=%s full_catalog=%s timeout_ms=%s retries=%s category_limit=%s pages=%s max_pages=%s per_page=%s include_images=%s strict_validation=%s pending=%s dispatched_now=%s",
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
            job.strict_validation,
            len(self._pending_jobs),
            dispatched,
        )
        return {"job_id": job.job_id, "status": "queued"}

    def _global_status(self) -> dict[str, Any]:
        summary = self._job_store.summary()
        return {
            "workers_total": len(self._workers),
            "jobs_total": summary["jobs_total"],
            "jobs_by_status": summary["jobs_by_status"],
            "jobs_pending_dispatch": len(self._pending_jobs),
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
                    "busy": self._worker_busy.get(idx + 1, False),
                    "job_id": self._worker_current_job.get(idx + 1),
                }
            )
        return rows

    async def _dispatch(self, request: ParsedRequest) -> dict[str, Any]:
        try:
            if isinstance(request, PingRequest):
                return {"ok": True, "action": "pong", "timestamp": utc_now_iso()}

            if isinstance(request, SubmitStoreRequest):
                payload = await self._enqueue_job(request.model_dump(exclude_none=True))
                return {"ok": True, "action": request.action} | payload

            if isinstance(request, StatusRequest):
                if request.job_id:
                    job = self._job_store.get(str(request.job_id))
                    if not job:
                        return {"ok": False, "action": request.action, "error": "Job not found."}
                    return {"ok": True, "action": request.action, "job": job}
                return {"ok": True, "action": request.action, "summary": self._global_status()}

            if isinstance(request, JobsRequest):
                return {
                    "ok": True,
                    "action": request.action,
                    "jobs": self._job_store.sorted_jobs(),
                }

            if isinstance(request, WorkersRequest):
                return {"ok": True, "action": request.action, "workers": self._workers_status()}

            if isinstance(request, ShutdownRequest):
                self._stop_event.set()
                return {
                    "ok": True,
                    "action": request.action,
                    "message": "Shutdown scheduled.",
                }

            if isinstance(request, HelpRequest):
                return {
                    "ok": True,
                    "action": request.action,
                    "actions": [
                        "ping",
                        "submit_store",
                        "status",
                        "jobs",
                        "workers",
                        "shutdown",
                    ],
                }

            if isinstance(request, UnknownRequest):
                return {"ok": False, "action": request.action, "error": "Unknown action."}

            return {"ok": False, "error": "Unsupported request model."}
        except Exception as exc:
            LOGGER.exception("Dispatch failed: request=%s error=%s", request, exc)
            return {"ok": False, "action": getattr(request, "action", None), "error": str(exc)}

    async def _handle_client(self, websocket: Any) -> None:
        LOGGER.info("WebSocket client connected: %s", getattr(websocket, "remote_address", None))
        async for message in websocket:
            try:
                payload = json.loads(message)
                if not isinstance(payload, dict):
                    raise ValueError("Request must be a JSON object.")
                request = parse_request(payload)
            except (json.JSONDecodeError, ValidationError, ValueError) as exc:
                LOGGER.warning("Invalid client message: %s", exc)
                response = {"ok": False, "error": f"Invalid JSON payload: {exc}"}
                await websocket.send(json.dumps(response, ensure_ascii=False))
                continue

            response = await self._dispatch(request)
            await websocket.send(json.dumps(response, ensure_ascii=False))
        LOGGER.info("WebSocket client disconnected: %s", getattr(websocket, "remote_address", None))

    async def run(self, *, bootstrap_store_code: str | None = None) -> None:
        websockets = require_websockets_module()
        try:
            self.start_workers()
            self._collector_task = asyncio.create_task(self._collect_results())
            self._heartbeat_task = asyncio.create_task(self._log_heartbeat())

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
                LOGGER.info(
                    "Example: {\"action\":\"submit_store\",\"store_code\":\"C001\",\"city_id\":%s}",
                    self.defaults.city_id,
                )

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
