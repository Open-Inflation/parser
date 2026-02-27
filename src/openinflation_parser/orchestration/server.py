from __future__ import annotations

import asyncio
from collections import deque
import contextlib
import hashlib
import hmac
import json
import logging
import multiprocessing as mp
import secrets
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
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
    StreamJobLogRequest,
    StatusRequest,
    SubmitStoreRequest,
    UnknownRequest,
    WorkersRequest,
    parse_request,
)
from .utils import DEFAULT_WORKER_SERVICE_NAME, require_websockets_module, utc_now_iso
from .worker import worker_process_loop

try:
    from opentelemetry import trace as otel_trace  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    otel_trace = None


LOGGER = logging.getLogger(__name__)

DEFAULT_LOG_TAIL_LINES = 200
MAX_LOG_TAIL_LINES = 5000
LOG_STREAM_POLL_INTERVAL_SEC = 0.4


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
        uptrace_worker_service_name: str = DEFAULT_WORKER_SERVICE_NAME,
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

    def _spawn_worker(self, worker_id: int, *, replace: bool) -> None:
        index = worker_id - 1
        previous_queue = self._worker_queues.get(worker_id)
        if replace and previous_queue is not None:
            with contextlib.suppress(Exception):
                previous_queue.close()

        worker_queue = self._ctx.Queue()
        process = self._ctx.Process(
            target=worker_process_loop,
            args=(
                worker_id,
                self._worker_proxy(index),
                self.log_level,
                worker_queue,
                self._result_queue,
                self.max_jobs_per_worker,
                self._uptrace_dsn,
                self._uptrace_worker_service_name,
                self._uptrace_environment,
            ),
            daemon=False,
            name=f"orchestrator-worker-{worker_id}",
        )
        process.start()

        if replace:
            self._workers[index] = process
        else:
            self._workers.append(process)
        self._worker_queues[worker_id] = worker_queue
        self._worker_busy.setdefault(worker_id, False)
        self._worker_current_job.setdefault(worker_id, None)

        LOGGER.info(
            "Worker started: index=%s pid=%s proxy=%s max_jobs_per_worker=%s telemetry_service=%s",
            worker_id,
            process.pid,
            self._worker_proxy(index) or "none",
            self.max_jobs_per_worker,
            self._uptrace_worker_service_name,
        )

    def _ensure_worker_alive(self, worker_id: int) -> bool:
        process = self._workers[worker_id - 1]
        if process.is_alive():
            return True

        if self._worker_busy.get(worker_id, False):
            return False
        if self._worker_current_job.get(worker_id) is not None:
            return False

        LOGGER.warning(
            "Worker is not alive and will be restarted: index=%s old_pid=%s",
            worker_id,
            process.pid,
        )
        self._spawn_worker(worker_id, replace=True)
        return self._workers[worker_id - 1].is_alive()

    def _download_public_host(self) -> str:
        if self.download_host in {"0.0.0.0", "::"}:
            return "127.0.0.1"
        return self.download_host

    @staticmethod
    def _sha256_file(path: str) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as file_stream:
            while True:
                chunk = file_stream.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _download_signature(
        self,
        *,
        job_id: str,
        expires_ts: int,
        checksum: str,
    ) -> str:
        payload = f"{job_id}:{expires_ts}:{checksum}".encode("utf-8")
        return hmac.new(self._download_secret, payload, hashlib.sha256).hexdigest()

    def _verify_download_signature(
        self,
        *,
        job_id: str,
        expires_ts: int,
        checksum: str,
        signature: str,
    ) -> bool:
        expected = self._download_signature(
            job_id=job_id,
            expires_ts=expires_ts,
            checksum=checksum,
        )
        return hmac.compare_digest(expected, signature)

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            return None
        if isinstance(value, str):
            token = value.strip()
            if not token:
                return None
            try:
                return int(token)
            except ValueError:
                return None
        return None

    @staticmethod
    def _iso_to_timestamp(value: Any) -> int | None:
        if not isinstance(value, str):
            return None
        token = value.strip()
        if not token:
            return None
        try:
            parsed = datetime.fromisoformat(token)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())

    def _resolve_download_expires_ts(self, job_state: dict[str, Any]) -> int | None:
        expires_ts = self._safe_int(job_state.get("download_expires_ts"))
        if expires_ts is not None:
            return expires_ts

        expires_ts = self._iso_to_timestamp(job_state.get("download_expires_at"))
        if expires_ts is not None:
            return expires_ts

        finished_ts = self._iso_to_timestamp(job_state.get("finished_at"))
        if finished_ts is None:
            return None
        return finished_ts + self.download_url_ttl_sec

    def _set_download_metadata(self, job_state: dict[str, Any]) -> None:
        if str(job_state.get("status")) != "success":
            return

        checksum = str(job_state.get("output_gz_sha256", "")).strip()
        if not checksum:
            return

        finished_ts = self._iso_to_timestamp(job_state.get("finished_at"))
        base_ts = finished_ts if finished_ts is not None else int(time.time())
        expires_ts = base_ts + self.download_url_ttl_sec
        job_state["download_expires_ts"] = expires_ts
        job_state["download_expires_at"] = datetime.fromtimestamp(
            expires_ts, tz=timezone.utc
        ).isoformat()

    def _build_download_url(
        self,
        *,
        job_id: str,
        checksum: str,
        expires_ts: int,
    ) -> str:
        signature = self._download_signature(
            job_id=job_id,
            expires_ts=expires_ts,
            checksum=checksum,
        )
        query = urlencode(
            {
                "job_id": job_id,
                "expires": str(expires_ts),
                "sha256": checksum,
                "sig": signature,
            }
        )
        return f"http://{self._download_public_host()}:{self.download_port}/download?{query}"

    def _job_download_data(self, job_state: dict[str, Any]) -> dict[str, Any] | None:
        if str(job_state.get("status")) != "success":
            return None

        job_id = str(job_state.get("job_id", "")).strip()
        output_gz = str(job_state.get("output_gz", "")).strip()
        checksum = str(job_state.get("output_gz_sha256", "")).strip()
        if not job_id or not output_gz or not checksum:
            return None
        if not Path(output_gz).is_file():
            return None

        expires_ts = self._resolve_download_expires_ts(job_state)
        if expires_ts is None or expires_ts < int(time.time()):
            return None

        download_url = self._build_download_url(
            job_id=job_id,
            checksum=checksum,
            expires_ts=expires_ts,
        )
        expires_at = str(job_state.get("download_expires_at", "")).strip()
        if not expires_at:
            expires_at = datetime.fromtimestamp(expires_ts, tz=timezone.utc).isoformat()
        return {
            "download_url": download_url,
            "download_sha256": checksum,
            "download_expires_at": expires_at,
        }

    def _present_job(self, job_state: dict[str, Any]) -> dict[str, Any]:
        payload = dict(job_state)
        payload.pop("download_expires_ts", None)
        download_data = self._job_download_data(job_state)
        if download_data is not None:
            payload.update(download_data)
        else:
            payload.pop("download_url", None)
            payload.pop("download_sha256", None)
            payload.pop("download_expires_at", None)
        return payload

    @staticmethod
    def _tail_lines(path: Path, *, lines_limit: int) -> list[str]:
        if lines_limit <= 0:
            return []
        buffer: deque[str] = deque(maxlen=lines_limit)
        with path.open("r", encoding="utf-8", errors="replace") as file_stream:
            for raw_line in file_stream:
                buffer.append(raw_line.rstrip("\r\n"))
        return list(buffer)

    @staticmethod
    def _read_log_chunk(path: Path, *, offset: int) -> tuple[bytes, int]:
        safe_offset = max(0, offset)
        with path.open("rb") as file_stream:
            file_stream.seek(safe_offset)
            chunk = file_stream.read()
            return chunk, file_stream.tell()

    async def _stream_job_log(self, websocket: Any, request: StreamJobLogRequest) -> None:
        job_id = request.job_id.strip()
        if not job_id:
            await websocket.send(
                json.dumps(
                    {
                        "ok": False,
                        "action": request.action,
                        "event": "error",
                        "error": "Field 'job_id' is required.",
                    },
                    ensure_ascii=False,
                )
            )
            return

        tail_lines = request.tail_lines
        if tail_lines is None:
            tail_lines = DEFAULT_LOG_TAIL_LINES
        tail_lines = max(0, min(MAX_LOG_TAIL_LINES, int(tail_lines)))

        sent_snapshot = False
        sent_waiting = False
        offset = 0
        pending_fragment = ""
        terminal_statuses = {"success", "error"}

        while True:
            job_state = self._job_store.get(job_id)
            if job_state is None:
                await websocket.send(
                    json.dumps(
                        {
                            "ok": False,
                            "action": request.action,
                            "event": "error",
                            "job_id": job_id,
                            "error": "Job not found.",
                        },
                        ensure_ascii=False,
                    )
                )
                return

            job_status = str(job_state.get("status", "unknown")).strip().lower()
            worker_id = self._worker_id_from_value(job_state.get("worker_id"))
            log_path_token = str(job_state.get("output_worker_log", "")).strip()
            if not log_path_token:
                if not sent_waiting:
                    await websocket.send(
                        json.dumps(
                            {
                                "ok": True,
                                "action": request.action,
                                "event": "waiting",
                                "job_id": job_id,
                                "worker_id": worker_id,
                                "status": job_status,
                                "message": "Worker log is not available yet.",
                            },
                            ensure_ascii=False,
                        )
                    )
                    sent_waiting = True

                if job_status in terminal_statuses:
                    await websocket.send(
                        json.dumps(
                            {
                                "ok": True,
                                "action": request.action,
                                "event": "end",
                                "job_id": job_id,
                                "worker_id": worker_id,
                                "status": job_status,
                                "lines": [],
                            },
                            ensure_ascii=False,
                        )
                    )
                    return

                await asyncio.sleep(LOG_STREAM_POLL_INTERVAL_SEC)
                continue

            log_path = Path(log_path_token)
            if not log_path.is_file():
                if not sent_waiting:
                    await websocket.send(
                        json.dumps(
                            {
                                "ok": True,
                                "action": request.action,
                                "event": "waiting",
                                "job_id": job_id,
                                "worker_id": worker_id,
                                "status": job_status,
                                "message": "Worker log file is being prepared.",
                            },
                            ensure_ascii=False,
                        )
                    )
                    sent_waiting = True

                if job_status in terminal_statuses:
                    await websocket.send(
                        json.dumps(
                            {
                                "ok": False,
                                "action": request.action,
                                "event": "error",
                                "job_id": job_id,
                                "worker_id": worker_id,
                                "status": job_status,
                                "error": "Worker log file is missing.",
                            },
                            ensure_ascii=False,
                        )
                    )
                    return

                await asyncio.sleep(LOG_STREAM_POLL_INTERVAL_SEC)
                continue

            sent_waiting = False

            if not sent_snapshot:
                snapshot_lines = await asyncio.to_thread(
                    self._tail_lines,
                    log_path,
                    lines_limit=tail_lines,
                )
                await websocket.send(
                    json.dumps(
                        {
                            "ok": True,
                            "action": request.action,
                            "event": "snapshot",
                            "job_id": job_id,
                            "worker_id": worker_id,
                            "status": job_status,
                            "lines": snapshot_lines,
                            "tail_lines": tail_lines,
                        },
                        ensure_ascii=False,
                    )
                )
                try:
                    offset = log_path.stat().st_size
                except FileNotFoundError:
                    offset = 0
                sent_snapshot = True

            chunk, new_offset = await asyncio.to_thread(
                self._read_log_chunk,
                log_path,
                offset=offset,
            )
            if new_offset < offset:
                offset = 0
                pending_fragment = ""
                continue
            offset = new_offset

            if chunk:
                decoded = pending_fragment + chunk.decode("utf-8", errors="replace")
                lines = decoded.splitlines()
                if decoded.endswith(("\n", "\r")):
                    pending_fragment = ""
                else:
                    pending_fragment = lines.pop() if lines else decoded
                if lines:
                    await websocket.send(
                        json.dumps(
                            {
                                "ok": True,
                                "action": request.action,
                                "event": "append",
                                "job_id": job_id,
                                "worker_id": worker_id,
                                "status": job_status,
                                "lines": lines,
                            },
                            ensure_ascii=False,
                        )
                    )

            if job_status in terminal_statuses:
                if pending_fragment:
                    await websocket.send(
                        json.dumps(
                            {
                                "ok": True,
                                "action": request.action,
                                "event": "append",
                                "job_id": job_id,
                                "worker_id": worker_id,
                                "status": job_status,
                                "lines": [pending_fragment],
                            },
                            ensure_ascii=False,
                        )
                    )
                await websocket.send(
                    json.dumps(
                        {
                            "ok": True,
                            "action": request.action,
                            "event": "end",
                            "job_id": job_id,
                            "worker_id": worker_id,
                            "status": job_status,
                        },
                        ensure_ascii=False,
                    )
                )
                return

            await asyncio.sleep(LOG_STREAM_POLL_INTERVAL_SEC)

    def _build_download_app(self) -> Any:
        try:
            from fastapi import FastAPI, HTTPException, Query
            from fastapi.responses import FileResponse
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Packages 'fastapi' and 'uvicorn' are required for download URLs."
            ) from exc

        app = FastAPI(
            title="OpenInflation Orchestrator Download API",
            docs_url=None,
            redoc_url=None,
            openapi_url=None,
        )
        orchestrator = self

        @app.get("/download")
        async def download(
            job_id: str = Query(..., min_length=1),
            expires: int = Query(...),
            sha256: str = Query(..., min_length=1),
            sig: str = Query(..., min_length=1),
        ) -> Any:
            if expires < int(time.time()):
                raise HTTPException(status_code=403, detail="Download URL has expired")

            if not orchestrator._verify_download_signature(
                job_id=job_id,
                expires_ts=expires,
                checksum=sha256,
                signature=sig,
            ):
                raise HTTPException(status_code=403, detail="Invalid signature")

            job_state = orchestrator._job_store.get(job_id)
            if not job_state or str(job_state.get("status")) != "success":
                raise HTTPException(status_code=404, detail="Job result not found")

            expected_expires = orchestrator._resolve_download_expires_ts(job_state)
            if expected_expires is None or expected_expires != expires:
                raise HTTPException(status_code=403, detail="Download token mismatch")

            output_gz = str(job_state.get("output_gz", "")).strip()
            stored_checksum = str(job_state.get("output_gz_sha256", "")).strip()
            if not output_gz or not stored_checksum or stored_checksum != sha256:
                raise HTTPException(status_code=403, detail="Checksum mismatch")

            file_path = Path(output_gz)
            if not file_path.is_file():
                raise HTTPException(status_code=404, detail="Result file is missing")

            try:
                actual_checksum = await asyncio.to_thread(
                    orchestrator._sha256_file,
                    str(file_path),
                )
            except Exception as exc:
                LOGGER.exception("Failed to compute checksum for %s", file_path)
                raise HTTPException(
                    status_code=500,
                    detail="Failed to validate file checksum",
                ) from exc

            if actual_checksum != sha256:
                raise HTTPException(status_code=403, detail="File checksum validation failed")

            return FileResponse(
                path=str(file_path),
                media_type="application/gzip",
                filename=file_path.name,
            )

        return app

    async def _start_download_server(self) -> None:
        if self._download_task is not None:
            return
        try:
            import uvicorn
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Package 'uvicorn' is required for download URLs."
            ) from exc

        app = self._build_download_app()
        config = uvicorn.Config(
            app=app,
            host=self.download_host,
            port=self.download_port,
            log_level=self.log_level.lower(),
            access_log=False,
        )
        self._download_server = uvicorn.Server(config=config)
        self._download_task = asyncio.create_task(
            self._download_server.serve(),
            name="orchestrator-download-api",
        )
        await asyncio.sleep(0.15)
        if self._download_task.done():
            exc = self._download_task.exception()
            if exc is not None:
                raise RuntimeError(
                    f"Failed to start download API on {self.download_host}:{self.download_port}"
                ) from exc
        LOGGER.info(
            "Download API listening on http://%s:%s/download",
            self.download_host,
            self.download_port,
        )

    def start_workers(self) -> None:
        LOGGER.info("Starting %s workers", self.worker_count)
        for index in range(self.worker_count):
            worker_id = index + 1
            self._spawn_worker(worker_id, replace=False)
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
            current_job_id = self._worker_current_job.get(worker_id)
            if current_job_id == job_id:
                self._worker_busy[worker_id] = False
                self._worker_current_job[worker_id] = None
                return
            if current_job_id is None:
                self._worker_busy[worker_id] = False

        for candidate_worker_id, current_job_id in self._worker_current_job.items():
            if current_job_id == job_id:
                self._worker_busy[candidate_worker_id] = False
                self._worker_current_job[candidate_worker_id] = None
                return

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
            if self._worker_current_job.get(worker_id) is not None:
                continue
            if not self._ensure_worker_alive(worker_id):
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

            job_id = str(job_state.get("job_id", "unknown"))
            worker_id = self._worker_id_from_value(job_state.get("worker_id"))
            reason: str | None = None
            if worker_id is None:
                if status == "queued" and any(item.job_id == job_id for item in self._pending_jobs):
                    continue
                reason = "worker_id is missing"
            elif not worker_alive.get(worker_id, False):
                reason = f"worker_id={worker_id} is not alive"
            else:
                slot_job_id = self._worker_current_job.get(worker_id)
                if slot_job_id != job_id:
                    reason = (
                        f"worker_id={worker_id} currently assigned to "
                        f"{slot_job_id or 'none'}"
                    )

            if reason is None:
                continue

            worker_label = str(worker_id) if worker_id is not None else "unknown"
            job_state["status"] = "error"
            job_state["finished_at"] = utc_now_iso()
            job_state["message"] = (
                "Job reconciled as orphaned by orchestrator heartbeat "
                f"({reason})."
            )
            self._job_store.upsert(job_state)
            self._release_worker_slot_for_job(job_id=job_id, worker_id=worker_id)
            reconciled += 1
            LOGGER.warning(
                "Job %s marked as error by reconcile: worker_id=%s reason=%s",
                job_id,
                worker_label,
                reason,
            )
        return reconciled

    def _reconcile_worker_slots(self) -> tuple[int, int]:
        normalized = 0
        restarted = 0
        for worker_id, process in enumerate(self._workers, start=1):
            busy = bool(self._worker_busy.get(worker_id, False))
            current_job_id = self._worker_current_job.get(worker_id)

            if current_job_id is not None:
                job_state = self._job_store.get(current_job_id)
                job_status = str(job_state.get("status", "")).strip().lower() if job_state else "missing"
                if job_state is None or job_status not in {"queued", "running"}:
                    LOGGER.warning(
                        "Clearing stale worker slot: worker=%s pid=%s alive=%s job_id=%s job_status=%s",
                        worker_id,
                        process.pid,
                        process.is_alive(),
                        current_job_id,
                        job_status,
                    )
                    self._release_worker_slot_for_job(job_id=current_job_id, worker_id=worker_id)
                    normalized += 1
                    busy = False
                    current_job_id = None

            if current_job_id is None and busy:
                self._worker_busy[worker_id] = False
                normalized += 1
                LOGGER.warning(
                    "Clearing inconsistent worker busy flag: worker=%s pid=%s alive=%s",
                    worker_id,
                    process.pid,
                    process.is_alive(),
                )

            if (
                not process.is_alive()
                and self._worker_current_job.get(worker_id) is None
                and not self._worker_busy.get(worker_id, False)
            ):
                if self._ensure_worker_alive(worker_id):
                    restarted += 1

        return normalized, restarted

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
                output_worker_log = str(event.get("output_worker_log", "")).strip()
                if output_worker_log:
                    job_state["output_worker_log"] = output_worker_log
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
                    try:
                        job_state["output_gz_sha256"] = self._sha256_file(
                            str(event["output_gz"])
                        )
                    except Exception:
                        LOGGER.exception(
                            "Failed to compute output_gz checksum for job %s",
                            job_id,
                        )
                if "output_worker_log" in event:
                    output_worker_log = str(event.get("output_worker_log", "")).strip()
                    if output_worker_log:
                        job_state["output_worker_log"] = output_worker_log
                    else:
                        job_state.pop("output_worker_log", None)
                if job_state["status"] == "success":
                    self._set_download_metadata(job_state)
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

    def _cleanup_expired_download_artifacts(self) -> int:
        now_ts = int(time.time())
        cleaned_jobs = 0

        for job_state in self._job_store.values():
            if str(job_state.get("status")) != "success":
                continue

            artifact_keys = ("output_json", "output_gz", "output_worker_log")
            has_artifact_paths = any(str(job_state.get(key, "")).strip() for key in artifact_keys)
            # Job was already cleaned in a previous heartbeat/run.
            if job_state.get("artifacts_deleted_at") and not has_artifact_paths:
                continue

            expires_ts = self._resolve_download_expires_ts(job_state)
            if expires_ts is None or expires_ts > now_ts:
                continue

            job_id = str(job_state.get("job_id", "unknown"))
            paths_to_delete: list[Path] = []
            seen_paths: set[str] = set()
            for key in artifact_keys:
                raw_path = str(job_state.get(key, "")).strip()
                if not raw_path or raw_path in seen_paths:
                    continue
                seen_paths.add(raw_path)
                paths_to_delete.append(Path(raw_path))

            deleted_files = 0
            deletion_failed = False
            for path in paths_to_delete:
                if not path.exists():
                    continue
                if not path.is_file():
                    LOGGER.warning(
                        "Expired artifact path is not a file for job %s: %s",
                        job_id,
                        path,
                    )
                    deletion_failed = True
                    continue
                try:
                    path.unlink()
                    deleted_files += 1
                except Exception:
                    LOGGER.exception(
                        "Failed to delete expired artifact for job %s: %s",
                        job_id,
                        path,
                    )
                    deletion_failed = True

            if deletion_failed:
                LOGGER.warning(
                    "Will retry expired artifact cleanup for job %s on next heartbeat",
                    job_id,
                )
                continue

            for key in (
                "output_json",
                "output_gz",
                "output_gz_sha256",
                "output_worker_log",
                "download_url",
                "download_sha256",
                "download_expires_at",
                "download_expires_ts",
            ):
                job_state.pop(key, None)
            job_state["artifacts_deleted_at"] = utc_now_iso()
            self._job_store.upsert(job_state)
            cleaned_jobs += 1
            LOGGER.info(
                "Expired download artifacts cleaned: job=%s deleted_files=%s",
                job_id,
                deleted_files,
            )

        return cleaned_jobs

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

        city_id = normalize_city_id(request.get("city_id", self.defaults.city_id))

        job = WorkerJob(
            job_id=uuid4().hex,
            parser_name=parser_name,
            store_code=store_code,
            output_dir=str(request.get("output_dir", self.defaults.output_dir)),
            country_id=int(request.get("country_id", self.defaults.country_id)),
            city_id=city_id,
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
            "Job enqueued: id=%s store=%s parser=%s city_id=%s full_catalog=%s timeout_ms=%s category_limit=%s pages=%s max_pages=%s per_page=%s include_images=%s strict_validation=%s pending=%s dispatched_now=%s",
            job.job_id,
            job.store_code,
            job.parser_name,
            job.city_id,
            job.full_catalog,
            job.api_timeout_ms,
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
            worker_id = idx + 1
            alive = process.is_alive()
            busy = bool(self._worker_busy.get(worker_id, False))
            job_id = self._worker_current_job.get(worker_id)
            if alive and busy and job_id:
                state = "running"
            elif alive and not busy and job_id is None:
                state = "idle"
            elif not alive and (busy or job_id):
                state = "stale"
            elif not alive:
                state = "dead"
            else:
                state = "unknown"
            rows.append(
                {
                    "index": worker_id,
                    "pid": process.pid,
                    "alive": alive,
                    "proxy": self._worker_proxy(idx),
                    "busy": busy,
                    "job_id": job_id,
                    "state": state,
                }
            )
        return rows

    def _is_authenticated(self, request: ParsedRequest) -> bool:
        if self._auth_password is None:
            return True
        request_password = getattr(request, "password", None)
        if not isinstance(request_password, str):
            return False
        return secrets.compare_digest(request_password, self._auth_password)

    async def _dispatch(self, request: ParsedRequest) -> dict[str, Any]:
        try:
            if not self._is_authenticated(request):
                return {
                    "ok": False,
                    "action": getattr(request, "action", None),
                    "error": "Unauthorized. Provide valid 'password'.",
                }

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
                    return {
                        "ok": True,
                        "action": request.action,
                        "job": self._present_job(job),
                    }
                return {"ok": True, "action": request.action, "summary": self._global_status()}

            if isinstance(request, JobsRequest):
                return {
                    "ok": True,
                    "action": request.action,
                    "jobs": [self._present_job(job) for job in self._job_store.sorted_jobs()],
                }

            if isinstance(request, WorkersRequest):
                return {"ok": True, "action": request.action, "workers": self._workers_status()}

            if isinstance(request, StreamJobLogRequest):
                return {
                    "ok": False,
                    "action": request.action,
                    "error": "Action 'stream_job_log' is a streaming command and must be handled in websocket session mode.",
                }

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
                    "auth_required": self._auth_password is not None,
                    "actions": [
                        "ping",
                        "submit_store",
                        "status",
                        "jobs",
                        "workers",
                        "stream_job_log",
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

            if isinstance(request, StreamJobLogRequest):
                try:
                    await self._stream_job_log(websocket, request)
                except Exception as exc:
                    LOGGER.warning("Log stream request failed: job_id=%s error=%s", request.job_id, exc)
                    response = {
                        "ok": False,
                        "action": request.action,
                        "event": "error",
                        "job_id": request.job_id,
                        "error": str(exc),
                    }
                    with contextlib.suppress(Exception):
                        await websocket.send(json.dumps(response, ensure_ascii=False))
                continue

            response = await self._dispatch(request)
            await websocket.send(json.dumps(response, ensure_ascii=False))
        LOGGER.info("WebSocket client disconnected: %s", getattr(websocket, "remote_address", None))

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
                    "city_id": self.defaults.city_id,
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
