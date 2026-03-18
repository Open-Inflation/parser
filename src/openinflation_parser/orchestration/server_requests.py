from __future__ import annotations

import contextlib
import json
import logging
import secrets
from typing import Any

from pydantic import ValidationError

from .requests import (
    CancelJobRequest,
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
from .utils import utc_now_iso


LOGGER = logging.getLogger(__name__)


class OrchestratorRequestsMixin:
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

            if isinstance(request, CancelJobRequest):
                payload = await self._cancel_job(
                    job_id=request.job_id.strip(),
                    reason="Cancelled by API request",
                )
                return {"ok": True, "action": request.action} | payload

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
                        "cancel_job",
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
                if not self._is_authenticated(request):
                    response = {
                        "ok": False,
                        "action": request.action,
                        "event": "error",
                        "job_id": request.job_id,
                        "error": "Unauthorized. Provide valid 'password'.",
                    }
                    await websocket.send(json.dumps(response, ensure_ascii=False))
                    continue
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
