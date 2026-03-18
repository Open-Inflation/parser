from __future__ import annotations

import asyncio
from collections import deque
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from .requests import StreamJobLogRequest
from .utils import utc_now_iso


LOGGER = logging.getLogger(__name__)

DEFAULT_LOG_TAIL_LINES = 200
MAX_LOG_TAIL_LINES = 5000
LOG_STREAM_POLL_INTERVAL_SEC = 0.4


class OrchestratorDownloadsMixin:
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
        terminal_statuses = {"success", "error", "cancelled"}

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

    def _cleanup_expired_download_artifacts(self) -> int:
        now_ts = int(time.time())
        cleaned_jobs = 0

        for job_state in self._job_store.values():
            if str(job_state.get("status")) != "success":
                continue

            artifact_keys = ("output_json", "output_gz", "output_worker_log")
            has_artifact_paths = any(str(job_state.get(key, "")).strip() for key in artifact_keys)
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
