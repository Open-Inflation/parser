from __future__ import annotations

import asyncio
import contextlib
from dataclasses import asdict
import logging
from typing import Any

from .models import WorkerJob, coerce_bool
from .utils import utc_now_iso
from .worker import worker_process_loop


LOGGER = logging.getLogger(__name__)


class OrchestratorWorkersMixin:
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

    async def _recycle_worker_slot(
        self,
        *,
        worker_id: int,
        expected_pid: int | None,
    ) -> bool:
        if self._is_stopped:
            return False
        if worker_id < 1 or worker_id > len(self._workers):
            return False

        self._workers_pending_recycle.add(worker_id)
        try:
            process = self._workers[worker_id - 1]
            current_pid = process.pid
            if expected_pid is not None and current_pid != expected_pid:
                LOGGER.debug(
                    "Skip worker recycle due to pid mismatch: worker=%s expected_pid=%s current_pid=%s",
                    worker_id,
                    expected_pid,
                    current_pid,
                )
                return False

            await asyncio.to_thread(process.join, 2.0)
            latest_process = self._workers[worker_id - 1]
            if latest_process is not process:
                LOGGER.debug(
                    "Skip worker recycle because slot already replaced: worker=%s expected_pid=%s current_pid=%s",
                    worker_id,
                    expected_pid,
                    latest_process.pid,
                )
                return False

            if process.is_alive():
                LOGGER.warning(
                    "Worker did not exit during recycle window, terminating: worker=%s pid=%s",
                    worker_id,
                    process.pid,
                )
                process.terminate()
                await asyncio.to_thread(process.join, 2.0)

            latest_process = self._workers[worker_id - 1]
            if latest_process is not process:
                LOGGER.debug(
                    "Skip worker recycle because slot replaced after terminate: worker=%s expected_pid=%s current_pid=%s",
                    worker_id,
                    expected_pid,
                    latest_process.pid,
                )
                return False

            if self._is_stopped:
                return False
            self._spawn_worker(worker_id, replace=True)
            self._worker_busy[worker_id] = False
            self._worker_current_job[worker_id] = None
            return True
        finally:
            self._workers_pending_recycle.discard(worker_id)

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

    @staticmethod
    def _worker_pid_from_value(value: Any) -> int | None:
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
            if worker_id in self._workers_pending_recycle:
                continue
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

    def _worker_id_for_job(self, *, job_id: str, fallback: int | None) -> int | None:
        if fallback is not None:
            return fallback
        for worker_id, current_job_id in self._worker_current_job.items():
            if current_job_id == job_id:
                return worker_id
        return None

    async def _cancel_job(self, *, job_id: str, reason: str) -> dict[str, Any]:
        normalized_job_id = str(job_id).strip()
        if not normalized_job_id:
            raise ValueError("Field 'job_id' is required.")

        job_state = self._job_store.get(normalized_job_id)
        if job_state is None:
            raise ValueError("Job not found.")

        current_status = str(job_state.get("status", "unknown")).strip().lower()
        if current_status in {"success", "error", "cancelled"}:
            return {
                "job_id": normalized_job_id,
                "status": current_status,
                "already_terminal": True,
            }

        finished_at = utc_now_iso()
        message = reason.strip() or "Cancelled by API request"

        if current_status == "queued":
            self._pending_jobs = [item for item in self._pending_jobs if item.job_id != normalized_job_id]
            job_state["status"] = "cancelled"
            job_state["finished_at"] = finished_at
            job_state["message"] = message
            self._job_store.upsert(job_state)
            fallback_worker_id = self._worker_id_from_value(job_state.get("worker_id"))
            worker_id = self._worker_id_for_job(job_id=normalized_job_id, fallback=fallback_worker_id)
            self._release_worker_slot_for_job(job_id=normalized_job_id, worker_id=worker_id)
            await self._try_dispatch_jobs()
            LOGGER.info("Cancelled queued job: id=%s", normalized_job_id)
            return {"job_id": normalized_job_id, "status": "cancelled"}

        if current_status == "running":
            fallback_worker_id = self._worker_id_from_value(job_state.get("worker_id"))
            worker_id = self._worker_id_for_job(job_id=normalized_job_id, fallback=fallback_worker_id)

            job_state["status"] = "cancelled"
            job_state["finished_at"] = finished_at
            job_state["message"] = message
            self._job_store.upsert(job_state)

            if worker_id is not None and 1 <= worker_id <= len(self._workers):
                process = self._workers[worker_id - 1]
                self._release_worker_slot_for_job(job_id=normalized_job_id, worker_id=worker_id)
                self._worker_busy[worker_id] = False
                self._worker_current_job[worker_id] = None

                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 2.0)
                if process.is_alive():
                    process.kill()
                    await asyncio.to_thread(process.join, 2.0)

                if not self._is_stopped:
                    latest_process = self._workers[worker_id - 1]
                    if latest_process is process:
                        self._spawn_worker(worker_id, replace=True)
            else:
                self._release_worker_slot_for_job(job_id=normalized_job_id, worker_id=worker_id)

            await self._try_dispatch_jobs()
            LOGGER.info("Cancelled running job: id=%s worker_id=%s", normalized_job_id, worker_id)
            return {"job_id": normalized_job_id, "status": "cancelled"}

        raise ValueError(f"Job cannot be cancelled from status={current_status!r}.")

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
                if worker_id in self._workers_pending_recycle:
                    continue
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
                event_worker_pid = self._worker_pid_from_value(event.get("worker_pid"))
                if event_worker_pid is not None:
                    job_state["worker_pid"] = event_worker_pid
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
                finished_worker_pid = self._worker_pid_from_value(event.get("worker_pid"))
                worker_will_exit = coerce_bool(event.get("worker_will_exit", False))
                current_status = str(job_state.get("status", "")).strip().lower()
                if current_status == "cancelled":
                    LOGGER.info(
                        "Ignoring finished event for cancelled job: id=%s worker=%s",
                        job_id,
                        event.get("worker_id"),
                    )
                    self._release_worker_slot_for_job(job_id=job_id, worker_id=finished_worker_id)
                    if worker_will_exit and finished_worker_id is not None:
                        await self._recycle_worker_slot(
                            worker_id=finished_worker_id,
                            expected_pid=finished_worker_pid,
                        )
                    await self._try_dispatch_jobs()
                    continue
                job_state["status"] = event.get("status", "error")
                job_state["finished_at"] = event.get("timestamp")
                if finished_worker_id is not None:
                    job_state["worker_id"] = finished_worker_id
                if finished_worker_pid is not None:
                    job_state["worker_pid"] = finished_worker_pid
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
                if worker_will_exit and finished_worker_id is not None:
                    recycled = await self._recycle_worker_slot(
                        worker_id=finished_worker_id,
                        expected_pid=finished_worker_pid,
                    )
                    if recycled:
                        LOGGER.info(
                            "Worker recycled after max_jobs_per_worker: worker=%s previous_pid=%s",
                            finished_worker_id,
                            finished_worker_pid,
                        )
                await self._try_dispatch_jobs()
                continue

            LOGGER.warning("Unknown result event for job %s: %s", job_id, event_name)
