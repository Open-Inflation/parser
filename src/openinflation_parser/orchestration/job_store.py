from __future__ import annotations

from contextlib import closing
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


LOGGER = logging.getLogger(__name__)


ACTIVE_STATUSES = {"queued", "running"}


def _parse_iso(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


class JobStore:
    """In-memory job state with optional sqlite persistence and pruning."""

    def __init__(
        self,
        *,
        max_history: int,
        retention_seconds: int,
        sqlite_path: str | None,
    ) -> None:
        self.max_history = max(1, max_history)
        self.retention_seconds = max(60, retention_seconds)
        self.sqlite_path = sqlite_path
        self._jobs: dict[str, dict[str, Any]] = {}

        if self.sqlite_path:
            self._init_db()
            self._load_from_db()

    def _connect(self) -> sqlite3.Connection:
        if self.sqlite_path is None:
            raise RuntimeError("sqlite_path is disabled")
        path = Path(self.sqlite_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(path)

    def _init_db(self) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _load_from_db(self) -> None:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM jobs").fetchall()
        loaded = 0
        for (payload_str,) in rows:
            try:
                payload = json.loads(payload_str)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            job_id = str(payload.get("job_id", "")).strip()
            if not job_id:
                continue
            self._jobs[job_id] = payload
            loaded += 1
        if loaded:
            LOGGER.info("Loaded job history from sqlite: %s", loaded)

    def _persist_upsert(self, job: dict[str, Any]) -> None:
        if self.sqlite_path is None:
            return
        job_id = str(job["job_id"])
        payload = json.dumps(job, ensure_ascii=False)
        updated_at = str(
            job.get("finished_at")
            or job.get("started_at")
            or job.get("created_at")
            or datetime.now(timezone.utc).isoformat()
        )
        with closing(self._connect()) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO jobs(job_id, payload, updated_at) VALUES(?,?,?)",
                (job_id, payload, updated_at),
            )
            conn.commit()

    def _persist_delete(self, job_ids: list[str]) -> None:
        if self.sqlite_path is None or not job_ids:
            return
        with closing(self._connect()) as conn:
            conn.executemany("DELETE FROM jobs WHERE job_id = ?", [(job_id,) for job_id in job_ids])
            conn.commit()

    def upsert(self, job: dict[str, Any]) -> None:
        job_id = str(job["job_id"])
        self._jobs[job_id] = job
        self._persist_upsert(job)

    def get(self, job_id: str) -> dict[str, Any] | None:
        return self._jobs.get(job_id)

    def values(self) -> list[dict[str, Any]]:
        return list(self._jobs.values())

    def sorted_jobs(self) -> list[dict[str, Any]]:
        return sorted(self._jobs.values(), key=lambda row: str(row.get("created_at", "")))

    def summary(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for item in self._jobs.values():
            status = str(item.get("status", "unknown"))
            counts[status] = counts.get(status, 0) + 1
        return {
            "jobs_total": len(self._jobs),
            "jobs_by_status": counts,
        }

    def prune(self) -> int:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=self.retention_seconds)
        to_delete: set[str] = set()

        terminal: list[tuple[str, datetime]] = []
        for job_id, job in self._jobs.items():
            status = str(job.get("status", "unknown"))
            if status in ACTIVE_STATUSES:
                continue
            candidate_ts = (
                _parse_iso(job.get("finished_at"))
                or _parse_iso(job.get("started_at"))
                or _parse_iso(job.get("created_at"))
            )
            if candidate_ts is not None and candidate_ts < cutoff:
                to_delete.add(job_id)
                continue
            terminal.append((job_id, candidate_ts or now))

        terminal_count = len(terminal)
        if terminal_count > self.max_history:
            overflow = terminal_count - self.max_history
            for job_id, _ts in sorted(terminal, key=lambda item: item[1])[:overflow]:
                to_delete.add(job_id)

        if not to_delete:
            return 0

        for job_id in to_delete:
            self._jobs.pop(job_id, None)
        self._persist_delete(list(to_delete))
        LOGGER.debug("Pruned jobs from history: %s", len(to_delete))
        return len(to_delete)
