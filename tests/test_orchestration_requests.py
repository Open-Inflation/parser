from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Queue
import sqlite3

import pytest

from openinflation_parser.orchestration.models import JobDefaults, WorkerJob
from openinflation_parser.orchestration.server import OrchestratorServer
from openinflation_parser.orchestration.job_store import JobStore
from openinflation_parser.orchestration.requests import (
    SubmitStoreRequest,
    UnknownRequest,
    parse_request,
)


def test_parse_request_submit_store_model() -> None:
    request = parse_request(
        {
            "action": "submit_store",
            "store_code": "C001",
            "parser": "fixprice",
            "city_id": "3",
        }
    )
    assert isinstance(request, SubmitStoreRequest)
    assert request.city_id == 3


def test_parse_request_requires_action() -> None:
    with pytest.raises(ValueError):
        parse_request({"store_code": "C001"})


def test_parse_request_unknown_action() -> None:
    request = parse_request({"action": "something_new"})
    assert isinstance(request, UnknownRequest)


def test_parse_request_rejects_extra_fields() -> None:
    with pytest.raises(Exception):
        parse_request({"action": "ping", "extra": 1})


def test_job_store_prunes_by_max_history(tmp_path: Path) -> None:
    db = tmp_path / "jobs.sqlite"
    store = JobStore(
        max_history=2,
        retention_seconds=10**9,
        sqlite_path=str(db),
    )

    now = datetime.now(timezone.utc)
    for idx in range(4):
        created_at = (now + timedelta(seconds=idx)).isoformat()
        finished_at = (now + timedelta(seconds=idx + 1)).isoformat()
        store.upsert(
            {
                "job_id": f"j{idx}",
                "status": "success",
                "created_at": created_at,
                "finished_at": finished_at,
            }
        )
    pruned = store.prune()
    assert pruned == 2
    assert len(store.values()) == 2


def test_job_store_persists_and_loads(tmp_path: Path) -> None:
    db = tmp_path / "jobs.sqlite"
    store = JobStore(
        max_history=10,
        retention_seconds=86400,
        sqlite_path=str(db),
    )
    payload = {
        "job_id": "abc",
        "status": "queued",
        "created_at": "2026-01-01T00:00:00+00:00",
    }
    store.upsert(payload)

    reloaded = JobStore(
        max_history=10,
        retention_seconds=86400,
        sqlite_path=str(db),
    )
    loaded = reloaded.get("abc")
    assert loaded is not None
    assert loaded["status"] == "queued"


def _job_defaults() -> JobDefaults:
    return JobDefaults(
        parser_name="fixprice",
        output_dir="./output",
        country_id=2,
        city_id=3,
        api_timeout_ms=90000.0,
        request_retries=3,
        request_retry_backoff_sec=1.5,
        category_limit=1,
        pages_per_category=1,
        max_pages_per_category=200,
        products_per_page=24,
        full_catalog=False,
        include_images=False,
        strict_validation=False,
    )


def test_job_store_closes_sqlite_connections(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db = tmp_path / "jobs.sqlite"
    opened_connections: list[sqlite3.Connection] = []

    def _patched_connect(self: JobStore) -> sqlite3.Connection:
        conn = sqlite3.connect(db)
        opened_connections.append(conn)
        return conn

    monkeypatch.setattr(JobStore, "_connect", _patched_connect)

    store = JobStore(
        max_history=1,
        retention_seconds=10**9,
        sqlite_path=str(db),
    )
    now = datetime.now(timezone.utc)
    store.upsert(
        {
            "job_id": "j1",
            "status": "success",
            "created_at": now.isoformat(),
            "finished_at": (now + timedelta(seconds=1)).isoformat(),
        }
    )
    store.upsert(
        {
            "job_id": "j2",
            "status": "success",
            "created_at": (now + timedelta(seconds=2)).isoformat(),
            "finished_at": (now + timedelta(seconds=3)).isoformat(),
        }
    )
    store.prune()

    assert opened_connections
    for conn in opened_connections:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")


def test_reconcile_orphaned_running_jobs_marks_error() -> None:
    defaults = _job_defaults()
    server = OrchestratorServer(
        host="127.0.0.1",
        port=8765,
        worker_count=1,
        proxies=[],
        defaults=defaults,
        jobs_db_path=None,
    )

    class _DeadProcess:
        pid = 123

        @staticmethod
        def is_alive() -> bool:
            return False

    server._workers = [_DeadProcess()]  # type: ignore[assignment]
    server._job_store.upsert(
        {
            "job_id": "job-1",
            "status": "running",
            "created_at": "2026-01-01T00:00:00+00:00",
            "started_at": "2026-01-01T00:00:10+00:00",
            "worker_id": 1,
        }
    )

    reconciled = server._reconcile_orphaned_running_jobs()
    assert reconciled == 1

    job = server._job_store.get("job-1")
    assert job is not None
    assert job["status"] == "error"
    assert "Worker process stopped before reporting job completion" in str(job.get("message", ""))
    assert job.get("finished_at") is not None


def test_run_calls_stop_when_bootstrap_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    defaults = _job_defaults()
    server = OrchestratorServer(
        host="127.0.0.1",
        port=8765,
        worker_count=1,
        proxies=[],
        defaults=defaults,
        jobs_db_path=None,
    )
    state = {"stop_called": False}

    def _start_workers() -> None:
        return None

    async def _collect_results() -> None:
        return None

    async def _log_heartbeat() -> None:
        return None

    async def _enqueue_job(_request: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("bootstrap enqueue failed")

    async def _stop() -> None:
        state["stop_called"] = True

    monkeypatch.setattr(server, "start_workers", _start_workers)
    monkeypatch.setattr(server, "_collect_results", _collect_results)
    monkeypatch.setattr(server, "_log_heartbeat", _log_heartbeat)
    monkeypatch.setattr(server, "_enqueue_job", _enqueue_job)
    monkeypatch.setattr(server, "stop", _stop)

    with pytest.raises(RuntimeError, match="bootstrap enqueue failed"):
        asyncio.run(server.run(bootstrap_store_code="C001"))

    assert state["stop_called"] is True


def test_dispatch_rules_allow_same_proxy_for_different_parsers() -> None:
    defaults = _job_defaults()
    server = OrchestratorServer(
        host="127.0.0.1",
        port=8765,
        worker_count=2,
        proxies=["http://127.0.0.1:8080"],
        defaults=defaults,
        jobs_db_path=None,
    )

    class _AliveProcess:
        @staticmethod
        def is_alive() -> bool:
            return True

    queue_1: Queue = Queue()
    queue_2: Queue = Queue()
    server._workers = [_AliveProcess(), _AliveProcess()]  # type: ignore[assignment]
    server._worker_queues = {1: queue_1, 2: queue_2}
    server._worker_busy = {1: False, 2: False}
    server._worker_current_job = {1: None, 2: None}

    jobs = [
        WorkerJob(
            job_id="j-fix-1",
            parser_name="fixprice",
            store_code="C001",
            output_dir="./output",
        ),
        WorkerJob(
            job_id="j-fix-2",
            parser_name="fixprice",
            store_code="C002",
            output_dir="./output",
        ),
        WorkerJob(
            job_id="j-chizhik-1",
            parser_name="chizhik",
            store_code="moskva",
            output_dir="./output",
        ),
    ]
    for job in jobs:
        server._job_store.upsert(
            {
                "job_id": job.job_id,
                "status": "queued",
                "created_at": "2026-01-01T00:00:00+00:00",
                "store_code": job.store_code,
                "parser": job.parser_name,
            }
        )
    server._pending_jobs = list(jobs)

    dispatched = asyncio.run(server._try_dispatch_jobs())
    assert dispatched == 2
    assert len(server._pending_jobs) == 1
    assert server._pending_jobs[0].parser_name == "fixprice"

    payload_1 = queue_1.get_nowait()
    payload_2 = queue_2.get_nowait()
    parsers = {str(payload_1["parser_name"]), str(payload_2["parser_name"])}
    assert parsers == {"fixprice", "chizhik"}
