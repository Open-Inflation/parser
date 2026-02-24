from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

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
