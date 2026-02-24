from __future__ import annotations

from pathlib import Path

from openinflation_parser.orchestrator import choose_worker_count, load_proxy_list
from openinflation_parser.parsers import get_parser, get_parser_adapter


def test_choose_worker_count_ram_only() -> None:
    workers = choose_worker_count(
        available_ram_gb=8.0,
        ram_per_worker_gb=2.0,
        proxies_count=0,
        max_workers=None,
    )
    assert workers == 4


def test_choose_worker_count_with_proxy_limit() -> None:
    workers = choose_worker_count(
        available_ram_gb=16.0,
        ram_per_worker_gb=2.0,
        proxies_count=3,
        max_workers=None,
    )
    assert workers == 3


def test_choose_worker_count_with_max_limit() -> None:
    workers = choose_worker_count(
        available_ram_gb=16.0,
        ram_per_worker_gb=2.0,
        proxies_count=0,
        max_workers=2,
    )
    assert workers == 2


def test_choose_worker_count_min_one_worker() -> None:
    workers = choose_worker_count(
        available_ram_gb=0.2,
        ram_per_worker_gb=2.0,
        proxies_count=0,
        max_workers=None,
    )
    assert workers == 1


def test_load_proxy_list_deduplicates_and_reads_file(tmp_path: Path) -> None:
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text(
        "\n".join(
            [
                "http://127.0.0.1:8080",
                "http://127.0.0.1:8080",
                "",
                "# comment",
                "socks5://127.0.0.1:9090",
            ]
        ),
        encoding="utf-8",
    )

    proxies = load_proxy_list(["http://127.0.0.1:8080"], str(proxy_file))
    assert proxies == [
        "http://127.0.0.1:8080",
        "socks5://127.0.0.1:9090",
    ]


def test_worker_job_parses_retry_settings() -> None:
    from openinflation_parser.orchestrator import WorkerJob

    job = WorkerJob.from_payload(
        {
            "job_id": "j1",
            "parser_name": "fixprice",
            "store_code": "C001",
            "output_dir": "./output",
            "api_timeout_ms": 120000,
            "request_retries": 5,
            "request_retry_backoff_sec": 2.5,
        }
    )

    assert job.api_timeout_ms == 120000.0
    assert job.request_retries == 5
    assert job.request_retry_backoff_sec == 2.5


def test_worker_job_accepts_string_city_id() -> None:
    from openinflation_parser.orchestrator import WorkerJob

    job = WorkerJob.from_payload(
        {
            "job_id": "j2",
            "parser_name": "chizhik",
            "store_code": "moskva",
            "output_dir": "./output",
            "city_id": "fdc6b0ad-4096-43e7-a2e9-16404d2e1f68",
        }
    )

    assert job.city_id == "fdc6b0ad-4096-43e7-a2e9-16404d2e1f68"


def test_parser_registry_includes_chizhik() -> None:
    parser_cls = get_parser("chizhik")
    assert parser_cls.__name__ == "ChizhikParser"


def test_parser_adapter_registry_includes_fixprice() -> None:
    adapter = get_parser_adapter("fixprice")
    assert adapter.name == "fixprice"
