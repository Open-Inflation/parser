from __future__ import annotations

from pathlib import Path

from openinflation_parser.orchestrator import choose_worker_count, load_proxy_list


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
