from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path

from .models import JobDefaults, normalize_city_id
from .server import OrchestratorServer
from .utils import detect_available_ram_gb, load_proxy_list, setup_logging, choose_worker_count


LOGGER = logging.getLogger(__name__)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OpenInflation orchestration server")
    parser.add_argument("--host", default="127.0.0.1", help="WebSocket host")
    parser.add_argument("--port", type=int, default=8765, help="WebSocket port")

    parser.add_argument(
        "--parser",
        default="fixprice",
        choices=["fixprice", "chizhik"],
        help="Parser name",
    )
    parser.add_argument("--output-dir", default="./output", help="Output directory for payload files")
    parser.add_argument("--country-id", type=int, default=2, help="Default country id")
    parser.add_argument(
        "--city-id",
        type=str,
        default=None,
        help="Default city id (int for fixprice, string/int for chizhik).",
    )
    parser.add_argument(
        "--api-timeout-ms",
        type=float,
        default=90000.0,
        help="Parser API request timeout in milliseconds.",
    )
    parser.add_argument(
        "--request-retries",
        type=int,
        default=3,
        help="Retry count for retryable API errors (timeouts).",
    )
    parser.add_argument(
        "--request-retry-backoff-sec",
        type=float,
        default=1.5,
        help="Base backoff (seconds) for retries (exponential).",
    )

    parser.add_argument(
        "--category-limit",
        type=int,
        default=1,
        help="Categories per store job (used when --full-catalog is off).",
    )
    parser.add_argument(
        "--pages-per-category",
        type=int,
        default=1,
        help="Pages per category (used when --full-catalog is off).",
    )
    parser.add_argument(
        "--max-pages-per-category",
        type=int,
        default=200,
        help="Safety page cap per category query for --full-catalog mode.",
    )
    parser.add_argument("--products-per-page", type=int, default=24, help="Items per page (1..27)")
    parser.add_argument(
        "--full-catalog",
        action="store_true",
        help="Traverse all categories/subcategories and paginate each query until empty page (capped by --max-pages-per-category).",
    )
    parser.add_argument("--include-images", action="store_true", help="Download product images")
    parser.add_argument(
        "--strict-validation",
        action="store_true",
        help="Enable strict pydantic validation for mapped models (can fail on missing fields).",
    )

    parser.add_argument(
        "--proxy",
        action="append",
        default=[],
        help="Proxy URL. Can be used multiple times.",
    )
    parser.add_argument(
        "--proxy-file",
        default=None,
        help="Path to file with proxies (one per line).",
    )

    parser.add_argument(
        "--ram-per-worker-gb",
        type=float,
        default=1.5,
        help="Expected RAM footprint per worker in GiB.",
    )
    parser.add_argument(
        "--available-ram-gb",
        type=float,
        default=None,
        help="Override available RAM detection (GiB).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Optional hard cap for worker count.",
    )
    parser.add_argument(
        "--bootstrap-store-code",
        default=None,
        help="Submit this store code immediately after startup.",
    )
    parser.add_argument(
        "--jobs-max-history",
        type=int,
        default=1000,
        help="Max number of terminal jobs to keep in history.",
    )
    parser.add_argument(
        "--jobs-retention-sec",
        type=int,
        default=86400,
        help="TTL for terminal jobs in seconds.",
    )
    parser.add_argument(
        "--jobs-db-path",
        default="./output/orchestrator_jobs.sqlite",
        help="SQLite path for persisted job states (set empty string to disable).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Application log level.",
    )
    return parser


async def run_orchestrator(args: argparse.Namespace) -> None:
    setup_logging(args.log_level)
    proxies = load_proxy_list(args.proxy, args.proxy_file)
    available_ram_gb = args.available_ram_gb
    if available_ram_gb is None:
        available_ram_gb = detect_available_ram_gb()

    worker_count = choose_worker_count(
        available_ram_gb=available_ram_gb,
        ram_per_worker_gb=args.ram_per_worker_gb,
        proxies_count=len(proxies),
        max_workers=args.max_workers,
    )

    defaults = JobDefaults(
        parser_name=args.parser,
        output_dir=args.output_dir,
        country_id=args.country_id,
        city_id=normalize_city_id(args.city_id),
        api_timeout_ms=max(1000.0, args.api_timeout_ms),
        request_retries=max(0, args.request_retries),
        request_retry_backoff_sec=max(0.1, args.request_retry_backoff_sec),
        category_limit=max(1, args.category_limit),
        pages_per_category=max(1, args.pages_per_category),
        max_pages_per_category=max(1, args.max_pages_per_category),
        products_per_page=max(1, min(27, args.products_per_page)),
        full_catalog=args.full_catalog,
        include_images=args.include_images,
        strict_validation=args.strict_validation,
    )

    jobs_db_path = args.jobs_db_path.strip() if isinstance(args.jobs_db_path, str) else None
    if jobs_db_path == "":
        jobs_db_path = None

    LOGGER.info(
        "Starting orchestrator config: %s",
        json.dumps(
            {
                "host": args.host,
                "port": args.port,
                "parser": args.parser,
                "workers": worker_count,
                "available_ram_gb": round(available_ram_gb, 2),
                "ram_per_worker_gb": args.ram_per_worker_gb,
                "proxies": len(proxies),
                "output_dir": str(Path(args.output_dir).expanduser().resolve()),
                "log_level": args.log_level,
                "full_catalog": args.full_catalog,
                "max_pages_per_category": max(1, args.max_pages_per_category),
                "api_timeout_ms": max(1000.0, args.api_timeout_ms),
                "request_retries": max(0, args.request_retries),
                "request_retry_backoff_sec": max(0.1, args.request_retry_backoff_sec),
                "strict_validation": args.strict_validation,
                "jobs_max_history": max(1, args.jobs_max_history),
                "jobs_retention_sec": max(60, args.jobs_retention_sec),
                "jobs_db_path": jobs_db_path,
            },
            ensure_ascii=False,
        ),
    )
    LOGGER.info("WebSocket endpoint: ws://%s:%s", args.host, args.port)

    server = OrchestratorServer(
        host=args.host,
        port=args.port,
        worker_count=worker_count,
        proxies=proxies,
        defaults=defaults,
        log_level=args.log_level,
        jobs_max_history=max(1, args.jobs_max_history),
        jobs_retention_sec=max(60, args.jobs_retention_sec),
        jobs_db_path=jobs_db_path,
    )
    await server.run(bootstrap_store_code=args.bootstrap_store_code)


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        asyncio.run(run_orchestrator(args))
    except KeyboardInterrupt:
        LOGGER.info("Orchestrator interrupted by user.")
