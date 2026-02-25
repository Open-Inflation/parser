from __future__ import annotations

import asyncio
import logging
import traceback
from pathlib import Path
from typing import Any

from ..parsers import ParserRunSettings, get_parser_adapter
from .models import WorkerJob
from .utils import safe_store_code, setup_logging, utc_now_iso, write_store_bundle


LOGGER = logging.getLogger(__name__)
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(processName)s | %(name)s | %(message)s"


def _worker_job_log_path(*, job: WorkerJob, worker_id: int) -> Path:
    output_dir = Path(job.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / (
        f".worker_{worker_id}_{safe_store_code(job.store_code)}_{safe_store_code(job.job_id)}.log"
    )


def _attach_worker_job_log_handler(
    *,
    job: WorkerJob,
    worker_id: int,
    log_level: str,
) -> tuple[Path, logging.Handler]:
    log_path = _worker_job_log_path(job=job, worker_id=worker_id)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(handler)
    return log_path, handler


def _detach_worker_job_log_handler(handler: logging.Handler) -> None:
    root_logger = logging.getLogger()
    root_logger.removeHandler(handler)
    handler.close()


async def execute_store_job(
    job: WorkerJob,
    proxy: str | None,
    *,
    worker_id: int,
    worker_log_path: str | None = None,
) -> tuple[str, str]:
    LOGGER.info(
        "Worker %s started job %s for store=%s parser=%s city_id=%s full_catalog=%s include_images=%s timeout_ms=%s strict_validation=%s",
        worker_id,
        job.job_id,
        job.store_code,
        job.parser_name,
        job.city_id,
        job.full_catalog,
        job.include_images,
        job.api_timeout_ms,
        job.strict_validation,
    )

    adapter = get_parser_adapter(job.parser_name)
    parser = adapter.create_parser(
        settings=ParserRunSettings(
            country_id=job.country_id,
            city_id=job.city_id,
            timeout_ms=job.api_timeout_ms,
            include_images=job.include_images,
            strict_validation=job.strict_validation,
        ),
        proxy=proxy,
    )
    store_city_id = adapter.city_id_for_store_info(job.city_id)

    async with parser:
        categories = await parser.collect_categories()
        selected_categories = (
            categories
            if job.full_catalog
            else categories[: max(1, job.category_limit)]
        )
        product_queries = parser.build_catalog_queries(
            categories,
            full_catalog=job.full_catalog,
            category_limit=job.category_limit,
        )
        LOGGER.info(
            "Worker %s job %s selected categories: full_catalog=%s requested=%s available=%s selected=%s queries=%s",
            worker_id,
            job.job_id,
            job.full_catalog,
            job.category_limit,
            len(categories),
            len(selected_categories),
            len(product_queries),
        )

        if not product_queries:
            raise ValueError("No categories available to collect products.")

        page_limit = (
            max(1, job.max_pages_per_category)
            if job.full_catalog
            else max(1, job.pages_per_category)
        )

        products = await parser.collect_products_for_queries(
            product_queries,
            page_limit=page_limit,
            items_per_page=job.products_per_page,
        )
        LOGGER.info(
            "Worker %s job %s collected products=%s page_limit=%s",
            worker_id,
            job.job_id,
            len(products),
            page_limit,
        )

        stores = await parser.collect_store_info(
            country_id=job.country_id,
            city_id=store_city_id,
            store_code=job.store_code,
        )
        LOGGER.info(
            "Worker %s job %s store search result count=%s",
            worker_id,
            job.job_id,
            len(stores),
        )
        if not stores:
            raise ValueError(
                f"Store code {job.store_code!r} not found. "
                "Use a valid store code and provide city_id when possible."
            )

        store = stores[0].model_copy(update={"categories": selected_categories, "products": products})
        json_path, json_gz_path = write_store_bundle(
            store,
            output_dir=job.output_dir,
            store_code=job.store_code,
            worker_log_path=worker_log_path,
        )
        LOGGER.info(
            "Worker %s finished job %s successfully: json=%s archive=%s",
            worker_id,
            job.job_id,
            json_path,
            json_gz_path,
        )
        return json_path, json_gz_path


def worker_process_loop(
    worker_id: int,
    proxy: str | None,
    log_level: str,
    job_queue: Any,
    result_queue: Any,
) -> None:
    setup_logging(log_level)
    LOGGER.info("Worker %s booted (proxy=%s)", worker_id, proxy or "none")

    while True:
        payload = job_queue.get()
        if payload is None:
            LOGGER.info("Worker %s received stop signal", worker_id)
            break

        try:
            job = WorkerJob.from_payload(payload)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("Worker %s received invalid job payload", worker_id)
            result_queue.put(
                {
                    "event": "finished",
                    "status": "error",
                    "worker_id": worker_id,
                    "job_id": str(payload.get("job_id", "unknown")),
                    "timestamp": utc_now_iso(),
                    "message": f"Invalid job payload: {exc}",
                }
            )
            continue

        worker_log_path: Path | None = None
        worker_log_handler: logging.Handler | None = None
        try:
            worker_log_path, worker_log_handler = _attach_worker_job_log_handler(
                job=job,
                worker_id=worker_id,
                log_level=log_level,
            )
            LOGGER.info(
                "Worker %s job %s log file attached: %s",
                worker_id,
                job.job_id,
                worker_log_path,
            )
        except Exception:
            LOGGER.exception(
                "Worker %s failed to initialize job log file for job %s",
                worker_id,
                job.job_id,
            )
        LOGGER.info(
            "Worker %s picked job %s (store=%s worker_log=%s)",
            worker_id,
            job.job_id,
            job.store_code,
            worker_log_path if worker_log_path is not None else "none",
        )
        result_queue.put(
            {
                "event": "started",
                "status": "running",
                "worker_id": worker_id,
                "job_id": job.job_id,
                "timestamp": utc_now_iso(),
                "output_worker_log": (
                    str(worker_log_path) if worker_log_path is not None else None
                ),
            }
        )

        job_succeeded = False
        try:
            json_path, json_gz_path = asyncio.run(
                execute_store_job(
                    job,
                    proxy=proxy,
                    worker_id=worker_id,
                    worker_log_path=str(worker_log_path) if worker_log_path is not None else None,
                )
            )
            result_queue.put(
                {
                    "event": "finished",
                    "status": "success",
                    "worker_id": worker_id,
                    "job_id": job.job_id,
                    "timestamp": utc_now_iso(),
                    "output_json": json_path,
                    "output_gz": json_gz_path,
                    "output_worker_log": (
                        str(worker_log_path) if worker_log_path is not None else None
                    ),
                }
            )
            job_succeeded = True
        except Exception as exc:
            LOGGER.exception("Worker %s failed job %s: %s", worker_id, job.job_id, exc)
            result_queue.put(
                {
                    "event": "finished",
                    "status": "error",
                    "worker_id": worker_id,
                    "job_id": job.job_id,
                    "timestamp": utc_now_iso(),
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )
        finally:
            if worker_log_handler is not None:
                _detach_worker_job_log_handler(worker_log_handler)
            if not job_succeeded and worker_log_path is not None:
                try:
                    worker_log_path.unlink(missing_ok=True)
                except Exception:
                    LOGGER.exception(
                        "Worker %s failed to cleanup log file for failed job %s: %s",
                        worker_id,
                        job.job_id,
                        worker_log_path,
                    )
