from __future__ import annotations

import base64
import json
import tarfile
from pathlib import Path

from openinflation_dataclass import AdministrativeUnit, Card, RetailUnit, Schedule

from openinflation_parser.orchestrator import choose_worker_count, load_proxy_list
from openinflation_parser.orchestration.utils import write_store_bundle
from openinflation_parser.parsers import get_parser, get_parser_adapter
from openinflation_parser.parsers.runtime import INLINE_IMAGE_TOKEN_PREFIX


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


def test_choose_worker_count_with_proxy_reuse_factor() -> None:
    workers = choose_worker_count(
        available_ram_gb=20.0,
        ram_per_worker_gb=2.0,
        proxies_count=3,
        max_workers=None,
        proxy_reuse_factor=2,
    )
    assert workers == 6


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


def test_worker_job_parses_timeout_settings() -> None:
    from openinflation_parser.orchestrator import WorkerJob

    job = WorkerJob.from_payload(
        {
            "job_id": "j1",
            "parser_name": "fixprice",
            "store_code": "C001",
            "output_dir": "./output",
            "api_timeout_ms": 120000,
        }
    )

    assert job.api_timeout_ms == 120000.0


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


def test_parser_registry_includes_perekrestok() -> None:
    parser_cls = get_parser("perekrestok")
    assert parser_cls.__name__ == "PerekrestokParser"


def test_parser_adapter_registry_includes_perekrestok() -> None:
    adapter = get_parser_adapter("perekrestok")
    assert adapter.name == "perekrestok"


def test_write_store_bundle_materializes_images_and_archives(tmp_path: Path) -> None:
    main_token = INLINE_IMAGE_TOKEN_PREFIX + "jpg:" + base64.b64encode(b"\xff\xd8\xffmain").decode("ascii")
    gallery_token = INLINE_IMAGE_TOKEN_PREFIX + "png:" + base64.b64encode(
        b"\x89PNG\r\n\x1a\ngallery"
    ).decode("ascii")

    schedule = Schedule.model_construct(open_from=None, closed_from=None)
    administrative_unit = AdministrativeUnit.model_construct(
        settlement_type=None,
        name="Москва",
        alias="moskva",
        country="RUS",
        region=None,
        longitude=None,
        latitude=None,
    )
    card = Card.model_construct(
        sku="sku-1",
        plu="plu-1",
        source_page_url=None,
        title="Product",
        description=None,
        adult=None,
        new=None,
        promo=None,
        season=None,
        hit=None,
        data_matrix=None,
        brand=None,
        producer_name=None,
        producer_country=None,
        composition=None,
        meta_data=None,
        expiration_date_in_days=None,
        rating=None,
        reviews_count=None,
        price=None,
        discount_price=None,
        loyal_price=None,
        wholesale_price=None,
        price_unit=None,
        unit=None,
        available_count=None,
        package_quantity=None,
        package_unit=None,
        categories_uid=None,
        main_image=main_token,
        images=[gallery_token],
    )
    store = RetailUnit.model_construct(
        retail_type=None,
        code="C001",
        address=None,
        schedule_weekdays=schedule,
        schedule_saturday=schedule,
        schedule_sunday=schedule,
        temporarily_closed=None,
        longitude=None,
        latitude=None,
        administrative_unit=administrative_unit,
        categories=None,
        products=[card],
    )
    worker_log = tmp_path / "worker.log"
    worker_log.write_text("worker-log-line\n", encoding="utf-8")

    json_path, archive_path = write_store_bundle(
        store,
        output_dir=str(tmp_path),
        store_code="C001",
        worker_log_path=str(worker_log),
    )

    json_file = Path(json_path)
    archive_file = Path(archive_path)
    assert json_file.is_file()
    assert archive_file.is_file()

    payload = json.loads(json_file.read_text(encoding="utf-8"))
    product = payload["products"][0]
    main_image_path = product["main_image"]
    gallery_image_path = product["images"][0]
    assert main_image_path.startswith("images/")
    assert main_image_path.endswith(".jpg")
    assert gallery_image_path.startswith("images/")
    assert gallery_image_path.endswith(".png")

    with tarfile.open(archive_file, "r:gz") as archive:
        names = set(archive.getnames())
        assert "meta.json" in names
        assert "worker.log" in names
        assert main_image_path in names
        assert gallery_image_path in names

        main_payload = archive.extractfile(main_image_path)
        gallery_payload = archive.extractfile(gallery_image_path)
        worker_payload = archive.extractfile("worker.log")
        assert main_payload is not None
        assert gallery_payload is not None
        assert worker_payload is not None
        assert main_payload.read() == b"\xff\xd8\xffmain"
        assert gallery_payload.read() == b"\x89PNG\r\n\x1a\ngallery"
        assert worker_payload.read().decode("utf-8") == "worker-log-line\n"


def test_write_store_bundle_drops_invalid_inline_image_tokens(tmp_path: Path) -> None:
    invalid_main_token = INLINE_IMAGE_TOKEN_PREFIX + "jpg:not-base64"
    invalid_gallery_token = INLINE_IMAGE_TOKEN_PREFIX + "png:bad-@@"

    schedule = Schedule.model_construct(open_from=None, closed_from=None)
    administrative_unit = AdministrativeUnit.model_construct(
        settlement_type=None,
        name="Москва",
        alias="moskva",
        country="RUS",
        region=None,
        longitude=None,
        latitude=None,
    )
    card = Card.model_construct(
        sku="sku-2",
        plu="plu-2",
        source_page_url=None,
        title="Product 2",
        description=None,
        adult=None,
        new=None,
        promo=None,
        season=None,
        hit=None,
        data_matrix=None,
        brand=None,
        producer_name=None,
        producer_country=None,
        composition=None,
        meta_data=None,
        expiration_date_in_days=None,
        rating=None,
        reviews_count=None,
        price=None,
        discount_price=None,
        loyal_price=None,
        wholesale_price=None,
        price_unit=None,
        unit=None,
        available_count=None,
        package_quantity=None,
        package_unit=None,
        categories_uid=None,
        main_image=invalid_main_token,
        images=[invalid_gallery_token],
    )
    store = RetailUnit.model_construct(
        retail_type=None,
        code="C002",
        address=None,
        schedule_weekdays=schedule,
        schedule_saturday=schedule,
        schedule_sunday=schedule,
        temporarily_closed=None,
        longitude=None,
        latitude=None,
        administrative_unit=administrative_unit,
        categories=None,
        products=[card],
    )

    json_path, archive_path = write_store_bundle(
        store,
        output_dir=str(tmp_path),
        store_code="C002",
    )
    payload = json.loads(Path(json_path).read_text(encoding="utf-8"))
    product = payload["products"][0]
    assert product["main_image"] is None
    assert product["images"] is None

    with tarfile.open(archive_path, "r:gz") as archive:
        names = set(archive.getnames())
        assert names == {"meta.json"}


def test_write_store_bundle_uses_prepared_images_dir(tmp_path: Path) -> None:
    schedule = Schedule.model_construct(open_from=None, closed_from=None)
    administrative_unit = AdministrativeUnit.model_construct(
        settlement_type=None,
        name="Москва",
        alias="moskva",
        country="RUS",
        region=None,
        longitude=None,
        latitude=None,
    )
    card = Card.model_construct(
        sku="sku-3",
        plu="plu-3",
        source_page_url=None,
        title="Product 3",
        description=None,
        adult=None,
        new=None,
        promo=None,
        season=None,
        hit=None,
        data_matrix=None,
        brand=None,
        producer_name=None,
        producer_country=None,
        composition=None,
        meta_data=None,
        expiration_date_in_days=None,
        rating=None,
        reviews_count=None,
        price=None,
        discount_price=None,
        loyal_price=None,
        wholesale_price=None,
        price_unit=None,
        unit=None,
        available_count=None,
        package_quantity=None,
        package_unit=None,
        categories_uid=None,
        main_image="images/sku-3/main_001_deadbeefdeadbeef.jpg",
        images=["images/sku-3/gallery_001_feedfacefeedface.png"],
    )
    store = RetailUnit.model_construct(
        retail_type=None,
        code="C003",
        address=None,
        schedule_weekdays=schedule,
        schedule_saturday=schedule,
        schedule_sunday=schedule,
        temporarily_closed=None,
        longitude=None,
        latitude=None,
        administrative_unit=administrative_unit,
        categories=None,
        products=[card],
    )

    prepared_images_root = tmp_path / "prepared" / "images" / "sku-3"
    prepared_images_root.mkdir(parents=True, exist_ok=True)
    (prepared_images_root / "main_001_deadbeefdeadbeef.jpg").write_bytes(b"\xff\xd8\xffprepared-main")
    (prepared_images_root / "gallery_001_feedfacefeedface.png").write_bytes(
        b"\x89PNG\r\n\x1a\nprepared-gallery"
    )

    json_path, archive_path = write_store_bundle(
        store,
        output_dir=str(tmp_path),
        store_code="C003",
        prepared_images_dir=str((tmp_path / "prepared" / "images")),
    )

    payload = json.loads(Path(json_path).read_text(encoding="utf-8"))
    product = payload["products"][0]
    assert product["main_image"] == "images/sku-3/main_001_deadbeefdeadbeef.jpg"
    assert product["images"] == ["images/sku-3/gallery_001_feedfacefeedface.png"]

    with tarfile.open(archive_path, "r:gz") as archive:
        names = set(archive.getnames())
        assert "meta.json" in names
        assert "images/sku-3/main_001_deadbeefdeadbeef.jpg" in names
        assert "images/sku-3/gallery_001_feedfacefeedface.png" in names
        main_payload = archive.extractfile("images/sku-3/main_001_deadbeefdeadbeef.jpg")
        gallery_payload = archive.extractfile("images/sku-3/gallery_001_feedfacefeedface.png")
        assert main_payload is not None
        assert gallery_payload is not None
        assert main_payload.read() == b"\xff\xd8\xffprepared-main"
        assert gallery_payload.read() == b"\x89PNG\r\n\x1a\nprepared-gallery"
