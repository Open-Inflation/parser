from __future__ import annotations

from openinflation_parser.parsers.perekrestok.catalog import PerekrestokCatalogMixin
from openinflation_parser.parsers.runtime import ParserRuntimeMixin


class _DummyPerekrestokCatalog(PerekrestokCatalogMixin, ParserRuntimeMixin):
    pass


def test_image_url_from_node_uses_width_height_from_contract() -> None:
    url = _DummyPerekrestokCatalog._image_url_from_node(
        {
            "cropUrlTemplate": (
                "https://tsx.x5static.net/i/%s/xdelivery/files/4e/21/"
                "2fca192f74d11d27f95310b5233b.jpg"
            ),
            "width": 898,
            "height": 898,
        }
    )
    assert (
        url
        == "https://tsx.x5static.net/i/898x898/xdelivery/files/4e/21/2fca192f74d11d27f95310b5233b.jpg"
    )


def test_image_url_from_node_without_placeholder_returns_template_as_is() -> None:
    url = _DummyPerekrestokCatalog._image_url_from_node(
        {
            "cropUrlTemplate": "https://tsx.x5static.net/static/image.jpg",
            "width": 100,
            "height": 100,
        }
    )
    assert url == "https://tsx.x5static.net/static/image.jpg"


def test_image_url_from_node_with_missing_dimensions_returns_none() -> None:
    url = _DummyPerekrestokCatalog._image_url_from_node(
        {
            "cropUrlTemplate": "https://tsx.x5static.net/i/%s/xdelivery/files/path.jpg",
            "width": None,
            "height": 200,
        }
    )
    assert url is None
