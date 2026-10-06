from unittest.mock import patch

import pytest

from reporters.reporter import ConvertedReporter, ObjectStoreReporter, S3Rec


def _rec(key: str, size: int = 10) -> S3Rec:
    return S3Rec(key, None, None, None, None, size, "STANDARD")


@pytest.fixture
def object_store_reporter():
    with patch("reporters.reporter.S3Client") as s3, patch("reporters.reporter.GrinClient"):
        paginator = s3.return_value.client.get_paginator.return_value
        paginator.paginate.return_value = [
            {"Contents": [_rec("a")._asdict(), _rec("b")._asdict()]},
            {},
        ]
        yield ObjectStoreReporter()


def test_can_retrieve_stored_objects(object_store_reporter):
    assert len(object_store_reporter.objects_in_store()) == 2


def test_report_barcodes_returns_keys(object_store_reporter):
    assert object_store_reporter.report(format="barcodes") == ["a", "b"]


def test_report_table_is_csv(object_store_reporter):
    out = object_store_reporter.report(format="table")
    assert out.splitlines()[0].startswith("Key,")


def test_converted_reporter_intersects():
    with (
        patch("reporters.reporter.S3Client"),
        patch("reporters.reporter.GrinClient") as grin,
        patch("reporters.reporter.objects_in_store", return_value=[_rec("a"), _rec("z")]),
    ):
        grin.return_value.converted_books = [{"barcode": "a"}, {"barcode": "b"}]
        table = dict(ConvertedReporter().report())
    assert table["converted and stored"] == 1
    assert table["converted, not yet stored"] == 1
