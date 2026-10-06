from pathlib import Path

import pytest

from pipeline.book_ledger import BookLedger
from pipeline.plumbing import Pipeline, Token, dump_token
from reporters.progress_reporter import ProgressReport, human_bytes

LEDGER = """barcode,date_chosen,date_completed,status
b1,,,
b2,,,
b3,,,
b4,,,
b5,,,
b6,,,
"""


@pytest.fixture
def setup(tmp_path):
    ledger_file = tmp_path / "ledger.csv"
    ledger_file.write_text(LEDGER)
    buckets = {}
    for name in ("start", "done"):
        buckets[name] = tmp_path / name
        buckets[name].mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    config = {
        "global": {"token_archive": str(archive)},
        "buckets": [{"name": n, "path": str(p)} for n, p in buckets.items()],
    }
    ledger = BookLedger(str(ledger_file))
    pipeline = Pipeline(config)
    return config, ledger, pipeline, buckets, archive


def test_human_bytes():
    assert human_bytes(10) == "10 B"
    assert human_bytes(1024**3) == "1.00 GiB"


def test_counts_volume_and_notes(setup):
    config, ledger, pipeline, buckets, archive = setup
    # b1, b2 stored (b1 has a page count in archive, b2 does not); b3 failed in ledger;
    # b4 has an .err token; b5 waiting in start; b6 untouched.
    ledger.mark_book_failed("b3", "boom")
    ledger.mark_book_completed("b1")
    dump_token(Token({"barcode": "b1", "page_count": 120}), archive / "b1.json")
    dump_token(Token({"barcode": "b4"}), buckets["start"] / "b4.err")
    dump_token(Token({"barcode": "b5"}), buckets["start"] / "b5.json")
    sizes = {"b1": 1000, "b2": 2000, "zz": 5}
    report = ProgressReport(config, ledger, pipeline, lambda: sizes)

    d = report._collect()
    assert d["total"] == 6
    assert d["transferred"] == 2
    assert d["errored"] == 2
    assert d["remaining"] == 2
    assert d["remaining_in_pipeline"] == 1
    assert d["transferred"] + d["errored"] + d["remaining"] == d["total"]
    assert d["bytes"] == 3005
    assert d["pages"] == 120
    assert d["books_without_pages"] == 2
    assert d["not_in_ledger"] == 1

    text = report.report()
    assert text.startswith("# GRIN Siphon progress report")
    assert "## Progress by count" in text and "## Progress by volume" in text
    assert "| Transferred" in text
    assert "lower bound" in text
    assert "awaiting `tidy errors`" in text


def test_err_token_for_stored_book_not_counted_as_error(setup):
    config, ledger, pipeline, buckets, _ = setup
    dump_token(Token({"barcode": "b1"}), buckets["start"] / "b1.err")
    report = ProgressReport(config, ledger, pipeline, lambda: {"b1": 1})
    assert report._collect()["errored"] == 0


def test_sections(setup):
    config, ledger, pipeline, *_ = setup
    report = ProgressReport(config, ledger, pipeline, lambda: {})
    assert "by volume" not in report.report(section="count")
    assert "by count" not in report.report(section="volume")


def test_list_sizes_reports_progress():
    from unittest.mock import MagicMock, patch

    from clients.object_store import S3Client

    with patch("clients.object_store.boto3"):
        client = S3Client(Path("/tmp"), "bkt")
    pages = [
        {"Contents": [{"Key": "a", "Size": 1}, {"Key": "b", "Size": 2}]},
        {"Contents": [{"Key": "c", "Size": 3}]},
        {},
    ]
    client.client = MagicMock()
    client.client.get_paginator.return_value.paginate.return_value = pages
    seen = []
    assert client.list_sizes(seen.append) == {"a": 1, "b": 2, "c": 3}
    assert seen == [2, 3, 3]
