import shutil
from pathlib import Path

import pytest

from pipeline.book_ledger import BookLedger
from pipeline.manager import Manager
from pipeline.plumbing import Pipeline


@pytest.fixture
def test_config(shared_datadir):
    tmpdir = Path("/tmp/test_manager")
    if tmpdir.is_dir():
        shutil.rmtree(tmpdir)
    tmpdir.mkdir

    test_token_dir = Path(tmpdir) / "tokens"
    test_token_dir.mkdir(parents=True)

    ledger_file = shared_datadir / "test_ledger.csv"
    test_ledger_file = tmpdir / "ledger.csv"
    shutil.copy(ledger_file, test_ledger_file)

    processing_bucket = Path(tmpdir) / "processing_bucket"
    processing_bucket.mkdir(parents=True)

    start_bucket = Path(tmpdir) / "start_bucket"
    start_bucket.mkdir(parents=True)

    requested_bucket = Path(tmpdir) / "requested_bucket"
    requested_bucket.mkdir(parents=True)

    converted_bucket = Path(tmpdir) / "converted_bucket"
    converted_bucket.mkdir(parents=True)

    config = {}
    config["global"] = {}
    config["global"]["ledger_file"] = str(test_ledger_file)
    config["global"]["token_bag"] = str(test_token_dir)
    config["global"]["processing_bucket"] = str(processing_bucket)
    config["buckets"] = [
        {"name": "start", "path": start_bucket},
        {"name": "requested", "path": requested_bucket},
        {"name": "converted", "path": converted_bucket},
    ]
    return config


def test_manager(shared_datadir, test_config, monkeypatch):
    manager = Manager(test_config)
    every_book = {book.barcode for book in manager.ledger.books.values()}
    monkeypatch.setattr(manager, "grin_processed_barcodes", lambda: every_book)
    pipeline = Pipeline(test_config)

    assert isinstance(manager.pipeline_status, str)
    assert len(pipeline.snapshot["start"]["waiting_tokens"]) == 0
    assert len(list(pipeline.bucket("start").glob("*.json"))) == 0

    status = manager.ledger_status
    assert status["chosen"] == 0
    assert status["completed"] == 0
    assert status["unprocessed"] == 9

    assert manager.token_bag_status == 0

    manager.fill_token_bag(5)
    assert manager.token_bag_status == 5
    assert manager.ledger_status["chosen"] == 5
    assert manager.ledger_status["completed"] == 0
    assert manager.ledger_status["unprocessed"] == 4
    assert all([tok.get_prop("processing_bucket") is None for tok in manager.secretary.bag.tokens])

    manager.stage()

    assert manager.token_bag_status == 0
    waiting_in_start = pipeline.snapshot["start"]["waiting_tokens"]
    assert len(waiting_in_start) == 5

    assert all(
        [
            tok.get_prop("processing_bucket") == str(test_config["global"]["processing_bucket"])
            for tok in manager.secretary.bag.tokens
        ]
    )


def test_status_command_uses_instance_config(test_config, capsys):
    from unittest.mock import patch

    manager = Manager(test_config)
    with patch("pipeline.manager.StatusReporter") as reporter:
        reporter.return_value.report.return_value = [["all", 3]]
        assert manager._status_command() is False
    reporter.assert_called_once_with(test_config)
    assert "all" in capsys.readouterr().out


def test_report_commands(test_config, capsys, tmp_path):
    from unittest.mock import patch

    test_config["global"]["report_dir"] = str(tmp_path / "reports")
    manager = Manager(test_config)
    with patch("reporters.progress_reporter.S3Client") as s3:
        s3.return_value.list_sizes.return_value = {"345": 2048}
        assert manager.commands["report progress"]["fn"]() is False
        out = capsys.readouterr().out
        assert "Progress by count" in out and "Progress by volume" in out
        assert "2.00 KiB" in out
        assert manager.commands["save report"]["fn"]() is False
    saved = list((tmp_path / "reports").glob("progress-*.md"))
    assert len(saved) == 1
    assert "Progress by count" in saved[0].read_text()


def test_fill_token_bag_skips_books_grin_has_not_processed(test_config, monkeypatch):
    manager = Manager(test_config)
    processed = {"32101078166681", "32101078165139"}
    monkeypatch.setattr(manager, "grin_processed_barcodes", lambda: processed)

    manager.fill_token_bag(5)

    assert manager.token_bag_status == 2
    assert {book.barcode for book in manager.ledger.all_chosen_books} == processed


def test_tidy_errors_resets_books_not_yet_available(test_config, tmp_path):
    from pipeline.filters.requester import Requester
    from pipeline.plumbing import Token, dump_token

    test_config["global"]["token_archive"] = str(tmp_path / "archive")
    manager = Manager(test_config)
    start = manager.pipeline.bucket("start")

    unavailable = Token({"barcode": "32101078166681"})
    unavailable.write_log(Requester.ERRORS.NOTAVAILABLE, "ERROR")
    unavailable.write_log("Stage did not run successfully", "ERROR")
    dump_token(unavailable, start / "32101078166681.err")
    broken = Token({"barcode": "32101078165139"})
    broken.write_log("decryption failed", "ERROR")
    broken.write_log("Stage did not run successfully", "ERROR")
    dump_token(broken, start / "32101078165139.err")
    manager.ledger.choose_book("32101078166681")
    manager.ledger.choose_book("32101078165139")

    manager._tidy_errors_command()

    ledger = BookLedger(test_config["global"]["ledger_file"])
    assert ledger.entry("32101078166681").status is None
    assert ledger.entry("32101078166681").date_chosen is None
    assert ledger.entry("32101078165139").status == "failed"
    assert ledger.entry("32101078165139").failure_reason == "decryption failed"
    assert list(start.glob("*.err")) == []
    assert (tmp_path / "archive" / "unavailable" / "32101078166681.err").exists()
