import shutil
from pathlib import Path

import pytest

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


def test_manager(shared_datadir, test_config):
    manager = Manager(test_config)
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
    from collections import namedtuple
    from unittest.mock import patch

    Obj = namedtuple("Obj", ["Key", "Size"])
    test_config["global"]["report_dir"] = str(tmp_path / "reports")
    manager = Manager(test_config)
    with patch("reporters.progress_reporter.S3Client") as s3:
        s3.return_value.list_objects.return_value = [Obj("345", 2048)]
        assert manager.commands["report progress"]["fn"]() is False
        out = capsys.readouterr().out
        assert "Progress by count" in out and "Progress by volume" in out
        assert "2.00 KiB" in out
        assert manager.commands["save report"]["fn"]() is False
    saved = list((tmp_path / "reports").glob("progress-*.md"))
    assert len(saved) == 1
    assert "Progress by count" in saved[0].read_text()
