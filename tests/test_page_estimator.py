import io
import random
import tarfile
from unittest.mock import MagicMock

from pipeline.book_ledger import BookLedger
from pipeline.filters.decryptor import count_pages_in
from pipeline.plumbing import Pipeline, Token, dump_token
from reporters.page_estimator import PageSample, estimate_pages, top_up_sample
from reporters.progress_reporter import ProgressReport, round_sig


def test_count_pages_in_non_seekable_stream():
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name in ("1.jp2", "2.jp2", "1.txt"):
            info = tarfile.TarInfo(name)
            info.size = 1
            tar.addfile(info, io.BytesIO(b"x"))

    class Forward:
        def __init__(self, data):
            self._b = io.BytesIO(data)

        def read(self, n=-1):
            return self._b.read(n)

    assert count_pages_in(Forward(buf.getvalue())) == 2


def test_estimate_recovers_known_ratio():
    # exactly 1 page per 100 bytes
    sample = [(p, p * 100) for p in (100, 200, 300, 400)]
    est, low, high = estimate_pages(sample, rest_bytes=1_000_000)
    assert est == 10_000
    assert low <= est <= high


def test_estimate_needs_enough_data():
    assert estimate_pages([], 10) is None
    assert estimate_pages([(5, 100)], 10) is None


def test_top_up_is_persistent_and_skips_failures(tmp_path):
    sample = PageSample(tmp_path)
    candidates = {f"b{i}": 100 * (i + 1) for i in range(10)}

    def count(barcode):
        if barcode == "b3":
            raise OSError("boom")
        return int(candidates[barcode] / 100)

    added = top_up_sample(sample, candidates, count, 5, random.Random(1))
    assert added <= 5 and added == len(sample.data)
    assert "b3" not in sample.data
    assert PageSample(tmp_path).data == sample.data  # reloaded from disk
    before = dict(sample.data)
    top_up_sample(sample, candidates, count, 5, random.Random(2))
    assert all(sample.data[k] == v for k, v in before.items())  # earlier measurements kept
    assert len(sample.data) <= 5


def test_report_uses_sample_for_estimate(tmp_path):
    ledger_file = tmp_path / "ledger.csv"
    ledger_file.write_text("barcode,date_chosen,date_completed,status\n" + "".join(
        f"b{i},,,\n" for i in range(6)))
    archive = tmp_path / "archive"
    archive.mkdir()
    done = tmp_path / "done"
    done.mkdir()
    config = {
        "global": {"token_archive": str(archive)},
        "buckets": [{"name": "done", "path": str(done)}],
    }
    dump_token(Token({"barcode": "b0", "page_count": 10}), archive / "b0.json")
    sizes = {f"b{i}": 1000 for i in range(6)}
    report = ProgressReport(config, BookLedger(str(ledger_file)), Pipeline(config), lambda: sizes)

    # b1..b3 measured by sampling at exactly 1 page per 100 bytes (10 pages each)
    count = MagicMock(return_value=10)
    assert report.estimate_pages(target=3, count_fn=count) == 3
    assert count.call_count == 3

    text = report.report(section="volume")
    assert "Pages measured in sample" in text
    assert "Total pages (estimated)" in text
    assert "≈ 60" in text  # 10 recorded + 30 measured + ~20 estimated for the other 2 books


def test_round_sig():
    assert round_sig(89_403_099) == 89_400_000
    assert round_sig(81_527_200) == 81_500_000
    assert round_sig(96_991_309) == 97_000_000
    assert round_sig(42) == 42
    assert round_sig(0) == 0


def test_sample_size_comes_from_config(tmp_path):
    ledger_file = tmp_path / "ledger.csv"
    ledger_file.write_text(
        "barcode,date_chosen,date_completed,status\n" + "".join(f"b{i},,,\n" for i in range(6))
    )
    archive = tmp_path / "archive"
    archive.mkdir()
    config = {"global": {"token_archive": str(archive), "page_sample_size": 2}, "buckets": []}
    sizes = {f"b{i}": 1000 for i in range(6)}
    report = ProgressReport(config, BookLedger(str(ledger_file)), Pipeline(config), lambda: sizes)
    assert report.estimate_pages(count_fn=lambda b: 5) == 2
    assert report.estimate_pages(target=4, count_fn=lambda b: 5) == 2  # explicit target wins
