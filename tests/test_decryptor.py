import io
import tarfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pipeline.filters.decryptor import Decryptor, count_pages
from pipeline.plumbing import Pipe, Token

BARCODE = "1234567"


def make_tarball(path: Path, names: list[str]) -> None:
    with tarfile.open(path, "w:gz") as tar:
        for name in names:
            info = tarfile.TarInfo(name)
            info.size = 1
            tar.addfile(info, io.BytesIO(b"x"))


@pytest.fixture
def decryptor(tmp_path, monkeypatch):
    monkeypatch.setenv("DECRYPTION_PASSPHRASE", "secret")
    (tmp_path / "in").mkdir()
    (tmp_path / "out").mkdir()
    (tmp_path / "proc").mkdir()
    (tmp_path / "proc" / f"{BARCODE}.tar.gz.gpg").write_text("encrypted")
    return Decryptor(Pipe(tmp_path / "in", tmp_path / "out"))


@pytest.fixture
def token(tmp_path):
    return Token({"barcode": BARCODE, "processing_bucket": str(tmp_path / "proc")})


def test_count_pages_counts_only_page_images(tmp_path):
    tarball = tmp_path / "book.tgz"
    make_tarball(tarball, ["00000001.jp2", "00000002.JP2", "00000001.txt", "notes.xml"])
    assert count_pages(tarball) == 2


def test_process_token_records_page_count(decryptor, token):
    def fake_gpg(cmd, **kwargs):
        make_tarball(Path(cmd[cmd.index("--output") + 1]), ["1.jp2", "2.jp2", "3.jp2"])
        return SimpleNamespace(returncode=0)

    with patch("pipeline.filters.decryptor.subprocess.run", side_effect=fake_gpg):
        assert decryptor.process_token(token) is True
    assert token.content["page_count"] == 3


def test_unreadable_tarball_does_not_fail_decryption(decryptor, token):
    def fake_gpg(cmd, **kwargs):
        Path(cmd[cmd.index("--output") + 1]).write_text("not a tarball")
        return SimpleNamespace(returncode=0)

    with patch("pipeline.filters.decryptor.subprocess.run", side_effect=fake_gpg):
        assert decryptor.process_token(token) is True
    assert "page_count" not in token.content
    assert token.content["decryption_status"] == "success"
    assert any("Could not count pages" in e["message"] for e in token.content["log"])
