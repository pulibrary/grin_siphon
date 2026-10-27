import tempfile
from pathlib import Path

from pipeline.filters.requester import Requester
from pipeline.plumbing import Pipe, Token, dump_token

# def test_request_success():
#     with tempfile.TemporaryDirectory() as tmpdir:
#         pipe_in = Path(tmpdir) / "in"
#         pipe_out = Path(tmpdir) / "out"

#         pipe_in.mkdir()
#         pipe_out.mkdir()

#         pipe = Pipe(pipe_in, pipe_out)

#         # This token should fail
#         tok = Token({"barcode": "12345"})
#         dump_token(tok, pipe_in / Path(tok.name).with_suffix(".json"))


#         requester = Requester(pipe)
#         requester.run_once()

#         assert len(list(pipe.input.glob("*.*"))) == 0
#         assert len(list(pipe.output.glob("*.*"))) == 1


def test_request_failure():
    with tempfile.TemporaryDirectory() as tmpdir:
        pipe_in = Path(tmpdir) / "in"
        pipe_out = Path(tmpdir) / "out"

        pipe_in.mkdir()
        pipe_out.mkdir()

        pipe = Pipe(pipe_in, pipe_out)

        # This token should fail
        tok = Token({"barcode": "12345"})
        dump_token(tok, pipe_in / Path(tok.name).with_suffix(".json"))

        assert len(list(pipe.input.glob("*.json"))) == 1
        assert len(list(pipe.input.glob("*.err"))) == 0
        assert len(list(pipe.output.glob("*.*"))) == 0

        requester = Requester(pipe)
        requester.run_once()
        assert len(list(pipe.input.glob("*.json"))) == 0
        assert len(list(pipe.input.glob("*.err"))) == 1
        assert len(list(pipe.output.glob("*.*"))) == 0


def test_request_not_yet_available(monkeypatch):
    """GRIN's 'Not yet available' reply means the book was never queued, so it is an error."""
    import pipeline.filters.requester as requester_module

    class FakeGrinClient:
        def convert_book(self, barcode):
            return {barcode: "Not yet available for conversion"}

    monkeypatch.setattr(requester_module, "GrinClient", FakeGrinClient)

    with tempfile.TemporaryDirectory() as tmpdir:
        pipe_in = Path(tmpdir) / "in"
        pipe_out = Path(tmpdir) / "out"
        pipe_in.mkdir()
        pipe_out.mkdir()
        pipe = Pipe(pipe_in, pipe_out)

        tok = Token({"barcode": "12345"})
        dump_token(tok, pipe_in / Path(tok.name).with_suffix(".json"))

        Requester(pipe).run_once()
        assert len(list(pipe.input.glob("*.err"))) == 1
        assert len(list(pipe.output.glob("*.*"))) == 0
