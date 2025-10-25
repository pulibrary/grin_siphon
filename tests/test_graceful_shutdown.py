import shutil
import signal
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pipeline.plumbing import Filter, Pipe, Token, dump_token


@pytest.fixture
def test_paths():
    """Create test directories for filter testing."""
    tmpdir = Path("/tmp/test_graceful_shutdown")
    if tmpdir.is_dir():
        shutil.rmtree(tmpdir)
    tmpdir.mkdir(parents=True)

    inpath = tmpdir / "in"
    inpath.mkdir(parents=True)

    outpath = tmpdir / "out"
    outpath.mkdir(parents=True)

    yield {"tmpdir": tmpdir, "inpath": inpath, "outpath": outpath}

    # Cleanup
    if tmpdir.is_dir():
        shutil.rmtree(tmpdir)


@pytest.fixture
def test_pipe(test_paths):
    """Create a test pipe."""
    return Pipe(test_paths["inpath"], test_paths["outpath"])


class TestFilter(Filter):
    """A simple test filter that just logs to the token."""

    def __init__(self, pipe: Pipe, poll_interval: int = 1) -> None:
        super().__init__(pipe, poll_interval)

    def validate_token(self, token) -> bool:
        return True

    def process_token(self, token) -> bool:
        self.log_to_token(token, "INFO", "test filter processed token")
        return True


class SlowFilter(Filter):
    """A filter that simulates slow processing."""

    def __init__(self, pipe: Pipe, poll_interval: int = 1, process_time: float = 2.0) -> None:
        super().__init__(pipe, poll_interval)
        self.process_time = process_time

    def validate_token(self, token) -> bool:
        return True

    def process_token(self, token) -> bool:
        # Simulate slow processing
        time.sleep(self.process_time)
        self.log_to_token(token, "INFO", "slow filter processed token")
        return True


def test_shutdown_flag_initialized(test_pipe):
    """Test that shutdown flag is initialized to False."""
    filter = TestFilter(test_pipe)
    assert filter.shutdown_requested is False


def test_handle_shutdown_sets_flag(test_pipe):
    """Test that shutdown handler sets the shutdown flag."""
    filter = TestFilter(test_pipe)
    assert filter.shutdown_requested is False

    # Simulate receiving SIGTERM
    filter._handle_shutdown(signal.SIGTERM, None)

    assert filter.shutdown_requested is True


def test_recover_orphaned_tokens(test_paths, test_pipe):
    """Test that orphaned .bak files are recovered on filter initialization."""
    # Create some orphaned .bak files
    token1 = Token({"barcode": "12345"})
    token2 = Token({"barcode": "67890"})

    bak_file1 = test_paths["inpath"] / "12345.bak"
    bak_file2 = test_paths["inpath"] / "67890.bak"

    dump_token(token1, bak_file1)
    dump_token(token2, bak_file2)

    assert bak_file1.exists()
    assert bak_file2.exists()

    # Initialize filter (should recover orphaned tokens)
    filter = TestFilter(test_pipe)

    # Check that .bak files were converted to .json
    json_file1 = test_paths["inpath"] / "12345.json"
    json_file2 = test_paths["inpath"] / "67890.json"

    assert json_file1.exists()
    assert json_file2.exists()
    assert not bak_file1.exists()
    assert not bak_file2.exists()


def test_no_orphaned_tokens_on_clean_start(test_pipe):
    """Test that filter initializes normally when no orphaned tokens exist."""
    # Should not raise any errors
    filter = TestFilter(test_pipe)
    assert filter is not None


def test_run_once_with_shutdown_flag(test_paths, test_pipe):
    """Test that run_once still processes token even if shutdown is requested."""
    # Create a token
    token = Token({"barcode": "11111"})
    token_file = test_paths["inpath"] / "11111.json"
    dump_token(token, token_file)

    filter = TestFilter(test_pipe)
    filter.shutdown_requested = True

    # run_once should still process the token
    result = filter.run_once()

    assert result is True
    # Token should be moved to output
    assert not token_file.exists()
    assert (test_paths["outpath"] / "11111.json").exists()


def test_run_forever_exits_on_shutdown(test_paths, test_pipe):
    """Test that run_forever exits when shutdown is requested."""
    # Create a token
    token = Token({"barcode": "22222"})
    token_file = test_paths["inpath"] / "22222.json"
    dump_token(token, token_file)

    filter = TestFilter(test_pipe, poll_interval=0.1)

    # Set up to trigger shutdown after a short delay
    def delayed_shutdown():
        time.sleep(0.3)
        filter.shutdown_requested = True

    import threading

    shutdown_thread = threading.Thread(target=delayed_shutdown)
    shutdown_thread.start()

    # This should process the token and then exit
    filter.run_forever()

    shutdown_thread.join()

    # Verify the filter exited gracefully
    assert filter.shutdown_requested is True
    # Token should have been processed
    assert (test_paths["outpath"] / "22222.json").exists()


def test_shutdown_completes_current_token(test_paths, test_pipe):
    """Test that shutdown waits for current token to complete processing."""
    # Create a token
    token = Token({"barcode": "33333"})
    token_file = test_paths["inpath"] / "33333.json"
    dump_token(token, token_file)

    # Use slow filter to simulate long processing
    filter = SlowFilter(test_pipe, poll_interval=0.1, process_time=1.0)

    def trigger_shutdown_during_processing():
        # Wait a bit to ensure processing has started
        time.sleep(0.2)
        filter.shutdown_requested = True

    import threading

    shutdown_thread = threading.Thread(target=trigger_shutdown_during_processing)
    shutdown_thread.start()

    # Start processing
    start_time = time.time()
    filter.run_forever()
    elapsed_time = time.time() - start_time

    shutdown_thread.join()

    # Verify token was fully processed despite shutdown request
    assert (test_paths["outpath"] / "33333.json").exists()
    # Should have taken at least the processing time
    assert elapsed_time >= 1.0


def test_orphaned_token_during_processing(test_paths, test_pipe):
    """Test recovery of token that was being processed when filter was killed."""
    # Simulate a token that was being processed (.bak file exists)
    token = Token({"barcode": "44444"})
    bak_file = test_paths["inpath"] / "44444.bak"
    dump_token(token, bak_file)

    # Create new filter instance (simulating restart)
    filter = TestFilter(test_pipe)

    # Token should have been recovered to .json
    json_file = test_paths["inpath"] / "44444.json"
    assert json_file.exists()
    assert not bak_file.exists()

    # Should be able to process the recovered token
    result = filter.run_once()
    assert result is True
    assert (test_paths["outpath"] / "44444.json").exists()


def test_multiple_orphaned_tokens_recovered(test_paths, test_pipe):
    """Test that multiple orphaned tokens are all recovered."""
    # Create multiple orphaned tokens
    for i in range(5):
        token = Token({"barcode": f"5555{i}"})
        bak_file = test_paths["inpath"] / f"5555{i}.bak"
        dump_token(token, bak_file)

    # Initialize filter
    filter = TestFilter(test_pipe)

    # All should be recovered
    for i in range(5):
        json_file = test_paths["inpath"] / f"5555{i}.json"
        bak_file = test_paths["inpath"] / f"5555{i}.bak"
        assert json_file.exists()
        assert not bak_file.exists()


def test_signal_handlers_registered(test_pipe):
    """Test that signal handlers are properly registered."""
    original_sigterm = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)

    filter = TestFilter(test_pipe)

    # Signal handlers should be set
    assert signal.getsignal(signal.SIGTERM) != original_sigterm
    assert signal.getsignal(signal.SIGINT) != original_sigint
