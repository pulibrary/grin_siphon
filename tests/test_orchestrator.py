import shutil
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import pytest


@pytest.fixture
def test_config():
    """Create a minimal test configuration for the orchestrator."""
    tmpdir = Path("/tmp/test_orchestrator")
    if tmpdir.is_dir():
        shutil.rmtree(tmpdir)
    tmpdir.mkdir(parents=True)

    start_bucket = tmpdir / "start"
    start_bucket.mkdir(parents=True)

    requested_bucket = tmpdir / "requested"
    requested_bucket.mkdir(parents=True)

    converted_bucket = tmpdir / "converted"
    converted_bucket.mkdir(parents=True)

    config = {
        "global": {
            "log_level": "INFO",
            "poll_interval": 5,
        },
        "buckets": [
            {"name": "start", "path": str(start_bucket)},
            {"name": "requested", "path": str(requested_bucket)},
            {"name": "converted", "path": str(converted_bucket)},
        ],
        "filters": [
            {
                "name": "requester",
                "class": "Requester",
                "script": "src/pipeline/filters/requester.py",
                "pipe": {"in": "start", "out": "requested"},
            },
            {
                "name": "downloader",
                "class": "Downloader",
                "script": "src/pipeline/filters/downloader.py",
                "pipe": {"in": "converted", "out": "requested"},
            },
        ],
    }
    return config


@pytest.fixture
def orchestrator(test_config):
    """Create an orchestrator instance with mocked config."""
    # Mock the module-level config load before importing
    with patch("pipeline.config_loader.load_config", return_value=test_config):
        # Clear the module cache to force reimport with our mocked config
        if "pipeline.orchestrator" in sys.modules:
            del sys.modules["pipeline.orchestrator"]

        from pipeline.orchestrator import Orchestrator

        # Patch the config in the orchestrator module
        with patch("pipeline.orchestrator.config", test_config):
            orch = Orchestrator()
            yield orch
            # Cleanup: stop any running processes
            orch.stop_filters()


def test_start_filter_by_name_success(orchestrator, test_config):
    """Test successfully starting a specific filter by name."""
    with patch("pipeline.orchestrator.config", test_config):
        # Mock subprocess.Popen to avoid actually starting a process
        with patch("subprocess.Popen") as mock_popen:
            mock_process = Mock()
            mock_process.poll.return_value = None  # Process is running
            mock_popen.return_value = mock_process

            result = orchestrator.start_filter_by_name("requester")

            assert result is True
            assert len(orchestrator.processes) == 1
            assert orchestrator.processes[0][0] == "requester"
            mock_popen.assert_called_once()


def test_start_filter_by_name_not_found(orchestrator, test_config):
    """Test attempting to start a filter that doesn't exist in config."""
    with patch("pipeline.orchestrator.config", test_config):
        result = orchestrator.start_filter_by_name("nonexistent_filter")

        assert result is False
        assert len(orchestrator.processes) == 0


def test_start_filter_by_name_already_running(orchestrator, test_config):
    """Test attempting to start a filter that is already running."""
    with patch("pipeline.orchestrator.config", test_config):
        with patch("subprocess.Popen") as mock_popen:
            mock_process = Mock()
            mock_process.poll.return_value = None  # Process is running
            mock_popen.return_value = mock_process

            # Start the filter once
            result1 = orchestrator.start_filter_by_name("requester")
            assert result1 is True
            assert len(orchestrator.processes) == 1

            # Try to start it again
            result2 = orchestrator.start_filter_by_name("requester")
            assert result2 is False
            assert len(orchestrator.processes) == 1  # Still only one process


def test_stop_filter_by_name_success(orchestrator, test_config):
    """Test successfully stopping a specific filter by name."""
    with patch("pipeline.orchestrator.config", test_config):
        with patch("subprocess.Popen") as mock_popen:
            mock_process = Mock()
            mock_process.poll.return_value = None  # Process is running
            mock_popen.return_value = mock_process

            # Start a filter
            orchestrator.start_filter_by_name("requester")
            assert len(orchestrator.processes) == 1

            # Stop it
            result = orchestrator.stop_filter_by_name("requester")

            assert result is True
            assert len(orchestrator.processes) == 0
            mock_process.terminate.assert_called_once()
            mock_process.wait.assert_called_once_with(timeout=5)


def test_stop_filter_by_name_not_found(orchestrator, test_config):
    """Test attempting to stop a filter that isn't running."""
    with patch("pipeline.orchestrator.config", test_config):
        result = orchestrator.stop_filter_by_name("nonexistent_filter")

        assert result is False


def test_stop_filter_by_name_with_timeout(orchestrator, test_config):
    """Test stopping a filter that doesn't terminate gracefully."""
    with patch("pipeline.orchestrator.config", test_config):
        with patch("subprocess.Popen") as mock_popen:
            mock_process = Mock()
            mock_process.poll.return_value = None
            # Simulate timeout on wait
            mock_process.wait.side_effect = subprocess.TimeoutExpired("cmd", 5)
            mock_popen.return_value = mock_process

            # Start a filter
            orchestrator.start_filter_by_name("requester")
            assert len(orchestrator.processes) == 1

            # Stop it (should timeout and then kill)
            result = orchestrator.stop_filter_by_name("requester")

            assert result is True
            assert len(orchestrator.processes) == 0
            mock_process.terminate.assert_called_once()
            mock_process.wait.assert_called_once_with(timeout=5)
            mock_process.kill.assert_called_once()


def test_stop_filter_by_name_removes_correct_process(orchestrator, test_config):
    """Test that stopping a filter removes only that filter from processes list."""
    with patch("pipeline.orchestrator.config", test_config):
        with patch("subprocess.Popen") as mock_popen:
            # Create two different mock processes
            mock_process1 = Mock()
            mock_process1.poll.return_value = None
            mock_process2 = Mock()
            mock_process2.poll.return_value = None

            mock_popen.side_effect = [mock_process1, mock_process2]

            # Start two filters
            orchestrator.start_filter_by_name("requester")
            orchestrator.start_filter_by_name("downloader")
            assert len(orchestrator.processes) == 2

            # Stop only one
            result = orchestrator.stop_filter_by_name("requester")

            assert result is True
            assert len(orchestrator.processes) == 1
            assert orchestrator.processes[0][0] == "downloader"
            mock_process1.terminate.assert_called_once()
            mock_process2.terminate.assert_not_called()


def test_start_multiple_filters_then_stop_individually(orchestrator, test_config):
    """Integration test: start multiple filters and stop them individually."""
    with patch("pipeline.orchestrator.config", test_config):
        with patch("subprocess.Popen") as mock_popen:
            mock_process1 = Mock()
            mock_process1.poll.return_value = None
            mock_process2 = Mock()
            mock_process2.poll.return_value = None

            mock_popen.side_effect = [mock_process1, mock_process2]

            # Start two filters
            assert orchestrator.start_filter_by_name("requester") is True
            assert orchestrator.start_filter_by_name("downloader") is True
            assert len(orchestrator.processes) == 2

            # Stop them individually
            assert orchestrator.stop_filter_by_name("downloader") is True
            assert len(orchestrator.processes) == 1

            assert orchestrator.stop_filter_by_name("requester") is True
            assert len(orchestrator.processes) == 0
