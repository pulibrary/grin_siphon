import json
import logging
import signal
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Optional

logger: logging.Logger = logging.getLogger(__name__)


class Token:
    """
    Represents a processing unit with barcode and metadata.

    A Token is the fundamental unit of work in the pipeline, containing
    all metadata and processing history for a book as it moves through
    the pipeline stages.

    Attributes:
        content (dict): Dictionary containing token metadata including barcode,
                       processing history, and stage-specific data.
    """

    def __init__(self, content: dict):
        self.content = content

    def __repr__(self) -> str:
        return f"Token({self.name})"

    def get_prop(self, prop) -> str | None:
        return self.content.get(prop)

    def put_prop(self, prop: str, val: str) -> str | None:
        self.content[prop] = val
        return self.get_prop(val)

    @property
    def name(self) -> str | None:
        return self.get_prop("barcode")

    def write_log(self, message: str, level: Optional[str] = None, stage: Optional[str] = None):
        """Add a log entry to the token's processing history.

        Args:
            message (str): Log message describing the event
            level (Optional[str]): Log level (e.g., 'INFO', 'ERROR', 'WARNING')
            stage (Optional[str]): Pipeline stage name where the event occurred
        """
        entry: dict = {"timestamp": str(datetime.now(timezone.utc)), "message": message}
        if stage:
            entry["stage"] = stage
        if level:
            entry["level"] = level

        self.content.setdefault("log", []).append(entry)


# Utilities for reading and writing Tokens


def load_token(token_file: Path) -> Token:
    """Load a token from a JSON file.

    Args:
        token_file (Path): Path to the JSON token file

    Returns:
        Token: The loaded token instance
    """
    with token_file.open("r") as f:
        token_info = json.load(f)
        return Token(token_info)


def dump_token(token, destination: Path) -> None:
    """Save a token to a JSON file.

    Args:
        token (Token): The token to save
        destination (Path): Path where the token file should be written
    """
    with destination.open("w+") as f:
        json.dump(token.content, fp=f, indent=2)


class Pipe:
    """
    Manages token flow between two pipeline buckets.

    A Pipe handles the movement of tokens from an input bucket to an output
    bucket, including token locking (marking) to prevent concurrent processing,
    error handling, and atomic operations.

    Attributes:
        input (Path): Input bucket directory path
        output (Path): Output bucket directory path
        token (Token | None): Currently held token being processed
    """

    def __init__(self, in_path: Path, out_path: Path) -> None:
        self.input = in_path
        self.output = out_path
        self.token: Token | None = None

    def __repr__(self) -> str:
        return f"Pipe('{self.input}', '{self.output}')"

    def in_path(self, token) -> Path:
        if token is not None and token.name is not None:
            return self.input / Path(token.name).with_suffix(".json")
        else:
            raise ValueError("no token or token name")

    def out_path(self, token) -> Path:
        if self.token and self.token.name:
            return self.output / Path(token.name).with_suffix(".json")
        else:
            raise ValueError("no token or token name")

    def marked_path(self, token) -> Path:
        if self.token and self.token.name:
            return self.input / Path(token.name).with_suffix(".bak")
        else:
            raise ValueError("no token or token name")

    def error_path(self, token) -> Path:
        if self.token and self.token.name:
            return self.input / Path(token.name).with_suffix(".err")
        else:
            raise ValueError("no token or token name")

    @property
    def token_in_path(self) -> Path:
        if self.token and self.token.name:
            return self.input
        else:
            raise ValueError("pipe doesn't contain a token")

    @property
    def token_out_path(self) -> Path:
        if self.token and self.token.name:
            return self.output

        else:
            raise ValueError("pipe doesn't contain a token")

    @property
    def token_marked_path(self) -> Path:
        if self.token and self.token.name:
            return self.input
        else:
            raise ValueError("pipe doesn't contain a token")

    @property
    def token_error_path(self) -> Path:
        if self.token and self.token.name:
            return self.input
        else:
            raise ValueError("pipe doesn't contain a token")

    def list_input_tokens(self) -> list[Token] | None:
        all_tokens = []
        for f in self.input.glob("*.json"):
            all_tokens.append(load_token(f))
        return all_tokens

    def take_token(self, barcode: str | None = None):
        """Take the next available token from the input bucket.

        Finds the first available JSON token file, loads it, and marks it
        as being processed (renames to .bak extension) to prevent concurrent
        access by other processes.

        Returns:
            Token | None: The taken token, or None if no tokens are available
        """
        # Ensure we don't have a token already being processed
        if self.token is not None:
            logging.error("there's already a current token")
            return None

        if barcode is None:
            try:
                # Find the first available token file in the input bucket
                token_path = next(self.input.glob("*.json"))
                # Load the token and mark it as being processed
                self.token = load_token(token_path)
                self.mark_token()  # Rename to .bak to prevent concurrent access
                return self.token

            except StopIteration:
                # No tokens available for processing
                return None
        else:
            try:
                token_path = self.input / Path(barcode).with_suffix(".json")
                self.token = load_token(token_path)
                self.mark_token()  # Rename to .bak to prevent concurrent access
                return self.token
            except FileNotFoundError:
                logging.error(f"{token_path} does not exist")
                return None

    def mark_token(self):
        """Mark the current token as being processed by renaming its file."""
        if self.token and self.token.name:
            unmarked_path: Path = self.in_path(self.token)
            marked_path: Path = self.marked_path(self.token)
            if unmarked_path.is_file():
                # Rename .json to .bak to signal it's being processed
                unmarked_path.rename(marked_path)
            else:
                FileNotFoundError(f"{unmarked_path} does not exist")

    def delete_marked_token(self):
        marked_path: Path = self.marked_path(self.token)
        marked_path.unlink()

    def put_token(self, errorFlg: bool = False) -> None:
        """Move the current token to the output bucket or error state.

        Args:
            errorFlg (bool): If True, save token with .err extension instead
                           of moving to output bucket. Defaults to False.
        """
        if self.token:
            if errorFlg:
                error_path = self.error_path(self.token)
                dump_token(self.token, error_path)
            else:
                dump_token(self.token, self.out_path(self.token))

            self.delete_marked_token()
            self.token = None

    def put_token_back(self, errorFlg: bool = False) -> None:
        if self.token:
            if errorFlg:
                put_back_path = self.error_path(self.token)
            else:
                put_back_path = self.in_path(self.token)

            dump_token(self.token, put_back_path)

            self.delete_marked_token()
            self.token = None


class Filter:
    """
    Base class for pipeline processing stages.

    Filters are the individual processing stages that transform tokens as they
    flow through the pipeline. Each filter implements specific validation and
    processing logic while handling errors and logging consistently.

    Attributes:
        pipe (Pipe): The pipe for token input/output operations
        stage_name (str): Name of the processing stage for logging
    """

    def __init__(self, pipe: Pipe, poll_interval: int = 5):
        self.pipe = pipe
        self.stage_name: str = self.__class__.__name__.lower()
        self.poll_interval = poll_interval
        self.shutdown_requested = False

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        # Recover any orphaned tokens from previous interrupted runs
        self._recover_orphaned_tokens()

    def log_to_token(self, token, level, message):
        token.write_log(message, level, self.stage_name)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals (SIGTERM, SIGINT) gracefully.

        Sets a flag to request shutdown after the current token completes
        processing. This ensures tokens are not left in an inconsistent state.

        Args:
            signum: Signal number
            frame: Current stack frame
        """
        logging.info(f"{self.stage_name}: Shutdown signal received, will exit after current token")
        self.shutdown_requested = True

    def _recover_orphaned_tokens(self):
        """Recover orphaned .bak files from previous interrupted runs.

        When a filter process is killed abruptly, tokens may be left in .bak
        state. This method converts them back to .json so they can be
        reprocessed on the next run.
        """
        for bak_file in self.pipe.input.glob("*.bak"):
            json_file = bak_file.with_suffix(".json")
            logging.warning(
                f"{self.stage_name}: Recovering orphaned token: {bak_file.name} -> {json_file.name}"
            )
            bak_file.rename(json_file)

    def run_once(self) -> bool:
        """Process a single token if available.

        Takes a token from the input pipe, validates it, processes it,
        and moves it to the appropriate output location (success or error).

        Returns:
            bool: True if a token was processed (successfully or with error),
                 False if no tokens were available
        """
        token: Token | None = self.pipe.take_token()
        if not token:
            # logging.info("No tokens available")
            return False

        if self.validate_token(token) is False:
            self.log_to_token(token, "ERROR", "Token did not validate")
            logging.error("token did not validate")
            self.pipe.put_token(errorFlg=True)

            return False

        try:
            processed: bool = self.process_token(token)
            if processed:
                logging.debug(f"Processed token: {token.name}")
                self.log_to_token(token, "INFO", "Stage completed successfully")
                self.pipe.put_token()
            else:
                logging.error(f"Did not proces token: {token.name}")
                self.log_to_token(token, "ERROR", "Stage did not run successfully")
                self.pipe.put_token(errorFlg=True)

            return True

        except Exception as e:
            self.log_to_token(token, "ERROR", f"in {self.stage_name}: {str(e)}")
            logging.error(f"Error processing {token.name}: {str(e)}")
            self.pipe.put_token(errorFlg=True)
            return False

    def run_forever(self):
        """Continuously process tokens with polling and graceful shutdown.

        Processes tokens in a loop until a shutdown signal is received.
        When shutdown is requested, completes the current token and exits
        cleanly to avoid leaving tokens in an inconsistent state.

        Args:
            poll_interval (int): Seconds to wait between polls when no
                               tokens are available. Defaults to 5.
        """
        while not self.shutdown_requested:
            if not self.run_once():
                sleep(self.poll_interval)

        logging.info(f"{self.stage_name}: Exiting gracefully")

    def process_token(self, token: Token):
        """Process a token - must be implemented by subclasses.

        Args:
            token (Token): The token to process

        Returns:
            bool: True if processing succeeded, False otherwise

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement this")

    def validate_token(self, token: Token) -> bool:
        """Validate a token before processing - must be implemented by subclasses.

        Args:
            token (Token): The token to validate

        Returns:
            bool: True if token is valid for processing, False otherwise

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement this")


class Pipeline:
    """
    Manages bucket directories and token flow throughout the pipeline.

    The Pipeline class provides the infrastructure for token-based processing,
    managing the directory structure (buckets) and providing utilities for
    creating pipes between processing stages.

    Attributes:
        config (dict): Pipeline configuration dictionary
        buckets (dict): Mapping of bucket names to Path objects
    """

    def __init__(self, config: dict | None = None):
        self.config = config
        self.buckets = {}
        if config is not None:
            for rec in self.config.get("buckets", {}):
                name = rec.get("name", "")
                location = Path(rec.get("path", "/dev/null"))
                self.add_bucket(name, location)

    def add_bucket(self, name: str, location: Path):
        self.buckets[name] = location

    def bucket(self, name: str) -> Path:
        if p := self.buckets.get(name):
            return p
        else:
            raise ValueError(f"no such bucket: {name}")

    def pipe(self, in_bucket: str, out_bucket: str):
        """Create a pipe between two buckets.

        Args:
            in_bucket (str): Name of the input bucket
            out_bucket (str): Name of the output bucket

        Returns:
            Pipe: A pipe instance for moving tokens between the buckets
        """
        return Pipe(self.bucket(in_bucket), self.bucket(out_bucket))

    @property
    def snapshot(self):
        """Get current status of all buckets in the pipeline.

        Returns:
            dict: Dictionary mapping bucket names to status information including
                 waiting tokens (.json), errored tokens (.err), and tokens
                 currently being processed (.bak)
        """
        buckets = {}
        for name, location in self.buckets.items():
            info = {
                "waiting_tokens": [f.name for f in Path(location).glob("*.json")],
                "errored_tokens": [f.name for f in Path(location).glob("*.err")],
                "in_process_tokens": [f.name for f in Path(location).glob("*.bak")],
            }
            buckets[name] = info
        return buckets
