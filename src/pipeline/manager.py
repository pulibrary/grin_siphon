import logging
import os
import signal
import sys
from pathlib import Path

from tabulate import tabulate

from pipeline.book_ledger import BookLedger
from pipeline.config_loader import load_config
from pipeline.plumbing import Pipeline
from pipeline.secretary import Secretary
from pipeline.stager import Stager
from pipeline.synchronizer import Synchronizer
from pipeline.token_bag import TokenBag
from reporters.reporter import StatusReporter


class Manager:
    """
    High-level coordinator that manages the token bag, ledger, and staging operations.

    The Manager serves as the main interface for controlling the pipeline workflow,
    providing a REPL interface for interactive management of books, tokens, and
    pipeline operations.

    Attributes:
        config (dict): Pipeline configuration dictionary
        ledger (BookLedger): Manages available books and processing status
        token_bag (TokenBag): Holds tokens ready for processing
        secretary (Secretary): Manages book selection from ledger into token bag
        stager (Stager): Moves tokens from bag to pipeline start
        pipeline (Pipeline): Manages bucket directories and token flow
        synchronizer (Synchronizer): Syncs GRIN converted files with pipeline
        processes (list): List of running subprocess references
        commands (dict): Available REPL commands and their handlers
    """

    def __init__(self, config: dict):
        self.config = config
        self.ledger = BookLedger(config["global"]["ledger_file"])
        self.token_bag = TokenBag(config["global"]["token_bag"])
        self.secretary = Secretary(self.token_bag, self.ledger)
        processing_bucket = Path(config["global"]["processing_bucket"])
        # Find the start bucket path from the bucket configuration
        start_bucket = Path(
            [bucket["path"] for bucket in self.config["buckets"] if bucket["name"] == "start"][
                0
            ]  # Take the first (and should be only) start bucket
        )
        self.stager = Stager(self.secretary, processing_bucket, start_bucket)
        self.pipeline = Pipeline(config)
        self.synchronizer = Synchronizer(config)
        self.processes = []
        self.commands = {
            "exit": {"help": "exit manager", "fn": self._exit_command},
            "pipeline status": {
                "help": "print pipeline status",
                "fn": lambda: print(self.pipeline_status),
            },
            "ledger status": {
                "help": "print ledger status",
                "fn": lambda: print(self.ledger_status),
            },
            "token bag status": {
                "help": "print token bag status",
                "fn": lambda: print(self.token_bag_status),
            },
            "fill token bag": {
                "help": "fill token bag with tokens",
                "fn": self._fill_token_bag_command,
            },
            "synchronize": {
                "help": "sync GRIN converted with pipeline",
                "fn": self._synchronize_command,
            },
            "stage": {
                "help": "put the tokens into the pipeline",
                "fn": self._stage_command,
            },
            "status": {
                "help": "get status of GRIN queues",
                "fn": self._status_command,
            },
            "show errors": {
                "help": "list all error tokens with failure reason",
                "fn": self._show_errors_command,
            },
            "tidy errors": {
                "help": "record error tokens as failed in ledger and remove them",
                "fn": self._tidy_errors_command,
            },
            "tidy done": {
                "help": "mark completed tokens in done bucket as completed in ledger and remove them",
                "fn": self._tidy_done_command,
            },
            "help": {"help": "show commands", "fn": self._help_command},
        }

    @property
    def pipeline_status(self):
        return self.pipeline.snapshot

    @property
    def ledger_status(self):
        return {
            "chosen": len(self.ledger.all_chosen_books),
            "completed": len(self.ledger.all_completed_books),
            "failed": len(self.ledger.all_failed_books),
            "unprocessed": len(self.ledger.all_unprocessed_books),
        }

    @property
    def token_bag_status(self):
        return self.secretary.bag_size

    def fill_token_bag(self, how_many: int | None = None):
        """Fill the token bag with a specified number of books from the ledger.

        Args:
            how_many (int): Number of books to select from the ledger. Defaults to 1.
        """
        if how_many is None:
            how_many_input = input("How many tokens to take (int)? ")
            how_many = int(how_many_input)
        try:
            self.secretary.choose_books(how_many)
            self.secretary.commit()
        except ValueError:
            print("number of tokens must be an integer")

    def stage(self):
        """Stage tokens from the token bag into the pipeline start bucket.

        Updates token metadata and moves tokens from the bag directory to the
        pipeline's start bucket for processing.
        """
        self.stager.update_tokens()
        self.stager.stage()

    def repl(self):
        """Run the interactive Read-Eval-Print Loop for pipeline management.

        Provides a command-line interface for managing pipeline operations
        including status checks, token bag management, and pipeline control.
        """
        while True:
            cmd = input("manager> ").strip()
            if cmd in self.commands:
                if self.commands[cmd]["fn"]():
                    break
            else:
                print(f"Unknown command: {cmd}")

    def _exit_command(self):
        print("\nExiting Manager.")
        return True  # Signal to exit

    def _fill_token_bag_command(self):
        self.fill_token_bag()
        print(self.token_bag_status)
        return False  # Don't exit

    def _help_command(self):
        for k, v in self.commands.items():
            print(f"{k}: {v.get('help')}")
        return False

    def _synchronize_command(self):
        synced = self.synchronizer.synchronize(stage=True)
        print(f"Number of files synchronized: {len(synced)}")
        return False

    def _stage_command(self):
        self.stager.update_tokens()
        self.stager.stage()
        print("Staged.")
        return False

    def _status_command(self):
        reporter = StatusReporter(config)
        report = reporter.report()
        if report is not None:
            print(tabulate(report))

        return False

    def _last_error_message(self, token) -> str | None:
        log = token.content.get("log", [])
        for entry in reversed(log):
            if entry.get("level") in ("ERROR", "WARNING"):
                return entry.get("message")
        return None

    def _show_errors_command(self):
        from pipeline.plumbing import load_token

        rows = []
        for name, path in self.pipeline.buckets.items():
            for err_file in sorted(path.glob("*.err")):
                token = load_token(err_file)
                reason = self._last_error_message(token) or "unknown"
                rows.append([token.name, name, reason])
        if rows:
            print(tabulate(rows, headers=["barcode", "bucket", "reason"]))
        else:
            print("No error tokens found.")
        return False

    def _tidy_errors_command(self):
        from pipeline.plumbing import load_token

        count = 0
        for name, path in self.pipeline.buckets.items():
            for err_file in sorted(path.glob("*.err")):
                token = load_token(err_file)
                barcode = token.name
                reason = self._last_error_message(token)
                try:
                    self.ledger.mark_book_failed(barcode, reason)
                    err_file.unlink()
                    count += 1
                except ValueError:
                    print(f"  Warning: {barcode} not found in ledger, skipping")
        if count:
            self.ledger.write_ledger()
            print(f"Marked {count} book(s) as failed and removed error tokens.")
        else:
            print("No error tokens found.")
        return False

    def _tidy_done_command(self):
        from pipeline.plumbing import load_token

        done_path = self.pipeline.buckets.get("done")
        if done_path is None:
            print("No 'done' bucket configured.")
            return False
        count = 0
        for token_file in sorted(done_path.glob("*.json")):
            token = load_token(token_file)
            barcode = token.name
            try:
                self.ledger.mark_book_completed(barcode)
                token_file.unlink()
                count += 1
            except ValueError:
                print(f"  Warning: {barcode} not found in ledger, skipping")
        if count:
            self.ledger.write_ledger()
            print(f"Marked {count} book(s) as completed and removed done tokens.")
        else:
            print("No done tokens found.")
        return False

    def run(self):
        """Run the Manager with signal handlers for graceful shutdown.

        Sets up SIGINT and SIGTERM handlers and starts the REPL interface.
        The manager will handle Ctrl+C and termination signals gracefully.
        """

        def shutdown_handler(signum, frame):
            print("\nShutting down Manager...")
            # do other things
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        self.repl()


if __name__ == "__main__":
    from pipeline.config_loader import load_config

    config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
    config: dict = load_config(config_path)

    # Set up logging
    log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())

    logging.basicConfig(level=log_level)

    manager = Manager(config)
    manager.run()
