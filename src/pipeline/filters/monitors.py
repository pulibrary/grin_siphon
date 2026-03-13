import logging
import os
import sys
from pathlib import Path

from clients import GrinClient
from pipeline.plumbing import Filter, Pipe, Token

logger: logging.Logger = logging.getLogger(__name__)


class Monitor(Filter):
    """
    A subclass of Filter that processes all the tokens in
    the input bucket at once and then waits. A standard Filter
    keeps taking tokens from the input bucket until that
    bucket is empty; Monitors check for some condition, and if
    the condition is not met, they put the token back, so the
    wait interval has to occur after a run through all the tokens.
    """

    def __init__(self, pipe: Pipe, poll_interval: int = 60) -> None:
        super().__init__(pipe, poll_interval)

    def set_up_run(self):
        pass  # implemented by subclasses

    def run_once(self) -> bool:
        """
        Process all the tokens in the input pipe.
        """
        return_val = True  # always return True

        # First set up the run: class-specific actions
        self.set_up_run()

        # Then, get a list of all the tokens in the input pipe.

        barcodes = [tok.name for tok in self.pipe.list_input_tokens()]
        for barcode in barcodes:
            token: Token | None = self.pipe.take_token(barcode)
            if token and self.validate_token(token):
                try:
                    processed: bool = self.process_token(token)
                    if processed:
                        self.log_to_token(token, "INFO", f"{self.stage_name} ran successfully")
                        self.pipe.put_token()
                    else:
                        logging.error(f"{self.stage_name} did not process {token.name}")
                        self.log_to_token(
                            token,
                            "WARNING",
                            f"{self.stage_name} did not run successfully",
                        )
                        self.pipe.put_token(errorFlg=True)

                except Exception as e:
                    self.log_to_token(token, "ERROR", f"in {self.stage_name}: {str(e)}")
                    logging.error(f"Error processing {token.name}: {str(e)}")
                    self.pipe.put_token(errorFlg=True)
        return return_val

    def validate_token(self, token: Token) -> bool:
        """Validate that the token has required fields for downloading.

        Args:
            token (Token): Token to validate

        Returns:
            bool: Always returns True as all tokens are valid for monitoring
        """
        return True


class RequestMonitor(Monitor):
    """Monitors the Requested bucket, examining
    each token to see if its book has been converted
    by GRIN yet.  If so, it moves the token to the
    Converted bucket."""

    def __init__(self, pipe, poll_interval) -> None:
        super().__init__(pipe, poll_interval)
        self._converted_barcodes = None
        self._in_process_barcodes = None
        self._failed_info = None

    def set_up_run(self):
        self._converted_barcodes = None
        self._in_process_barcodes = None
        self._failed_info = None

    @property
    def converted_barcodes(self):
        if self._converted_barcodes is None:
            client = GrinClient()
            self._converted_barcodes = [rec["barcode"] for rec in client.converted_books]
        return self._converted_barcodes

    @property
    def in_process_barcodes(self):
        if self._in_process_barcodes is None:
            client = GrinClient()
            self._in_process_barcodes = [rec["barcode"] for rec in client.in_process_books]
        return self._in_process_barcodes

    @property
    def failed_info(self):
        """Dict mapping barcode -> convert_failed_info for all books in GRIN's failed queue."""
        if self._failed_info is None:
            client = GrinClient()
            self._failed_info = {
                rec["barcode"]: rec["convert_failed_info"]
                for rec in client.failed_books
            }
        return self._failed_info

    def is_in_process(self, token: Token) -> bool:
        return token.get_prop("barcode") in self.in_process_barcodes

    def is_converted(self, token: Token) -> bool:
        return token.get_prop("barcode") in self.converted_barcodes

    def run_once(self) -> bool:
        self.set_up_run()

        barcodes = [tok.name for tok in self.pipe.list_input_tokens()]

        for barcode in barcodes:
            token: Token | None = self.pipe.take_token(barcode)
            if token:
                if self.is_in_process(token):
                    self.pipe.put_token_back()

                elif self.is_converted(token):
                    self.log_to_token(token, "INFO", "Book has been converted")
                    self.pipe.put_token()

                elif barcode in self.failed_info:
                    info = self.failed_info[barcode]
                    if "Temporary" in info:
                        self.log_to_token(token, "WARNING", f"Temporary GRIN conversion failure, will retry: {info}")
                        self.pipe.put_token_back()
                    else:
                        self.log_to_token(token, "ERROR", f"GRIN conversion failed: {info}")
                        self.pipe.put_token(errorFlg=True)

                else:
                    self.log_to_token(
                        token,
                        "ERROR",
                        "Book is in neither in_process nor converted queues",
                    )
                    self.pipe.put_token(errorFlg=True)


if __name__ == "__main__":
    if "POLL_INTERVAL" not in os.environ:
        print("Please set the POLL_INTERVAL environment variable.")
        sys.exit(1)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))

    monitor = RequestMonitor(pipe, int(os.environ.get("POLL_INTERVAL")))
    logger.info("starting request monitor")
    monitor.run_forever()
