import logging
import os
import sys
from pathlib import Path

from clients import GrinClient
from pipeline.plumbing import Filter, Pipe, Token




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

    def set_up_run(self):
        self._converted_barcodes = None
        self._in_process_barcodes = None

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

    def is_in_process(self, token: Token) -> bool:
        return token.get_prop("barcode") in self.in_process_barcodes

    def is_converted(self, token: Token) -> bool:
        return token.get_prop("barcode") in self.converted_barcodes

    def run_once(self) -> bool:
        # First, set up the run
        self.set_up_run()

        # Then, get a list of all the tokens in the input pipe.
        barcodes = [tok.name for tok in self.pipe.list_input_tokens()]

        # Iterate over the list of barcodes. If the barcode is in the
        # in_process list from GRIN, leave it where it is.  If it is
        # in the converted list from GRIN, move it to the converted_bucket.
        # If it is in neither, raise an error.

        for barcode in barcodes:
            token: Token | None = self.pipe.take_token(barcode)
            if token:
                if self.is_in_process(token):
                    self.pipe.put_token_back()

                elif self.is_converted(token):
                    self.log_to_token(token, "INFO", "Book has been converted")
                    self.pipe.put_token()

                else:
                    self.log_to_token(
                        token,
                        "ERROR",
                        "Book is in neither in_proces or converted queues",
                    )
                    self.pipe.put_token(errorFlg=True)


if __name__ == "__main__":
    from pipeline.config_loader import load_config
    from pipeline.logging_config import configure_logging
    import argparse

    if "POLL_INTERVAL" not in os.environ:
        print("Please set the POLL_INTERVAL environment variable.")
        sys.exit(1)

    if "PIPELINE_CONFIG" not in os.environ:
        print("Please set the PIPELINE_CONFIG environment variable.")
        sys.exit(1)

    config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
    config: dict = load_config(config_path)
        
    # Set up logging
    log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())
    configure_logging(log_level)


    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))

    monitor = RequestMonitor(pipe, int(os.environ.get("POLL_INTERVAL")))
    logging.info("starting request monitor")
    monitor.run_forever()
