import logging
import os
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path

from clients import GrinClient
from pipeline.plumbing import Filter, Pipe, Token

class Requester(Filter):
    """
    Pipeline filter that initiates conversion requests for books.

    The Requester filter takes tokens containing book barcodes and submits
    them to the GRIN service for conversion processing. This is typically
    the first processing stage in the pipeline.
    """

    class ERRORS(StrEnum):
        NOTALLOWED = "Not allowed to be downloaded"
        OTHERERROR = "Other error"

    def __init__(self, pipe: Pipe) -> None:
        super().__init__(pipe)

    def validate_token(self, token: Token) -> bool:
        """Validate that the token contains required fields for conversion request.

        Args:
            token (Token): Token to validate

        Returns:
            bool: Always returns True as all tokens are valid for conversion requests
        """
        return True

    def process_token(self, token: Token) -> bool:
        """Submit a book conversion request to the GRIN service.

        Extracts the barcode from the token and submits it to the GRIN client
        for conversion processing. Logs the response status to the token.

        Args:
            token (Token): Token containing the book barcode to convert

        Returns:
            bool: True if the conversion request was successful, False otherwise
        """
        successflg = False
        barcode = token.content["barcode"]
        response = GrinClient().convert_book(barcode)
        if response is not None:
            status = response[barcode]
            if status in self.ERRORS:
                logging.error(f"request error for {barcode}: {status}")
                self.log_to_token(token, "ERROR", status)
                successflg = False
            else:
                self.log_to_token(token, "INFO", status)
                token.put_prop("when_requested", str(datetime.now(timezone.utc)))
                successflg = True
        else:
            logging.error(f"submission of barcode for conversion failed: {barcode}")
            successflg = False

        return successflg


if __name__ == "__main__":
    from pipeline.config_loader import load_config
    from pipeline.logging_config import configure_logging
    import argparse


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

    requester: Requester = Requester(pipe)
    logging.info("starting requester")
    requester.run_forever()
