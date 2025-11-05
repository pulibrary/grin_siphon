import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pipeline.plumbing import Filter, Pipe, Token

class Cleaner(Filter):
    """
    Final cleanup filter that moves processed files to a finished directory.

    The Cleaner filter performs the final stage of processing by deleting
    the processed tarball.

    Attributes:
        finished_bucket (Path): Directory where completed files are stored
    """

    def __init__(self, pipe: Pipe, finished_bucket: str | None = None) -> None:
        super().__init__(pipe)
        if finished_bucket is not None:
            self.finished_bucket = Path(finished_bucket)
        else:
            self.finished_bucket = Path("/dev/null")

    def source_file(self, token: Token) -> Path:
        """Get the path to the source file to be moved.

        Args:
            token (Token): Token containing barcode and processing bucket

        Returns:
            Path: Path to the .tgz file in the processing bucket
        """
        input_path = Path(token.content["processing_bucket"])
        input_filename: Path = Path(token.content["barcode"]).with_suffix(".tgz")
        return input_path / input_filename

    def destination_file(self, token: Token) -> Path:
        """Get the destination path for the file in the finished bucket.

        Args:
            token (Token): Token containing barcode

        Returns:
            Path: Path where the file will be moved in the finished bucket
        """
        filename = Path(token.content["barcode"]).with_suffix(".tgz")
        destination_path = self.finished_bucket / filename
        return destination_path

    def validate_token(self, token) -> bool:
        """Validate that source file and target directory exist.

        Args:
            token (Token): Token to validate

        Returns:
            bool: True if source file exists and target directory is valid
        """
        status: bool = True
        if self.source_file(token).exists() is False:
            logging.error(f"file to clean does not exist: {self.source_file(token)}")
            self.log_to_token(
                token,
                "ERROR",
                f"file to clean does not exist: {self.source_file(token)}",
            )
            status = False

        if self.finished_bucket.is_dir() is False:
            logging.error(f"target directory does not exist: {self.finished_bucket}")
            self.log_to_token(
                token,
                "ERROR",
                f"target directory does not exist: {self.finished_bucket}",
            )
            status = False

        return status

    def process_token(self, token: Token, retain_file=False) -> bool:
        """Move the processed file to the finished directory.

        Args:
            token (Token): Token containing file paths

        Returns:
            bool: True if file was moved successfully, False otherwise
        """
        successflg = False
        try:
            if retain_file is True:
                self.source_file(token).rename(self.destination_file(token))
                self.log_to_token(token, "INFO", "Object moved to done")
            else:
                self.source_file(token).unlink()
                self.log_to_token(token, "INFO", "Object deleted")
            token.put_prop("when_finished", str(datetime.now(timezone.utc)))
            successflg = True
        except PermissionError as e:
            self.log_to_token(token, "ERROR", f"Object not moved! {e}")
            successflg = False
        return successflg


class SeedingCleaner(Cleaner):
    """
    An intermediate cleaner, while we work through the
    backlogged converted pile.

    This cleaner deletes the processed tarball to free space
    and then moves a new token from the token_bag to the
    converted_bucket.
    """

    def __init__(
        self,
        pipe: Pipe,
        finished_bucket: str | None = None,
        token_bag: str | None = None,
        converted_bucket: str | None = None,
    ) -> None:
        super().__init__(pipe, finished_bucket)
        if converted_bucket is not None:
            self.converted_bucket = Path(converted_bucket)

        if token_bag is not None:
            self.token_bag = Path(token_bag)

    def process_token(self, token: Token, retain_file=False) -> bool:
        successflg = False
        try:
            if retain_file is True:
                self.source_file(token).rename(self.destination_file(token))
                self.log_to_token(token, "INFO", "Object moved to done")
            else:
                self.source_file(token).unlink()
                self.log_to_token(token, "INFO", "Object deleted")
            successflg = True
        except PermissionError as e:
            self.log_to_token(token, "ERROR", f"Object not moved! {e}")
            successflg = False

        if successflg:
            try:
                # Use a generator expression to find the first file and next() to retrieve it
                first_file = next(
                    (item for item in self.token_bag.iterdir() if item.is_file()), None
                )

                target_file = self.converted_bucket / first_file.name
                first_file.rename(target_file)
                successflg = True

            except FileNotFoundError:
                successflg = False

        return successflg


if __name__ == "__main__":
    from pipeline.config_loader import load_config
    from pipeline.logging_config import configure_logging
    import argparse

    if "PIPELINE_CONFIG" not in os.environ:
        print("Please set the PIPELINE_CONFIG environment variable.")
        sys.exit(1)

    if "FINISHED_BUCKET" not in os.environ:
        print("Please set the FINISHED_BUCKET environment variable.")
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
    finished_bucket = os.environ.get("FINISHED_BUCKET")

    cleaner: Cleaner = Cleaner(pipe, finished_bucket)
    logging.info("starting cleaner")
    cleaner.run_forever()
