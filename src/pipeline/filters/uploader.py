import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from clients import S3Client
from pipeline.plumbing import Filter, Pipe, Token


class Uploader(Filter):
    """
    Base class for uploading processed files to object storage.

    Provides common functionality for validating and uploading files,
    with specific implementations for different storage backends.
    """

    def __init__(self, pipe: Pipe) -> None:
        super().__init__(pipe)

    def infile(self, token: Token) -> Path:
        """Get the path to the processed file to upload.

        Args:
            token (Token): Token containing barcode and processing bucket

        Returns:
            Path: Path to the .tgz file to upload
        """
        input_path = Path(token.content["processing_bucket"])
        input_filename: Path = Path(token.content["barcode"]).with_suffix(".tgz")
        return input_path / input_filename

    def validate_token(self, token) -> bool:
        status: bool = True
        if self.infile(token).exists() is False:
            logging.error(f"source file does not exist: {self.infile(token)}")
            self.log_to_token(token, "ERROR", f"source file does not exist: {self.infile(token)}")
            status = False
        return status

    def process_token(self, token: Token) -> bool:
        raise NotImplementedError("Subclasses must implement 'process_token'.")


class AWSUploader(Uploader):
    """
    Uploader implementation for AWS S3 object storage.

    Handles uploading processed book files to S3, including duplicate
    detection to avoid re-uploading existing objects.

    Attributes:
        client (S3Client): S3 client for storage operations
    """

    def __init__(self, pipe: Pipe, s3_client: S3Client) -> None:
        super().__init__(pipe)
        self.client = s3_client

    def validate_token(self, token: Token) -> bool:
        status: bool = True
        barcode = token.get_prop("barcode")
        if barcode is not None:
            if self.client.object_exists(barcode):
                token.put_prop("upload_status", "duplicate")
        return status

    def process_token(self, token: Token) -> bool:
        """Upload the processed file to S3 storage.

        Checks for duplicates and either skips upload or stores the object.
        Updates token with upload status.

        Args:
            token (Token): Token containing file and metadata

        Returns:
            bool: True if upload succeeded or was skipped (duplicate), False otherwise
        """
        successflg = False
        barcode = token.get_prop("barcode")

        logging.debug(f"Store operation starting: {barcode}")
        if token.get_prop("upload_status") == "duplicate":
            self.log_to_token(token, "INFO", "Object already stored.")
            successflg = True

        else:
            status = self.client.store_object(barcode)

            logging.debug(f"Store operation complete: {barcode}")
            if status is True:
                self.log_to_token(token, "INFO", "Object stored")
                token.put_prop("upload_status", "success")
                token.put_prop("when_uploaded", str(datetime.now(timezone.utc)))
                successflg = True
            else:
                logging.error(f"{self.stage_name} did not store object {barcode}: {status}")
                self.log_to_token(token, "ERROR", "Object not stored")
                token.put_prop("upload_status", "fail")

                successflg = False

        return successflg


if __name__ == "__main__":
    from pipeline.config_loader import load_config
    from pipeline.logging_config import configure_logging
    import argparse

    if "OBJECT_STORE" not in os.environ:
        print("Please set the OBJECT_STORE environment variable.")
        sys.exit(1)

    if "LOCAL_DIR" not in os.environ:
        print("Please set the LOCAL_DIR environment variable (probably the processing bucket).")
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
    s3_client = S3Client(os.environ.get("LOCAL_DIR"), os.environ.get("OBJECT_STORE"))

    uploader: AWSUploader = AWSUploader(pipe, s3_client)
    logging.debug("starting uploader")
    uploader.run_forever()
