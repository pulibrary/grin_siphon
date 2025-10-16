# downloader.py

# Use to download files from GRIN.

import logging
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

from clients import GrinClient
from pipeline.plumbing import Filter, Pipe, Token

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

logger: logging.Logger = logging.getLogger(__name__)

# Utility functions for measuring free disk space


def free_space(path: str | Path) -> int:
    """Return free space in bytes on the filesystem containing `path`."""
    usage = shutil.disk_usage(path)
    return usage.free


def has_enough_space(target_dir: str | Path, url: str, margin: float = 1.05) -> bool:
    """Check if there's enough disk space to download `url` into `target_dir`."""
    # Get file size from HTTP HEAD
    response = httpx.head(url, allow_redirects=True)
    size = int(response.headers.get("Content-Length", 0))
    return free_space(target_dir) > size * margin


def not_enough_space(target_dir: str | Path, margin: float = 0.002) -> bool:
    """Returns True if we're almost out of space."""
    usage = shutil.disk_usage(target_dir)
    return usage.free / usage.total < margin


class Downloader(Filter):
    """
    Pipeline filter that downloads converted files from the GRIN service.

    The Downloader filter retrieves converted book files from GRIN after
    conversion requests have been processed. It downloads files to the
    processing bucket specified in the token.

    Downloader idles if the disk is getting too full.
    """

    def run_once(self) -> bool:
        """Don't run if the disk is getting too full;
        otherwise invoke parent method."""
        if self.enough_space():
            super().run_once()
        else:
            return False

    def __init__(self, pipe: Pipe, download_bucket: str, disk_threshold: float = 0.02):
        super().__init__(pipe)
        self.download_bucket = Path(download_bucket)
        self.disk_threshold = disk_threshold

    def enough_space(self) -> bool:
        usage = shutil.disk_usage(self.download_bucket)
        return usage.free / usage.total > self.disk_threshold

    def enough_space_for(self, token) -> bool:
        """Check if there's enough disk space to download `url` into `target_dir`."""
        dest = token.content["processing_bucket"]
        # Get file size from HTTP HEAD
        response = httpx.head(url, allow_redirects=True)
        size = int(response.headers.get("Content-Length", 0))
        return free_space(dest) > size * margin

    def validate_token(self, token: Token) -> bool:
        """Validate that the token has required fields for downloading.

        Args:
            token (Token): Token to validate

        Returns:
            bool: Return False if there isn't enough room to
                  download the file.
        """
        validp: bool = True
        if not self.enough_space_for(token):
            logging.error("not enough space to download file")
            self.log_to_token(token, "ERROR", "Not enough room to download file")
            validp = False

        return validp

    def process_token(self, token: Token) -> bool:
        """Download the converted book file from GRIN.

        Uses the GrinClient to download the book file to the processing bucket
        directory specified in the token.

        Args:
            token (Token): Token containing barcode and processing bucket info

        Returns:
            bool: True if download completed successfully
        """
        completed: bool = False
        barcode = token.content["barcode"]
        dest = str(Path(token.content["processing_bucket"]))
        grin_client = GrinClient()
        grin_client.download_book(barcode, dest)
        token.put_prop("when_downloaded", str(datetime.now(timezone.utc)))
        completed = True
        return completed


if __name__ == "__main__":
    if "DOWNLOAD_BUCKET" not in os.environ:
        print("Please set the DOWNLOAD_BUCKET environment variable.")
        sys.exit(1)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))
    download_bucket = os.environ.get("DOWNLOAD_BUCKET")

    downloader: Downloader = Downloader(pipe, download_bucket)
    logger.info("starting downloader")
    downloader.run_forever()
