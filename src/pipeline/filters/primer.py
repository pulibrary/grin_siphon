# downloader.py

# Use to download files from GRIN.

import logging
import json
from pathlib import Path
from clients import GrinClient

logger: logging.Logger = logging.getLogger(__name__)

"""
If the first bucket in the pipeline is empty, the pipeline must be primed. This class
uses the GRIN API to fetch all the barcodes in the GRIN converted resource and creates
tokens for all of them.

NB: this is a very naive implementation, just for testing the pipeline.
"""


class Primer:
    def __init__(self, grin_client, to_bucket, processing_bucket):
        self.grin_client: GrinClient = grin_client
        self.to_bucket = Path(to_bucket)
        self.processing_bucket = processing_bucket

    def replentish_tokens(self):
        processed_books = [p.stem for p in Path(self.processing_bucket).glob("*.tgz")]
        converted_books = self.grin_client.converted_books
        unprocessed_books = None
        if converted_books:
            unprocessed_books = [
                book for book in converted_books if book["barcode"] not in processed_books
            ]
        if unprocessed_books:
            for book in unprocessed_books:
                token_info = {
                    "barcode": book["barcode"],
                    "processing_bucket": self.processing_bucket,
                }
                token_filepath: Path = self.to_bucket / Path(f"{book['barcode']}.json")
                with open(token_filepath, "w") as f:
                    json.dump(token_info, fp=f, indent=2)

    def generate_token(self, barcode):
        token_info = {"barcode": barcode, "processing_bucket": self.processing_bucket}
        token_filepath: Path = self.to_bucket / Path(f"{barcode}.json")
        with open(token_filepath, "w") as f:
            json.dump(token_info, fp=f, indent=2)
        return token_info


class PrimeToStore:
    def __init__(self, grin_client: GrinClient, to_bucket: str, processing_bucket: str):
        self.grin_client: GrinClient = grin_client
        self.to_bucket = Path(to_bucket)
        self.processing_bucket = processing_bucket

    def unprocessed_books(self):
        unprocessed_books = [p.stem for p in Path(self.processing_bucket).glob("*.tgz")]
        return unprocessed_books

    def replentish_tokens(self):
        for barcode in self.unprocessed_books():
            token_info = self.generate_token(barcode)
            token_filepath: Path = self.to_bucket / Path(f"{barcode}.json")
            with token_filepath.open("w") as f:
                json.dump(token_info, fp=f, indent=2)

    def generate_token(self, barcode):
        token_info = {"barcode": barcode, "processing_bucket": self.processing_bucket}
        token_filepath: Path = self.to_bucket / Path(f"{barcode}.json")
        with token_filepath.open("w") as f:
            json.dump(token_info, fp=f, indent=2)
        return token_info
