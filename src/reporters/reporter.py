import os
from collections import namedtuple
from csv import DictWriter
from io import StringIO
from pathlib import Path

from tabulate import tabulate

from clients import GrinClient, S3Client
from pipeline.book_ledger import Book, BookLedger
from pipeline.config_loader import load_config
from pipeline.plumbing import Pipeline
from pipeline.secretary import Secretary
from pipeline.token_bag import TokenBag

S3Rec = namedtuple(
    "S3Rec",
    [
        "Key",
        "LastModified",
        "ETag",
        "ChecksumAlgorithm",
        "ChecksumType",
        "Size",
        "StorageClass",
    ],
)


def objects_in_store():
    s3_client = S3Client("/tmp")
    paginator = s3_client.client.get_paginator("list_objects_v2")

    page_iterator = paginator.paginate(Bucket=self.s3_client.bucket_name)
    objects = []
    for page in page_iterator:
        contents = page.get("Contents", [])
        for object in contents:
            objects.append(S3Rec(**object))

    return objects


class Reporter:
    """Family of classes that gather information about aspects of GRIN
    and the GRIN pipeline: what barcodes have already been processed;
    what barcodes are in process, converted, available; what barcodes
    are available but have not yet been processed; etc."""

    def __init__(self) -> None:
        pass

    def report(self, **kwargs) -> dict:
        pass


class ObjectStoreReporter(Reporter):
    def __init__(self):
        super().__init__()
        self.s3_client = S3Client("/tmp")
        self.grin_client = GrinClient()

    def objects_in_store(self):
        paginator = self.s3_client.client.get_paginator("list_objects_v2")

        page_iterator = paginator.paginate(Bucket=self.s3_client.bucket_name)
        objects = []
        for page in page_iterator:
            contents = page.get("Contents", [])
            for object in contents:
                objects.append(S3Rec(**object))

        return objects

    def format_as_table_to_print(self, oblist):
        with StringIO() as out_buf:
            writer = DictWriter(out_buf, S3Rec._fields)
            writer.writeheader()
            for ob in oblist:
                writer.writerow(ob._asdict())
            csv_string = out_buf.getvalue()
        return csv_string

    def report(self, **kwargs) -> str:
        format: str = kwargs.get("format")
        match format:
            case "table":
                return self.format_as_table_to_print(self.objects_in_store())
            case "barcodes":
                return [ob.key for ob in self.objects_in_store()]
            case _:
                return ""


class ConvertedReporter(Reporter):
    """Snapshot of GRIN Converted Queue and what has already been
    stored in AWS.
    """

    def __init__(self) -> None:
        super().__init__()
        self.s3_client = S3Client("/tmp")
        self.grin_client = GrinClient()

    def report(self):
        grin_barcodes = set([rec["barcode"] for rec in GrinClient().converted_books])
        stored_barcodes = set([obj.Key for obj in objects_in_store()])

        converted_and_stored = grin_barcodes.intersect(stored_barcodes)


class StatusReporter(Reporter):
    def __init__(self, config: dict) -> None:
        super().__init__()
        self.bag = TokenBag(Path(config.get("global", {}).get("token_bag", None)))
        self.ledger = BookLedger(Path(config.get("global", {}).get("ledger_file", None)))
        self._all_grin_books = None
        self._failed_grin_books = None
        self._available_grin_books = None
        self._in_process_grin_books = None
        self._converted_grin_books = None
        self._ledger = None
        self._s3_books = None

    @property
    def all_grin_books(self):
        if self._all_grin_books is None:
            grin = GrinClient()
            self._all_grin_books = grin.all_books
        return self._all_grin_books

    @property
    def failed_grin_books(self):
        if self._failed_grin_books is None:
            grin = GrinClient()
            self._failed_grin_books = grin.failed_books
        return self._failed_grin_books

    @property
    def available_grin_books(self):
        if self._available_grin_books is None:
            grin = GrinClient()
            self._available_grin_books = grin.available_books
        return self._available_grin_books

    @property
    def in_process_grin_books(self):
        if self._in_process_grin_books is None:
            grin = GrinClient()
            self._in_process_grin_books = grin.in_process_books
        return self._in_process_grin_books

    @property
    def converted_grin_books(self):
        if self._converted_grin_books is None:
            grin = GrinClient()
            self._converted_grin_books = grin.converted_books
        return self._converted_grin_books

    @property
    def s3_books(self):
        if self._s3_books is None:
            s3 = S3Client("/tmp")
            self._s3_books = {}
            for object in s3.list_objects():
                self._s3_books[object.Key] = object
        return self._s3_books

    @property
    def grin_data_table(self):
        table = [
            ["all", len(self.all_grin_books)],
            ["available", len(self.available_grin_books)],
            ["converted", len(self.converted_grin_books)],
            ["in process", len(self.in_process_grin_books)],
            ["failed", len(self.failed_grin_books)],
        ]
        return table

    def report(self, **kwargs):
        grin_data = self.grin_data_table
        return self.grin_data_table
