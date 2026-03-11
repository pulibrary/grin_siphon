from csv import DictWriter
from dataclasses import fields
from pathlib import Path

from clients import GrinClient, S3Client
from pipeline.book_ledger import Book, BookStatus


class LedgerGenerator:
    def __init__(self) -> None:
        self.grin = GrinClient()
        self.s3 = S3Client("/tmp")

        self._grin_books = None
        self._ledger = None
        self._s3_barcodes = None
        self._s3_books = None

    @property
    def grin_books(self):
        if self._grin_books is None:
            self._grin_books = self.grin.all_books
        return self._grin_books

    @property
    def s3_books(self):
        if self._s3_books is None:
            self._s3_books = {}
            for object in self.s3.list_objects():
                self._s3_books[object.Key] = object
        return self._s3_books

    @property
    def ledger(self):
        if self._ledger is None:
            self._ledger = []
            for row in self.grin_books:
                record = {
                    "barcode": row["barcode"],
                    "date_chosen": None,
                    "date_completed": None,
                    "status": None,
                }
                self._ledger.append(record)
        return self._ledger

    def synchronize_ledger(self):
        """Mark as completed each book in
        the ledger that is in the s3 bucket."""

        for rec in self.ledger:
            if s3_book := self.s3_books.get(rec["barcode"]):
                rec["status"] = BookStatus.COMPLETED
                rec["date_completed"] = str(s3_book.LastModified)

    def dump_ledger(self, file_path: Path):
        fieldnames = [f.name for f in fields(Book)]
        with file_path.open("w+", newline="") as f:
            writer = DictWriter(f, fieldnames)
            writer.writeheader()
            writer.writerows(self.ledger)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate a book ledger from GRIN")
    parser.add_argument("--ledger_file", required=True, help="Path to write the ledger CSV")
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Pre-populate completed status for books already in S3",
    )
    args = parser.parse_args()

    gen = LedgerGenerator()
    if args.sync:
        gen.synchronize_ledger()
    gen.dump_ledger(Path(args.ledger_file))
