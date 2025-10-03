# book_ledger.py
import csv
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path


class BookStatus(StrEnum):
    CHOSEN = "chosen"
    COMPLETED = "completed"


@dataclass
class Book:
    """
    Represents a book record in the processing ledger.

    Tracks the processing status and timestamps for a digitized book
    throughout its lifecycle in the pipeline.

    Attributes:
        barcode (str): Unique identifier for the book
        date_chosen (str | None): Timestamp when book was selected for processing
        date_completed (str | None): Timestamp when processing was completed
        status (str | None): Current processing status ('chosen', 'completed', etc.)
    """

    barcode: str
    date_chosen: str | None
    date_completed: str | None
    status: str | None

    def __init__(
        self, barcode: str, date_chosen: str, date_completed: str, status: str | None
    ) -> None:
        self.barcode = barcode
        self.date_chosen = None
        self.date_completed = None
        self.status = None

        if date_chosen:
            self.date_chosen = date_chosen
        if date_completed:
            self.date_completed = date_completed
        if status:
            self.status = status


class BookLedger:
    def __init__(self, csv_file: str) -> None:
        self.csv_file = Path(csv_file)
        self.books = {}
        with self.csv_file.open("r", encoding="utf-8") as f:
            reader: csv.DictReader = csv.DictReader(f)

            self._fieldnames = reader.fieldnames

            for row in reader:
                barcode = row.get("barcode")
                self.books[barcode] = Book(**row)

    def write_ledger(self, backup=True):
        if backup is True:
            backup_path = Path(f"{str(self.csv_file)}~")
            shutil.copy2(self.csv_file, backup_path)

        with self.csv_file.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self._fieldnames)
            writer.writeheader()
            for _, book in self.books.items():
                writer.writerow(asdict(book))

    def entry(self, barcode) -> Book | None:
        return self.books.get(barcode)

    def set_book(self, barcode, book: Book):
        self.books[barcode] = book

    def choose_book(self, barcode) -> Book:
        """Mark a book as chosen for processing.

        Updates the book's status to 'chosen' and sets the chosen timestamp.
        Args: barcode (str): Barcode of the book to choose
        Returns: Book: The updated book record
        Raises:  ValueError: If the book is not found in the ledger
        """

        entry: Book | None = self.entry(barcode)
        if entry:
            entry.status = BookStatus.CHOSEN
            entry.date_chosen = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return entry
        else:
            raise ValueError(f"book {barcode} not in ledger")

    def mark_book_completed(self, barcode):
        entry: Book | None = self.entry(barcode)
        if entry:
            entry.status = BookStatus.COMPLETED
            entry.date_completed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return entry
        else:
            raise ValueError(f"book {barcode} not in ledger")

    @property
    def all_chosen_books(self) -> list[Book]:
        return [book for _, book in self.books.items() if book.status == BookStatus.CHOSEN]

    @property
    def all_completed_books(self) -> list[Book]:
        return [book for _, book in self.books.items() if book.status == BookStatus.COMPLETED]

    @property
    def all_unprocessed_books(self) -> list[Book]:
        return [book for _, book in self.books.items() if book.status is None]
