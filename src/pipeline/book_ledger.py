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


class BookLedgerOld:
    """
    Tracks available books and their processing status.

    The BookLedger manages a CSV file containing book records and their
    processing status, providing methods to select books for processing
    and track their progress through the pipeline.

    Attributes:
        csv_file (Path): Path to the CSV ledger file
        _books (dict[str, Book] | None): Cached book records keyed by barcode
        _fieldnames (list): CSV column names from the ledger file
    """

    def __init__(self, csv_file):
        self.csv_file = Path(csv_file)
        self._books: dict[str, Book] | None = None
        self._fieldnames = []

    def read_ledger(self) -> dict[str, Book]:
        """Read all book records from the CSV ledger file.

        Returns:
            dict[str, Book]: Dictionary mapping barcodes to Book objects
        """
        self._books = {}
        with self.csv_file.open("r") as f:
            reader: csv.DictReader = csv.DictReader(f)
            self._fieldnames = reader.fieldnames
            for row in reader:
                barcode = row.get("barcode")
                self._books[barcode] = Book(**row)
        return books

    def write_ledger(self, backup=True):
        """Write all book records back to the CSV ledger file.

        Args:
            backup (bool): Whether to create a backup of the existing file.
                          Defaults to True.
        """
        if backup is True:
            backup_path = Path(f"{str(self.csv_file)}~")
            shutil.copy2(self.csv_file, backup_path)
            with self.csv_file.open("r") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
            if fieldnames:
                with self.csv_file.open("w") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for _, book in self.books.items():
                        writer.writerow(asdict(book))
            else:
                raise ValueError("no fieldnames")

    def refresh(self) -> None:
        self.write_ledger()
        self._books = self.read_ledger()

    def entry(self, barcode) -> Book | None:
        return self.books.get(barcode)

    @property
    def books(self):
        if self._books is None:
            self._books = self.read_ledger()
        return self._books

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
