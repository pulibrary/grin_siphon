from pathlib import Path

from pipeline.book_ledger import Book, BookLedger
from pipeline.plumbing import Token
from pipeline.token_bag import TokenBag


class Secretary:
    """
    Manages book selection from the ledger into the token bag.

    The Secretary coordinates between the BookLedger and TokenBag,
    handling the selection of books for processing and their conversion
    into tokens ready for the pipeline.

    Attributes:
        bag (TokenBag): The token bag for storing selected books
        ledger (BookLedger): The book ledger containing available books
    """

    def __init__(self, bag: TokenBag, ledger: BookLedger) -> None:
        self.bag = bag
        self.ledger = ledger

        self.bag.load()
        # self.ledger.read_ledger()

    @property
    def bag_size(self) -> int:
        return self.bag.size

    def find_in_bag(self, barcode) -> Token | None:
        return self.bag.find_token(barcode)

    def find_in_ledger(self, barcode) -> Book | None:
        return self.ledger.entry(barcode)

    @property
    def unprocessed_books(self) -> list[Book]:
        book_list = []
        books = self.ledger.all_unprocessed_books
        if books:
            book_list = books
        return book_list

    @property
    def chosen_books(self) -> list[Book]:
        book_list = []
        books = self.ledger.all_chosen_books
        if books:
            book_list = books
        return book_list

    def status(self):
        stats = {}
        stats["bag_current_size"] = len(self.bag.tokens)
        return stats

    def choose_book(self, barcode: str) -> Book:
        if self.ledger.entry(barcode) is not None:
            book: Book = self.ledger.choose_book(barcode)
            self.bag.add_book(book.barcode)
            return book
        else:
            error_msg = f"{barcode} is not in ledger"
            raise KeyError(error_msg)

    def choose_books(self, how_many: int):
        """Select multiple books from the ledger for processing.

        Args:
            how_many (int): Maximum number of books to select
        """
        unprocessed_books = self.unprocessed_books
        # Clamp the request to available books to prevent overselection
        if how_many > len(unprocessed_books):
            how_many = len(unprocessed_books)
        # Take the first N unprocessed books (FIFO selection)
        books_to_choose: list[Book] | None = unprocessed_books[0:how_many]
        if books_to_choose:
            # Convert each selected book into a token in the bag
            for book in books_to_choose:
                self.choose_book(book.barcode)

    def mark_book_completed(self, barcode: str):
        entry = self.ledger.entry(barcode)
        if entry:
            self.ledger.mark_book_completed(barcode)
        else:
            error_msg = f"{barcode} is not in ledger"
            raise KeyError(error_msg)

    def pour_bag(self, bucket: Path):
        self.bag.pour_into(bucket)

    def commit(self) -> None:
        self.ledger.write_ledger()
        self.bag.dump()
