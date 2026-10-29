from pipeline.book_ledger import Book, BookLedger, BookStatus


def test_book_ledger_read(shared_datadir):
    test_csv_file = shared_datadir / "test_ledger.csv"
    ledger = BookLedger(test_csv_file)

    assert len(ledger.books) == 9


def test_book_ledger_book(shared_datadir):
    test_csv_file = shared_datadir / "test_ledger.csv"
    ledger = BookLedger(test_csv_file)

    barcode = "32101078166681"

    book: Book | None = ledger.entry(barcode)
    assert book is not None and book.status is None


def test_book_ledger_choose_book(shared_datadir):
    test_csv_file = shared_datadir / "test_ledger.csv"
    ledger = BookLedger(test_csv_file)

    barcode = "32101078166681"

    book: Book | None = ledger.entry(barcode)
    assert book is not None
    assert book.status is None
    assert book in ledger.all_unprocessed_books
    assert book not in ledger.all_chosen_books
    assert book not in ledger.all_completed_books

    ledger.choose_book(barcode)

    assert book.status == BookStatus.CHOSEN
    assert book not in ledger.all_unprocessed_books
    assert book in ledger.all_chosen_books
    assert book not in ledger.all_completed_books


def test_book_ledger_mark_book_complete(shared_datadir):
    test_csv_file = shared_datadir / "test_ledger.csv"
    ledger = BookLedger(test_csv_file)

    barcode = "32101078166681"

    book: Book | None = ledger.entry(barcode)
    assert book is not None
    assert book.status is None

    assert book in ledger.all_unprocessed_books
    assert book not in ledger.all_chosen_books
    assert book not in ledger.all_completed_books

    ledger.mark_book_completed(barcode)

    assert book.status == BookStatus.COMPLETED
    assert book.status == BookStatus.COMPLETED
    assert book not in ledger.all_unprocessed_books
    assert book not in ledger.all_chosen_books
    assert book in ledger.all_completed_books


def test_book_ledger_reset_book(shared_datadir):
    ledger = BookLedger(shared_datadir / "test_ledger.csv")
    barcode = "32101078166681"
    ledger.mark_book_failed(barcode, "Book is in neither in_process nor converted queues")

    book = ledger.reset_book(barcode)

    assert book.status is None
    assert book.failure_reason is None
    assert book.date_chosen is None
    assert book in ledger.all_unprocessed_books
