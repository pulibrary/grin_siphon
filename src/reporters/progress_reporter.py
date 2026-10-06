import json
import os
import sys
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from tabulate import tabulate

from clients import S3Client
from pipeline.book_ledger import BookLedger
from pipeline.plumbing import Pipeline, load_token
from reporters.reporter import Reporter


CACHE_NAME = "page_counts.cache"


def human_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{n} B" if unit == "B" else f"{size:,.2f} {unit}"
        size /= 1024
    return f"{n} B"


def s3_sizes(bucket: str | None = None) -> dict[str, int]:
    """Key -> size for every object in the bucket, printing progress to stderr."""
    client = S3Client(Path("/tmp"), bucket) if bucket else S3Client(Path("/tmp"))

    def progress(n: int) -> None:
        print(f"\rListing {client.bucket_name}: {n:,} objects...", end="", file=sys.stderr)

    try:
        return client.list_sizes(progress)
    finally:
        print(file=sys.stderr)


class ProgressReport(Reporter):
    """Management-facing progress reports in Markdown.

    "Transferred" means the object is in S3. Page counts come from the
    ``page_count`` property recorded by the Decryptor in each token.

    Args:
        config: Pipeline configuration.
        ledger: Book ledger to report on.
        pipeline: Pipeline whose buckets are inspected for error tokens.
        list_sizes: Callable returning a mapping of S3 object key to size in bytes.
    """

    def __init__(
        self,
        config: dict,
        ledger: BookLedger,
        pipeline: Pipeline,
        list_sizes: Callable[[], dict[str, int]] | None = None,
    ) -> None:
        super().__init__()
        self.config = config
        self.ledger = ledger
        self.pipeline = pipeline
        bucket = config.get("global", {}).get("object_store")
        self.list_sizes = list_sizes or (lambda: s3_sizes(bucket))

    def _errored_in_pipeline(self) -> set[str]:
        return {
            Path(name).stem
            for info in self.pipeline.snapshot.values()
            for name in info["errored_tokens"]
        }

    def _scan_archive(self, archive: Path) -> dict[str, int]:
        """Page counts for archived tokens, parsing only files not seen before.

        Archived tokens never change, so results (including "no page_count",
        stored as null) are cached in ``<archive>/page_counts.cache``, keyed by
        file name. Only new files are parsed on later runs.
        """
        cache_file = archive / CACHE_NAME
        cache: dict[str, int | None] = {}
        try:
            cache = json.loads(cache_file.read_text())
        except (OSError, ValueError):
            pass

        def save() -> None:
            tmp = cache_file.with_name(CACHE_NAME + ".tmp")
            tmp.write_text(json.dumps(cache))
            tmp.replace(cache_file)

        files = [e.name for e in os.scandir(archive) if e.name.endswith(".json")]
        new = [n for n in files if n not in cache]
        try:
            for i, name in enumerate(new, 1):
                pages = load_token(archive / name).get_prop("page_count")
                cache[name] = int(pages) if pages is not None else None
                if i % 1000 == 0:
                    print(
                        f"\rReading archived tokens: {i:,}/{len(new):,}...",
                        end="",
                        file=sys.stderr,
                    )
                if i % 5000 == 0:
                    save()
        finally:
            if new:
                print(file=sys.stderr)
                save()
        return {Path(n).stem: cache[n] for n in files if cache.get(n) is not None}

    def _page_counts(self) -> dict[str, int]:
        """Page counts by barcode, from tokens in the done bucket and the archive."""
        counts: dict[str, int] = {}
        if archive := self.config["global"].get("token_archive"):
            if Path(archive).is_dir():
                counts.update(self._scan_archive(Path(archive)))
        if done := self.pipeline.buckets.get("done"):
            for token_file in Path(done).glob("*.json"):
                pages = load_token(token_file).get_prop("page_count")
                if pages is not None:
                    counts[token_file.stem] = int(pages)
        return counts

    def _collect(self) -> dict:
        sizes = self.list_sizes()
        stored = set(sizes)
        ledger_codes = set(self.ledger.books)
        in_pipeline = {
            Path(name).stem
            for info in self.pipeline.snapshot.values()
            for name in info["waiting_tokens"] + info["in_process_tokens"]
        }
        failed_in_ledger = {b.barcode for b in self.ledger.all_failed_books} - stored
        err_tokens = self._errored_in_pipeline() - stored - failed_in_ledger
        errored = failed_in_ledger | err_tokens
        transferred = ledger_codes & stored
        remaining = ledger_codes - stored - errored
        page_counts = self._page_counts()
        with_pages = {c for c in stored if c in page_counts}
        return {
            "total": len(ledger_codes),
            "transferred": len(transferred),
            "remaining": len(remaining),
            "remaining_in_pipeline": len(remaining & in_pipeline),
            "errored": len(errored),
            "failed_in_ledger": len(failed_in_ledger),
            "err_tokens": len(err_tokens),
            "objects": len(stored),
            "not_in_ledger": len(stored - ledger_codes),
            "bytes": sum(sizes.values()),
            "pages": sum(page_counts[c] for c in with_pages),
            "books_with_pages": len(with_pages),
            "books_without_pages": len(stored) - len(with_pages),
            "ledger_completed": len(self.ledger.all_completed_books),
        }

    @staticmethod
    def _pct(part: int, whole: int) -> str:
        return f"{part / whole * 100:.1f}%" if whole else "n/a"

    @staticmethod
    def _table(rows: list[list], headers: list[str]) -> str:
        return tabulate(rows, headers=headers, tablefmt="pipe", colalign=("left", "right", "right"))

    def _count_section(self, d: dict) -> str:
        rows = [
            ["Transferred", f"{d['transferred']:,}", self._pct(d["transferred"], d["total"])],
            ["Remaining", f"{d['remaining']:,}", self._pct(d["remaining"], d["total"])],
            ["of which in the pipeline", f"{d['remaining_in_pipeline']:,}", ""],
            ["Errored", f"{d['errored']:,}", self._pct(d["errored"], d["total"])],
            ["**Total**", f"**{d['total']:,}**", ""],
        ]
        return "## Progress by count\n\n" + self._table(rows, ["Barcodes", "Count", "Share"])

    def _volume_section(self, d: dict) -> str:
        rows = [
            ["Objects in storage", f"{d['objects']:,}"],
            ["Total size", f"{human_bytes(d['bytes'])} ({d['bytes']:,} bytes)"],
            ["Pages transferred", f"{d['pages']:,}"],
            ["Books with a page count", f"{d['books_with_pages']:,}"],
        ]
        return "## Progress by volume\n\n" + tabulate(
            rows, headers=["Measure", "Value"], tablefmt="pipe", colalign=("left", "right")
        )

    def _notes(self, d: dict) -> str:
        notes = []
        if d["books_without_pages"]:
            notes.append(
                f"Page counts are missing for {d['books_without_pages']:,} transferred "
                "book(s), so the page total is a lower bound."
            )
        if d["errored"]:
            notes.append(
                f"Errored: {d['failed_in_ledger']:,} marked failed in the ledger, "
                f"{d['err_tokens']:,} awaiting `tidy errors`."
            )
        if d["ledger_completed"] != d["transferred"]:
            notes.append(
                f"The ledger shows {d['ledger_completed']:,} completed but storage holds "
                f"{d['transferred']:,} of its books; run `tidy done` or reconcile."
            )
        if d["not_in_ledger"]:
            notes.append(f"{d['not_in_ledger']:,} stored object(s) are not in the ledger.")
        return "\n".join(f"- {n}" for n in notes)

    def report(self, **kwargs) -> str:
        """Build the Markdown report.

        Keyword Args:
            section: "count", "volume", or omitted for both.
        """
        d = self._collect()
        section = kwargs.get("section")
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        parts = [f"# GRIN Siphon progress report\n\n_Generated {stamp}_"]
        if section in (None, "count"):
            parts.append(self._count_section(d))
        if section in (None, "volume"):
            parts.append(self._volume_section(d))
        if notes := self._notes(d):
            parts.append("### Notes\n\n" + notes)
        return "\n\n".join(parts) + "\n"
