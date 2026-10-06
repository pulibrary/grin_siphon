"""Estimate page totals for books whose page count was never recorded.

Books transferred before the Decryptor began recording ``page_count`` have no
count, and the only place their pages live is inside the tarball in S3. We
measure a random sample, then scale by bytes (pages-per-byte ratio estimator).
"""

import json
import math
import random
import sys
from collections.abc import Callable
from pathlib import Path

SAMPLE_NAME = "page_sample.cache"
Z95 = 1.96


class PageSample:
    """Measured (pages, bytes) for sampled books, persisted between runs."""

    def __init__(self, directory: Path) -> None:
        self.path = directory / SAMPLE_NAME
        self.data: dict[str, dict[str, int]] = {}
        try:
            self.data = json.loads(self.path.read_text())
        except (OSError, ValueError):
            pass

    def add(self, barcode: str, pages: int, size: int) -> None:
        self.data[barcode] = {"pages": pages, "bytes": size}
        tmp = self.path.with_name(SAMPLE_NAME + ".tmp")
        tmp.write_text(json.dumps(self.data))
        tmp.replace(self.path)


def top_up_sample(
    sample: PageSample,
    candidates: dict[str, int],
    count_fn: Callable[[str], int],
    target: int,
    rng: random.Random | None = None,
) -> int:
    """Measure random books until the sample holds ``target`` books.

    Args:
        sample: Persistent sample to extend.
        candidates: barcode -> size for books eligible to be sampled.
        count_fn: Returns the page count of one book (reads it from storage).
        target: Desired total sample size.
        rng: Random source (injectable for tests).

    Returns:
        Number of books newly measured. A book that fails to read is skipped.
    """
    rng = rng or random.Random()
    pool = sorted(set(candidates) - set(sample.data))
    need = min(target - len(sample.data), len(pool))
    added = 0
    for i, barcode in enumerate(rng.sample(pool, need) if need > 0 else [], 1):
        print(f"Sampling {i}/{need}: {barcode} ...", end=" ", file=sys.stderr, flush=True)
        try:
            pages = count_fn(barcode)
        except Exception as e:  # noqa: BLE001 - one bad object must not stop the sample
            print(f"failed ({e})", file=sys.stderr)
            continue
        sample.add(barcode, pages, candidates[barcode])
        added += 1
        print(f"{pages} pages", file=sys.stderr)
    return added


def estimate_pages(sample: list[tuple[int, int]], rest_bytes: int) -> tuple[int, int, int] | None:
    """Ratio estimate of total pages in ``rest_bytes`` bytes of unmeasured books.

    Args:
        sample: (pages, bytes) per sampled book.
        rest_bytes: Total size of the books being estimated.

    Returns:
        (estimate, low, high) with a 95% interval, or None if the sample is too small.
    """
    n = len(sample)
    total_bytes = sum(b for _, b in sample)
    if n < 2 or total_bytes <= 0:
        return None
    ratio = sum(p for p, _ in sample) / total_bytes
    resid_var = sum((p - ratio * b) ** 2 for p, b in sample) / (n - 1)
    mean_bytes = total_bytes / n
    se = rest_bytes * math.sqrt(resid_var / n) / mean_bytes
    est = ratio * rest_bytes
    return round(est), max(0, round(est - Z95 * se)), round(est + Z95 * se)
