# client.py

# This module implements a GrinClient class that can be used
# to access the Google Books library portal.  It borrows code
# from the example given in the GRIN Overview document (not
# publicly available.

import csv
import functools
import io
import os
import threading
import time

import httplib2
import httpx
from dotenv import load_dotenv

from clients.auth_util import build_auth_header, load_creds_or_die

load_dotenv()


def rate_limiter(max_calls, period):
    """
    Decorator to limit the rate of function calls by blocking until calls are allowed.

    :param max_calls: Maximum number of calls allowed in the period.
    :param period: Period in seconds over which max_calls are allowed.
    """

    def decorator(func):
        call_times = []
        lock = threading.Lock()

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal call_times
            while True:
                with lock:
                    now = time.time()
                    # Remove timestamps outside the period window
                    call_times = [t for t in call_times if now - t < period]

                    if len(call_times) < max_calls:
                        call_times.append(now)
                        break

                    # Calculate time to wait until next slot frees
                    earliest = min(call_times)
                    sleep_time = period - (now - earliest)
                    if sleep_time < 0:
                        sleep_time = 0.01  # minimal wait to yield
                time.sleep(sleep_time)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def table_to_dictlist(table, fields) -> list:
    return [dict(zip(fields, row)) for row in table]


class GrinClient:
    def __init__(self, directory: str = "PRNC") -> None:
        load_dotenv()  # ensure .env is read
        secrets = os.environ["GOOGLE_SECRETS_FILE"]
        token = os.environ["GOOGLE_TOKEN_FILE"]

        creds = load_creds_or_die(secrets, token)
        self.auth_header = build_auth_header(creds)
        # self.base_url = base_url.rstrip("/")

        self.base_url = "https://books.google.com/libraries"
        self.directory = directory

        self._converted = None
        self._all_books = None
        self._available = None
        self._in_process = None
        self._failed = None

    # @property
    # def auth_header(self):
    #     return {"Authorization": f"Bearer {self.creds.access_token}"}

    @rate_limiter(max_calls=30, period=60)
    def make_grin_request(self, url, method="GET", data=None):
        """Makes an HTTP request to grin using httpx
        and returns the response."""
        if method == "GET":
            response = httpx.get(url, headers=self.auth_header)
        elif method == "POST":
            response = httpx.post(url, headers=self.auth_header)
        else:
            raise ValueError(f"{method} method not supported by make_grin_request")

        return response

    def resource_url(self, resource_str):
        return f"{self.base_url}/{self.directory}/{resource_str}"

    def _request(self, url: str, method: str = "GET", **kwargs):
        # Always include auth header, and surface *useful* errors
        headers = kwargs.pop("headers", {})
        headers = {**self.auth_header, **headers}
        try:
            r = httpx.request(method, url, headers=headers, follow_redirects=True, **kwargs)
            r.raise_for_status()
            return r
        except httpx.HTTPStatusError as e:
            # Show server payload to understand the failure
            text = getattr(e.response, "text", "")
            raise RuntimeError(
                f"{e.request.method} {e.request.url} -> {e.response.status_code}\n{text}"
            ) from e

    def grin_data(self, book_type) -> list:
        url = self.resource_url(f"_{book_type}?format=text&mode=all")
        # response = self.make_grin_request(url)
        response = self._request(url)
        tsv_data = io.StringIO(response.text)
        reader = csv.reader(tsv_data, delimiter="\t")
        table = []
        for row in reader:
            table.append(row)
        return table

    @property
    def failed_books(self):
        fields: list = [
            "barcode",
            "scanned_date",
            "processed_date",
            "analyzed_date",
            "convert_failed_date",
            "convert_failed_info",
            "ocrd_date",
            "detailed_conversion_info",
            "link",
        ]
        data = self.grin_data("failed")
        return table_to_dictlist(data, fields)

    @property
    def available_books(self):
        fields: list = [
            "barcode",
            "scanned_date",
            "processed_date",
            "analyzed_date",
            "ocrd_date",
            "link",
        ]
        data = self.grin_data("available")
        return table_to_dictlist(data, fields)

    @property
    def in_process_books(self):
        fields: list = [
            "barcode",
            "scanned_date",
            "processed_date",
            "analyzed_date",
            "ocrd_date",
            "link",
        ]
        data = self.grin_data("in_process")
        return table_to_dictlist(data, fields)

    @property
    def all_books(self):
        fields: list = [
            "barcode",
            "scanned_date",
            "processed_date",
            "analyzed_date",
            "converted_date",
            "ocrd_date",
            "link",
        ]
        data = self.grin_data("all_books")
        return table_to_dictlist(data, fields)

    @property
    def converted_books(self) -> list | None:
        """the GRIN for converted_books returns
        a different format from the other apis;
        the barcode needs to be extracted from the first field."""
        fields = ["file", "scanned_date", "converted_date", "downloaded_date", "link"]
        data = self.grin_data("converted")
        dictlist = []
        for row in data:
            barcode = row[0].split(".")[0]
            row_dict = dict(zip(fields, row))
            row_dict["barcode"] = barcode
            dictlist.append(row_dict)
        return dictlist

    def convert_book(self, barcode: str):
        result = {}
        url = f"{self.resource_url('_process')}?barcodes={barcode}"
        response = httpx.post(
            url=url, headers=self.auth_header, follow_redirects=True
        ).raise_for_status()
        with io.StringIO(response.text) as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                result[row["Barcode"]] = row["Status"]
        return result

    def convert_books(self, barcode_list):
        responses = {}
        for barcode in barcode_list:
            url = f"{self.resource_url('_process')}?barcodes={barcode}"
            response = httpx.post(url=url, headers=self.auth_header, follow_redirects=True)

            with io.StringIO(response.text) as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    responses[row["Barcode"]] = row["Status"]

        return responses

    def convert(self, barcode_list: list):
        url = f"{self.resource_url('_process')}?process_format=json"
        data = {
            #  'barcodes': '\n'.join(barcode_list)
            "barcodes": barcode_list
        }
        response = httpx.post(url=url, headers=self.auth_header, follow_redirects=True, data=data)

        response_dict = {}
        with io.StringIO(response.text) as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                response_dict[row["Barcode"]] = row["Status"]

        return response_dict

    def download_file(self, url, outpath):
        with httpx.stream("GET", url, headers=self.auth_header, follow_redirects=True) as response:
            response.raise_for_status()
            with open(outpath, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)

    def file_size(self, barcode) -> int:
        fname = f"{barcode}.tar.gz.gpg"
        src_url = self.resource_url(fname)
        response = httpx.head(src_url, allow_redirects=True)
        return int(response.headers.get("Content-Length", 0))

    def download_book(self, barcode, target_dir):
        fname = f"{barcode}.tar.gz.gpg"
        src_url = self.resource_url(fname)
        outpath = f"{target_dir}/{fname}"
        self.download_file(src_url, outpath)
