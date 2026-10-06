# object_store.py

# This module implements clients to object-storage services that may
# be used to upload Google Books objects (tarballs) to a storage
# system.

from pathlib import Path
from collections import namedtuple
import boto3
from botocore.config import Config

S3Object = namedtuple(
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


class ObjectStore:
    def __init__(self):
        self.object_service = None


class S3Client(ObjectStore):
    def __init__(self, local_cache: Path, bucket_name: str = "google-books-dev"):
        super().__init__()
        self.object_service = "Amazon S3"
        self.bucket_name = bucket_name
        self.cache = local_cache
        self.client = boto3.client(
            "s3",
            config=Config(connect_timeout=10, read_timeout=60, retries={"max_attempts": 3}),
        )

    def object_exists(self, key: str) -> bool:
        try:
            self.client.get_object_attributes(
                Bucket=self.bucket_name, Key=key, ObjectAttributes=["ETag"]
            )
            return True
        except self.client.exceptions.NoSuchKey:
            return False

    def store_file(self, file_path, object_name=None) -> bool:
        if object_name is None:
            object_name = Path(file_path).stem
        try:
            self.client.upload_file(file_path, self.bucket_name, object_name)
            return True

        except self.client.exceptions.NoCredentialsError as e:
            print(f"AWS credentials not available: {e}")
            return False

    def store_object(self, barcode, overwrite=False) -> bool:
        result = False
        file_path = self.cache / Path(barcode).with_suffix(".tgz")
        if file_path.is_file():
            if self.object_exists(barcode):
                print(f"object {barcode} has already been stored.")
                if overwrite is True:
                    print(f"overwriting {barcode}")
                    result = self.store_file(file_path, barcode)
            else:
                result = self.store_file(file_path, barcode)
        return result

    def stream_object(self, key: str):
        """Return the object's body as a forward-only binary stream."""
        return self.client.get_object(Bucket=self.bucket_name, Key=key)["Body"]

    def list_sizes(self, progress=None) -> dict[str, int]:
        """Map every object key in the bucket to its size in bytes.

        Cheaper than list_objects() on a large bucket: no per-object records.

        Args:
            progress: Optional callable, called with the running object count
                after each page of results.
        """
        sizes: dict[str, int] = {}
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name):
            for obj in page.get("Contents", []):
                sizes[obj["Key"]] = obj["Size"]
            if progress:
                progress(len(sizes))
        return sizes

    def list_objects(self):
        paginator = self.client.get_paginator("list_objects_v2")

        page_iterator = paginator.paginate(Bucket=self.bucket_name)
        objects = []
        for page in page_iterator:
            contents = page.get("Contents", [])
            for object in contents:
                objects.append(S3Object(**object))
        return objects
