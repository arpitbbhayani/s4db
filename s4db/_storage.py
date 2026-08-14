import datetime
import re

import boto3
from botocore.exceptions import ClientError

from .errors import ConflictError

# Legacy (writer 0):     data_NNNNNN.s4db
# Namespaced writers:    data_XXXXXXXX_NNNNNN.s4db  (8 hex writer id, decimal seq)
_DATA_FILE_RE = re.compile(r"^data_(?:[0-9a-f]{8}_)?(\d{6,})\.s4db$")

_PRECONDITION_CODES = ("PreconditionFailed", "412", "ConditionalRequestConflict")


def is_data_filename(name: str) -> bool:
    """Returns True if name matches either data-file naming scheme."""
    return _DATA_FILE_RE.match(name) is not None


class S3Storage:
    def __init__(self, bucket: str, prefix: str, **boto_kwargs):
        """Creates an S3Storage backed by the given bucket and key prefix.

        Any extra boto_kwargs (e.g. region_name, endpoint_url) are forwarded directly
        to boto3.client, making it easy to target non-AWS S3-compatible services.
        """
        self.bucket = bucket
        self.prefix = prefix
        self._client = boto3.client("s3", **boto_kwargs)

    def _key(self, filename: str) -> str:
        """Returns the full S3 key for filename by prepending the configured prefix."""
        return self.prefix + filename

    def _conditional_kwargs(self, if_match: str | None, if_none_match: bool) -> dict:
        """Builds the conditional-write kwargs for put_object; empty when unfenced."""
        kwargs: dict = {}
        if if_none_match:
            kwargs["IfNoneMatch"] = "*"
        elif if_match is not None:
            kwargs["IfMatch"] = if_match
        return kwargs

    def _put(self, body, filename: str, if_match: str | None, if_none_match: bool) -> str:
        """Issues one PUT (optionally conditional) and returns the new ETag.

        Raises ConflictError when a conditional PUT fails its precondition,
        i.e. another writer created or replaced the object first.
        """
        try:
            response = self._client.put_object(
                Bucket=self.bucket,
                Key=self._key(filename),
                Body=body,
                **self._conditional_kwargs(if_match, if_none_match),
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] in _PRECONDITION_CODES:
                raise ConflictError(f"conditional PUT of {filename} failed: another writer won") from exc
            raise
        return response["ETag"]

    def upload(self, local_path: str, filename: str, *, if_match: str | None = None, if_none_match: bool = False) -> str:
        """Uploads a file from disk as a single PUT and returns its ETag.

        Data files are capped well under S3's 5 GB single-PUT limit, so no
        multipart transfer is used. Pass if_none_match=True to create-if-absent,
        or if_match=<etag> to replace only the version this writer last wrote.
        """
        with open(local_path, "rb") as fh:
            return self._put(fh, filename, if_match, if_none_match)

    def upload_bytes(self, data: bytes, filename: str, *, if_match: str | None = None, if_none_match: bool = False) -> str:
        """Uploads raw bytes as a single PUT and returns the new ETag.

        The same conditional-write options as upload() apply; a conditional
        failure raises ConflictError.
        """
        return self._put(data, filename, if_match, if_none_match)

    def download_file(self, filename: str, local_path: str) -> None:
        """Downloads an S3 object and writes it to local_path on disk."""
        self._client.download_file(self.bucket, self._key(filename), local_path)

    def download_bytes(self, filename: str) -> bytes:
        """Downloads an S3 object and returns its full contents as bytes."""
        return self.download_bytes_with_etag(filename)[0]

    def download_bytes_with_etag(self, filename: str) -> tuple[bytes, str]:
        """Downloads an S3 object, returning (contents, etag).

        The ETag identifies the exact object version read, for later
        compare-and-swap on overwrite.
        """
        response = self._client.get_object(Bucket=self.bucket, Key=self._key(filename))
        return response["Body"].read(), response["ETag"]

    def read_range(self, filename: str, start: int, length: int) -> bytes:
        """Fetches a byte range from an S3 object using an HTTP Range request.

        start and length are both in bytes; the range is inclusive on both ends per S3 semantics.
        Use this to read a single entry without downloading the entire data file.
        """
        end = start + length - 1
        response = self._client.get_object(
            Bucket=self.bucket,
            Key=self._key(filename),
            Range=f"bytes={start}-{end}",
        )
        return response["Body"].read()

    def exists(self, filename: str) -> bool:
        """Returns True if the file exists in S3, False on 404/NoSuchKey. Re-raises other errors."""
        try:
            self._client.head_object(Bucket=self.bucket, Key=self._key(filename))
            return True
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                return False
            raise

    def is_missing_error(self, exc: ClientError) -> bool:
        """Returns True if exc is S3's object-does-not-exist error."""
        return exc.response["Error"]["Code"] in ("404", "NoSuchKey")

    def delete(self, filename: str) -> None:
        """Deletes the S3 object for filename. Silent no-op if the object does not exist."""
        self._client.delete_object(Bucket=self.bucket, Key=self._key(filename))

    def list_data_files(self) -> list[str]:
        """Lists all data files under the prefix, returning filenames sorted by name.

        Only returns files matching the data-file naming patterns; index and other
        files are ignored. Uses pagination to handle buckets with more than 1000 objects.
        """
        return [name for name, _ in self.list_data_objects()]

    def list_data_objects(self) -> list[tuple[str, datetime.datetime]]:
        """Lists all data files under the prefix as (filename, last_modified), sorted by name.

        last_modified lets callers (garbage collection) distinguish long-dead
        orphans from files another writer uploaded moments ago.
        """
        paginator = self._client.get_paginator("list_objects_v2")
        results = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                name = obj["Key"][len(self.prefix):]
                if _DATA_FILE_RE.match(name):
                    results.append((name, obj["LastModified"]))
        results.sort()
        return results
