import re

import boto3
from botocore.exceptions import ClientError


_DATA_FILE_RE = re.compile(r"^data_(\d{6})\.s4db$")


class S3Storage:
    def __init__(self, bucket: str, prefix: str, **boto_kwargs):
        self.bucket = bucket
        self.prefix = prefix
        self._client = boto3.client("s3", **boto_kwargs)

    def _key(self, filename: str) -> str:
        return self.prefix + filename

    def upload(self, local_path: str, filename: str) -> None:
        self._client.upload_file(local_path, self.bucket, self._key(filename))

    def upload_bytes(self, data: bytes, filename: str) -> None:
        self._client.put_object(Bucket=self.bucket, Key=self._key(filename), Body=data)

    def download_bytes(self, filename: str) -> bytes:
        response = self._client.get_object(Bucket=self.bucket, Key=self._key(filename))
        return response["Body"].read()

    def read_range(self, filename: str, start: int, length: int) -> bytes:
        end = start + length - 1
        response = self._client.get_object(
            Bucket=self.bucket,
            Key=self._key(filename),
            Range=f"bytes={start}-{end}",
        )
        return response["Body"].read()

    def exists(self, filename: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=self._key(filename))
            return True
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                return False
            raise

    def delete(self, filename: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=self._key(filename))

    def list_data_files(self) -> list[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        filenames = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = key[len(self.prefix):]
                if _DATA_FILE_RE.match(name):
                    filenames.append(name)
        filenames.sort()
        return filenames
