import glob as _glob
import os

from ._format import pack_file_header, pack_entry, unpack_entry_at, unpack_file_header, iter_file_entries, FLAG_TOMBSTONE, HEADER_SIZE
from ._index import Index
from ._storage import S3Storage
from .compaction import compact as run_compaction

_INDEX_FILENAME = "index.idx"
_DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024  # 64 MB


def _data_filename(file_num: int) -> str:
    return f"data_{file_num:06d}.s4db"


class S4DB:
    def __init__(
        self,
        local_dir: str,
        bucket: str,
        prefix: str,
        max_file_size: int = _DEFAULT_MAX_FILE_SIZE,
        **boto_kwargs,
    ):
        self.local_dir = local_dir
        self.bucket = bucket
        self.prefix = prefix
        self.max_file_size = max_file_size
        self.storage = S3Storage(bucket, prefix, **boto_kwargs)
        self._index = Index()

        os.makedirs(local_dir, exist_ok=True)

        # Makes sure index is loaded in memory
        # Be it from local or from S3
        local_index = os.path.join(local_dir, _INDEX_FILENAME)
        if os.path.exists(local_index):
            with open(local_index, "rb") as fh:
                self._index = Index.from_bytes(fh.read())
        elif self.storage.exists(_INDEX_FILENAME):
            self.storage.download_file(_INDEX_FILENAME, local_index)
            with open(local_index, "rb") as fh:
                self._index = Index.from_bytes(fh.read())

    def download(self) -> None:
        """Download all data files and the index from S3 into local_dir."""
        for filename in self.storage.list_data_files():
            self.storage.download_file(filename, os.path.join(self.local_dir, filename))

        local_index = os.path.join(self.local_dir, _INDEX_FILENAME)
        self.storage.download_file(_INDEX_FILENAME, local_index)
        with open(local_index, "rb") as fh:
            self._index = Index.from_bytes(fh.read())

    def upload(self) -> None:
        """Upload all local data files and the index to S3."""
        for path in sorted(_glob.glob(os.path.join(self.local_dir, "data_*.s4db"))):
            self.storage.upload(path, os.path.basename(path))
        local_index = os.path.join(self.local_dir, _INDEX_FILENAME)
        self.storage.upload(local_index, _INDEX_FILENAME)

    def get(self, key: str) -> str | None:
        entry = self._index.get(key)
        if entry is None:
            return None
        local_path = os.path.join(self.local_dir, _data_filename(entry.file_num))
        if os.path.exists(local_path):
            with open(local_path, "rb") as fh:
                fh.seek(entry.offset)
                raw = fh.read(entry.length)
        else:
            raw = self.storage.read_range(_data_filename(entry.file_num), entry.offset, entry.length)
        _, value, _, _ = unpack_entry_at(raw, 0)
        return value

    def put(self, items: dict[str, str]) -> None:
        entries = [(k, v, False) for k, v in items.items()]
        self._write_entries(entries)

    def delete(self, keys: list[str]) -> None:
        tombstones = [
            (k, None, True)
            for k in keys
            if self._index.get(k) is not None
        ]
        if tombstones:
            self._write_entries(tombstones)

    def compact(self) -> None:
        run_compaction(self)

    def rebuild_index(self) -> None:
        data_files = sorted(_glob.glob(os.path.join(self.local_dir, "data_*.s4db")))
        new_index = Index()
        last_file_num = 0

        for path in data_files:
            with open(path, "rb") as fh:
                data = fh.read()
            _, file_num = unpack_file_header(data)
            last_file_num = file_num
            for offset, length, key, value, flags in iter_file_entries(data):
                if flags == FLAG_TOMBSTONE:
                    new_index.delete(key)
                else:
                    new_index.put(key, file_num, offset, length)

        new_index.next_file_num = last_file_num + 1 if data_files else 1
        self._index = new_index
        self._save_index()

    def _save_index(self) -> None:
        data = self._index.to_bytes()
        local_path = os.path.join(self.local_dir, _INDEX_FILENAME)
        with open(local_path, "wb") as fh:
            fh.write(data)

    def _write_entries(self, entries: list[tuple[str, str | None, bool]]) -> None:
        # Resume writing into the latest file if it still has room, otherwise start a new one
        latest_file_num = self._index.next_file_num - 1
        latest_path = os.path.join(self.local_dir, _data_filename(latest_file_num))
        if latest_file_num >= 1 and os.path.exists(latest_path) and os.path.getsize(latest_path) < self.max_file_size:
            file_num = latest_file_num
            fh = open(latest_path, "ab")
        else:
            file_num = self._index.next_file_num
            fh = open(os.path.join(self.local_dir, _data_filename(file_num)), "wb")
            fh.write(pack_file_header(file_num))

        written: list[tuple[str, int, int, int]] = []

        def roll():
            nonlocal file_num, fh
            fh.close()
            file_num += 1
            fh = open(os.path.join(self.local_dir, _data_filename(file_num)), "wb")
            fh.write(pack_file_header(file_num))

        try:
            for key, value, is_tombstone in entries:
                packed = pack_entry(key, value, deleted=is_tombstone)
                pos = fh.tell()
                if pos > HEADER_SIZE and pos + len(packed) > self.max_file_size:
                    roll()
                offset = fh.tell()
                fh.write(packed)
                if not is_tombstone:
                    written.append((key, file_num, offset, len(packed)))
        finally:
            fh.close()

        # Update in-memory index
        for key, fn, offset, length in written:
            self._index.put(key, fn, offset, length)
        for key, value, is_tombstone in entries:
            if is_tombstone:
                self._index.delete(key)

        self._index.next_file_num = file_num + 1
        self._save_index()

    def __enter__(self) -> "S4DB":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass
