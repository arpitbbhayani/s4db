import glob as _glob
import os
import tempfile

from ._format import pack_file_header, pack_entry, unpack_entry_at, unpack_file_header, stream_file_entries, FLAG_TOMBSTONE, HEADER_SIZE
from ._index import Index
from ._storage import S3Storage
from .compaction import compact as run_compaction

_INDEX_FILENAME = "index.idx"
_DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024  # 64 MB


def _data_filename(file_num: int) -> str:
    """Returns the canonical filename for a data file given its sequence number."""
    return f"data_{file_num:06d}.s4db"


class S4DB:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        local_dir: str | None = None,
        max_file_size: int = _DEFAULT_MAX_FILE_SIZE,
        **boto_kwargs,
    ):
        """Opens (or creates) an S4DB database backed by an S3 bucket.

        local_dir is optional. If omitted, no local directory is created or used until
        a write operation (put/delete) is called, at which point a temporary directory
        is created automatically. Pass local_dir explicitly to control where data files
        are stored on disk.

        On init the index is fetched from S3 into memory; if it does not exist the
        database starts empty. The index is never read from a local file on startup.
        max_file_size controls when data files are rolled over (default 64 MB).
        Extra boto_kwargs are forwarded to the S3 client.
        """
        self.bucket = bucket
        self.prefix = prefix
        self.local_dir = local_dir
        self.max_file_size = max_file_size
        self.storage = S3Storage(bucket, prefix, **boto_kwargs)
        self._index = Index()

        # Load index from S3 into memory only - no local file caching on init
        if self.storage.exists(_INDEX_FILENAME):
            self._index = Index.from_bytes(self.storage.download_bytes(_INDEX_FILENAME))

    def _get_local_dir(self) -> str:
        """Returns local_dir, creating a temporary directory if none was provided."""
        if self.local_dir is None:
            self.local_dir = tempfile.mkdtemp(prefix="s4db_")
        os.makedirs(self.local_dir, exist_ok=True)
        return self.local_dir

    def download(self) -> None:
        """Download all data files and the index from S3 into local_dir."""
        local_dir = self._get_local_dir()
        for filename in self.storage.list_data_files():
            self.storage.download_file(filename, os.path.join(local_dir, filename))

        self._index = Index.from_bytes(self.storage.download_bytes(_INDEX_FILENAME))

    def upload(self) -> None:
        """Upload all local data files and the index to S3."""
        if self.local_dir:
            for path in sorted(_glob.glob(os.path.join(self.local_dir, "data_*.s4db"))):
                self.storage.upload(path, os.path.basename(path))
        self.storage.upload_bytes(self._index.to_bytes(), _INDEX_FILENAME)

    def get(self, key: str) -> str | None:
        """Returns the value for key, or None if it does not exist or has been deleted.

        If local_dir is set and the data file is present locally, reads from disk.
        Otherwise fetches only that entry's bytes from S3 using a range request -
        no local directory is needed for read-only access.
        """
        entry = self._index.get(key)
        if entry is None:
            return None
        filename = _data_filename(entry.file_num)
        if self.local_dir:
            local_path = os.path.join(self.local_dir, filename)
            if os.path.exists(local_path):
                with open(local_path, "rb") as fh:
                    fh.seek(entry.offset)
                    raw = fh.read(entry.length)
                _, value, _, _ = unpack_entry_at(raw, 0)
                return value
        raw = self.storage.read_range(filename, entry.offset, entry.length)
        _, value, _, _ = unpack_entry_at(raw, 0)
        return value

    def put(self, items: dict[str, str]) -> None:
        """Writes one or more key/value pairs, appending to the current data file.

        Overwrites any existing value for a key. Persists to disk and updates
        the in-memory index and saved index file before returning.
        """
        entries = [(k, v, False) for k, v in items.items()]
        self._write_entries(entries)

    def delete(self, keys: list[str]) -> None:
        """Writes tombstones for each key that currently exists in the index.

        Keys not present in the index are silently skipped - no tombstone is written
        for them. The index is updated and saved after writing.
        """
        tombstones = [
            (k, None, True)
            for k in keys
            if self._index.get(k) is not None
        ]
        if tombstones:
            self._write_entries(tombstones)

    def keys(self) -> list[str]:
        """Returns a list of all live keys in the database."""
        return list(self._index.entries.keys())

    def iter(self, local: bool = False):
        """Yields (key, value) pairs for every live key in the database.

        Iterates over the index and yields each key with its value.

        If local is False (default), each value is fetched via get(), which uses
        an S3 range request for files not present locally - one S3 call per key.

        If local is True, all data files referenced by the index are downloaded
        from S3 into local_dir before iteration begins. Files already present
        locally are not replaced. Values are then read from disk - no S3 calls
        during the iteration itself.

        Use local=True when you need to iterate over many keys and want to avoid
        a separate S3 request per key.
        """
        if local:
            local_dir = self._get_local_dir()
            # Download only files referenced by the index that are not already local
            needed = {_data_filename(e.file_num) for e in self._index.entries.values()}
            for filename in needed:
                local_path = os.path.join(local_dir, filename)
                if not os.path.exists(local_path):
                    self.storage.download_file(filename, local_path)
            # Read values from disk and yield
            for key, entry in self._index.entries.items():
                local_path = os.path.join(local_dir, _data_filename(entry.file_num))
                with open(local_path, "rb") as fh:
                    fh.seek(entry.offset)
                    raw = fh.read(entry.length)
                _, value, _, _ = unpack_entry_at(raw, 0)
                yield key, value
        else:
            for key in self._index.entries:
                value = self.get(key)
                if value is not None:
                    yield key, value

    def flush(self) -> None:
        """Flushes the in-memory index to disk."""
        self._save_index()

    def compact(self) -> None:
        """Triggers compaction, rewriting data files to reclaim space from deleted/stale entries."""
        run_compaction(self)

    def rebuild_index(self) -> None:
        """Reconstructs the in-memory index by replaying all local data files in order.

        Useful for disaster recovery when the index file is lost or corrupted. Applies
        entries sequentially so later writes correctly overwrite earlier ones and tombstones
        remove deleted keys. Persists the rebuilt index to disk before returning.
        """
        local_dir = self._get_local_dir()
        data_files = sorted(_glob.glob(os.path.join(local_dir, "data_*.s4db")))
        new_index = Index()
        last_file_num = 0

        for path in data_files:
            with open(path, "rb") as fh:
                _, file_num = unpack_file_header(fh.read(HEADER_SIZE))
                last_file_num = file_num
                for offset, raw, key, flags in stream_file_entries(fh):
                    if flags == FLAG_TOMBSTONE:
                        new_index.delete(key)
                    else:
                        new_index.put(key, file_num, offset, len(raw))

        new_index.next_file_num = last_file_num + 1 if data_files else 1
        self._index = new_index
        self.flush()

    def _save_index(self) -> None:
        """Serializes the in-memory index and writes it to the local index file."""
        data = self._index.to_bytes()
        local_path = os.path.join(self._get_local_dir(), _INDEX_FILENAME)
        with open(local_path, "wb") as fh:
            fh.write(data)

    def _write_entries(self, entries: list[tuple[str, str | None, bool]]) -> None:
        """Appends a batch of entries to the current data file, rolling to a new file when needed.

        Entries is a list of (key, value, is_tombstone) tuples.

        A new data file is created when any of the following is true:
          - No previous file exists (fresh database, next_file_num == 1).
          - The latest data file is not present on disk.
          - The latest data file is at or over max_file_size.
        Otherwise, entries are appended to the existing file.

        Mid-batch rolling: if writing the next entry would push the current file past
        max_file_size and the file already contains at least one entry (pos > HEADER_SIZE),
        roll() closes the file and opens the next sequentially numbered one before writing.
        A single entry that exceeds max_file_size on its own is never split; it is written
        to an otherwise-empty file, making that file exceed the soft limit.

        Index and on-disk state are updated after all entries are written.
        """
        local_dir = self._get_local_dir()

        # Resume writing into the latest file if it still has room, otherwise start a new one
        latest_file_num = self._index.next_file_num - 1
        latest_path = os.path.join(local_dir, _data_filename(latest_file_num))
        if latest_file_num >= 1 and os.path.exists(latest_path) and os.path.getsize(latest_path) < self.max_file_size:
            file_num = latest_file_num
            fh = open(latest_path, "ab")
        else:
            file_num = self._index.next_file_num
            fh = open(os.path.join(local_dir, _data_filename(file_num)), "wb")
            fh.write(pack_file_header(file_num))

        written: list[tuple[str, int, int, int]] = []

        def roll():
            """Closes the current file and opens the next sequentially numbered data file."""
            nonlocal file_num, fh
            fh.close()
            file_num += 1
            fh = open(os.path.join(local_dir, _data_filename(file_num)), "wb")
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
        self.flush()

    def __enter__(self) -> "S4DB":
        """Supports use as a context manager; returns self."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """No-op exit; included so S4DB can be used in a with statement."""
        pass
