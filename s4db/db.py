import os
import tempfile

from ._format import pack_file_header, pack_entry, HEADER_SIZE
from ._index import Index
from ._storage import S3Storage

_INDEX_FILENAME = "index.json"
_DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024  # 64 MB


def _data_filename(file_num: int) -> str:
    return f"data_{file_num:06d}.s4db"


class S4DB:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        max_file_size: int = _DEFAULT_MAX_FILE_SIZE,
        local_dir: str | None = None,
        **boto_kwargs,
    ):
        self.bucket = bucket
        self.prefix = prefix
        self.max_file_size = max_file_size
        self.local_dir = local_dir or tempfile.mkdtemp(prefix="s4db_")
        self.storage = S3Storage(bucket, prefix, **boto_kwargs)
        self._index = Index()

        if self.storage.exists(_INDEX_FILENAME):
            raw = self.storage.download_bytes(_INDEX_FILENAME)
            self._index = Index.from_json(raw)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, key: str) -> str | None:
        entry = self._index.get(key)
        if entry is None:
            return None
        filename = _data_filename(entry.file_num)
        raw = self.storage.read_range(filename, entry.offset, entry.length)
        from ._format import unpack_entry_at
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
        from .compaction import compact
        compact(self)

    def rebuild_index(self) -> None:
        filenames = self.storage.list_data_files()
        new_index = Index()
        last_file_num = 0

        for filename in filenames:
            data = self.storage.download_bytes(filename)
            from ._format import unpack_file_header, iter_file_entries, FLAG_TOMBSTONE
            _, file_num = unpack_file_header(data)
            last_file_num = file_num
            for offset, length, key, value, flags in iter_file_entries(data):
                if flags == FLAG_TOMBSTONE:
                    new_index.delete(key)
                else:
                    new_index.put(key, file_num, offset, length)

        new_index.next_file_num = last_file_num + 1 if filenames else 1
        self._index = new_index
        self.storage.upload_bytes(self._index.to_json(), _INDEX_FILENAME)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_entries(self, entries: list[tuple[str, str | None, bool]]) -> None:
        os.makedirs(self.local_dir, exist_ok=True)

        state = {
            "file_num": self._index.next_file_num,
            "buf": bytearray(pack_file_header(self._index.next_file_num)),
        }
        # Tracks (key, file_num, offset, length) for non-tombstone entries
        written: list[tuple[str, int, int, int]] = []

        def flush():
            file_num = state["file_num"]
            buf = bytes(state["buf"])
            filename = _data_filename(file_num)
            local_path = os.path.join(self.local_dir, filename)
            with open(local_path, "wb") as fh:
                fh.write(buf)
            self.storage.upload(local_path, filename)

        for key, value, is_tombstone in entries:
            packed = pack_entry(key, value, deleted=is_tombstone)
            buf = state["buf"]

            # Roll file only when current buffer has content beyond header
            if len(buf) > HEADER_SIZE and len(buf) + len(packed) > self.max_file_size:
                flush()
                state["file_num"] += 1
                state["buf"] = bytearray(pack_file_header(state["file_num"]))
                buf = state["buf"]

            entry_offset = len(buf)
            buf += packed
            state["buf"] = buf

            if not is_tombstone:
                written.append((key, state["file_num"], entry_offset, len(packed)))

        # Flush final buffer
        if len(state["buf"]) > HEADER_SIZE:
            flush()

        last_file_num = state["file_num"]

        # Update in-memory index
        for key, file_num, offset, length in written:
            self._index.put(key, file_num, offset, length)

        # Remove tombstoned keys from index
        for key, value, is_tombstone in entries:
            if is_tombstone:
                self._index.delete(key)

        self._index.next_file_num = last_file_num + 1
        self.storage.upload_bytes(self._index.to_json(), _INDEX_FILENAME)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "S4DB":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass
