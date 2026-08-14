import datetime
import glob as _glob
import os
import random
import shutil
import tempfile
import warnings
from concurrent.futures import ThreadPoolExecutor

from botocore.exceptions import ClientError

from ._format import (
    HEADER_SIZE,
    MAX_KEY_BYTES,
    FLAG_TOMBSTONE,
    pack_entry,
    pack_file_header,
    stream_file_entries,
    unpack_entry_at,
    unpack_file_header,
)
from ._index import Index, IndexEntry, make_file_id, split_file_id
from ._naming import data_filename as _data_filename, parse_data_filename as _parse_data_filename
from ._storage import S3Storage
from .compaction import compact as run_compaction
from .errors import ConflictError, CorruptEntryError, UnsyncedWritesError

_INDEX_FILENAME = "index.idx"
_DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024  # 64 MB
_REBASE_ATTEMPTS = 8


class S4DB:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        local_dir: str | None = None,
        max_file_size: int = _DEFAULT_MAX_FILE_SIZE,
        fenced: bool = True,
        **boto_kwargs,
    ):
        """Opens (or creates) an S4DB database backed by an S3 bucket.

        local_dir is optional. If omitted, no local directory is created or used until
        a write operation (put/delete) is called, at which point a temporary directory
        is created automatically (and removed again on close()). Pass local_dir
        explicitly to control where data files are stored on disk.

        Opening costs a single GET of the index object; if it does not exist the
        database starts empty. The index is never read from a local file on startup.
        max_file_size controls when data files are rolled over (default 64 MB).

        Each instance writes data files under its own random writer namespace, so
        concurrent writers can never overwrite each other's data. With fenced=True
        (the default) every upload is a conditional PUT and the index commit is a
        compare-and-swap: a concurrent commit is detected, merged per key
        (last-committer-wins), and retried, rather than silently lost. Set
        fenced=False only for S3-compatible services that lack conditional writes;
        unfenced concurrent commits can lose index updates.

        Extra boto_kwargs are forwarded to the S3 client.
        """
        self.bucket = bucket
        self.prefix = prefix
        self.local_dir = local_dir
        self.max_file_size = max_file_size
        self.fenced = fenced
        self.storage = S3Storage(bucket, prefix, **boto_kwargs)
        self._index = Index()
        self._index_etag: str | None = None

        self._writer_id = random.SystemRandom().randrange(1, 2**32)
        self._seq = 0  # last file sequence number allocated by this instance
        self._active: str | None = None  # filename currently open for appends
        # Files this instance has created or adopted for upload:
        # filename -> {"etag": last uploaded ETag or None, "dirty": bool,
        #              "unconditional": upload without preconditions (recovery)}
        self._files: dict[str, dict] = {}
        # Uncommitted logical writes since the last successful upload:
        # key -> IndexEntry (put) or None (delete). Used to re-base onto a
        # concurrent writer's committed index after a lost compare-and-swap.
        self._journal: dict[str, IndexEntry | None] = {}
        self._owns_local_dir = False

        # Load the index from S3 into memory in one GET; absent index = new database.
        try:
            data, etag = self.storage.download_bytes_with_etag(_INDEX_FILENAME)
            self._index = Index.from_bytes(data)
            self._index_etag = etag
        except ClientError as exc:
            if not self.storage.is_missing_error(exc):
                raise

    # ------------------------------------------------------------------ state

    def _get_local_dir(self) -> str:
        """Returns local_dir, creating a temporary directory if none was provided."""
        if self.local_dir is None:
            self.local_dir = tempfile.mkdtemp(prefix="s4db_")
            self._owns_local_dir = True
        os.makedirs(self.local_dir, exist_ok=True)
        return self.local_dir

    def _has_unsynced_writes(self) -> bool:
        """Returns True if any local write has not yet been committed to S3."""
        return bool(self._journal) or any(f["dirty"] for f in self._files.values())

    # ------------------------------------------------------------------- sync

    def download(self, force: bool = False) -> None:
        """Download all data files and the index from S3 into local_dir.

        Files this instance wrote are left untouched (the local copy is always
        at least as new as the uploaded one). Raises UnsyncedWritesError if
        there are uncommitted local writes that refreshing the index would
        orphan; pass force=True to discard them and mirror S3's state.
        """
        if self._has_unsynced_writes():
            if not force:
                raise UnsyncedWritesError(
                    "local writes have not been uploaded; upload() first or pass force=True to discard them"
                )
            self._journal.clear()
            self._files.clear()
            self._active = None

        local_dir = self._get_local_dir()
        for filename in self.storage.list_data_files():
            if filename in self._files:
                continue
            self.storage.download_file(filename, os.path.join(local_dir, filename))

        data, etag = self.storage.download_bytes_with_etag(_INDEX_FILENAME)
        self._index = Index.from_bytes(data)
        self._index_etag = etag

    def upload(self, rebase: bool = True) -> None:
        """Publish local writes: PUT changed data files, then commit the index.

        Only files written since the last upload are transferred; a grown file
        replaces its S3 object with an extended version, so every published
        entry location stays valid. The index PUT is the atomic commit point.

        When fenced, data files upload with create-if-absent or replace-if-
        unchanged preconditions and the index commit is a compare-and-swap on
        the ETag this writer last observed. If another writer committed first,
        the default rebase=True reloads the winning index, re-applies this
        writer's uncommitted entries on top (per-key last-writer-wins), and
        retries; rebase=False raises ConflictError instead, leaving the data
        files uploaded (in this writer's own namespace) and the index untouched.
        """
        for filename in sorted(name for name, f in self._files.items() if f["dirty"]):
            state = self._files[filename]
            path = os.path.join(self._get_local_dir(), filename)
            if not self.fenced or state["unconditional"]:
                etag = self.storage.upload(path, filename)
            elif state["etag"] is None:
                etag = self.storage.upload(path, filename, if_none_match=True)
            else:
                etag = self.storage.upload(path, filename, if_match=state["etag"])
            state.update(etag=etag, dirty=False, unconditional=False)

        self._commit_index(rebase=rebase)

    def _commit_index(self, rebase: bool) -> None:
        """PUTs the serialized index, compare-and-swapping when fenced."""
        if not self.fenced:
            self._index_etag = self.storage.upload_bytes(self._index.to_bytes(), _INDEX_FILENAME)
            self._journal.clear()
            return

        for attempt in range(_REBASE_ATTEMPTS):
            try:
                if self._index_etag is None:
                    etag = self.storage.upload_bytes(self._index.to_bytes(), _INDEX_FILENAME, if_none_match=True)
                else:
                    etag = self.storage.upload_bytes(self._index.to_bytes(), _INDEX_FILENAME, if_match=self._index_etag)
            except ConflictError:
                if not rebase:
                    raise
                # Another writer committed since we loaded. Adopt its index and
                # replay our uncommitted writes on top: per-key last-writer-wins.
                data, remote_etag = self.storage.download_bytes_with_etag(_INDEX_FILENAME)
                merged = Index.from_bytes(data)
                merged.next_file_num = max(merged.next_file_num, self._index.next_file_num)
                for key, entry in self._journal.items():
                    if entry is None:
                        merged.delete(key)
                    else:
                        merged.put(key, entry.file_id, entry.offset, entry.length)
                self._index = merged
                self._index_etag = remote_etag
                continue
            self._index_etag = etag
            self._journal.clear()
            return
        raise ConflictError(f"index commit lost the race {_REBASE_ATTEMPTS} times; giving up")

    # ------------------------------------------------------------------ reads

    def get(self, key: str) -> str | bytes | None:
        """Returns the value for key, or None if it does not exist or has been deleted.

        If local_dir is set and the data file is present locally with the entry's
        full byte span, reads from disk. Otherwise fetches only that entry's bytes
        from S3 using a range request - no local directory is needed for read-only
        access. A local file that is too short for the entry (a stale mirror)
        falls back to S3 instead of failing.
        """
        entry = self._index.get(key)
        if entry is None:
            return None
        filename = _data_filename(entry.file_id)
        if self.local_dir:
            local_path = os.path.join(self.local_dir, filename)
            if os.path.exists(local_path):
                with open(local_path, "rb") as fh:
                    fh.seek(entry.offset)
                    raw = fh.read(entry.length)
                if len(raw) == entry.length:
                    _, value, _, _ = unpack_entry_at(raw, 0)
                    return value
        raw = self.storage.read_range(filename, entry.offset, entry.length)
        _, value, _, _ = unpack_entry_at(raw, 0)
        return value

    def get_many(self, keys: list[str], max_workers: int = 8) -> dict[str, str | bytes | None]:
        """Fetches many keys at once, issuing S3 range requests concurrently.

        Returns {key: value} with None for keys that do not exist. Latency for
        cold reads approaches one round-trip instead of one per key; local
        reads are unaffected.
        """
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            values = list(pool.map(self.get, keys))
        return dict(zip(keys, values))

    def keys(self) -> list[str]:
        """Returns a list of all live keys in the database."""
        return list(self._index.entries.keys())

    def __len__(self) -> int:
        return len(self._index.entries)

    def __contains__(self, key: str) -> bool:
        return key in self._index.entries

    def __iter__(self):
        return iter(self._index.entries)

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
            needed = {_data_filename(e.file_id) for e in self._index.entries.values()}
            for filename in needed:
                local_path = os.path.join(local_dir, filename)
                if not os.path.exists(local_path):
                    self.storage.download_file(filename, local_path)
            # Read values from disk and yield
            for key, entry in self._index.entries.items():
                local_path = os.path.join(local_dir, _data_filename(entry.file_id))
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

    def items(self, local: bool = False):
        """Alias for iter(): yields (key, value) pairs for every live key."""
        return self.iter(local=local)

    # ----------------------------------------------------------------- writes

    @staticmethod
    def _validate_key(key) -> None:
        """Rejects keys the storage format cannot represent, before any bytes are written."""
        if not isinstance(key, str):
            raise TypeError(f"keys must be str, got {type(key).__name__}")
        if len(key.encode("utf-8")) > MAX_KEY_BYTES:
            raise ValueError(f"key exceeds {MAX_KEY_BYTES} UTF-8 bytes")

    def put(self, items: dict[str, str | bytes]) -> None:
        """Writes one or more key/value pairs, appending to the current data file.

        Values may be str or bytes; get() returns whichever type was stored.
        Overwrites any existing value for a key. All keys and values are
        validated before anything is written, so a bad item cannot leave a
        partially applied batch. Writes are local until upload().
        """
        for key, value in items.items():
            self._validate_key(key)
            if not isinstance(value, (str, bytes)):
                raise TypeError(f"values must be str or bytes, got {type(value).__name__} for key {key!r}")
        if not items:
            return
        self._write_entries([(k, v, False) for k, v in items.items()])

    def delete(self, keys: list[str]) -> None:
        """Writes tombstones for each key that currently exists in the index.

        Keys not present in the index are silently skipped - no tombstone is written
        for them. Writes are local until upload().
        """
        for key in keys:
            self._validate_key(key)
        tombstones = [
            (k, None, True)
            for k in keys
            if self._index.get(k) is not None
        ]
        if tombstones:
            self._write_entries(tombstones)

    def _roll_file(self, local_dir: str):
        """Allocates the next file in this writer's namespace and opens it with a header.

        Returns (file_id, file handle). The new file is tracked as dirty so the
        next upload() transfers it.
        """
        self._seq += 1
        file_id = make_file_id(self._writer_id, self._seq)
        filename = _data_filename(file_id)
        fh = open(os.path.join(local_dir, filename), "wb")
        fh.write(pack_file_header(self._seq))
        self._files[filename] = {"etag": None, "dirty": True, "unconditional": False}
        self._active = filename
        return file_id, fh

    def _write_entries(self, entries: list[tuple[str, str | bytes | None, bool]]) -> None:
        """Appends a batch of entries to the current data file, rolling to a new file when needed.

        Entries is a list of (key, value, is_tombstone) tuples.

        Appends resume into this instance's active file while it is under
        max_file_size; otherwise a new file is allocated in this writer's
        namespace. Mid-batch, if writing the next entry would push the current
        file past max_file_size and the file already contains at least one
        entry, the writer rolls to the next file. A single entry that exceeds
        max_file_size on its own is never split; it is written to an
        otherwise-empty file, making that file exceed the soft limit.

        The in-memory index and the journal are updated after all entries are
        written; nothing is persisted to S3 (that is upload()'s job), and no
        local index file is written - recovery never reads one.
        """
        local_dir = self._get_local_dir()

        active_path = os.path.join(local_dir, self._active) if self._active else None
        if active_path and os.path.exists(active_path) and os.path.getsize(active_path) < self.max_file_size:
            file_id = make_file_id(self._writer_id, self._seq)
            fh = open(active_path, "ab")
        else:
            file_id, fh = self._roll_file(local_dir)

        written: list[tuple[str, int, int, int]] = []

        try:
            for key, value, is_tombstone in entries:
                packed = pack_entry(key, value, deleted=is_tombstone)
                pos = fh.tell()
                if pos > HEADER_SIZE and pos + len(packed) > self.max_file_size:
                    fh.close()
                    file_id, fh = self._roll_file(local_dir)
                offset = fh.tell()
                fh.write(packed)
                # Mark dirty only on an actual write: a batch that rolls past
                # the resumed file without appending must not re-upload it.
                self._files[self._active]["dirty"] = True
                if not is_tombstone:
                    written.append((key, file_id, offset, len(packed)))
        finally:
            fh.close()

        # Update the in-memory index and the rebase journal
        for key, fid, offset, length in written:
            self._index.put(key, fid, offset, length)
            self._journal[key] = IndexEntry(file_id=fid, offset=offset, length=length)
        for key, value, is_tombstone in entries:
            if is_tombstone:
                self._index.delete(key)
                self._journal[key] = None

    def flush(self) -> None:
        """Writes a local snapshot of the in-memory index to local_dir.

        Purely informational: recovery never reads the local index file (a
        restarted process loads the index from S3, and rebuild_index() replays
        data files). The write is atomic (temp file + rename)."""
        self._save_index()

    def _save_index(self) -> None:
        """Serializes the in-memory index and atomically writes it to the local index file."""
        data = self._index.to_bytes()
        local_dir = self._get_local_dir()
        final_path = os.path.join(local_dir, _INDEX_FILENAME)
        fd, tmp_path = tempfile.mkstemp(dir=local_dir, prefix=".index-", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
            os.replace(tmp_path, final_path)
        except BaseException:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    # --------------------------------------------------------------- recovery

    def compact(self) -> None:
        """Triggers compaction, rewriting data files to reclaim space from deleted/stale entries."""
        run_compaction(self)

    def rebuild_index(self, from_s3: bool = False) -> None:
        """Reconstructs the in-memory index by replaying data files in filename order.

        Replays local data files; with from_s3=True, first downloads every data
        file listed under the prefix (listing does not need the index object,
        so a lost index is recoverable from S3 alone). Every entry's CRC is
        verified during replay; replay of a file stops at its first bad entry
        (the signature of a torn write) with a warning, so garbage is never
        indexed. Entries apply sequentially, so later writes supersede earlier
        ones and tombstones delete. Replay order across different writers'
        files is lexicographic and therefore arbitrary; rebuild_index is a
        recovery tool for a single surviving writer, not a merge protocol.

        After a crash, replayed files are marked for (unconditional) re-upload
        so the next upload() publishes any orphaned entries along with the
        rebuilt index.
        """
        local_dir = self._get_local_dir()
        if from_s3:
            for filename in self.storage.list_data_files():
                local_path = os.path.join(local_dir, filename)
                if not os.path.exists(local_path):
                    self.storage.download_file(filename, local_path)

        data_files = sorted(_glob.glob(os.path.join(local_dir, "data_*.s4db")))
        new_index = Index()
        max_legacy_seq = 0

        for path in data_files:
            filename = os.path.basename(path)
            file_id = _parse_data_filename(filename)
            if file_id is None:
                continue
            writer_id, seq = split_file_id(file_id)
            if writer_id == 0:
                max_legacy_seq = max(max_legacy_seq, seq)
            with open(path, "rb") as fh:
                unpack_file_header(fh.read(HEADER_SIZE))
                for offset, raw, key, flags in stream_file_entries(fh):
                    try:
                        unpack_entry_at(raw, 0)
                    except CorruptEntryError:
                        warnings.warn(
                            f"rebuild_index: stopping replay of {filename} at offset {offset}: torn or corrupt entry"
                        )
                        break
                    if flags & FLAG_TOMBSTONE:
                        new_index.delete(key)
                    else:
                        new_index.put(key, file_id, offset, len(raw))
            # Anything replayed must reach S3 with the rebuilt index. These
            # files may already exist remotely (fully or partially uploaded
            # before a crash), so they upload unconditionally: recovery assumes
            # the writer that produced them is dead.
            if filename not in self._files or self._files[filename]["dirty"]:
                self._files[filename] = {"etag": None, "dirty": True, "unconditional": True}

        new_index.next_file_num = max_legacy_seq + 1
        self._index = new_index
        # The journal is superseded: the rebuilt index already contains every
        # local write. Rebase-merging after a rebuild would be wrong, so clear it.
        self._journal.clear()
        self._active = None

    def gc(self, grace_seconds: int = 24 * 3600) -> list[str]:
        """Deletes S3 data files that the live index does not reference.

        A crash between the data-file PUTs and the index PUT of an upload()
        leaves objects no index ever references; compaction conflicts leave
        replacement files behind the same way. gc() removes them and returns
        the deleted filenames.

        Objects younger than grace_seconds (default 24h) are kept, so a
        concurrent writer mid-sync -- files uploaded, index commit in flight --
        does not lose data. Run gc from a quiesced writer; raises
        UnsyncedWritesError if this instance has uncommitted writes.
        """
        if self._has_unsynced_writes():
            raise UnsyncedWritesError("upload() before running gc()")
        referenced = {_data_filename(e.file_id) for e in self._index.entries.values()}
        now = datetime.datetime.now(datetime.timezone.utc)
        deleted = []
        for filename, last_modified in self.storage.list_data_objects():
            if filename in referenced or filename in self._files:
                continue
            if (now - last_modified).total_seconds() < grace_seconds:
                continue
            self.storage.delete(filename)
            deleted.append(filename)
        return deleted

    # -------------------------------------------------------------- lifecycle

    def close(self) -> None:
        """Releases local resources; removes the temporary directory if this instance created it.

        Does not upload: pending writes are discarded exactly as on process
        exit. Call upload() first for durability."""
        if self._owns_local_dir and self.local_dir and os.path.isdir(self.local_dir):
            shutil.rmtree(self.local_dir, ignore_errors=True)
            self.local_dir = None
            self._owns_local_dir = False

    def __enter__(self) -> "S4DB":
        """Supports use as a context manager; returns self."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Cleans up owned temporary directories. Does not upload."""
        self.close()
