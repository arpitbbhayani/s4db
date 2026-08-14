## API reference

### `__init__(bucket, prefix, local_dir=None, max_file_size=..., fenced=True)`

```python
db = S4DB(
    bucket="my-bucket",
    prefix="my-db/",
    local_dir="/tmp/my-db",       # optional; a temp dir is created automatically if omitted
    max_file_size=64*1024*1024,   # optional, default 64 MB
    fenced=True,                  # optional; conditional writes + CAS index commits
    region_name="ap-south-1",     # any extra kwargs go to boto3.client("s3", ...)
)
```

- `local_dir` is optional. If not provided, no directory is touched until a `put()` or `delete()` is called, at which point a temporary directory is created automatically (and removed again by `close()` / the context manager).
- Read-only operations (`get`, `keys`) never require a local directory - they use the in-memory index and S3 range requests.
- Opening costs a single GET: the index is loaded from S3 into memory; it is never read from a local file. A missing index object means a new, empty database.
- Every instance writes data files under its own random *writer namespace* (`data_XXXXXXXX_NNNNNN.s4db`), so concurrent writers can never overwrite each other's data files.
- With `fenced=True` (default), uploads use S3 conditional writes: data files are created with `If-None-Match` / replaced with `If-Match`, and the index commit is a compare-and-swap. Concurrent commits are detected and merged per key (last-committer-wins) instead of silently lost. Set `fenced=False` only for S3-compatible services without conditional-write support.

### `put(items: dict[str, str | bytes]) -> None`

Writes one or more key/value pairs in a single append to the current data file.

```python
db.put({"key1": "value1", "blob": b"\x00\x01"})
```

- Values may be `str` or `bytes`; `get()` returns whichever type was stored.
- Values are Snappy-compressed only when compression actually shrinks them.
- Overwrites any existing value for a key.
- All keys and values are validated up front (keys must be `str`, at most 65,535 UTF-8 bytes), so a bad item cannot leave a partially applied batch.
- If the current data file would exceed `max_file_size`, a new file is opened before writing.
- Does not push to S3 automatically - call `upload()` when ready to sync.

### `get(key: str) -> str | bytes | None`

Returns the value for a key, or `None` if the key does not exist or has been deleted.

```python
value = db.get("key1")
```

- Looks up the key in the index to get the file id and byte offset.
- If `local_dir` is set and the data file is present there with the entry's full byte span, reads exactly those bytes from disk. A stale (too-short) local file falls back to S3 instead of failing.
- Otherwise fetches only that entry's bytes from S3 using a range request - the full file is never downloaded, and no local directory is needed.
- Every entry's CRC is verified before its value is decompressed or decoded; corruption raises `CorruptEntryError`.

### `get_many(keys: list[str], max_workers=8) -> dict[str, str | bytes | None]`

Fetches many keys at once, issuing S3 range requests concurrently.

```python
values = db.get_many(["k1", "k2", "k3"])
```

- Returns `{key: value}` with `None` for missing keys.
- Cold bulk reads approach one round-trip of latency instead of one per key.

### `keys() -> list[str]` / `len(db)` / `key in db` / `for key in db`

The index is directly queryable: `keys()` returns all live keys, `len()` counts them, `in` tests membership, and iterating the instance yields keys. All read only the in-memory index - no disk or S3 access.

### `iter(local=False)` / `items(local=False)`

Yields `(key, value)` pairs for every live key in the database.

- `local=False` (default) - for each key, calls `get()`, which uses an S3 range request when the file is not local.
- `local=True` - first downloads all data files referenced by the index that are not already present in `local_dir`; values are then read from disk with no S3 calls during iteration.

### `delete(keys: list[str]) -> None`

Writes tombstone entries for each key that exists in the index.

- Keys not present in the index are silently skipped; no tombstone is written for them.
- Removes the keys from the in-memory index immediately.
- Tombstones consume space until `compact()` is run.

### `upload(rebase=True) -> None`

Publishes local writes: PUTs data files written or grown since the last upload, then commits the index. The index PUT is the atomic commit point.

- Only *changed* files are transferred (dirty-file tracking); a grown file replaces its S3 object with an extended version, so published entry locations stay valid.
- Each file is one PUT (no multipart); the 64 MB default cap is far below S3's 5 GB single-PUT limit.
- When fenced, a concurrent writer's commit is detected at the index compare-and-swap. With `rebase=True` (default) the winning index is reloaded, this writer's uncommitted entries are re-applied on top (per-key last-writer-wins), and the commit retries. With `rebase=False` a lost race raises `ConflictError` and nothing is lost: the data files are already uploaded in this writer's own namespace.

### `download(force=False) -> None`

Downloads all data files and the index from S3 into `local_dir`.

- Files this instance wrote are left untouched.
- Raises `UnsyncedWritesError` if there are uncommitted local writes that refreshing the index would orphan; `force=True` discards them.

### `flush() -> None`

Writes a local snapshot of the in-memory index (atomic temp-file + rename). Purely informational: recovery never reads a local index file. `put()`/`delete()` do **not** call it.

### `compact() -> None`

Rewrites all data files to reclaim space from deleted and overwritten entries.

- Requires a synced state (`upload()` first) and a complete local mirror: every file the index references must be present with the full referenced byte span, or `CompactionError` is raised.
- Survivors are CRC-verified and written into new files in this writer's namespace.
- Publish order makes the index PUT the commit point: new files upload first, then the index commits (compare-and-swap when fenced), and only then are old objects deleted. A crash mid-compaction leaves garbage for `gc()`, never data loss.
- A lost compare-and-swap raises `ConflictError` and leaves the database untouched.

### `rebuild_index(from_s3=False) -> None`

Reconstructs the index by replaying data files from scratch.

- With `from_s3=True`, first downloads every data file listed under the prefix - listing does not need the index object, so a *lost index is recoverable from S3 alone*.
- Every entry's CRC is verified during replay; replay of a file stops at its first bad entry (a torn write) with a warning, so garbage is never indexed.
- Replayed files are marked for re-upload, so the next `upload()` publishes orphaned entries along with the rebuilt index.
- Replay order across different writers' files is arbitrary; treat rebuild as a single-survivor recovery tool, not a merge protocol.

### `gc(grace_seconds=86400) -> list[str]`

Deletes S3 data files that the live index does not reference (orphans from crashed uploads or conflicted compactions) and returns their names. Objects younger than `grace_seconds` are kept so a concurrent writer mid-sync is not disturbed. Raises `UnsyncedWritesError` if this instance has uncommitted writes.

### `close()` / context manager

`close()` removes the temporary directory if this instance created one; it does **not** upload. The context manager calls `close()` on exit.

```python
with S4DB("my-bucket", "my-db/") as db:
    db.put({"k": "v"})
    db.upload()
```

### Exceptions

All errors derive from `s4db.S4DBError`: `ConflictError` (lost a fenced commit race), `UnsyncedWritesError` (operation would discard uncommitted writes), `CorruptEntryError` / `CorruptIndexError` (CRC or parse failure), `CompactionError` (compaction preconditions unmet).
