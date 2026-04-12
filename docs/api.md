## API reference

### `__init__(bucket, prefix, local_dir=None, max_file_size=...)`

```python
db = S4DB(
    bucket="my-bucket",
    prefix="my-db/",
    local_dir="/tmp/my-db",       # optional; a temp dir is created automatically if omitted
    max_file_size=64*1024*1024,   # optional, default 64 MB
    region_name="ap-south-1",     # any extra kwargs go to boto3.client("s3", ...)
)
```

- `local_dir` is optional. If not provided, no directory is touched until a `put()` or `delete()` is called, at which point a temporary directory is created automatically.
- Read-only operations (`get`, `keys`) never require a local directory - they use the in-memory index and S3 range requests.
- The index is always loaded from S3 into memory on init; it is never read from a local file.

### `put(items: dict[str, str]) -> None`

Writes one or more key/value pairs in a single append to the current data file.

```python
db.put({"key1": "value1", "key2": "value2"})
```

- Overwrites any existing value for a key.
- If the current data file would exceed `max_file_size`, a new file is opened before writing.
- Creates `local_dir` (or a temp dir) on first call if none was provided.
- Does not push to S3 automatically - call `upload()` when ready to sync.

### `get(key: str) -> str | None`

Returns the value for a key, or `None` if the key does not exist or has been deleted.

```python
value = db.get("key1")
```

- Looks up the key in the index to get the file number and byte offset.
- If `local_dir` is set and the data file is present there, reads exactly those bytes from disk.
- Otherwise fetches only that entry's bytes from S3 using a range request - the full file is never downloaded, and no local directory is needed.
- Call `download()` first if you want all reads served from disk.

### `keys() -> list[str]`

Returns a list of all live keys currently in the database.

```python
all_keys = db.keys()
```

- Reads directly from the in-memory index - no disk or S3 access.
- Only returns keys that are live (not deleted). Tombstoned keys are never included.
- The order of the returned list is not guaranteed.

### `iter(local=False) -> Generator[tuple[str, str], ...]`

Yields `(key, value)` pairs for every live key in the database.

```python
for key, value in db.iter():
    print(key, value)
```

The `local` parameter controls how values are read:

- `local=False` (default) - for each key, calls `get()` which fetches only that entry's bytes from S3 using a range request. No files are downloaded. Use this for sparse access or when disk space is limited.
- `local=True` - before iteration, downloads all data files referenced by the index that are not already present in `local_dir`. Existing local files are not replaced. Values are then read from disk - no S3 calls during iteration itself. Use this when iterating over many keys to avoid one S3 request per key.

```python
# S3 range request per key (default)
for key, value in db.iter():
    process(key, value)

# Download missing files first, then read from disk
for key, value in db.iter(local=True):
    process(key, value)
```

- Deleted keys are never yielded.
- The iteration order is not guaranteed.
- `iter(local=True)` creates `local_dir` (or a temp dir) if none was provided.

### `delete(keys: list[str]) -> None`

Writes tombstone entries for each key that exists in the index.

```python
db.delete(["key1", "key2"])
```

- Keys not present in the index are silently skipped; no tombstone is written for them.
- Removes the keys from the in-memory index immediately.
- Tombstones consume space until `compact()` is run.

### `download() -> None`

Downloads all data files and the index from S3 into `local_dir`.

```python
db.download()
```

- Creates `local_dir` (or a temp dir) if none was provided.
- Use this when you want all subsequent reads served from disk with no S3 round trips.
- Overwrites any local files with the same name.

### `upload() -> None`

Pushes all local data files and the in-memory index to S3.

```python
db.upload()
```

- The index is serialized directly from memory - no local index file is required.
- If `local_dir` is not set, only the index is uploaded (no local data files exist).
- Useful after bulk operations like `compact()` or `rebuild_index()` to force a full re-sync.
- Does not check whether S3 already has the latest version - it uploads everything.

### `flush() -> None`

Writes the in-memory index to disk.

```python
db.flush()
```

- Creates `local_dir` (or a temp dir) if none was provided.
- `put()` and `delete()` already call `flush()` internally.

### `compact() -> None`

Rewrites all data files to reclaim space from deleted and overwritten entries.

```python
db.compact()
```

- Reads every entry from every local data file.
- Retains only entries whose (file number, byte offset) still matches the in-memory index - stale overwrites and tombstones are dropped.
- Writes the surviving entries into new sequentially numbered files, respecting `max_file_size`.
- Clears and rebuilds the index from the new locations, saves it, removes the old local files, deletes the old S3 objects, and uploads the new files and index.
- Run `download()` first if `local_dir` may be out of date.
- All data files must be present locally; compaction does not fetch missing files from S3.

### `rebuild_index() -> None`

Reconstructs the index by replaying all local data files from scratch.

```python
db.rebuild_index()
```

- Scans every `data_*.s4db` file in `local_dir` in order, applying puts and tombstones sequentially.
- Later entries correctly overwrite earlier ones for the same key.
- Saves the rebuilt index to disk. Does not push to S3 automatically.
- Use this for recovery when the index file is lost or corrupted.
- Run `download()` first to ensure all data files are present locally.

### Context manager

`S4DB` supports the context manager protocol. The `__exit__` is a no-op - there is no connection to close - but the pattern keeps resource handling consistent.

```python
with S4DB("my-bucket", "my-db/") as db:
    db.put({"k": "v"})
    print(db.get("k"))
```
