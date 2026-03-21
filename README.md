# s4db - Simple DB on S3

A lightweight key-value store where keys and values are strings. Data is written to numbered binary files on disk and synced to S3. Values are Snappy-compressed. An in-memory index tracks the exact file and byte offset for every live key, so reads never scan - they seek directly.

## Installation

```bash
pip install s4db
```

`s4db` requires `python-snappy`, which links against the native Snappy C library.

```bash
# macOS
brew install snappy

# Ubuntu / Debian
apt-get install libsnappy-dev
```

## Quick start

```python
from s4db import S4DB

db = S4DB(
    bucket="my-bucket",
    prefix="my-db/",              # S3 key prefix; include a trailing slash
    region_name="ap-south-1",     # any extra kwargs go to boto3.client("s3", ...)
)

db.put({"hello": "world"})
print(db.get("hello"))  # "world"
db.delete(["hello"])
print(db.get("hello"))  # None
```

On `__init__`, the index is downloaded from S3 into memory. If no index exists, the database starts empty. No local directory is created or used until a write operation (`put` / `delete`) is called.

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

- **`local=False` (default)** - for each key, calls `get()` which fetches only that entry's bytes from S3 using a range request. No files are downloaded. Use this for sparse access or when disk space is limited.
- **`local=True`** - before iteration, downloads all data files referenced by the index that are not already present in `local_dir`. Existing local files are **not** replaced. Values are then read from disk - no S3 calls during iteration itself. Use this when iterating over many keys to avoid one S3 request per key.

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

## S3 layout

Given `bucket="my-bucket"` and `prefix="my-db/"`:

```
my-bucket/
  my-db/
    index.idx
    data_000001.s4db
    data_000002.s4db
    ...
```

Data files are named `data_NNNNNN.s4db` with zero-padded six-digit sequence numbers. The index file is always `index.idx`.

## Typical workflows

### Read-only from S3 - no local directory needed

```python
db = S4DB("my-bucket", "my-db/")
# Index is loaded from S3 into memory; gets use S3 range requests
print(db.get("some-key"))
print(db.keys())
```

### Write locally, sync later

```python
db = S4DB("my-bucket", "my-db/", local_dir="/tmp/my-db")
db.put({"a": "1", "b": "2"})
db.delete(["a"])
db.upload()   # push everything to S3 when done
```

### Write without specifying local_dir (temp dir created automatically)

```python
db = S4DB("my-bucket", "my-db/")
db.put({"a": "1"})   # temp dir created here on first write
db.upload()
```

### Full local mirror

```python
db = S4DB("my-bucket", "my-db/", local_dir="/tmp/my-db")
db.download()   # pull everything local
print(db.get("some-key"))   # served from disk, no S3 call
```

### Iterate over all key/value pairs

```python
# One S3 range request per key (no local files needed)
db = S4DB("my-bucket", "my-db/")
for key, value in db.iter():
    print(key, value)

# Download missing files first, then read entirely from disk
db = S4DB("my-bucket", "my-db/", local_dir="/tmp/my-db")
for key, value in db.iter(local=True):
    print(key, value)
```

### Periodic compaction

```python
db = S4DB("my-bucket", "my-db/", local_dir="/tmp/my-db")
db.download()   # ensure all data files are present
db.compact()    # rewrite, clean up S3, upload new files
```

### Index recovery

```python
db = S4DB("my-bucket", "my-db/", local_dir="/tmp/my-db")
db.download()       # pull all data files
db.rebuild_index()  # reconstruct index from data files
db.upload()         # push repaired index to S3
```

## Edge cases and gotchas

- `local_dir` is not required for read-only usage. A temporary directory is created automatically on the first `put()` or `delete()` call if none was provided.
- `put()` and `delete()` do not push to S3 automatically. Call `upload()` explicitly.
- `get()` on a key whose data file is not local will make a ranged S3 request on every call. Use `download()` if you expect repeated access to the same keys.
- `compact()` and `rebuild_index()` require all data files to be present in `local_dir`. Always run `download()` first if you are not certain the local directory is up to date.
- `delete()` silently skips keys that are not in the index. It never writes unnecessary tombstones.
- If the process is interrupted during `put()` or `delete()`, the data file may contain entries that the index does not reference. `rebuild_index()` will recover them.
- `max_file_size` is a soft limit. An entry is never split across files, but a single oversized entry can make a file exceed the limit slightly.
- `iter(local=False)` makes one S3 range request per key. For large datasets prefer `iter(local=True)` to batch the S3 downloads upfront.
- `iter(local=True)` only downloads files referenced by the current in-memory index. Files that contain only deleted or overwritten entries are not downloaded.

## Dependencies

- [boto3](https://github.com/boto/boto3) >= 1.26
- [python-snappy](https://github.com/andrix/python-snappy) >= 0.6

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

Tests use [moto](https://github.com/getmoto/moto) to mock S3 - no real AWS credentials required.

## License

MIT
