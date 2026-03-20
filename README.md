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
    local_dir="/tmp/my-db",       # created automatically if it does not exist
    bucket="my-bucket",
    prefix="my-db/",              # S3 key prefix; include a trailing slash
    max_file_size=64*1024*1024,   # optional, default 64 MB
    region_name="us-east-1",      # any extra kwargs go to boto3.client("s3", ...)
)

db.put({"hello": "world"})
print(db.get("hello"))  # "world"
db.delete(["hello"])
print(db.get("hello"))  # None
```

On `__init__`, the index is loaded from `local_dir` if present. If not found locally, it is downloaded from S3. If neither exists, the database starts empty. Data files are not downloaded automatically - only the index is.

## API reference

### `put(items: dict[str, str]) -> None`

Writes one or more key/value pairs in a single append to the current data file.

```python
db.put({"key1": "value1", "key2": "value2"})
```

- Overwrites any existing value for a key.
- If the current data file would exceed `max_file_size`, a new file is opened before writing.
- Updates the in-memory index and saves it to disk before returning.
- Does not push to S3 automatically - call `upload()` when ready to sync.

### `get(key: str) -> str | None`

Returns the value for a key, or `None` if the key does not exist or has been deleted.

```python
value = db.get("key1")
```

- Looks up the key in the index to get the file number and byte offset.
- If the data file is present in `local_dir`, reads exactly those bytes from disk.
- If the file is not local, fetches only that entry's bytes from S3 using a range request - the full file is never downloaded implicitly.
- Call `download()` first if you want all reads served from disk.

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

- Use this when pointing a fresh `local_dir` at an existing S3 database.
- After `download()`, all reads are served from disk with no S3 round trips.
- Overwrites any local files with the same name.

### `upload() -> None`

Pushes all local data files and the index to S3.

```python
db.upload()
```

- Useful after bulk operations like `compact()` or `rebuild_index()` to force a full re-sync.
- Does not check whether S3 already has the latest version - it uploads everything.

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
with S4DB("/tmp/my-db", "my-bucket", "my-db/") as db:
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

### Write locally, sync later

```python
db = S4DB("/tmp/my-db", "my-bucket", "my-db/")
db.put({"a": "1", "b": "2"})
db.delete(["a"])
db.upload()   # push everything to S3 when done
```

### Read-only from S3 without downloading all files

```python
db = S4DB("/tmp/my-db", "my-bucket", "my-db/")
# Index is loaded automatically; individual gets use S3 range requests
print(db.get("some-key"))
```

### Full local mirror

```python
db = S4DB("/tmp/my-db", "my-bucket", "my-db/")
db.download()   # pull everything local
print(db.get("some-key"))   # served from disk, no S3 call
```

### Periodic compaction

```python
db = S4DB("/tmp/my-db", "my-bucket", "my-db/")
db.download()   # ensure all data files are present
db.compact()    # rewrite, clean up S3, upload new files
```

### Index recovery

```python
db = S4DB("/tmp/my-db", "my-bucket", "my-db/")
db.download()       # pull all data files
db.rebuild_index()  # reconstruct index from data files
db.upload()         # push repaired index to S3
```

## Edge cases and gotchas

- `put()` and `delete()` do not push to S3 automatically. Call `upload()` explicitly.
- `get()` on a key whose data file is not local will make a ranged S3 request on every call. Use `download()` if you expect repeated access to the same keys.
- `compact()` and `rebuild_index()` require all data files to be present in `local_dir`. Always run `download()` first if you are not certain the local directory is up to date.
- `delete()` silently skips keys that are not in the index. It never writes unnecessary tombstones.
- If the process is interrupted during `put()` or `delete()`, the data file may contain entries that the index does not reference. `rebuild_index()` will recover them.
- `max_file_size` is a soft limit. An entry is never split across files, but a single oversized entry can make a file exceed the limit slightly.

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
