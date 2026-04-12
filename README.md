# s4db - Simple DB on S3

A lightweight embedded key-value store where keys and values are strings. Data is written to numbered binary files on disk and synced to S3. Values are Snappy-compressed. An in-memory index tracks the exact file and byte offset for every live key, so reads never scan - they seek directly.

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

## When to use s4db

s4db fits workloads that need durable key-value semantics on ephemeral compute - without a running database.

**Good fits**
- **Lambda / serverless state** - load index on cold start (~50 ms S3 RTT), mutate in memory, `upload()` once before return. No VPC, no connections.
- **Batch pipeline checkpoints** - write processed keys as you go, `upload()` periodically. Restart resumes from the existing index.
- **Read-heavy config / lookup tables** - write once, `download()` on each worker at startup, all reads from local disk at ~0.009 ms.
- **ETL joins** - pre-load a lookup table into s4db, upload to S3, workers download at startup. ~0.009 ms median lookup vs ~50 ms per S3 GET.
- **Experiment tracking** - log metrics and artefacts to s4db, sync to S3 at end of run. Queryable by key from any machine.

**Not a fit** - key space doesn't fit in RAM, range queries needed, concurrent writers on the same prefix, or sub-ms reads without a warm local copy.

**Key numbers** (real S3, ap-south-1 / ap-east-1):

| Operation | Latency / throughput |
| --------- | -------------------- |
| `get()` - local disk | ~0.009 ms median |
| `get()` - S3 range request | ~49 ms median, ~113 ms p99 |
| `put()` batched (1000 keys/call) | ~12 769 keys/sec |
| s4db vs naive one-object-per-key writes | **181x faster, 1000x fewer PUTs, 49x cheaper** |

Full numbers in [Benchmarks](docs/benchmarks.md).

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

## Documentation

- [API reference](docs/api.md)
- [Benchmarks](docs/benchmarks.md)

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
