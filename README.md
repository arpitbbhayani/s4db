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

## Benchmarks

Run the suite yourself (requires real AWS credentials and an S3 bucket):

```bash
python benchmarks/bench.py
```

All numbers below are from real S3 (ap-east-1). Results will vary with region
and network conditions, but the relative rankings and request-count comparisons
hold across environments.

### 1. Write throughput - batched vs unbatched

2000 key/value pairs, ~256 B values, one `upload()` at the end.

| Scenario                               | Throughput        | Elapsed    |
| -------------------------------------- | ----------------- | ---------- |
| `put(100 keys)` × 20 + `upload()`     | ~6 134 keys/sec   | ~326 ms    |
| `put(1 key)` × 2000 + `upload()`     | ~749 keys/sec     | ~2 669 ms  |
| Batched speedup                        | ~8×               |            |

Every `put()` call ends by rewriting the entire in-memory index to disk
(`_write_entries` → `flush()` → `_save_index()`). Unbatched writes trigger this
full index serialization on every single call - 2000 times vs 20 times for
batched - and that repeated disk I/O dominates elapsed time.

### 2. Read latency - S3 range request vs local disk

1,000 keys pre-loaded, 500 `get()` trials, Zipf-like hot/cold distribution
(top 20% of keys receive 80% of reads).

| Metric | S3 range request | Local disk | Speedup   |
| ------ | ---------------- | ---------- | --------- |
| mean   | 55.964 ms        | 0.008 ms   | ~6871×   |
| median | 49.411 ms        | 0.008 ms   | ~6350×   |
| p95    | 59.577 ms        | 0.009 ms   | ~6949×   |
| p99    | 293.937 ms       | 0.011 ms   | ~26044×  |
| min    | 38.538 ms        | 0.008 ms   | ~5120×   |
| max    | 345.966 ms       | 0.099 ms   | ~3511×   |

Local disk - hot vs cold key breakdown:

| Key set              | mean     | p95      | n    |
| -------------------- | -------- | -------- | ---- |
| Hot  (top 20%)       | 0.008 ms | 0.008 ms | 400  |
| Cold (bottom 80%)    | 0.008 ms | 0.010 ms | 100  |

The ~50 ms S3 mean is pure network round-trip time from ap-south-1. A range
request must cross the public internet to S3, wait for the object store to
seek to the byte offset, and stream the response back - all before the caller
gets a value. Even a fast region adds 35–60 ms of baseline RTT.

The p99 spike to ~294 ms is S3 tail latency: occasional GETs are queued
behind internal S3 housekeeping or hit a cold shard. This is a well-known
property of cloud object stores and does not reflect anything wrong with
the client.

Local disk reads land at ~0.008 ms regardless of whether the key is hot or
cold. The in-memory index stores the exact file number and byte offset for
every key, so every `get()` is a single `pread()` call to the right position
in the data file - no scanning, no cache warming needed. Hot and cold keys
are therefore indistinguishable at the disk layer.

### 3. s4db vs Naive S3 (one object per key)

The straw-man baseline: each key is a separate S3 object, written with
`put_object` and read with `get_object`. 2000 writes + 500 reads on a 1000-key
corpus. All numbers are from real S3 (ap-south-1).

| Metric               | Naive S3          | s4db (batched)    | s4db advantage    |
| -------------------- | ----------------- | ----------------- | ----------------- |
| Write throughput     | ~14 keys/sec      | ~2485 keys/sec    | ~181× faster      |
| S3 PUT requests      | 2000              | 2                 | 1000× fewer       |
| S3 GET requests      | 500               | 500               | equal             |
| Read mean latency    | 61.243 ms         | 52.796 ms         | ~1.2× faster      |
| Read p99 latency     | 125.905 ms        | 115.535 ms        | ~1.1× faster      |
| Estimated API cost   | $0.010200         | $0.000210         | ~49× cheaper      |

S3 API call breakdown (writes):

| Approach | Calls                                       |
| -------- | ------------------------------------------- |
| Naive S3 | `put_object` × 2000                         |
| s4db     | `upload_file` × 1 (data file) + `put_object` × 1 (index) |

**Why write throughput is 181× faster?**

Naive S3 makes 2000 sequential `put_object` calls - one per key.
Each call incurs a full S3 round-trip (~50 ms),
so 2000 writes take roughly 100 seconds of S3 wait
time. s4db appends all 2000 keys to a single local data file and calls
`upload()` once, issuing 2 S3 calls total - one multipart upload for the data
file and one `put_object` for the serialized index. The network round-trip cost
is therefore amortized across all 2000 keys instead of paid per key.

**Why S3 PUT count is 1000× lower?**

s4db's write path is append-local,
upload-once: `put()` writes to a local file, and `upload()` pushes the file to
S3 as a single object. No matter how many keys are in that batch, one data file
→ one S3 PUT. Naive S3 has no such batching; every key is its own object.

**Why read latency is similar (and s4db is slightly faster)?**

Both approaches issue one S3 GET per `get()` call, so both pay the
same ~50–60 ms network RTT. s4db uses a range request to fetch only
the exact bytes for that entry; Naive S3 fetches the full object.
For the 256 B values in this benchmark the objects
are tiny, so the object-size advantage is small - but the range request still
avoids transferring any surrounding data, giving s4db a modest edge at both
mean and p99.

**Why cost is 49× lower?**

S3 API pricing is per-request. At $0.005/1000 PUTs,
2000 PUTs cost $0.010. s4db's 2 PUTs cost effectively nothing in PUT fees;
the $0.000210 is almost entirely the 500 GET charges. Read-request counts are
equal, so the entire cost gap comes from write-side request reduction.

> Pricing basis: AWS S3 Standard ap-south-1 - PUT $0.005/1000 requests,
> GET $0.0004/1000 requests (2025).

### 4. The S3 tax - local Bitcask vs s4db

To quantify what S3 costs in latency terms, s4db's two read paths are compared
against a minimal local Bitcask (same append-only-log + in-memory-index design,
pure disk, no S3). 1000 keys pre-loaded, 500 `get()` trials. All numbers are
from real S3 (ap-south-1).

| Metric | Local Bitcask | s4db local disk | s4db S3 range |
| ------ | ------------- | --------------- | ------------- |
| mean   | 0.001 ms      | 0.008 ms        | 50.235 ms     |
| median | 0.001 ms      | 0.008 ms        | 49.332 ms     |
| p95    | 0.002 ms      | 0.009 ms        | 57.650 ms     |
| p99    | 0.003 ms      | 0.016 ms        | 111.778 ms    |
| min    | 0.000 ms      | 0.008 ms        | 37.437 ms     |
| max    | 0.007 ms      | 0.084 ms        | 130.381 ms    |

**Local Bitcask (~0.001 ms):** The fastest possible baseline - a raw binary file
with a 6-byte header per entry, a single `pread()` to the exact offset, and no
decompression. The entire read is one syscall after a memory lookup.

**s4db local disk (~0.008 ms, ~8x vs Bitcask):** Same append-only-log + in-memory-index
design, but `get()` also decompresses the value with Snappy and parses the s4db
entry format on top of the identical seek. No S3 is involved. That extra work
accounts for the ~8x gap over the bare Bitcask.

**s4db S3 range (~50 ms, ~6000x vs local disk):** Every `get()` issues an HTTP
range request to S3 in ap-south-1, paying a full network round-trip before a
single byte is returned. The ~50 ms mean is the baseline RTT to S3 from the test
host; the p99 spike to ~112 ms reflects occasional S3 tail latency when a GET
hits a cold shard or internal housekeeping. This is the explicit cost of
serverless durability - no local disk is required at all. When a local directory
is available, `download()` + local reads bring latency back to the local-disk
column.

### 5. Bulk write throughput at scale (100000 keys, 256 B values)

Pattern: `put(batch)` repeated until all keys are written, then one `upload()`.
`batch_size=1` is omitted: each `put(1)` flushes the full index to disk, making it
O(n²) in index size at 100 k scale.

| Batch size | keys/sec    | Elapsed     | S3 PUTs | Speedup vs batch 100 |
| ---------: | ----------: | ----------: | ------: | -------------------: |
| 100        | ~4519       | ~22128 ms   | 2       | 1×                   |
| 1000       | ~12769      | ~7832 ms    | 2       | ~2.8×                |
| 10000      | ~17195      | ~5816 ms    | 2       | ~3.8×                |

S3 PUT count is constant at 2 (one data file, one index) regardless of batch size,
because `upload()` is called once at the end.

Two effects determine the shape of these numbers:

**Flush cost (drives the speedup).**

Every `put()` serializes the full in-memory
index to disk before returning. With batch size 100, that flush runs 1 000 times
(100 000 / 100); with batch size 10 000 it runs only 10 times. Larger batches
amortize this O(index-size) serialization across more keys, which is why throughput
rises with batch size.

**Real S3 upload cost (caps the speedup at ~3.8×).**

After all the `put()` calls
finish, a single `upload()` pushes the data file and index to S3 over the network.
On real S3 that transfer takes several seconds regardless of batch size. At batch
100, the 1 000 flushes dominate (~17 s of disk I/O) and the upload is a small
fraction of the total. At batch 10 000, the 10 flushes complete in under a second,
but the upload still costs the same several seconds - it becomes the bottleneck.
The speedup ceiling is therefore set by the ratio of (flush work saved) to
(fixed upload cost), which on real S3 is only ~3.8× rather than the ~25× seen
with a mocked S3 where `upload()` is effectively free.

### Workload B - point read latency at scale (100 000 keys pre-loaded, 500 trials)

Cold condition: no local files; each `get()` issues an S3 range request.
Warm condition: `download()` called once before trials; reads served from local disk.

| Metric | Cold (S3 range) | Warm (local disk) | Speedup |
| ------ | --------------- | ----------------- | ------- |
| mean   | 4.875 ms        | 0.027 ms          | ~179×   |
| median | 4.625 ms        | 0.021 ms          | ~221×   |
| p95    | 6.061 ms        | 0.054 ms          | ~112×   |
| p99    | 7.978 ms        | 0.186 ms          | ~43×    |

At 100 000 keys the cold-read penalty grows relative to the 1000-key case above
because the data file is larger and moto's range-request handler does proportionally
more work. On real S3 the RTT dominates and the gap is driven by network latency,
not file size. Calling `download()` once at invocation start amortizes that cost
across all subsequent reads.

### Workload C - mixed read/write, realistic Lambda pattern (100 000 key space)

80% reads, 20% writes, 10 000 operations total. `upload()` called once at the end
to simulate end-of-invocation flush. Reads are served from local disk (`local_dir`
is set and the database was seeded before the mixed phase).

| Metric              | Value       |
| ------------------- | ----------- |
| Total throughput    | ~128 ops/sec |
| Read mean (p99)     | 0.048 ms (0.454 ms) |
| Write mean (p99)    | 38.7 ms (56.8 ms)   |
| S3 PUTs             | 2 (one data file, one index) |

Write latency dominates: every `put()` flushes the entire in-memory index (100 000
entries) to disk before returning, which costs ~38 ms per call. In a Lambda
invocation pattern, all writes should be batched into a single `put(batch)` +
`upload()` at the end rather than one `put()` per key. Read latency (0.048 ms
mean) is fast because local files are present; a cold invocation with S3 range
requests would see ~5 ms per read as shown in Workload B.

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
