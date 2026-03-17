# s4db

A lightweight key-value database backed by S3. Keys and values are strings. Values are snappy-compressed and packed into numbered binary data files that are written locally and pushed to S3. Reads use S3 byte-range fetches, only the bytes for the requested key are transferred.

## How it works

Writes are append-only. Each `put` or `delete` call creates one or more numbered data files (`data_000001.s4db`, `data_000002.s4db`, ...) and pushes them to S3. A separate index file (`index.json`) maps every live key to its file number, byte offset, and entry length. The index is loaded from S3 at startup and updated after every write.

Reads look up the key in the index, fetch exactly those bytes from S3 using a range request, verify the CRC, decompress the value, and return it.

Updates overwrite the index entry to point at the new location; the old bytes are left in place and cleaned up during compaction. Deletes write a tombstone entry to disk and remove the key from the index.

## Data file format

Each `.s4db` file starts with a 9-byte header followed by a sequence of entries.

```
File header:
  magic     4 bytes   b"S4DB"
  version   1 byte    0x01
  file_num  4 bytes   uint32, big-endian

Entry:
  flags     1 byte    0x00 = normal, 0x01 = tombstone
  key_len   4 bytes   uint32, big-endian
  value_len 4 bytes   uint32, big-endian  (0 for tombstones)
  key       key_len bytes, UTF-8
  value     value_len bytes, snappy-compressed  (absent for tombstones)
  crc32     4 bytes   uint32, big-endian, over all preceding entry bytes
```

## Index file format

`index.json` is stored at `{prefix}index.json` alongside the data files.

```json
{
  "version": 1,
  "next_file_num": 4,
  "entries": {
    "key1": [1, 9, 58],
    "key2": [3, 67, 42]
  }
}
```

Each entry value is `[file_num, byte_offset, entry_length]`. Deleted keys are removed from the index.

## Installation

```bash
pip install s4db
```

Requires `python-snappy`, which links against the native Snappy library.

```bash
# macOS
brew install snappy

# Ubuntu / Debian
apt-get install libsnappy-dev
```

## Usage

```python
from s4db import S4DB

db = S4DB(
    bucket="my-bucket",
    prefix="my-db/",
    max_file_size=64 * 1024 * 1024,  # 64 MB, default
    local_dir="/tmp/s4db",           # optional, defaults to a temp dir
    region_name="us-east-1",         # any extra kwargs go to boto3.client("s3", ...)
)
```

### Writing

```python
db.put({"key1": "value1", "key2": "value2"})
```

Creates one or more `.s4db` files locally, uploads them to S3, and updates the index. If a single batch exceeds `max_file_size`, it is split across multiple files automatically.

### Reading

```python
value = db.get("key1")   # returns the string value, or None if not found
```

Issues a single S3 byte-range GET for the exact bytes of that entry.

### Deleting

```python
db.delete(["key1", "key2"])
```

Writes tombstone entries to a new data file, removes the keys from the index, and pushes to S3. Only keys that currently exist in the index are written.

### Compaction

```python
db.compact()
```

Downloads all data files, keeps only the latest value for each key, discards tombstones, rewrites the live data into new numbered files, updates the index, and deletes the old files from S3.

### Rebuilding the index

```python
db.rebuild_index()
```

Scans every data file on S3 in order and reconstructs the index from scratch. Use this for recovery if the index file is lost or corrupted.

### Context manager

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
    index.json
    data_000001.s4db
    data_000002.s4db
    ...
```

## Dependencies

- [boto3](https://github.com/boto/boto3) >= 1.26
- [python-snappy](https://github.com/andrix/python-snappy) >= 0.6

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

Tests use [moto](https://github.com/getmoto/moto) to mock S3, no real AWS credentials required.

## License

MIT
