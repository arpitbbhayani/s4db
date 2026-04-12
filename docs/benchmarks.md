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
gets a value. Even a fast region adds 35-60 ms of baseline RTT.

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

#### Why write throughput is 181× faster?

Naive S3 makes 2000 sequential `put_object` calls - one per key.
Each call incurs a full S3 round-trip (~50 ms),
so 2000 writes take roughly 100 seconds of S3 wait
time. s4db appends all 2000 keys to a single local data file and calls
`upload()` once, issuing 2 S3 calls total - one multipart upload for the data
file and one `put_object` for the serialized index. The network round-trip cost
is therefore amortized across all 2000 keys instead of paid per key.

#### Why S3 PUT count is 1000× lower?

s4db's write path is append-local,
upload-once: `put()` writes to a local file, and `upload()` pushes the file to
S3 as a single object. No matter how many keys are in that batch, one data file
→ one S3 PUT. Naive S3 has no such batching; every key is its own object.

#### Why read latency is similar (and s4db is slightly faster)?

Both approaches issue one S3 GET per `get()` call, so both pay the
same ~50-60 ms network RTT. s4db uses a range request to fetch only
the exact bytes for that entry; Naive S3 fetches the full object.
For the 256 B values in this benchmark the objects
are tiny, so the object-size advantage is small - but the range request still
avoids transferring any surrounding data, giving s4db a modest edge at both
mean and p99.

#### Why cost is 49× lower?

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

#### Flush cost (drives the speedup)

Every `put()` serializes the full in-memory
index to disk before returning. With batch size 100, that flush runs 1 000 times
(100 000 / 100); with batch size 10000 it runs only 10 times. Larger batches
amortize this O(index-size) serialization across more keys, which is why throughput
rises with batch size.

#### Real S3 upload cost (caps the speedup at ~3.8×)

After all the `put()` calls
finish, a single `upload()` pushes the data file and index to S3 over the network.
On real S3 that transfer takes several seconds regardless of batch size. At batch
100, the 1000 flushes dominate (~17 s of disk I/O) and the upload is a small
fraction of the total. At batch 10000, the 10 flushes complete in under a second,
but the upload still costs the same several seconds - it becomes the bottleneck.
The speedup ceiling is therefore set by the ratio of (flush work saved) to
(fixed upload cost), which on real S3 is only ~3.8× rather than the ~25× seen
with a mocked S3 where `upload()` is effectively free.

### 6. Point read latency at scale (100 000 keys pre-loaded, 500 trials)

Cold condition: no local files; each `get()` issues an S3 range request.
Warm condition: `download()` called once before trials; reads served from local disk.
All numbers are from real S3 (ap-east-1).

| Metric | Cold (S3 range) | Warm (local disk) | Speedup  |
| ------ | --------------- | ----------------- | -------- |
| mean   | 49.739 ms       | 0.009 ms          | ~5407×   |
| median | 48.641 ms       | 0.009 ms          | ~5573×   |
| p95    | 56.514 ms       | 0.010 ms          | ~5580×   |
| p99    | 112.912 ms      | 0.018 ms          | ~6411×   |
| min    | 37.858 ms       | 0.008 ms          | ~4825×   |
| max    | 136.738 ms      | 0.121 ms          | ~1132×   |

#### Why cold reads are ~50 ms regardless of dataset size

Each cold `get()` issues an HTTP range request to S3, specifying the exact byte
range for that entry. S3 seeks to that offset server-side and streams back only
those bytes - it does not scan the whole file. The dominant cost is therefore
the network round-trip to S3 (~50 ms from ap-east-1), not the size of the data
file. The 100 000-key cold numbers are nearly identical to the 1 000-key cold
numbers in benchmark 2: scaling the dataset by 100× has no effect on read
latency because the range request size does not change.

#### Why warm reads are ~0.009 ms regardless of dataset size

After `download()`, all data files are present locally. `get()` looks up the
exact file number and byte offset in the in-memory index and issues a single
`pread()` to that position - no scanning, no searching, O(1) regardless of
how many keys are in the database. The ~0.009 ms warm latency is
indistinguishable from the 1 000-key case in benchmark 2 for the same reason:
the seek is to a known offset, so dataset size does not matter.

#### Why the p99 spike reaches ~113 ms

The cold p99 reflects S3 tail latency - occasional GETs that hit a cold shard
or internal housekeeping on the S3 side. This is consistent with the p99 spikes
observed in benchmarks 2 and 4 and is a known property of cloud object stores.
The warm p99 stays at 0.018 ms because disk seeks are deterministic and
unaffected by S3 internals.

#### The practical takeaway

Calling `download()` once at startup amortizes the full S3 transfer cost across
all subsequent reads. After that, every `get()` runs at local-disk speed - a
~5400× improvement at this scale.

### 7. Mixed read/write, realistic Lambda pattern (100000 key space)

80% reads, 19% writes, 10000 operations total. `upload()` called once at the end
to simulate end-of-invocation flush. All numbers from real S3 (ap-east-1).
Reads are served from local disk; writes go through the full `put()` path which
flushes the index after each call.

| Metric           | Value             |
| ---------------- | ----------------- |
| Total ops        | 10,000 (8,000 reads + 2,000 writes) |
| Elapsed          | 102,273 ms        |
| Throughput       | ~98 ops/sec       |
| S3 API calls     | `upload_file` x 1 + `put_object` x 1 |

Per-operation latency breakdown:

| Metric | Read latency | Write latency |
| ------ | ------------ | ------------- |
| mean   | 0.025 ms     | 48.631 ms     |
| median | 0.012 ms     | 27.151 ms     |
| p95    | 0.080 ms     | 225.625 ms    |
| p99    | 0.096 ms     | 239.459 ms    |

#### Why throughput is only ~98 ops/sec despite 80% fast reads

The total elapsed time is dominated almost entirely by write latency. With 2,000
writes at a mean of ~48.6 ms each, the write path alone accounts for roughly
97,000 ms of the 102,000 ms total. The 8,000 reads contribute only ~200 ms
(8,000 × 0.025 ms). In a mixed workload the slow operation sets the pace:
10,000 ops / 102 s = ~98 ops/sec, almost exactly what the write cost predicts.

#### Why each write costs ~27-48 ms when no S3 call is made

Every `put()` serializes the entire in-memory index to disk before returning
(`_write_entries` → `flush()` → `_save_index()`). At 100,000 entries the index
file is large, and that full serialization runs on every single `put()` call -
2,000 times in this benchmark. There is no S3 round-trip per write (the two S3
calls happen only at the final `upload()`), so the cost is pure local disk I/O
repeated 2,000 times.

#### Why write latency is skewed: median 27 ms but mean 48 ms and p99 239 ms

The distribution has a long right tail. Most index flushes complete in ~27 ms,
but roughly 1 in 20 spikes to 225+ ms. The spikes occur when the OS decides to
flush its dirty-page cache mid-operation: the index file is written frequently
enough that the kernel's write-back buffers fill up and a synchronous flush is
forced, stalling the `put()` call until the underlying storage catches up. The
mean is dragged above the median by these infrequent but expensive stalls.

#### The practical Lambda takeaway

In a Lambda invocation pattern, the correct approach is to batch all writes into
a single `put(batch)` + `upload()` at the end rather than calling `put()` once
per key. One batch call flushes the index once; 2,000 individual calls flush it
2,000 times. Read latency (0.025 ms mean) is fast because local files are
present from a prior `download()`. A cold invocation with no local files would
pay ~50 ms per read via S3 range requests, as shown in benchmark 6.
