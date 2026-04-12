"""
Benchmarks for s4db.

Measures four things:
  1. Write throughput  -- s4db batched put() + upload() vs s4db unbatched
  2. Read latency      -- S3 range request vs local disk, with a hot/cold key
                         distribution (Zipf-like: ~20% of keys receive ~80% of reads)
  3. s4db vs Naive S3  -- head-to-head: throughput, S3 API request count, and
                         estimated cost against the straw-man of one
                         put_object / get_object per key
  4. S3 tax            -- local Bitcask vs s4db (S3 range) vs s4db (local disk),
                         to quantify the latency overhead introduced by S3

Requires real AWS credentials and an S3 bucket (set BUCKET / PREFIX / REGION below).
Run with:
    python benchmarks/bench.py
"""

import io
import os
import random
import statistics
import string
import struct
import tempfile
import time
from collections import defaultdict

import boto3

from s4db import S4DB

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BUCKET = "s4db-bench"
PREFIX = "bench/"
REGION = "ap-south-1"

# Write benchmark
WRITE_TOTAL_KEYS = 2_000       # total key/value pairs written per scenario
BATCH_SIZE = 100               # keys per put() call for the batched scenario

# Read benchmark
READ_SETUP_KEYS = 1_000        # keys pre-loaded before read trials
READ_TRIALS = 500              # number of get() calls measured
VALUE_SIZE = 256               # bytes per value (random ASCII)
KEY_SIZE = 16                  # bytes per key

# Hot/cold split: top HOT_FRACTION of keys receive HOT_SHARE of reads
HOT_FRACTION = 0.2             # 20 % of keys
HOT_SHARE = 0.8                # receive 80 % of reads

# AWS standard S3 pricing (ap-south-1, 2026)
S3_PUT_COST_PER_1K = 0.005    # $ per 1,000 PUT/COPY/POST requests
S3_GET_COST_PER_1K = 0.0004   # $ per 1,000 GET requests


def _rand_str(n: int) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def _make_kv(n: int) -> dict[str, str]:
    return {_rand_str(KEY_SIZE): _rand_str(VALUE_SIZE) for _ in range(n)}


def _fmt(label: str, value: float, unit: str, width: int = 38) -> str:
    return f"  {label:<{width}} {value:>10.2f}  {unit}"


def _separator(char: str = "-", width: int = 60) -> str:
    return char * width


def _zipf_indices(total_keys: int, n_trials: int) -> list[int]:
    """
    Return n_trials key indices sampled from a hot/cold distribution.

    The top HOT_FRACTION keys are the "hot" set; they receive HOT_SHARE of
    all reads. The remaining keys share the rest uniformly.
    """
    n_hot = max(1, int(total_keys * HOT_FRACTION))
    hot_indices = list(range(n_hot))
    cold_indices = list(range(n_hot, total_keys))
    n_hot_reads = int(n_trials * HOT_SHARE)
    n_cold_reads = n_trials - n_hot_reads
    selected = (
        random.choices(hot_indices, k=n_hot_reads)
        + random.choices(cold_indices, k=n_cold_reads)
    )
    random.shuffle(selected)
    return selected


def _stats(lats: list[float]) -> dict:
    s = sorted(lats)
    return {
        "mean":   statistics.mean(lats),
        "median": statistics.median(lats),
        "p95":    s[int(0.95 * len(s))],
        "p99":    s[int(0.99 * len(s))],
        "min":    s[0],
        "max":    s[-1],
    }


class _CountingProxy:
    """Wraps a boto3 S3 client and counts every API call by method name."""

    def __init__(self, client):
        self._wrapped = client
        self.calls: dict[str, int] = defaultdict(int)

    def reset(self):
        self.calls.clear()

    @property
    def total_puts(self) -> int:
        return self.calls["put_object"] + self.calls["upload_file"]

    @property
    def total_gets(self) -> int:
        return self.calls["get_object"] + self.calls["download_file"]

    def __getattr__(self, name: str):
        attr = getattr(self._wrapped, name)
        if callable(attr):
            def _counted(*args, **kwargs):
                self.calls[name] += 1
                return attr(*args, **kwargs)
            return _counted
        return attr


def _inject_counter(db: S4DB) -> _CountingProxy:
    """Replace the storage client inside an S4DB instance with a counting proxy."""
    proxy = _CountingProxy(db.storage._client)
    db.storage._client = proxy
    return proxy


def _s3_delete_prefix(client, bucket: str, prefix: str) -> None:
    """Delete all objects under the given prefix (handles pagination)."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})


# ---------------------------------------------------------------------------
# Naive S3 helper  (straw man: one object per key)
# ---------------------------------------------------------------------------

class NaiveS3:
    """
    Bare-bones key/value store that maps each key to one S3 object.
    This is the approach s4db is designed to beat.
    """

    def __init__(self, client, bucket: str, prefix: str):
        self._client = client
        self._bucket = bucket
        self._prefix = prefix

    def put(self, items: dict[str, str]) -> None:
        for key, value in items.items():
            self._client.put_object(
                Bucket=self._bucket,
                Key=self._prefix + key,
                Body=value.encode(),
            )

    def get(self, key: str) -> str | None:
        try:
            resp = self._client.get_object(
                Bucket=self._bucket,
                Key=self._prefix + key,
            )
            return resp["Body"].read().decode()
        except self._client.exceptions.NoSuchKey:
            return None


# ---------------------------------------------------------------------------
# Local Bitcask  (pure-disk reference: append-only log + in-memory index)
# ---------------------------------------------------------------------------
# Wire format per entry: [key_len:2][val_len:4][key bytes][val bytes]
# The in-memory index maps key -> (offset, total_entry_len) in the log file.

_HDR = struct.Struct(">HI")   # 2-byte key len + 4-byte val len


class LocalBitcask:
    """
    Minimal Bitcask-style store: append-only flat file on local disk with an
    in-memory hash index.  No S3 involvement whatsoever - used as a baseline
    to quantify the latency s4db adds by going through S3.
    """

    def __init__(self, path: str):
        self._path = path
        self._index: dict[str, tuple[int, int]] = {}  # key -> (offset, entry_len)
        self._fh = open(path, "a+b")

    def close(self):
        self._fh.close()

    def put(self, items: dict[str, str]) -> None:
        for key, value in items.items():
            kb = key.encode()
            vb = value.encode()
            header = _HDR.pack(len(kb), len(vb))
            entry = header + kb + vb
            self._fh.seek(0, 2)   # seek to end
            offset = self._fh.tell()
            self._fh.write(entry)
            self._index[key] = (offset, len(entry))
        self._fh.flush()

    def get(self, key: str) -> str | None:
        loc = self._index.get(key)
        if loc is None:
            return None
        offset, entry_len = loc
        self._fh.seek(offset)
        raw = self._fh.read(entry_len)
        kl, vl = _HDR.unpack(raw[:_HDR.size])
        return raw[_HDR.size + kl: _HDR.size + kl + vl].decode()


# ---------------------------------------------------------------------------
# Benchmark 1 – Write throughput (s4db batched vs unbatched)
# ---------------------------------------------------------------------------

def bench_write_throughput():
    print(_separator("="))
    print("BENCHMARK 1 - Write Throughput  (s4db batched vs unbatched)")
    print(f"  {WRITE_TOTAL_KEYS} key/value pairs, value size ~{VALUE_SIZE} B")
    print(f"  batched: {WRITE_TOTAL_KEYS // BATCH_SIZE} calls of put({BATCH_SIZE} keys)  |  "
          f"unbatched: {WRITE_TOTAL_KEYS} calls of put(1 key)")
    print(_separator())

    data_all = _make_kv(WRITE_TOTAL_KEYS)
    keys_list = list(data_all.keys())

    # --- batched ---
    with tempfile.TemporaryDirectory() as tmpdir:
        db = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        batches = [
            {k: data_all[k] for k in keys_list[i: i + BATCH_SIZE]}
            for i in range(0, WRITE_TOTAL_KEYS, BATCH_SIZE)
        ]
        t0 = time.perf_counter()
        for batch in batches:
            db.put(batch)
        db.upload()
        batched_elapsed = time.perf_counter() - t0

    batched_tput = WRITE_TOTAL_KEYS / batched_elapsed

    # --- unbatched ---
    with tempfile.TemporaryDirectory() as tmpdir:
        db = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        t0 = time.perf_counter()
        for k in keys_list:
            db.put({k: data_all[k]})
        db.upload()
        unbatched_elapsed = time.perf_counter() - t0

    unbatched_tput = WRITE_TOTAL_KEYS / unbatched_elapsed
    speedup = unbatched_elapsed / batched_elapsed

    print(_fmt("Batched   put(100) + upload()",  batched_tput,   "keys/sec"))
    print(_fmt("Unbatched put(1)   + upload()",  unbatched_tput, "keys/sec"))
    print(_fmt("Batched elapsed",                batched_elapsed  * 1000, "ms total"))
    print(_fmt("Unbatched elapsed",              unbatched_elapsed * 1000, "ms total"))
    print(_fmt("Batched speedup",                speedup, "x faster"))
    print()

    return {
        "batched_keys_per_sec":   batched_tput,
        "unbatched_keys_per_sec": unbatched_tput,
        "batched_elapsed_ms":     batched_elapsed * 1000,
        "unbatched_elapsed_ms":   unbatched_elapsed * 1000,
        "speedup_x":              speedup,
    }


# ---------------------------------------------------------------------------
# Benchmark 2 – Read latency (S3 range vs local disk, hot/cold distribution)
# ---------------------------------------------------------------------------

def bench_read_latency():
    print(_separator("="))
    print("BENCHMARK 2 - Read Latency  (hot/cold distribution)")
    print(f"  {READ_SETUP_KEYS} keys pre-loaded, {READ_TRIALS} get() trials")
    print(f"  hot set: top {int(HOT_FRACTION*100)}% of keys → "
          f"{int(HOT_SHARE*100)}% of reads  (Zipf-like)")
    print(_separator())

    data = _make_kv(READ_SETUP_KEYS)
    keys_list = list(data.keys())
    key_rank = {k: i for i, k in enumerate(keys_list)}
    trial_indices = _zipf_indices(READ_SETUP_KEYS, READ_TRIALS)
    trial_keys = [keys_list[i] for i in trial_indices]
    n_hot = int(READ_SETUP_KEYS * HOT_FRACTION)

    s3_latencies: list[float] = []
    disk_latencies: list[float] = []

    # --- S3 range requests ---
    with tempfile.TemporaryDirectory() as tmpdir:
        db_write = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        db_write.put(data)
        db_write.upload()
        db_s3 = S4DB(bucket=BUCKET, prefix=PREFIX, region_name=REGION)
        for key in trial_keys:
            t0 = time.perf_counter()
            db_s3.get(key)
            s3_latencies.append((time.perf_counter() - t0) * 1000)

    # --- local disk reads ---
    with tempfile.TemporaryDirectory() as tmpdir:
        db_write = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        db_write.put(data)
        db_write.upload()
        db_disk = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        for key in trial_keys:
            t0 = time.perf_counter()
            db_disk.get(key)
            disk_latencies.append((time.perf_counter() - t0) * 1000)

    s3_stats   = _stats(s3_latencies)
    disk_stats = _stats(disk_latencies)

    header = f"  {'Metric':<12}  {'S3 range req':>14}  {'Local disk':>14}  {'Speedup':>10}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for metric in ("mean", "median", "p95", "p99", "min", "max"):
        s3v   = s3_stats[metric]
        diskv = disk_stats[metric]
        speedup = s3v / diskv if diskv > 0 else float("inf")
        print(f"  {metric:<12}  {s3v:>13.3f}ms  {diskv:>13.3f}ms  {speedup:>9.1f}x")

    # Hot-vs-cold breakdown (disk)
    hot_lats  = [disk_latencies[i] for i, k in enumerate(trial_keys) if key_rank[k] < n_hot]
    cold_lats = [disk_latencies[i] for i, k in enumerate(trial_keys) if key_rank[k] >= n_hot]
    print()
    print("  Local disk – hot vs cold key breakdown:")
    if hot_lats:
        print(f"    Hot  keys (top {int(HOT_FRACTION*100)}%)     "
              f"mean: {statistics.mean(hot_lats):.3f} ms  "
              f"p95: {sorted(hot_lats)[int(0.95*len(hot_lats))]:.3f} ms  "
              f"n={len(hot_lats)}")
    if cold_lats:
        print(f"    Cold keys (bottom {int((1-HOT_FRACTION)*100)}%)  "
              f"mean: {statistics.mean(cold_lats):.3f} ms  "
              f"p95: {sorted(cold_lats)[int(0.95*len(cold_lats))]:.3f} ms  "
              f"n={len(cold_lats)}")
    print()

    return {"s3": s3_stats, "disk": disk_stats}


# ---------------------------------------------------------------------------
# Benchmark 3 – s4db vs Naive S3  (head-to-head)
# ---------------------------------------------------------------------------

def bench_vs_naive():
    print(_separator("="))
    print("BENCHMARK 3 - s4db vs Naive S3  (head-to-head)")
    print(f"  Naive S3: one put_object / get_object per key")
    print(f"  s4db:     batched put({BATCH_SIZE}) + upload()  /  range-request get()")
    print(f"  {WRITE_TOTAL_KEYS} writes, {READ_TRIALS} reads ({READ_SETUP_KEYS} key corpus)")
    print(_separator())

    write_data = _make_kv(WRITE_TOTAL_KEYS)
    write_keys = list(write_data.keys())

    read_data = _make_kv(READ_SETUP_KEYS)
    read_keys = list(read_data.keys())
    trial_keys = [read_keys[i] for i in _zipf_indices(READ_SETUP_KEYS, READ_TRIALS)]

    # ---- Naive S3 write ----
    client = boto3.client("s3", region_name=REGION)
    proxy = _CountingProxy(client)
    naive = NaiveS3(proxy, BUCKET, PREFIX + "naive/")

    t0 = time.perf_counter()
    naive.put(write_data)
    naive_write_elapsed = time.perf_counter() - t0
    naive_write_calls = dict(proxy.calls)
    naive_write_puts = proxy.total_puts

    naive_write_tput = WRITE_TOTAL_KEYS / naive_write_elapsed
    _s3_delete_prefix(client, BUCKET, PREFIX + "naive/")

    # ---- s4db write (batched) ----
    with tempfile.TemporaryDirectory() as tmpdir:
        db = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        proxy = _inject_counter(db)
        batches = [
            {k: write_data[k] for k in write_keys[i: i + BATCH_SIZE]}
            for i in range(0, WRITE_TOTAL_KEYS, BATCH_SIZE)
        ]
        t0 = time.perf_counter()
        for batch in batches:
            db.put(batch)
        db.upload()
        s4db_write_elapsed = time.perf_counter() - t0
        s4db_write_calls = dict(proxy.calls)
        s4db_write_puts = proxy.total_puts

    s4db_write_tput = WRITE_TOTAL_KEYS / s4db_write_elapsed

    # ---- Naive S3 read ----
    client = boto3.client("s3", region_name=REGION)
    setup_proxy = _CountingProxy(client)
    naive_setup = NaiveS3(setup_proxy, BUCKET, PREFIX + "naive/")
    naive_setup.put(read_data)

    read_proxy = _CountingProxy(client)
    naive_read = NaiveS3(read_proxy, BUCKET, PREFIX + "naive/")
    naive_read_lats: list[float] = []
    for key in trial_keys:
        t0 = time.perf_counter()
        naive_read.get(key)
        naive_read_lats.append((time.perf_counter() - t0) * 1000)
    naive_read_gets = read_proxy.total_gets
    _s3_delete_prefix(client, BUCKET, PREFIX + "naive/")

    # ---- s4db read (S3 range) ----
    with tempfile.TemporaryDirectory() as tmpdir:
        db_write = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        db_write.put(read_data)
        db_write.upload()

        db_s3 = S4DB(bucket=BUCKET, prefix=PREFIX, region_name=REGION)
        proxy = _inject_counter(db_s3)
        s4db_read_lats: list[float] = []
        for key in trial_keys:
            t0 = time.perf_counter()
            db_s3.get(key)
            s4db_read_lats.append((time.perf_counter() - t0) * 1000)
        s4db_read_gets = proxy.total_gets

    # ---- Cost estimates ----
    def _cost(puts: int, gets: int) -> float:
        return (puts / 1000) * S3_PUT_COST_PER_1K + (gets / 1000) * S3_GET_COST_PER_1K

    naive_cost  = _cost(naive_write_puts, naive_read_gets)
    s4db_cost   = _cost(s4db_write_puts,  s4db_read_gets)
    cost_ratio  = naive_cost / s4db_cost if s4db_cost > 0 else float("inf")

    write_speedup = naive_write_elapsed / s4db_write_elapsed
    put_reduction = naive_write_puts / s4db_write_puts if s4db_write_puts else float("inf")

    naive_read_stats = _stats(naive_read_lats)
    s4db_read_stats  = _stats(s4db_read_lats)
    read_speedup     = naive_read_stats["mean"] / s4db_read_stats["mean"]

    # --- print results ---
    print(f"  {'':30}  {'Naive S3':>14}  {'s4db':>14}  {'s4db wins':>12}")
    print("  " + "-" * 74)

    print(f"  {'Write throughput':<30}  {naive_write_tput:>13,.0f}  {s4db_write_tput:>13,.0f}  "
          f"  {write_speedup:>8.1f}x")
    print(f"  {'S3 PUT requests (writes)':<30}  {naive_write_puts:>14,}  {s4db_write_puts:>14,}  "
          f"  {put_reduction:>7.0f}x fewer")
    get_reduction = naive_read_gets / s4db_read_gets if s4db_read_gets else float("inf")
    print(f"  {'S3 GET requests (reads)':<30}  {naive_read_gets:>14,}  {s4db_read_gets:>14,}  "
          f"  {get_reduction:>7.0f}x fewer")
    print(f"  {'Read mean latency':<30}  {naive_read_stats['mean']:>13.3f}ms"
          f"  {s4db_read_stats['mean']:>13.3f}ms  {read_speedup:>9.1f}x")
    print(f"  {'Read p99 latency':<30}  {naive_read_stats['p99']:>13.3f}ms"
          f"  {s4db_read_stats['p99']:>13.3f}ms")

    print()
    print(f"  Estimated S3 API cost  ({WRITE_TOTAL_KEYS} writes + {READ_TRIALS} reads)")
    print(f"    Naive S3  : {naive_write_puts:>6} PUTs + {naive_read_gets:>4} GETs"
          f"  ≈ ${naive_cost:.6f}")
    print(f"    s4db      : {s4db_write_puts:>6} PUTs + {s4db_read_gets:>4} GETs"
          f"  ≈ ${s4db_cost:.6f}")
    print(f"    s4db is ~{cost_ratio:.0f}x cheaper in S3 request fees")
    print()
    print(f"  S3 API call breakdown (writes):")
    print(f"    Naive : {naive_write_calls}")
    print(f"    s4db  : {s4db_write_calls}")
    print()

    return {
        "naive_write_tput":   naive_write_tput,
        "s4db_write_tput":    s4db_write_tput,
        "write_speedup_x":    write_speedup,
        "naive_write_puts":   naive_write_puts,
        "s4db_write_puts":    s4db_write_puts,
        "put_reduction_x":    put_reduction,
        "naive_read_gets":    naive_read_gets,
        "s4db_read_gets":     s4db_read_gets,
        "naive_read_stats":   naive_read_stats,
        "s4db_read_stats":    s4db_read_stats,
        "naive_cost_usd":     naive_cost,
        "s4db_cost_usd":      s4db_cost,
        "cost_ratio_x":       cost_ratio,
    }


# ---------------------------------------------------------------------------
# Benchmark 4 – S3 tax: local Bitcask vs s4db (S3 range) vs s4db (local disk)
# ---------------------------------------------------------------------------

def bench_s3_tax():
    print(_separator("="))
    print("BENCHMARK 4 - S3 Tax  (local Bitcask vs s4db)")
    print("  Shows the latency overhead that S3 adds on top of the same")
    print("  append-only-log + in-memory-index design running purely on disk.")
    print(f"  {READ_SETUP_KEYS} keys pre-loaded, {READ_TRIALS} get() trials")
    print(f"  hot set: top {int(HOT_FRACTION*100)}% of keys → "
          f"{int(HOT_SHARE*100)}% of reads  (Zipf-like)")
    print(_separator())

    data = _make_kv(READ_SETUP_KEYS)
    keys_list = list(data.keys())
    trial_indices = _zipf_indices(READ_SETUP_KEYS, READ_TRIALS)
    trial_keys = [keys_list[i] for i in trial_indices]

    bitcask_lats:   list[float] = []
    s4db_s3_lats:   list[float] = []
    s4db_disk_lats: list[float] = []

    # ---- Local Bitcask ----
    with tempfile.TemporaryDirectory() as tmpdir:
        bc = LocalBitcask(os.path.join(tmpdir, "bitcask.log"))
        bc.put(data)
        for key in trial_keys:
            t0 = time.perf_counter()
            bc.get(key)
            bitcask_lats.append((time.perf_counter() - t0) * 1000)
        bc.close()

    # ---- s4db – S3 range requests ----
    with tempfile.TemporaryDirectory() as tmpdir:
        db_write = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        db_write.put(data)
        db_write.upload()
        db_s3 = S4DB(bucket=BUCKET, prefix=PREFIX, region_name=REGION)
        for key in trial_keys:
            t0 = time.perf_counter()
            db_s3.get(key)
            s4db_s3_lats.append((time.perf_counter() - t0) * 1000)

    # ---- s4db – local disk (files already present) ----
    with tempfile.TemporaryDirectory() as tmpdir:
        db_write = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        db_write.put(data)
        db_write.upload()
        db_disk = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        for key in trial_keys:
            t0 = time.perf_counter()
            db_disk.get(key)
            s4db_disk_lats.append((time.perf_counter() - t0) * 1000)

    bc_stats   = _stats(bitcask_lats)
    s3_stats   = _stats(s4db_s3_lats)
    disk_stats = _stats(s4db_disk_lats)

    s3_tax_mean  = s3_stats["mean"]   / bc_stats["mean"]
    disk_tax_mean = disk_stats["mean"] / bc_stats["mean"]

    header = (f"  {'Metric':<12}  {'Local Bitcask':>15}  "
              f"{'s4db S3 range':>15}  {'s4db local disk':>16}")
    print(header)
    print("  " + "-" * (len(header) - 2))
    for metric in ("mean", "median", "p95", "p99", "min", "max"):
        bcv   = bc_stats[metric]
        s3v   = s3_stats[metric]
        diskv = disk_stats[metric]
        print(f"  {metric:<12}  {bcv:>14.3f}ms  {s3v:>14.3f}ms  {diskv:>15.3f}ms")

    print()
    print(f"  S3 tax (mean latency overhead vs local Bitcask):")
    print(f"    s4db S3 range  : {s3_tax_mean:.1f}x slower  "
          f"(+{s3_stats['mean'] - bc_stats['mean']:.3f} ms per read)")
    print(f"    s4db local disk: {disk_tax_mean:.1f}x slower  "
          f"(+{disk_stats['mean'] - bc_stats['mean']:.3f} ms per read)")
    print()
    print("  Interpretation:")
    print(f"    The local disk path of s4db adds ~{disk_tax_mean:.1f}x overhead vs a pure-local")
    print(f"    Bitcask (Snappy decompression + index lookup on top of a raw seek).")
    print(f"    S3 range requests add a further ~{s3_tax_mean / disk_tax_mean:.1f}x on top of that -")
    print(f"    that is the S3 network/protocol tax for serverless durability.")
    print()

    return {
        "bitcask":    bc_stats,
        "s4db_s3":    s3_stats,
        "s4db_disk":  disk_stats,
        "s3_tax_x":   s3_tax_mean,
        "disk_tax_x": disk_tax_mean,
    }


# ---------------------------------------------------------------------------
# Workload A – Bulk write throughput as a function of batch size
# ---------------------------------------------------------------------------

WA_TOTAL_KEYS  = 100_000
WA_VALUE_SIZE  = 256
# batch_size=1 is excluded: each put(1) flushes the whole index to disk,
# making it O(n²) at 100k scale (minutes vs seconds for larger batches).
WA_BATCH_SIZES = [100, 1_000, 10_000]


def bench_workload_a():
    print(_separator("="))
    print("WORKLOAD A - Bulk Write Throughput  (batch size sweep)")
    print(f"  {WA_TOTAL_KEYS:,} key/value pairs, value size {WA_VALUE_SIZE} B")
    print(f"  Batch sizes tested: {WA_BATCH_SIZES}")
    print(f"  Pattern: put(batch) × (total/batch) then one upload()")
    print(_separator())

    data_all = {_rand_str(KEY_SIZE): _rand_str(WA_VALUE_SIZE) for _ in range(WA_TOTAL_KEYS)}
    keys_list = list(data_all.keys())

    results = {}
    for batch_size in WA_BATCH_SIZES:
        with tempfile.TemporaryDirectory() as tmpdir:
            db = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
            proxy = _inject_counter(db)
            batches = [
                {k: data_all[k] for k in keys_list[i: i + batch_size]}
                for i in range(0, WA_TOTAL_KEYS, batch_size)
            ]
            t0 = time.perf_counter()
            for batch in batches:
                db.put(batch)
            db.upload()
            elapsed = time.perf_counter() - t0
            s3_puts = proxy.total_puts

        tput = WA_TOTAL_KEYS / elapsed
        results[batch_size] = {"elapsed_ms": elapsed * 1000, "keys_per_sec": tput, "s3_puts": s3_puts}

    header = f"  {'Batch size':>12}  {'keys/sec':>12}  {'elapsed (ms)':>14}  {'S3 PUTs':>10}  {'Speedup':>9}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    baseline = results[WA_BATCH_SIZES[0]]["elapsed_ms"]
    for bs in WA_BATCH_SIZES:
        r = results[bs]
        speedup = baseline / r["elapsed_ms"]
        print(f"  {bs:>12,}  {r['keys_per_sec']:>12,.0f}  {r['elapsed_ms']:>13,.1f}ms"
              f"  {r['s3_puts']:>10,}  {speedup:>8.1f}x")
    print()
    return results


# ---------------------------------------------------------------------------
# Workload B – Point read latency: cold (S3 range) vs warm (local after download)
# ---------------------------------------------------------------------------

WB_SETUP_KEYS = 100_000
WB_VALUE_SIZE = 256
WB_TRIALS     = 500


def bench_workload_b():
    print(_separator("="))
    print("WORKLOAD B - Point Read Latency  (cold S3 range vs warm local disk)")
    print(f"  {WB_SETUP_KEYS:,} keys pre-loaded, {WB_TRIALS} get() trials")
    print(f"  Cold : no local files - each get() issues an S3 range request")
    print(f"  Warm : download() called first - reads served from local disk")
    print(_separator())

    data = {_rand_str(KEY_SIZE): _rand_str(WB_VALUE_SIZE) for _ in range(WB_SETUP_KEYS)}
    keys_list = list(data.keys())
    trial_keys = random.choices(keys_list, k=WB_TRIALS)

    cold_lats: list[float] = []
    warm_lats: list[float] = []

    # ---- Cold: S3 range requests (no local dir) ----
    with tempfile.TemporaryDirectory() as tmpdir:
        db_w = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        # Write in large batches to keep setup fast
        for i in range(0, WB_SETUP_KEYS, 10_000):
            db_w.put({k: data[k] for k in keys_list[i: i + 10_000]})
        db_w.upload()

        db_cold = S4DB(bucket=BUCKET, prefix=PREFIX, region_name=REGION)
        for key in trial_keys:
            t0 = time.perf_counter()
            db_cold.get(key)
            cold_lats.append((time.perf_counter() - t0) * 1_000)

    # ---- Warm: download() then local disk reads ----
    with tempfile.TemporaryDirectory() as write_dir:
        db_w = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=write_dir, region_name=REGION)
        for i in range(0, WB_SETUP_KEYS, 10_000):
            db_w.put({k: data[k] for k in keys_list[i: i + 10_000]})
        db_w.upload()

        with tempfile.TemporaryDirectory() as read_dir:
            db_warm = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=read_dir, region_name=REGION)
            db_warm.download()   # pull all data files locally
            for key in trial_keys:
                t0 = time.perf_counter()
                db_warm.get(key)
                warm_lats.append((time.perf_counter() - t0) * 1_000)

    cold_stats = _stats(cold_lats)
    warm_stats = _stats(warm_lats)

    header = f"  {'Metric':<10}  {'Cold (S3 range)':>16}  {'Warm (local disk)':>18}  {'Speedup':>10}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for metric in ("mean", "median", "p95", "p99", "min", "max"):
        cv = cold_stats[metric]
        wv = warm_stats[metric]
        speedup = cv / wv if wv > 0 else float("inf")
        print(f"  {metric:<10}  {cv:>15.3f}ms  {wv:>17.3f}ms  {speedup:>9.1f}x")
    print()
    return {"cold": cold_stats, "warm": warm_stats}


# ---------------------------------------------------------------------------
# Workload C – Mixed read/write (80/20 split, 100k key space)
# ---------------------------------------------------------------------------

WC_KEY_SPACE     = 100_000
WC_VALUE_SIZE    = 256
WC_OPERATIONS    = 10_000
WC_READ_FRACTION = 0.80


def bench_workload_c():
    print(_separator("="))
    print("WORKLOAD C - Mixed Read/Write  (realistic Lambda invocation pattern)")
    print(f"  {WC_KEY_SPACE:,} key space, {WC_OPERATIONS:,} operations")
    print(f"  Mix: {int(WC_READ_FRACTION*100)}% reads / {int((1-WC_READ_FRACTION)*100)}% writes")
    print(f"  Upload() called once at the end (end-of-invocation flush pattern)")
    print(_separator())

    # Pre-populate the key space
    all_keys = [_rand_str(KEY_SIZE) for _ in range(WC_KEY_SPACE)]
    initial_data = {k: _rand_str(WC_VALUE_SIZE) for k in all_keys}

    # Build the operation sequence: 80% reads, 20% writes
    n_reads  = int(WC_OPERATIONS * WC_READ_FRACTION)
    n_writes = WC_OPERATIONS - n_reads
    read_ops  = [("get",  random.choice(all_keys)) for _ in range(n_reads)]
    write_ops = [("put",  random.choice(all_keys)) for _ in range(n_writes)]
    ops = read_ops + write_ops
    random.shuffle(ops)

    read_lats:  list[float] = []
    write_lats: list[float] = []
    total_start = 0.0
    total_end   = 0.0

    with tempfile.TemporaryDirectory() as tmpdir:
        # Seed the database
        db = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        for i in range(0, WC_KEY_SPACE, 10_000):
            db.put({k: initial_data[k] for k in all_keys[i: i + 10_000]})
        db.upload()

        # Re-open (simulates a fresh Lambda invocation loading from S3)
        db2 = S4DB(bucket=BUCKET, prefix=PREFIX, local_dir=tmpdir, region_name=REGION)
        proxy = _inject_counter(db2)

        total_start = time.perf_counter()
        for op, key in ops:
            t0 = time.perf_counter()
            if op == "get":
                db2.get(key)
                read_lats.append((time.perf_counter() - t0) * 1_000)
            else:
                db2.put({key: _rand_str(WC_VALUE_SIZE)})
                write_lats.append((time.perf_counter() - t0) * 1_000)
        db2.upload()
        total_end = time.perf_counter()
        s3_calls = dict(proxy.calls)

    total_elapsed = total_end - total_start
    total_ops     = len(ops)
    ops_per_sec   = total_ops / total_elapsed

    read_st  = _stats(read_lats)
    write_st = _stats(write_lats)

    print(f"  Total ops      : {total_ops:,}  ({n_reads:,} reads + {n_writes:,} writes)")
    print(f"  Elapsed        : {total_elapsed*1000:,.1f} ms")
    print(f"  Throughput     : {ops_per_sec:,.0f} ops/sec")
    print(f"  S3 API calls   : {s3_calls}")
    print()

    header = f"  {'Metric':<10}  {'Read latency':>14}  {'Write latency':>14}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for metric in ("mean", "median", "p95", "p99"):
        print(f"  {metric:<10}  {read_st[metric]:>13.3f}ms  {write_st[metric]:>13.3f}ms")
    print()
    return {"reads": read_st, "writes": write_st, "ops_per_sec": ops_per_sec}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    random.seed(42)
    print()
    print("s4db benchmark")
    print(_separator("="))
    print(f"  write total keys : {WRITE_TOTAL_KEYS}")
    print(f"  batch size       : {BATCH_SIZE}")
    print(f"  read setup keys  : {READ_SETUP_KEYS}")
    print(f"  read trials      : {READ_TRIALS}")
    print(f"  value size       : {VALUE_SIZE} B")
    print(f"  hot/cold split   : top {int(HOT_FRACTION*100)}% keys → "
          f"{int(HOT_SHARE*100)}% reads")
    print()

    bench_write_throughput()
    bench_read_latency()
    bench_vs_naive()
    bench_s3_tax()

    print()
    print("=" * 60)
    print("EXTENDED WORKLOADS  (100k key space)")
    print("=" * 60)
    bench_workload_a()
    bench_workload_b()
    bench_workload_c()

    print(_separator("="))

    print("Cleaning up S3 ...")
    client = boto3.client("s3", region_name=REGION)
    _s3_delete_prefix(client, BUCKET, PREFIX)
    print(f"  Deleted all objects under s3://{BUCKET}/{PREFIX}")

    print("Done.")
    print()
