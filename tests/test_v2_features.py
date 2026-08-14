"""Tests for the v2 behaviors: fencing, dirty tracking, torn-write recovery,
download guards, gc, safe compaction, and API additions."""

import glob as _glob
import io
import os

import boto3
import pytest
from moto import mock_aws

from s4db import (
    S4DB,
    CompactionError,
    ConflictError,
    CorruptEntryError,
    CorruptIndexError,
    UnsyncedWritesError,
)
from s4db._format import pack_entry, pack_file_header, unpack_entry_at, stream_file_entries
from s4db._index import Index
from s4db._naming import data_filename, parse_data_filename
from s4db._index import make_file_id

BUCKET = "test-bucket"
PREFIX = "mydb/"


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _open(tmp_path, name="w", **kwargs):
    d = tmp_path / name
    d.mkdir(exist_ok=True)
    return S4DB(local_dir=str(d), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1", **kwargs)


# ---------------------------------------------------------------------------
# Format: CRC ordering, value types, conditional compression
# ---------------------------------------------------------------------------


class TestFormatV2:
    def test_corrupt_value_raises_crc_not_decompress_error(self):
        # Corrupt a byte inside the VALUE, not the CRC trailer: the CRC check
        # must fire before any decompression touches the corrupted bytes.
        packed = bytearray(pack_entry("key", "x" * 1000))  # compressible -> stored compressed
        packed[20] ^= 0xFF
        with pytest.raises(CorruptEntryError, match="CRC mismatch"):
            unpack_entry_at(bytes(packed))

    def test_truncated_entry_raises_corrupt(self):
        packed = pack_entry("key", "value")
        with pytest.raises(CorruptEntryError, match="Truncated"):
            unpack_entry_at(packed[: len(packed) - 3])

    def test_bytes_value_roundtrip(self):
        blob = bytes(range(256))
        packed = pack_entry("bin", blob)
        _, value, _, _ = unpack_entry_at(packed)
        assert value == blob
        assert isinstance(value, bytes)

    def test_compressible_value_stored_smaller(self):
        raw = "a" * 10_000
        packed = pack_entry("k", raw)
        assert len(packed) < len(raw)
        _, value, _, _ = unpack_entry_at(packed)
        assert value == raw

    def test_incompressible_value_pays_no_framing(self):
        # 32 random-ish bytes do not compress; entry = overhead + key + raw value
        import secrets

        blob = secrets.token_bytes(32)
        packed = pack_entry("k", blob)
        assert len(packed) == 13 + 1 + 32

    def test_stream_stops_at_garbage_lengths(self):
        buf = bytearray(pack_file_header(1))
        buf += pack_entry("good", "entry")
        buf += b"\x00\xff\xff\xff\xff\xff\xff\xff\xff"  # absurd lengths -> torn
        results = list(stream_file_entries(io.BytesIO(bytes(buf))))
        assert [key for _, _, key, _ in results] == ["good"]


class TestIndexV2:
    def test_corrupt_index_crc_detected(self):
        idx = Index()
        idx.put("k", make_file_id(7, 1), 9, 20)
        raw = bytearray(idx.to_bytes())
        raw[12] ^= 0xFF
        with pytest.raises(CorruptIndexError):
            Index.from_bytes(bytes(raw))

    def test_v1_index_still_loads(self):
        # Hand-build a version-1 blob: header + one entry with a 4-byte file num
        import struct

        body = struct.pack("!BII", 1, 4, 1) + struct.pack("!H", 1) + b"k" + struct.pack("!IQI", 3, 9, 30)
        idx = Index.from_bytes(body)
        assert idx.next_file_num == 4
        e = idx.get("k")
        assert (e.file_id, e.offset, e.length) == (3, 9, 30)

    def test_filename_roundtrip_both_namespaces(self):
        legacy = make_file_id(0, 12)
        spaced = make_file_id(0xDEADBEEF, 3)
        assert data_filename(legacy) == "data_000012.s4db"
        assert data_filename(spaced) == "data_deadbeef_000003.s4db"
        assert parse_data_filename(data_filename(legacy)) == legacy
        assert parse_data_filename(data_filename(spaced)) == spaced
        assert parse_data_filename("index.idx") is None


# ---------------------------------------------------------------------------
# Validation and API additions
# ---------------------------------------------------------------------------


class TestValidationAndAPI:
    def test_oversized_key_rejected_before_write(self, s3, tmp_path):
        db = _open(tmp_path)
        with pytest.raises(ValueError, match="exceeds"):
            db.put({"k" * 70_000: "v"})
        assert _glob.glob(os.path.join(db.local_dir, "data_*.s4db")) == []

    def test_non_str_key_rejected(self, s3, tmp_path):
        db = _open(tmp_path)
        with pytest.raises(TypeError):
            db.put({42: "v"})

    def test_non_str_bytes_value_rejected(self, s3, tmp_path):
        db = _open(tmp_path)
        with pytest.raises(TypeError):
            db.put({"k": 3.14})

    def test_empty_put_is_noop(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({})
        assert _glob.glob(os.path.join(db.local_dir, "data_*.s4db")) == []

    def test_bytes_value_through_db(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"bin": b"\x00\x01\x02"})
        assert db.get("bin") == b"\x00\x01\x02"

    def test_len_contains_iter(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"a": "1", "b": "2"})
        assert len(db) == 2
        assert "a" in db
        assert "zz" not in db
        assert sorted(db) == ["a", "b"]

    def test_get_many(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"a": "1", "b": "2"})
        db.upload()
        reader = _open(tmp_path, "r")
        result = reader.get_many(["a", "b", "missing"])
        assert result == {"a": "1", "b": "2", "missing": None}

    def test_close_removes_owned_tempdir(self, s3):
        db = S4DB(bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        db.put({"k": "v"})
        tempdir = db.local_dir
        assert tempdir and os.path.isdir(tempdir)
        db.close()
        assert not os.path.isdir(tempdir)

    def test_close_keeps_user_dir(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"k": "v"})
        db.close()
        assert os.path.isdir(db.local_dir)


# ---------------------------------------------------------------------------
# Dirty tracking and upload
# ---------------------------------------------------------------------------


class TestDirtyTracking:
    def test_upload_skips_clean_files(self, s3, tmp_path):
        db = _open(tmp_path, max_file_size=64)
        db.put({"k1": "v" * 60})  # fills file 1
        db.put({"k2": "v" * 60})  # rolls to file 2
        db.upload()
        # Appending to file 2 must not re-upload file 1
        etags_before = {name: f["etag"] for name, f in db._files.items()}
        db.put({"k3": "x"})
        dirty = [name for name, f in db._files.items() if f["dirty"]]
        assert len(dirty) == 1
        db.upload()
        unchanged = [n for n in etags_before if n not in dirty]
        for name in unchanged:
            assert db._files[name]["etag"] == etags_before[name]

    def test_grown_file_reupload_keeps_locations_valid(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"a": "1"})
        db.upload()
        reader = _open(tmp_path, "r")
        db.put({"b": "2"})  # appends to the same file
        db.upload()
        # Reader's old index still resolves against the extended object
        assert reader.get("a") == "1"


# ---------------------------------------------------------------------------
# Download guard and stale mirrors
# ---------------------------------------------------------------------------


class TestDownloadGuard:
    def test_download_refuses_to_discard_unsynced_writes(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"seed": "x"})
        db.upload()
        db.put({"unsynced": "y"})
        with pytest.raises(UnsyncedWritesError):
            db.download()

    def test_download_force_discards(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"seed": "x"})
        db.upload()
        db.put({"unsynced": "y"})
        db.download(force=True)
        assert db.get("seed") == "x"
        assert db.get("unsynced") is None
        # Writes still work after a forced reset
        db.put({"after": "z"})
        assert db.get("after") == "z"

    def test_stale_local_mirror_falls_back_to_s3(self, s3, tmp_path):
        writer = _open(tmp_path, "w")
        writer.put({"a": "1"})
        writer.upload()
        reader = _open(tmp_path, "r")
        reader.download()
        writer.put({"b": "2"})  # extends the file
        writer.upload()
        # Fresh index + old (shorter) local file: read must fall back to S3
        reader2 = _open(tmp_path, "r")
        assert reader2.get("b") == "2"


# ---------------------------------------------------------------------------
# Fencing: conflicts, rebase, last-writer-wins
# ---------------------------------------------------------------------------


class TestFencing:
    def test_concurrent_writers_merge_disjoint_keys(self, s3, tmp_path):
        a = _open(tmp_path, "a")
        b = _open(tmp_path, "b")
        a.put({"from_a": "1"})
        b.put({"from_b": "2"})
        a.upload()
        b.upload()  # loses the CAS, rebases, retries
        fresh = _open(tmp_path, "r")
        assert fresh.get("from_a") == "1"
        assert fresh.get("from_b") == "2"

    def test_same_key_last_committer_wins(self, s3, tmp_path):
        a = _open(tmp_path, "a")
        b = _open(tmp_path, "b")
        a.put({"k": "from_a"})
        b.put({"k": "from_b"})
        a.upload()
        b.upload()
        fresh = _open(tmp_path, "r")
        assert fresh.get("k") == "from_b"

    def test_rebase_false_raises_conflict(self, s3, tmp_path):
        a = _open(tmp_path, "a")
        b = _open(tmp_path, "b")
        a.put({"x": "1"})
        b.put({"y": "2"})
        a.upload()
        with pytest.raises(ConflictError):
            b.upload(rebase=False)
        # The failed committer's writes are not lost: rebase later succeeds
        b.upload()
        fresh = _open(tmp_path, "r")
        assert fresh.get("y") == "2"

    def test_writers_never_share_data_files(self, s3, tmp_path):
        a = _open(tmp_path, "a")
        b = _open(tmp_path, "b")
        a.put({"x": "1"})
        b.put({"y": "2"})
        a.upload()
        b.upload()
        names = [obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)["Contents"]]
        data = [n for n in names if "data_" in n]
        assert len(data) == 2
        assert len(set(data)) == 2

    def test_delete_survives_rebase(self, s3, tmp_path):
        a = _open(tmp_path, "a")
        a.put({"k": "v", "other": "o"})
        a.upload()
        b = _open(tmp_path, "b")
        b.delete(["k"])
        a.put({"unrelated": "u"})
        a.upload()
        b.upload()  # rebases; the tombstone must survive the merge
        fresh = _open(tmp_path, "r")
        assert fresh.get("k") is None
        assert fresh.get("unrelated") == "u"

    def test_unfenced_mode_still_works(self, s3, tmp_path):
        db = _open(tmp_path, fenced=False)
        db.put({"k": "v"})
        db.upload()
        fresh = _open(tmp_path, "r", fenced=False)
        assert fresh.get("k") == "v"


# ---------------------------------------------------------------------------
# Torn writes and recovery
# ---------------------------------------------------------------------------


class TestTornWriteRecovery:
    def test_rebuild_stops_at_torn_tail(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"a": "1", "b": "2"})
        # Tear the tail: chop the last 3 bytes of the data file
        [path] = _glob.glob(os.path.join(db.local_dir, "data_*.s4db"))
        size = os.path.getsize(path)
        with open(path, "rb+") as fh:
            fh.truncate(size - 3)
        db.rebuild_index()
        assert db.get("a") == "1"
        assert db.get("b") is None  # torn entry not indexed

    def test_rebuild_stops_at_corrupt_entry_with_warning(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"a": "1", "b": "2", "c": "3"})
        [path] = _glob.glob(os.path.join(db.local_dir, "data_*.s4db"))
        # Corrupt the second entry's value byte: lengths stay sane, CRC fails
        entry_len = len(pack_entry("a", "1"))
        with open(path, "rb+") as fh:
            fh.seek(9 + entry_len + 10)
            byte = fh.read(1)
            fh.seek(-1, 1)
            fh.write(bytes([byte[0] ^ 0xFF]))
        with pytest.warns(UserWarning, match="torn or corrupt"):
            db.rebuild_index()
        assert db.get("a") == "1"

    def test_crash_before_sync_orphans_recovered(self, s3, tmp_path):
        # Process 1 syncs, writes more, then "crashes" (state discarded)
        p1 = _open(tmp_path, "shared")
        p1.put({"committed": "yes"})
        p1.upload()
        p1.put({"orphan": "bytes"})  # never uploaded

        # Process 2 restarts on the same local_dir: orphans invisible until rebuild
        p2 = _open(tmp_path, "shared")
        assert p2.get("orphan") is None
        p2.rebuild_index()
        assert p2.get("orphan") == "bytes"
        p2.upload()  # publishes replayed files (unconditionally) plus the index
        fresh = _open(tmp_path, "r")
        assert fresh.get("orphan") == "bytes"
        assert fresh.get("committed") == "yes"

    def test_lost_index_recovered_from_s3(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"k1": "v1", "k2": "v2"})
        db.upload()
        s3.delete_object(Bucket=BUCKET, Key=PREFIX + "index.idx")

        # A fresh process finds no index, but can rebuild from listed data files
        fresh = _open(tmp_path, "r")
        assert len(fresh) == 0
        fresh.rebuild_index(from_s3=True)
        assert fresh.get("k1") == "v1"
        assert fresh.get("k2") == "v2"
        fresh.upload()
        again = _open(tmp_path, "r2")
        assert again.get("k2") == "v2"


# ---------------------------------------------------------------------------
# Garbage collection
# ---------------------------------------------------------------------------


class TestGC:
    def test_gc_removes_unreferenced_files(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"k": "v"})
        db.upload()
        # Simulate a crashed writer's orphan: a data file no index references
        s3.put_object(Bucket=BUCKET, Key=PREFIX + "data_0000dead_000001.s4db", Body=b"S4DB\x02garbage")
        deleted = db.gc(grace_seconds=0)
        assert deleted == ["data_0000dead_000001.s4db"]
        assert db.get("k") == "v"

    def test_gc_respects_grace_period(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"k": "v"})
        db.upload()
        s3.put_object(Bucket=BUCKET, Key=PREFIX + "data_0000dead_000001.s4db", Body=b"S4DB\x02garbage")
        assert db.gc(grace_seconds=3600) == []  # too young to collect

    def test_gc_refuses_with_unsynced_writes(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"k": "v"})
        with pytest.raises(UnsyncedWritesError):
            db.gc()


# ---------------------------------------------------------------------------
# Compaction safety
# ---------------------------------------------------------------------------


class TestCompactionSafety:
    def test_compact_requires_sync(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"k": "v"})
        with pytest.raises(UnsyncedWritesError):
            db.compact()

    def test_compact_incomplete_mirror_raises(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"k": "v"})
        db.upload()
        reader = _open(tmp_path, "r")  # index loaded, data files not local
        with pytest.raises(CompactionError, match="not in local_dir"):
            reader.compact()

    def test_compact_truncated_local_file_raises(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"a": "1", "b": "2"})
        db.upload()
        [path] = _glob.glob(os.path.join(db.local_dir, "data_*.s4db"))
        with open(path, "rb+") as fh:
            fh.truncate(os.path.getsize(path) - 3)
        with pytest.raises(CompactionError, match="references up to"):
            db.compact()

    def test_compact_uploads_before_deleting(self, s3, tmp_path):
        """At every moment during compaction, a fresh reader can read every key."""
        db = _open(tmp_path)
        db.put({"a": "1", "b": "2"})
        db.put({"a": "updated"})
        db.upload()

        calls = []
        original_upload = db.storage.upload
        original_upload_bytes = db.storage.upload_bytes
        original_delete = db.storage.delete
        db.storage.upload = lambda *a, **k: (calls.append(("put", a[1])), original_upload(*a, **k))[1]
        db.storage.upload_bytes = lambda *a, **k: (calls.append(("put", a[1])), original_upload_bytes(*a, **k))[1]
        db.storage.delete = lambda *a, **k: (calls.append(("delete", a[0])), original_delete(*a, **k))[1]

        db.compact()
        first_delete = next(i for i, (op, _) in enumerate(calls) if op == "delete")
        index_put = next(i for i, (op, name) in enumerate(calls) if op == "put" and name == "index.idx")
        puts_before = [i for i, (op, name) in enumerate(calls) if op == "put" and name != "index.idx"]
        assert all(i < index_put for i in puts_before), "data files must upload before the index commit"
        assert index_put < first_delete, "the index commit must precede any delete"

        fresh = _open(tmp_path, "r")
        assert fresh.get("a") == "updated"
        assert fresh.get("b") == "2"

    def test_compact_conflict_leaves_database_intact(self, s3, tmp_path):
        a = _open(tmp_path, "a")
        a.put({"k": "v", "stale": "old"})
        a.put({"stale": "new"})
        a.upload()

        # Another writer commits between a's load and a's compact
        b = _open(tmp_path, "b")
        b.put({"from_b": "2"})
        b.upload()

        with pytest.raises(ConflictError):
            a.compact()
        fresh = _open(tmp_path, "r")
        assert fresh.get("k") == "v"
        assert fresh.get("stale") == "new"
        assert fresh.get("from_b") == "2"

    def test_compact_after_reload_succeeds(self, s3, tmp_path):
        db = _open(tmp_path)
        db.put({"a": "old"})
        db.put({"a": "new"})
        db.upload()
        db.compact()
        fresh = _open(tmp_path, "r")
        assert fresh.get("a") == "new"
