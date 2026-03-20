import glob as _glob
import io
import os
import tempfile

import boto3
import pytest
from moto import mock_aws

from s4db import S4DB
from s4db._format import (
    pack_file_header,
    unpack_file_header,
    pack_entry,
    unpack_entry_at,
    stream_file_entries,
    HEADER_SIZE,
    FLAG_NORMAL,
    FLAG_TOMBSTONE,
)
from s4db._index import Index, IndexEntry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BUCKET = "test-bucket"
PREFIX = "mydb/"


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3(aws_env):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def db(s3, tmp_path):
    return S4DB(
        local_dir=str(tmp_path),
        bucket=BUCKET,
        prefix=PREFIX,
        region_name="us-east-1",
    )


# ---------------------------------------------------------------------------
# _format tests
# ---------------------------------------------------------------------------


class TestFileHeader:
    def test_roundtrip(self):
        data = pack_file_header(42)
        assert len(data) == HEADER_SIZE
        version, file_num = unpack_file_header(data)
        assert version == 0x01
        assert file_num == 42

    def test_bad_magic(self):
        bad = b"XXXX" + b"\x01" + b"\x00\x00\x00\x01"
        with pytest.raises(ValueError, match="Invalid magic"):
            unpack_file_header(bad)


class TestEntry:
    def test_normal_entry_roundtrip(self):
        packed = pack_entry("hello", "world")
        key, value, flags, length = unpack_entry_at(packed)
        assert key == "hello"
        assert value == "world"
        assert flags == FLAG_NORMAL
        assert length == len(packed)

    def test_tombstone_entry_roundtrip(self):
        packed = pack_entry("gone", None, deleted=True)
        key, value, flags, length = unpack_entry_at(packed)
        assert key == "gone"
        assert value is None
        assert flags == FLAG_TOMBSTONE

    def test_unicode_key_value(self):
        packed = pack_entry("cléf", "valeur")
        key, value, flags, _ = unpack_entry_at(packed)
        assert key == "cléf"
        assert value == "valeur"

    def test_crc_mismatch_raises(self):
        packed = bytearray(pack_entry("k", "v"))
        packed[-1] ^= 0xFF  # corrupt CRC
        with pytest.raises(ValueError, match="CRC mismatch"):
            unpack_entry_at(bytes(packed))

    def test_entry_overhead(self):
        # value_len=0 tombstone: flags(1)+key_len(4)+value_len(4)+key_bytes+crc(4)
        packed = pack_entry("x", None, deleted=True)
        expected = 1 + 4 + 4 + 1 + 4  # overhead + 1-byte key
        assert len(packed) == expected


class TestStreamEntries:
    def test_iter_multiple(self):
        buf = bytearray(pack_file_header(1))
        pairs = [("a", "alpha"), ("b", "beta"), ("c", "gamma")]
        for k, v in pairs:
            buf += pack_entry(k, v)
        fh = io.BytesIO(bytes(buf))
        results = list(stream_file_entries(fh))
        assert len(results) == 3
        for i, (offset, raw, key, flags) in enumerate(results):
            assert key == pairs[i][0]
            assert flags == FLAG_NORMAL


# ---------------------------------------------------------------------------
# _index tests
# ---------------------------------------------------------------------------


class TestIndex:
    def test_put_get(self):
        idx = Index()
        idx.put("k1", 1, 9, 50)
        e = idx.get("k1")
        assert e.file_num == 1
        assert e.offset == 9
        assert e.length == 50

    def test_delete(self):
        idx = Index()
        idx.put("k1", 1, 9, 50)
        idx.delete("k1")
        assert idx.get("k1") is None

    def test_bytes_roundtrip(self):
        idx = Index()
        idx.next_file_num = 5
        idx.put("a", 2, 9, 30)
        idx.put("b", 3, 40, 25)
        raw = idx.to_bytes()
        idx2 = Index.from_bytes(raw)
        assert idx2.next_file_num == 5
        assert idx2.get("a") == IndexEntry(file_num=2, offset=9, length=30)
        assert idx2.get("b") == IndexEntry(file_num=3, offset=40, length=25)

    def test_missing_key_returns_none(self):
        idx = Index()
        assert idx.get("missing") is None


# ---------------------------------------------------------------------------
# S4DB integration tests
# ---------------------------------------------------------------------------


class TestS4DBInit:
    def test_init_no_index(self, s3, tmp_path):
        db = S4DB(local_dir=str(tmp_path), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        assert db._index.next_file_num == 1
        assert db._index.entries == {}

    def test_init_loads_existing_index(self, db):
        db.put({"x": "y"})
        # Re-open using same local_dir (index loaded from local file)
        db2 = S4DB(local_dir=db.local_dir, bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        assert db2.get("x") == "y"


class TestPutGet:
    def test_basic_put_get(self, db):
        db.put({"key1": "value1"})
        assert db.get("key1") == "value1"

    def test_multiple_keys(self, db):
        db.put({"a": "1", "b": "2", "c": "3"})
        assert db.get("a") == "1"
        assert db.get("b") == "2"
        assert db.get("c") == "3"

    def test_missing_key_returns_none(self, db):
        assert db.get("nonexistent") is None

    def test_overwrite_key(self, db):
        db.put({"key": "old"})
        db.put({"key": "new"})
        assert db.get("key") == "new"

    def test_empty_value(self, db):
        db.put({"k": ""})
        assert db.get("k") == ""

    def test_large_value(self, db):
        big = "x" * 100_000
        db.put({"big": big})
        assert db.get("big") == big


class TestDelete:
    def test_delete_existing_key(self, db):
        db.put({"k": "v"})
        db.delete(["k"])
        assert db.get("k") is None

    def test_delete_nonexistent_key_is_noop(self, db):
        # Should not raise, should not write tombstone
        db.delete(["ghost"])
        files = _glob.glob(os.path.join(db.local_dir, "data_*.s4db"))
        assert files == []

    def test_delete_multiple(self, db):
        db.put({"a": "1", "b": "2", "c": "3"})
        db.delete(["a", "c"])
        assert db.get("a") is None
        assert db.get("b") == "2"
        assert db.get("c") is None


class TestFileRolling:
    def test_file_rolls_when_max_size_exceeded(self, s3, tmp_path):
        # Use a tiny max_file_size to force rolling.
        # pack_entry("kN", "v"*50) = 21 bytes; file header = 9 bytes.
        # With max_file_size=29: after writing entry1 buf=30 bytes > 29,
        # so entry2 triggers a roll, producing multiple files.
        db = S4DB(
            local_dir=str(tmp_path),
            bucket=BUCKET,
            prefix=PREFIX,
            max_file_size=29,
            region_name="us-east-1",
        )
        db.put({"k1": "v" * 50, "k2": "v" * 50, "k3": "v" * 50})
        files = sorted(_glob.glob(os.path.join(db.local_dir, "data_*.s4db")))
        assert len(files) > 1
        # All keys still readable
        assert db.get("k1") == "v" * 50
        assert db.get("k2") == "v" * 50
        assert db.get("k3") == "v" * 50

    def test_no_empty_files(self, s3, tmp_path):
        db = S4DB(
            local_dir=str(tmp_path),
            bucket=BUCKET,
            prefix=PREFIX,
            max_file_size=50,
            region_name="us-east-1",
        )
        db.put({"k": "v"})
        files = sorted(_glob.glob(os.path.join(db.local_dir, "data_*.s4db")))
        for f in files:
            assert os.path.getsize(f) > HEADER_SIZE


class TestCompaction:
    def test_compact_removes_old_files(self, db):
        db.put({"a": "1", "b": "2"})
        db.put({"a": "updated"})
        old_files = sorted(_glob.glob(os.path.join(db.local_dir, "data_*.s4db")))
        assert len(old_files) >= 1
        db.compact()
        new_files = sorted(_glob.glob(os.path.join(db.local_dir, "data_*.s4db")))
        for f in old_files:
            assert f not in new_files

    def test_compact_keeps_latest_values(self, db):
        db.put({"a": "old_a", "b": "old_b"})
        db.put({"a": "new_a"})
        db.delete(["b"])
        db.compact()
        assert db.get("a") == "new_a"
        assert db.get("b") is None

    def test_compact_skips_tombstones(self, db):
        db.put({"x": "val"})
        db.delete(["x"])
        db.compact()
        assert db.get("x") is None
        # After compaction there should be no data files (all tombstoned)
        files = _glob.glob(os.path.join(db.local_dir, "data_*.s4db"))
        assert files == []

    def test_compact_preserves_all_live_keys(self, db):
        data = {f"key{i}": f"value{i}" for i in range(20)}
        db.put(data)
        db.compact()
        for k, v in data.items():
            assert db.get(k) == v


class TestRebuildIndex:
    def test_rebuild_index_matches_original(self, db):
        db.put({"a": "1", "b": "2"})
        db.delete(["a"])
        original_entries = dict(db._index.entries)
        db.rebuild_index()
        assert db._index.entries == original_entries

    def test_rebuild_index_updates_next_file_num(self, db):
        db.put({"k": "v"})
        db.rebuild_index()
        assert db._index.next_file_num > 1

    def test_rebuild_empty_db(self, db):
        db.rebuild_index()
        assert db._index.entries == {}
        assert db._index.next_file_num == 1


class TestContextManager:
    def test_context_manager(self, s3, tmp_path):
        with S4DB(local_dir=str(tmp_path), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1") as db:
            db.put({"ctx": "works"})
            assert db.get("ctx") == "works"

    def test_context_manager_returns_self(self, s3, tmp_path):
        db = S4DB(local_dir=str(tmp_path), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        with db as db2:
            assert db2 is db


class TestIndexPersistence:
    def test_index_persisted_after_put(self, db):
        db.put({"persist": "me"})
        local_index = os.path.join(db.local_dir, "index.idx")
        with open(local_index, "rb") as fh:
            idx = Index.from_bytes(fh.read())
        assert idx.get("persist") is not None

    def test_next_file_num_increments(self, db):
        assert db._index.next_file_num == 1
        db.put({"k": "v"})
        assert db._index.next_file_num == 2
        # Second put appends to the existing file (still has room), so next_file_num stays 2
        db.put({"k2": "v2"})
        assert db._index.next_file_num == 2
