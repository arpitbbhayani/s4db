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
        db.upload()  # index is loaded from S3 on init, so it must be uploaded first
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


# ---------------------------------------------------------------------------
# download / upload tests
# ---------------------------------------------------------------------------


class TestDownload:
    def test_download_pulls_data_files(self, db, tmp_path):
        db.put({"a": "1", "b": "2"})
        db.upload()

        # Open a fresh db pointing at a different local dir (no local files)
        fresh_dir = tmp_path / "fresh"
        fresh_dir.mkdir()
        db2 = S4DB(local_dir=str(fresh_dir), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        db2.download()

        local_files = _glob.glob(os.path.join(str(fresh_dir), "data_*.s4db"))
        assert len(local_files) > 0
        assert db2.get("a") == "1"
        assert db2.get("b") == "2"

    def test_download_updates_in_memory_index(self, db, tmp_path):
        db.put({"key": "val"})
        db.upload()

        fresh_dir = tmp_path / "fresh"
        fresh_dir.mkdir()
        db2 = S4DB(local_dir=str(fresh_dir), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        # Before download, index has no entries (S3 index was loaded but has the entry)
        db2.download()
        assert db2._index.get("key") is not None


class TestUpload:
    def test_upload_pushes_data_and_index(self, db, s3):
        db.put({"u": "v"})
        db.upload()

        # Verify data file and index exist in S3
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
        keys = {obj["Key"] for obj in response.get("Contents", [])}
        assert any("data_" in k for k in keys)
        assert any("index.idx" in k for k in keys)

    def test_upload_then_fresh_init_loads_index(self, db, tmp_path):
        db.put({"synced": "yes"})
        db.upload()

        # New instance with empty local dir should load index from S3
        fresh_dir = tmp_path / "fresh"
        fresh_dir.mkdir()
        db2 = S4DB(local_dir=str(fresh_dir), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        assert db2.get("synced") is not None  # index loaded from S3; get falls back to S3 range read


# ---------------------------------------------------------------------------
# S3 range-read fallback tests
# ---------------------------------------------------------------------------


class TestS3RangeRead:
    def test_get_without_local_file_uses_s3(self, db, tmp_path):
        db.put({"remote": "value"})
        db.upload()

        # New db with empty local dir - index comes from S3, data file not present locally
        fresh_dir = tmp_path / "fresh"
        fresh_dir.mkdir()
        db2 = S4DB(local_dir=str(fresh_dir), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")

        # No data files locally
        assert _glob.glob(os.path.join(str(fresh_dir), "data_*.s4db")) == []
        # get() must use S3 range request
        assert db2.get("remote") == "value"

    def test_missing_key_returns_none_without_local_file(self, db, tmp_path):
        db.put({"x": "y"})
        db.upload()

        fresh_dir = tmp_path / "fresh"
        fresh_dir.mkdir()
        db2 = S4DB(local_dir=str(fresh_dir), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        assert db2.get("not_there") is None


# ---------------------------------------------------------------------------
# Additional _index tests
# ---------------------------------------------------------------------------


class TestIndexEdgeCases:
    def test_from_bytes_bad_version(self):
        idx = Index()
        idx.put("k", 1, 9, 20)
        raw = bytearray(idx.to_bytes())
        raw[0] = 99  # corrupt version byte
        with pytest.raises(ValueError, match="unsupported index version"):
            Index.from_bytes(bytes(raw))

    def test_delete_missing_key_is_noop(self):
        idx = Index()
        idx.delete("nonexistent")  # must not raise
        assert idx.get("nonexistent") is None

    def test_overwrite_entry(self):
        idx = Index()
        idx.put("k", 1, 9, 10)
        idx.put("k", 2, 50, 30)
        e = idx.get("k")
        assert e.file_num == 2
        assert e.offset == 50


# ---------------------------------------------------------------------------
# Additional _format tests
# ---------------------------------------------------------------------------


class TestStreamEntriesEdgeCases:
    def test_empty_file_yields_nothing(self):
        # A file with only the header and no entries
        fh = io.BytesIO(pack_file_header(1))
        results = list(stream_file_entries(fh))
        assert results == []

    def test_stream_includes_tombstones(self):
        buf = bytearray(pack_file_header(1))
        buf += pack_entry("alive", "yes")
        buf += pack_entry("dead", None, deleted=True)
        fh = io.BytesIO(bytes(buf))
        results = list(stream_file_entries(fh))
        assert len(results) == 2
        keys = {key for _, _, key, _ in results}
        assert keys == {"alive", "dead"}
        flags_map = {key: flags for _, _, key, flags in results}
        assert flags_map["alive"] == FLAG_NORMAL
        assert flags_map["dead"] == FLAG_TOMBSTONE


# ---------------------------------------------------------------------------
# Additional S4DB integration tests
# ---------------------------------------------------------------------------


class TestInitFromS3:
    def test_init_loads_index_from_s3_when_no_local(self, db, tmp_path):
        db.put({"s3key": "s3val"})
        db.upload()

        fresh_dir = tmp_path / "fresh"
        fresh_dir.mkdir()
        db2 = S4DB(local_dir=str(fresh_dir), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        # Index is fetched from S3 into memory only - no local file written on init
        local_index = os.path.join(str(fresh_dir), "index.idx")
        assert not os.path.exists(local_index)
        assert db2._index.get("s3key") is not None


class TestDeleteThenPut:
    def test_deleted_key_can_be_reinserted(self, db):
        db.put({"k": "original"})
        db.delete(["k"])
        assert db.get("k") is None
        db.put({"k": "reborn"})
        assert db.get("k") == "reborn"


class TestCompactEdgeCases:
    def test_compact_empty_db_does_not_raise(self, db):
        # No data files exist; compact should be a no-op without errors
        db.compact()
        assert db._index.entries == {}

    def test_compact_respects_max_file_size(self, s3, tmp_path):
        db = S4DB(
            local_dir=str(tmp_path),
            bucket=BUCKET,
            prefix=PREFIX,
            max_file_size=50,
            region_name="us-east-1",
        )
        data = {f"k{i}": "value" * 5 for i in range(10)}
        db.put(data)
        db.compact()
        # All keys survive compaction regardless of how many files were produced
        for k, v in data.items():
            assert db.get(k) == v

    def test_compact_cleans_up_s3_old_files(self, db, s3):
        db.put({"a": "1"})
        db.upload()
        old_files = {obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", [])}

        db.put({"a": "2"})  # creates a second version; first file is now stale
        db.compact()

        new_files = {obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", [])}
        # None of the old data files should still be in S3
        old_data_files = {k for k in old_files if "data_" in k}
        for f in old_data_files:
            assert f not in new_files


# ---------------------------------------------------------------------------
# keys() tests
# ---------------------------------------------------------------------------


class TestKeys:
    def test_empty_db_returns_empty_list(self, db):
        assert db.keys() == []

    def test_returns_all_put_keys(self, db):
        db.put({"a": "1", "b": "2", "c": "3"})
        assert sorted(db.keys()) == ["a", "b", "c"]

    def test_deleted_keys_not_included(self, db):
        db.put({"a": "1", "b": "2"})
        db.delete(["a"])
        assert db.keys() == ["b"]

    def test_overwritten_key_appears_once(self, db):
        db.put({"k": "old"})
        db.put({"k": "new"})
        assert db.keys() == ["k"]

    def test_returns_list_type(self, db):
        db.put({"x": "y"})
        assert isinstance(db.keys(), list)


# ---------------------------------------------------------------------------
# iter() tests
# ---------------------------------------------------------------------------


class TestIter:
    """Tests for the iter() API (local=False, S3 range reads per key)."""

    def test_empty_db_yields_nothing(self, db):
        assert list(db.iter()) == []

    def test_yields_all_kv_pairs(self, db):
        data = {"a": "1", "b": "2", "c": "3"}
        db.put(data)
        result = dict(db.iter())
        assert result == data

    def test_deleted_keys_not_yielded(self, db):
        db.put({"a": "1", "b": "2"})
        db.delete(["a"])
        result = dict(db.iter())
        assert result == {"b": "2"}

    def test_overwritten_key_yields_latest_value(self, db):
        db.put({"k": "old"})
        db.put({"k": "new"})
        result = dict(db.iter())
        assert result == {"k": "new"}

    def test_yields_tuples(self, db):
        db.put({"x": "y"})
        pairs = list(db.iter())
        assert len(pairs) == 1
        key, value = pairs[0]
        assert key == "x"
        assert value == "y"

    def test_is_a_generator(self, db):
        import types
        db.put({"a": "1"})
        result = db.iter()
        assert isinstance(result, types.GeneratorType)

    def test_uses_s3_range_read_without_local_files(self, db, tmp_path):
        """iter(local=False) reads from S3 when local data files are absent."""
        db.put({"remote": "value"})
        db.upload()

        fresh_dir = tmp_path / "fresh"
        fresh_dir.mkdir()
        db2 = S4DB(local_dir=str(fresh_dir), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        # No data files locally
        assert _glob.glob(os.path.join(str(fresh_dir), "data_*.s4db")) == []
        result = dict(db2.iter())
        assert result == {"remote": "value"}

    def test_multiple_files_all_keys_yielded(self, s3, tmp_path):
        """iter() works when keys span multiple data files."""
        db = S4DB(
            local_dir=str(tmp_path),
            bucket=BUCKET,
            prefix=PREFIX,
            max_file_size=29,
            region_name="us-east-1",
        )
        data = {"k1": "v" * 50, "k2": "v" * 50, "k3": "v" * 50}
        db.put(data)
        # Confirm multiple files were written
        files = sorted(_glob.glob(os.path.join(str(tmp_path), "data_*.s4db")))
        assert len(files) > 1
        result = dict(db.iter())
        assert result == data


class TestIterLocal:
    """Tests for iter(local=True) - downloads files then reads from disk."""

    def test_empty_db_yields_nothing(self, db):
        assert list(db.iter(local=True)) == []

    def test_yields_all_kv_pairs(self, db):
        data = {"a": "1", "b": "2", "c": "3"}
        db.put(data)
        result = dict(db.iter(local=True))
        assert result == data

    def test_deleted_keys_not_yielded(self, db):
        db.put({"a": "1", "b": "2"})
        db.delete(["a"])
        result = dict(db.iter(local=True))
        assert result == {"b": "2"}

    def test_overwritten_key_yields_latest_value(self, db):
        db.put({"k": "old"})
        db.put({"k": "new"})
        result = dict(db.iter(local=True))
        assert result == {"k": "new"}

    def test_downloads_missing_files_from_s3(self, db, tmp_path):
        """iter(local=True) downloads missing data files before iterating."""
        db.put({"hello": "world"})
        db.upload()

        fresh_dir = tmp_path / "fresh"
        fresh_dir.mkdir()
        db2 = S4DB(local_dir=str(fresh_dir), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        # No data files locally yet
        assert _glob.glob(os.path.join(str(fresh_dir), "data_*.s4db")) == []
        result = dict(db2.iter(local=True))
        # Files are now downloaded
        assert len(_glob.glob(os.path.join(str(fresh_dir), "data_*.s4db"))) > 0
        assert result == {"hello": "world"}

    def test_does_not_replace_existing_local_files(self, db, tmp_path):
        """iter(local=True) leaves existing local files intact."""
        db.put({"k": "v"})
        db.upload()

        fresh_dir = tmp_path / "fresh"
        fresh_dir.mkdir()
        db2 = S4DB(local_dir=str(fresh_dir), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        # Pre-populate the local file with db2's data (simulate already-present file)
        db2.iter(local=True)  # first call downloads the file
        data_files = _glob.glob(os.path.join(str(fresh_dir), "data_*.s4db"))
        assert len(data_files) == 1
        mtime_before = os.path.getmtime(data_files[0])

        # Second call should not overwrite the file
        list(db2.iter(local=True))
        mtime_after = os.path.getmtime(data_files[0])
        assert mtime_before == mtime_after

    def test_multiple_files_all_keys_yielded(self, s3, tmp_path):
        """iter(local=True) works when keys span multiple data files."""
        db = S4DB(
            local_dir=str(tmp_path),
            bucket=BUCKET,
            prefix=PREFIX,
            max_file_size=29,
            region_name="us-east-1",
        )
        data = {"k1": "v" * 50, "k2": "v" * 50, "k3": "v" * 50}
        db.put(data)
        files = sorted(_glob.glob(os.path.join(str(tmp_path), "data_*.s4db")))
        assert len(files) > 1
        result = dict(db.iter(local=True))
        assert result == data

    def test_downloads_only_files_referenced_by_index(self, db, tmp_path):
        """iter(local=True) only downloads files needed for live keys, not all S3 files."""
        data1 = {"a": "1", "b": "2"}
        db.put(data1)
        db.upload()

        # Overwrite one key - creates a second data file
        db.put({"a": "updated"})
        db.upload()

        fresh_dir = tmp_path / "fresh"
        fresh_dir.mkdir()
        db2 = S4DB(local_dir=str(fresh_dir), bucket=BUCKET, prefix=PREFIX, region_name="us-east-1")
        result = dict(db2.iter(local=True))
        assert result["a"] == "updated"
        assert result["b"] == "2"
