from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .db import S4DB

from ._format import HEADER_SIZE, pack_file_header, stream_file_entries, unpack_entry_at
from ._index import Index, make_file_id, split_file_id
from ._naming import data_filename, parse_data_filename
from .errors import CompactionError, ConflictError, UnsyncedWritesError

_INDEX_FILENAME = "index.idx"


def compact(db: "S4DB") -> None:
    """Rewrites all data files to remove stale and deleted entries.

    Preconditions, verified before anything is touched: every local write is
    synced (compaction of unsynced state would strand the rebase journal), and
    every file the index references is present locally with at least the byte
    span the index expects -- an incomplete mirror raises CompactionError
    instead of silently publishing a smaller database.

    Live entries -- those whose (file, offset) still matches the index -- are
    CRC-verified and copied into new files in this writer's namespace. The
    publish order makes the index PUT the commit point for compaction too:
    new files are uploaded first, then the index is committed (compare-and-
    swapped when fenced), and only after the commit succeeds are the old
    objects deleted from S3 and disk. A crash at any point before the commit
    leaves garbage for gc(), never data loss; a crash after it leaves
    undeleted old objects, also gc-able. A lost compare-and-swap raises
    ConflictError and leaves the database exactly as it was (the uploaded
    replacement files become garbage for gc()).
    """
    if db._has_unsynced_writes():
        raise UnsyncedWritesError("upload() before compacting")

    local_dir = db._get_local_dir()

    # --- verify the mirror is complete for everything the index references
    span_needed: dict[str, int] = {}  # filename -> max byte end referenced
    for entry in db._index.entries.values():
        filename = data_filename(entry.file_id)
        end = entry.offset + entry.length
        span_needed[filename] = max(span_needed.get(filename, 0), end)
    for filename, end in span_needed.items():
        path = os.path.join(local_dir, filename)
        if not os.path.exists(path):
            raise CompactionError(f"index references {filename}, which is not in local_dir")
        if os.path.getsize(path) < end:
            raise CompactionError(
                f"local {filename} is {os.path.getsize(path)} bytes; index references up to byte {end}"
            )

    old_filenames = sorted(span_needed)
    # Every other local data file is dead weight (tombstone-only or fully
    # superseded); it is removed along with the rewritten ones.
    all_local = sorted(
        name for name in os.listdir(local_dir) if parse_data_filename(name) is not None
    )

    # --- rewrite live entries into new files in this writer's namespace
    new_paths: list[str] = []
    out_fh = None
    cur_file_id = 0

    def open_new_file() -> None:
        """Closes the current output file (if any) and opens the next one with a fresh header."""
        nonlocal out_fh, cur_file_id
        if out_fh is not None:
            out_fh.close()
        db._seq += 1
        cur_file_id = make_file_id(db._writer_id, db._seq)
        path = os.path.join(local_dir, data_filename(cur_file_id))
        new_paths.append(path)
        out_fh = open(path, "wb")
        out_fh.write(pack_file_header(split_file_id(cur_file_id)[1]))

    new_index = Index()
    new_index.next_file_num = db._index.next_file_num

    try:
        for filename in old_filenames:
            file_id = parse_data_filename(filename)
            with open(os.path.join(local_dir, filename), "rb") as in_fh:
                for entry_offset, raw, key, flags in stream_file_entries(in_fh):
                    idx_entry = db._index.get(key)
                    if idx_entry is None or idx_entry.file_id != file_id or idx_entry.offset != entry_offset:
                        continue  # tombstone, superseded, or unreferenced
                    unpack_entry_at(raw, 0)  # verify CRC before propagating bytes
                    if out_fh is None:
                        open_new_file()
                    if out_fh.tell() > HEADER_SIZE and out_fh.tell() + len(raw) > db.max_file_size:
                        open_new_file()
                    out_offset = out_fh.tell()
                    out_fh.write(raw)
                    new_index.put(key, cur_file_id, out_offset, len(raw))
    finally:
        if out_fh is not None:
            out_fh.close()

    # --- publish: new files first, then the index commit, then delete old
    uploaded: list[str] = []
    try:
        for path in new_paths:
            filename = os.path.basename(path)
            if db.fenced:
                etag = db.storage.upload(path, filename, if_none_match=True)
            else:
                etag = db.storage.upload(path, filename)
            db._files[filename] = {"etag": etag, "dirty": False, "unconditional": False}
            uploaded.append(filename)

        if not db.fenced:
            new_etag = db.storage.upload_bytes(new_index.to_bytes(), _INDEX_FILENAME)
        elif db._index_etag is None:
            new_etag = db.storage.upload_bytes(new_index.to_bytes(), _INDEX_FILENAME, if_none_match=True)
        else:
            new_etag = db.storage.upload_bytes(new_index.to_bytes(), _INDEX_FILENAME, if_match=db._index_etag)
    except ConflictError:
        # Another writer committed underneath us: our rewrite is stale. The
        # database is untouched; drop the replacement files (best effort --
        # anything left becomes gc() garbage) and report.
        for filename in uploaded:
            db.storage.delete(filename)
            db._files.pop(filename, None)
        for path in new_paths:
            os.remove(path)
        raise

    # Commit succeeded: swap in the new index, then clean up the old objects.
    db._index = new_index
    db._index_etag = new_etag
    db._active = None

    for filename in all_local:
        if os.path.basename(filename) in {os.path.basename(p) for p in new_paths}:
            continue
        db.storage.delete(filename)
        db._files.pop(filename, None)
        os.remove(os.path.join(local_dir, filename))
