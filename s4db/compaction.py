from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .db import S4DB


def compact(db: "S4DB") -> None:
    old_filenames = db.storage.list_data_files()

    # Build latest view: key -> value or None (tombstone)
    latest: dict[str, str | None] = {}
    for filename in old_filenames:
        data = db.storage.download_bytes(filename)
        from ._format import iter_file_entries, FLAG_TOMBSTONE
        for _offset, _length, key, value, flags in iter_file_entries(data):
            if flags == FLAG_TOMBSTONE:
                latest[key] = None
            else:
                latest[key] = value

    # Keep only live entries
    live_entries = [(k, v) for k, v in latest.items() if v is not None]

    # Reset index entries (keep next_file_num as-is; _write_entries will update it)
    db._index.entries.clear()

    # Write compacted files
    if live_entries:
        db._write_entries([(k, v, False) for k, v in live_entries])

    # Delete old files from S3
    for filename in old_filenames:
        db.storage.delete(filename)
