import struct
import zlib
from dataclasses import dataclass

from .errors import CorruptIndexError

# Binary format, version 2:
#   Header:    [1B version=2][4B next_file_num][4B num_entries]
#   Per entry: [2B key_len][key bytes][8B file_id][8B offset][4B length]
#   Trailer:   [4B crc32 of header + entries]
#
# file_id packs a 32-bit writer id in the high half and a 32-bit per-writer
# sequence number in the low half. Writer id 0 is the legacy (version 1)
# namespace, whose files are named data_NNNNNN.s4db.
#
# Version 1 (readable, no longer written):
#   Header:    [1B version=1][4B next_file_num][4B num_entries]
#   Per entry: [2B key_len][key bytes][4B file_num][8B offset][4B length]
_HEADER = struct.Struct("!BII")
_ENTRY_FIXED_V2 = struct.Struct("!QQI")  # file_id, offset, length
_ENTRY_FIXED_V1 = struct.Struct("!IQI")  # file_num, offset, length


def make_file_id(writer_id: int, seq: int) -> int:
    """Packs a 32-bit writer id and a 32-bit sequence number into a 64-bit file id."""
    return (writer_id << 32) | seq


def split_file_id(file_id: int) -> tuple[int, int]:
    """Unpacks a 64-bit file id into (writer_id, seq)."""
    return file_id >> 32, file_id & 0xFFFFFFFF


@dataclass
class IndexEntry:
    file_id: int
    offset: int
    length: int


class Index:
    def __init__(self):
        """Initializes an empty index with next_file_num starting at 1.

        next_file_num is the legacy writer-0 sequence counter, preserved for
        compatibility with version-1 databases; namespaced writers allocate
        their own sequence numbers and do not consult it.
        """
        self.entries: dict[str, IndexEntry] = {}
        self.next_file_num: int = 1

    def get(self, key: str) -> IndexEntry | None:
        """Returns the IndexEntry for key, or None if the key is not present."""
        return self.entries.get(key)

    def put(self, key: str, file_id: int, offset: int, length: int) -> None:
        """Inserts or overwrites the index entry for key with the given file location."""
        self.entries[key] = IndexEntry(file_id=file_id, offset=offset, length=length)

    def delete(self, key: str) -> None:
        """Removes key from the index. Silent no-op if the key does not exist."""
        self.entries.pop(key, None)

    def to_bytes(self) -> bytes:
        """Serializes the entire index to a compact binary blob (version 2).

        Format: fixed-size header (version, next_file_num, entry count) followed by
        each entry as [2B key_len][key bytes][8B file_id][8B offset][4B length],
        closed by a 4-byte CRC32 over everything before it. Returns the
        concatenated bytes; does not write to disk.
        """
        parts = [_HEADER.pack(2, self.next_file_num, len(self.entries))]
        for key, e in self.entries.items():
            key_bytes = key.encode("utf-8")
            parts.append(struct.pack("!H", len(key_bytes)))
            parts.append(key_bytes)
            parts.append(_ENTRY_FIXED_V2.pack(e.file_id, e.offset, e.length))
        body = b"".join(parts)
        crc = zlib.crc32(body) & 0xFFFFFFFF
        return body + struct.pack("!L", crc)

    @classmethod
    def from_bytes(cls, data: bytes) -> "Index":
        """Deserializes an Index from a binary blob produced by to_bytes().

        Reads both version 2 (current, CRC-checked) and version 1 (legacy,
        4-byte file numbers, no CRC; file numbers load as writer-0 file ids).
        Raises CorruptIndexError on an unsupported version, a CRC mismatch,
        or a truncated blob.
        """
        if len(data) < _HEADER.size:
            raise CorruptIndexError("index blob shorter than its header")
        version, next_file_num, num_entries = _HEADER.unpack_from(data, 0)
        if version == 2:
            body, trailer = data[:-4], data[-4:]
            (stored_crc,) = struct.unpack("!L", trailer)
            if zlib.crc32(body) & 0xFFFFFFFF != stored_crc:
                raise CorruptIndexError("index CRC mismatch")
            entry_fixed = _ENTRY_FIXED_V2
            end = len(body)
        elif version == 1:
            entry_fixed = _ENTRY_FIXED_V1
            end = len(data)
        else:
            raise CorruptIndexError(f"unsupported index version: {version}")

        idx = cls()
        idx.next_file_num = next_file_num
        pos = _HEADER.size
        try:
            for _ in range(num_entries):
                (key_len,) = struct.unpack_from("!H", data, pos)
                pos += 2
                key = data[pos : pos + key_len].decode("utf-8")
                pos += key_len
                file_id, offset, length = entry_fixed.unpack_from(data, pos)
                pos += entry_fixed.size
                idx.entries[key] = IndexEntry(file_id=file_id, offset=offset, length=length)
        except (struct.error, UnicodeDecodeError) as exc:
            raise CorruptIndexError(f"truncated or corrupt index entry: {exc}") from exc
        if pos != end:
            raise CorruptIndexError(f"index has {end - pos} trailing bytes after {num_entries} entries")
        return idx
