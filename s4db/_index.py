import struct
from dataclasses import dataclass

# Binary format:
#   Header:    [1B version][4B next_file_num][4B num_entries]
#   Per entry: [2B key_len][key bytes][4B file_num][8B offset][4B length]
_HEADER = struct.Struct("!BII")
_ENTRY_FIXED = struct.Struct("!IQI")  # file_num, offset, length


@dataclass
class IndexEntry:
    file_num: int
    offset: int
    length: int


class Index:
    def __init__(self):
        self.entries: dict[str, IndexEntry] = {}
        self.next_file_num: int = 1

    def get(self, key: str) -> IndexEntry | None:
        return self.entries.get(key)

    def put(self, key: str, file_num: int, offset: int, length: int) -> None:
        self.entries[key] = IndexEntry(file_num=file_num, offset=offset, length=length)

    def delete(self, key: str) -> None:
        self.entries.pop(key, None)

    def to_bytes(self) -> bytes:
        parts = [_HEADER.pack(1, self.next_file_num, len(self.entries))]
        for key, e in self.entries.items():
            key_bytes = key.encode("utf-8")
            parts.append(struct.pack("!H", len(key_bytes)))
            parts.append(key_bytes)
            parts.append(_ENTRY_FIXED.pack(e.file_num, e.offset, e.length))
        return b"".join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "Index":
        version, next_file_num, num_entries = _HEADER.unpack_from(data, 0)
        if version != 1:
            raise ValueError(f"unsupported index version: {version}")
        idx = cls()
        idx.next_file_num = next_file_num
        pos = _HEADER.size
        for _ in range(num_entries):
            (key_len,) = struct.unpack_from("!H", data, pos)
            pos += 2
            key = data[pos : pos + key_len].decode("utf-8")
            pos += key_len
            file_num, offset, length = _ENTRY_FIXED.unpack_from(data, pos)
            pos += _ENTRY_FIXED.size
            idx.entries[key] = IndexEntry(file_num=file_num, offset=offset, length=length)
        return idx
