import json
from dataclasses import dataclass


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

    def to_json(self) -> bytes:
        payload = {
            "version": 1,
            "next_file_num": self.next_file_num,
            "entries": {
                k: [e.file_num, e.offset, e.length]
                for k, e in self.entries.items()
            },
        }
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> "Index":
        payload = json.loads(data.decode("utf-8"))
        idx = cls()
        idx.next_file_num = payload["next_file_num"]
        idx.entries = {
            k: IndexEntry(file_num=v[0], offset=v[1], length=v[2])
            for k, v in payload["entries"].items()
        }
        return idx
