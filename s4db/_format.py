import struct
import zlib
import snappy

MAGIC = b"S4DB"
VERSION = 0x01
HEADER_SIZE = 9  # 4 magic + 1 version + 4 file_num

FLAG_NORMAL = 0x00
FLAG_TOMBSTONE = 0x01

# Entry overhead: 1 (flags) + 4 (key_len) + 4 (value_len) + 4 (crc) = 13
ENTRY_OVERHEAD = 13


def pack_file_header(file_num: int) -> bytes:
    return MAGIC + struct.pack(">BL", VERSION, file_num)


def unpack_file_header(data: bytes) -> tuple[int, int]:
    if data[:4] != MAGIC:
        raise ValueError(f"Invalid magic: {data[:4]!r}")
    version, file_num = struct.unpack(">BL", data[4:9])
    return version, file_num


def pack_entry(key: str, value: str | None, deleted: bool = False) -> bytes:
    key_bytes = key.encode("utf-8")
    if deleted:
        flags = FLAG_TOMBSTONE
        value_bytes = b""
    else:
        flags = FLAG_NORMAL
        value_bytes = snappy.compress(value.encode("utf-8"))

    key_len = len(key_bytes)
    value_len = len(value_bytes)

    header = struct.pack(">BLL", flags, key_len, value_len)
    body = header + key_bytes + value_bytes
    crc = zlib.crc32(body) & 0xFFFFFFFF
    return body + struct.pack(">L", crc)


def unpack_entry_at(data: bytes, offset: int = 0) -> tuple[str, str | None, int, int]:
    flags, key_len, value_len = struct.unpack(">BLL", data[offset : offset + 9])
    pos = offset + 9
    key = data[pos : pos + key_len].decode("utf-8")
    pos += key_len
    if flags == FLAG_TOMBSTONE:
        value = None
    else:
        compressed = data[pos : pos + value_len]
        value = snappy.decompress(compressed).decode("utf-8")
    pos += value_len
    stored_crc, = struct.unpack(">L", data[pos : pos + 4])
    entry_length = pos + 4 - offset
    # Verify CRC
    body = data[offset : pos]
    computed_crc = zlib.crc32(body) & 0xFFFFFFFF
    if computed_crc != stored_crc:
        raise ValueError(
            f"CRC mismatch at offset {offset}: expected {stored_crc:#010x}, got {computed_crc:#010x}"
        )
    return key, value, flags, entry_length


def stream_file_entries(fh):
    """Yield (offset, raw_bytes, key, flags) one entry at a time from a file handle."""
    fh.seek(HEADER_SIZE)
    while True:
        offset = fh.tell()
        header = fh.read(9)  # flags(1B) + key_len(4B) + value_len(4B)
        if len(header) < 9:
            break
        flags, key_len, value_len = struct.unpack(">BLL", header)
        rest = fh.read(key_len + value_len + 4)  # key + value + crc
        raw = header + rest
        key = raw[9 : 9 + key_len].decode("utf-8")
        yield offset, raw, key, flags
