import struct
import zlib

import snappy

from .errors import CorruptEntryError

MAGIC = b"S4DB"
VERSION = 0x02
HEADER_SIZE = 9  # 4 magic + 1 version + 4 file_num

FLAG_NORMAL = 0x00
FLAG_TOMBSTONE = 0x01
FLAG_NOCOMPRESS = 0x02  # value stored raw (compression did not shrink it)
FLAG_BINARY = 0x04      # value is bytes, not UTF-8 text

_KNOWN_FLAGS = FLAG_TOMBSTONE | FLAG_NOCOMPRESS | FLAG_BINARY

# Entry overhead: 1 (flags) + 4 (key_len) + 4 (value_len) + 4 (crc) = 13
ENTRY_OVERHEAD = 13

MAX_KEY_BYTES = 65535  # bounded by the index's 2-byte key-length field


def pack_file_header(file_num: int) -> bytes:
    """Serializes the 9-byte file header: magic bytes, version, and file sequence number.

    Only the low 32 bits of the file's sequence number are stored; the writer
    half of a file id is recoverable from the filename.
    """
    return MAGIC + struct.pack(">BL", VERSION, file_num & 0xFFFFFFFF)


def unpack_file_header(data: bytes) -> tuple[int, int]:
    """Parses a 9-byte file header, returning (version, file_seq). Raises ValueError on bad magic."""
    if data[:4] != MAGIC:
        raise ValueError(f"Invalid magic: {data[:4]!r}")
    version, file_num = struct.unpack(">BL", data[4:9])
    return version, file_num


def pack_entry(key: str, value: str | bytes | None, deleted: bool = False) -> bytes:
    """Serializes a key/value pair into a binary entry with a CRC trailer.

    Tombstone entries (deleted=True) carry an empty value body and set FLAG_TOMBSTONE.
    Values may be str (stored as UTF-8) or bytes (FLAG_BINARY). The value is
    Snappy-compressed only when compression shrinks it; otherwise it is stored
    raw and FLAG_NOCOMPRESS is set. Returns the complete entry bytes including
    flags, lengths, key, value, and CRC.
    """
    key_bytes = key.encode("utf-8")
    flags = FLAG_NORMAL
    if deleted:
        flags |= FLAG_TOMBSTONE
        value_bytes = b""
    else:
        if isinstance(value, bytes):
            flags |= FLAG_BINARY
            raw = value
        else:
            raw = value.encode("utf-8")
        compressed = snappy.compress(raw)
        if len(compressed) < len(raw):
            value_bytes = compressed
        else:
            flags |= FLAG_NOCOMPRESS
            value_bytes = raw

    header = struct.pack(">BLL", flags, len(key_bytes), len(value_bytes))
    body = header + key_bytes + value_bytes
    crc = zlib.crc32(body) & 0xFFFFFFFF
    return body + struct.pack(">L", crc)


def unpack_entry_at(data: bytes, offset: int = 0) -> tuple[str, str | bytes | None, int, int]:
    """Deserializes one entry from data starting at offset.

    Returns (key, value, flags, entry_length). value is None for tombstones,
    bytes for FLAG_BINARY entries, str otherwise. entry_length is the total
    byte span of this entry, useful for advancing to the next one.

    The CRC is verified over the raw stored bytes BEFORE any decompression or
    decoding, so corruption always surfaces as CorruptEntryError rather than
    as a decompressor or codec failure. Raises CorruptEntryError on a short
    buffer, CRC mismatch, or unknown flag bits.
    """
    if len(data) - offset < ENTRY_OVERHEAD:
        raise CorruptEntryError(f"Truncated entry at offset {offset}: header incomplete")
    flags, key_len, value_len = struct.unpack(">BLL", data[offset : offset + 9])
    pos = offset + 9
    end = pos + key_len + value_len + 4
    if end > len(data):
        raise CorruptEntryError(
            f"Truncated entry at offset {offset}: need {end - offset} bytes, have {len(data) - offset}"
        )

    # Verify the CRC over the stored bytes before interpreting any of them.
    body = data[offset : pos + key_len + value_len]
    (stored_crc,) = struct.unpack(">L", data[pos + key_len + value_len : end])
    computed_crc = zlib.crc32(body) & 0xFFFFFFFF
    if computed_crc != stored_crc:
        raise CorruptEntryError(
            f"CRC mismatch at offset {offset}: expected {stored_crc:#010x}, got {computed_crc:#010x}"
        )
    if flags & ~_KNOWN_FLAGS:
        raise CorruptEntryError(f"Unknown flag bits {flags:#04x} at offset {offset}")

    key = data[pos : pos + key_len].decode("utf-8")
    pos += key_len
    if flags & FLAG_TOMBSTONE:
        value: str | bytes | None = None
    else:
        value_bytes = data[pos : pos + value_len]
        if not flags & FLAG_NOCOMPRESS:
            value_bytes = snappy.decompress(value_bytes)
        value = value_bytes if flags & FLAG_BINARY else value_bytes.decode("utf-8")
    return key, value, flags, end - offset


def stream_file_entries(fh):
    """Yields (offset, raw_bytes, key, flags) for each entry in a data file handle.

    Seeks past the file header before reading. Stops cleanly at the first
    entry whose declared lengths run past the end of the file or whose
    key-length field exceeds MAX_KEY_BYTES -- both signatures of a torn tail.
    Does not validate CRCs - callers that need integrity checking should call
    unpack_entry_at on the yielded raw_bytes.
    """
    fh.seek(0, 2)
    file_size = fh.tell()
    fh.seek(HEADER_SIZE)
    while True:
        offset = fh.tell()
        header = fh.read(9)  # flags(1B) + key_len(4B) + value_len(4B)
        if len(header) < 9:
            break
        flags, key_len, value_len = struct.unpack(">BLL", header)
        if key_len > MAX_KEY_BYTES or offset + 9 + key_len + value_len + 4 > file_size:
            break  # torn or garbage entry: lengths point past EOF
        rest = fh.read(key_len + value_len + 4)  # key + value + crc
        raw = header + rest
        try:
            key = raw[9 : 9 + key_len].decode("utf-8")
        except UnicodeDecodeError:
            break  # garbage where a key should be: treat as torn
        yield offset, raw, key, flags
