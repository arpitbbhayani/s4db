import re

from ._index import make_file_id, split_file_id

# Legacy (writer 0):     data_NNNNNN.s4db
# Namespaced writers:    data_XXXXXXXX_NNNNNN.s4db  (8 hex writer id, decimal seq)
_DATA_FILE_RE = re.compile(r"^data_(?:([0-9a-f]{8})_)?(\d{6,})\.s4db$")


def data_filename(file_id: int) -> str:
    """Returns the canonical filename for a data file given its 64-bit file id.

    Writer id 0 keeps the legacy data_NNNNNN.s4db name so version-1 databases
    remain addressable; namespaced writers embed their id in the name, which
    is what keeps concurrent writers' uploads from ever colliding.
    """
    writer_id, seq = split_file_id(file_id)
    if writer_id == 0:
        return f"data_{seq:06d}.s4db"
    return f"data_{writer_id:08x}_{seq:06d}.s4db"


def parse_data_filename(name: str) -> int | None:
    """Returns the 64-bit file id encoded in a data filename, or None if it is not one.

    The filename, not the file header, is authoritative for the writer half of
    the id: headers predate writer namespaces and store only the sequence.
    """
    m = _DATA_FILE_RE.match(name)
    if m is None:
        return None
    writer_id = int(m.group(1), 16) if m.group(1) else 0
    return make_file_id(writer_id, int(m.group(2)))
