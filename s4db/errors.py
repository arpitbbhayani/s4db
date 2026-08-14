class S4DBError(Exception):
    """Base class for all s4db errors."""


class CorruptEntryError(S4DBError, ValueError):
    """A data-file entry failed its CRC check or could not be parsed."""


class CorruptIndexError(S4DBError, ValueError):
    """The serialized index failed its CRC check or could not be parsed."""


class ConflictError(S4DBError):
    """A fenced commit lost the compare-and-swap race to another writer.

    The losing writer's data files are already uploaded (they live in its own
    namespace, so nothing was overwritten); only the index commit failed.
    Call upload(rebase=True) to merge onto the winning index and retry, or
    reload and resolve manually.
    """


class UnsyncedWritesError(S4DBError):
    """The operation would discard or invalidate local writes that have not
    been synced to S3. Call upload() first, or pass force=True where offered."""


class CompactionError(S4DBError):
    """Compaction preconditions were not met (e.g., a data file referenced by
    the index is missing or truncated locally)."""
