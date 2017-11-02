"""
Provides a class wrapping the return value of VDI.list_changed_blocks for
extracting useful information from the CBT bitmap.
"""

import base64

from bitstring import BitArray


# 64K blocks
BLOCK_SIZE = 64 * 1024


def _bitmap_to_extents(cbt_bitmap):
    """
    Given a CBT bitmap with 64K block size, this function will return the
    list of changed (offset in bytes, length in bytes) extents.

    Args:
        cbt_bitmap (bytes-like object): the bitmap to turn into extents

    Returns:
        An iterator containing the increasingly ordered sequence of the
        non-overlapping extents corresponding to this bitmap.
    """
    start = None
    bitmap = BitArray(cbt_bitmap)
    for i in range(0, len(bitmap)):
        if bitmap[i]:
            if start is None:
                start = i
                length = 1
            else:
                length += 1
        else:
            if start is not None:
                yield (start * BLOCK_SIZE, length * BLOCK_SIZE)
                start = None
    if start is not None:
        yield (start * BLOCK_SIZE, length * BLOCK_SIZE)


def _merge_adjacent_extents(extents):
    """
    Coalesc the consecutive extents into one.

    Args:
        extents (iterator): increasingly ordered sequence of
            non-overlapping (offset, length) pairs

    >>> list(_merge_adjacent_extents(iter([(0,1),(1,3),(4,5)])))
    [(0, 9)]
    >>> list(_merge_adjacent_extents(iter([(0,1),(4,5)])))
    [(0, 1), (4, 5)]
    >>> list(_merge_adjacent_extents(iter([])))
    []
    >>> list(_merge_adjacent_extents(iter([(5,6)])))
    [(5, 6)]
    """
    if not extents:
        return
    last = next(extents)
    for extent in extents:
        if extent[0] == (last[0] + last[1]):
            last = (last[0], last[1] + extent[1])
        else:
            yield last
            last = extent
    yield last


def _get_changed_blocks_size(cbt_bitmap):
    """
    Returns the overall size of the changed 64K blocks in the
    given bitmap in bytes.
    """
    bitmap = BitArray(cbt_bitmap)
    modified = 0
    for bit in bitmap:
        if bit:
            modified += 1
    return modified * BLOCK_SIZE


def _get_disk_size(cbt_bitmap):
    bitmap = BitArray(cbt_bitmap)
    return len(bitmap) * BLOCK_SIZE


class CbtBitmap(object):
    """
    Wraps a base64-encoded CBT bitmap, as returned by
    VDI.list_changed_blocks, and provides methods for extracting various
    data from the bitmap.
    """
    def __init__(self, cbt_bitmap_b64):
        self.bitmap = base64.b64decode(cbt_bitmap_b64)

    def get_extents(self, merge_adjacent_extents):
        """
        Returns an iterator containing the increasingly ordered sequence
        of the non-overlapping extents corresponding to this bitmap.
        """
        extents = _bitmap_to_extents(self.bitmap)
        if merge_adjacent_extents:
            return _merge_adjacent_extents(extents)
        else:
            return extents

    def get_statistics(self):
        """
        Return the size of the disk, and the total size of the changed
        blocks in a dictionary.
        """
        stats = {}
        stats["size"] = _get_disk_size(self.bitmap)
        stats["changed_blocks_size"] = _get_changed_blocks_size(self.bitmap)
        return stats
