"""
Contains classes for downloading the given extents and writing
them to a file.
"""

import subprocess
from enum import Enum, unique
from pathlib import Path

from bitstring import BitArray


# 64K blocks
BLOCK_SIZE = 64 * 1024


def bitmap_to_extents(bitmap):
    """
    Given a CBT bitmap with 64K block size, this function will return the
    list of changed (offset in bytes, length in bytes) extents.
    """
    start = None
    bitmap = BitArray(bitmap)
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


def merge_adjacent_extents(extents):
    """
    Coalesc the consecutive extents into one.

    >>> list(merge_adjacent_extents(iter([(0,1),(1,3),(4,5)])))
    [(0, 9)]
    >>> list(merge_adjacent_extents(iter([(0,1),(4,5)])))
    [(0, 1), (4, 5)]
    >>> list(merge_adjacent_extents(iter([])))
    []
    >>> list(merge_adjacent_extents(iter([(5,6)])))
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


@unique
class OutputMode(Enum):
    """
    Defines how to write the data in the changed blocks to the output file.
    """
    OVERWRITE = 'r+b'
    APPEND = 'ab'


class PythonWriter(object):
    """
    Provides a way of reading extents using the specified NBD client object,
    (which may be implemented in pure Python, or may be a wrapper around
    nbd-client), and then writing these extents to the given output file.
    """
    def __init__(self, nbd_client):
        self._nbd_client = nbd_client

    def _write_extent(self, extent, out, block_size):
        (offset, length) = extent
        for current_offset in range(offset, length, block_size):
            length = min(block_size, length - current_offset)
            data = self._nbd_client.read(offset=current_offset, length=length)
            out.write(data)

    def write_extents(self, extents, out_file, block_size, output_mode):
        """
        Write the given extents to the output file using Python functions only.
        """
        with Path(out_file).open(output_mode) as out:
            for extent in extents:
                self._write_extent(
                    extent=extent, out=out, block_size=block_size)


class DdWriter(object):
    """
    Provides a way of reading extents from the specified /dev/nbdX device
    using nbd-client, and then writing them to the given output file using dd.
    """
    def __init__(self, nbd_device):
        self._nbd_device = nbd_device

    def _write_extent(self, extent, out_file, block_size, output_mode):
        (offset, length) = extent
        command = [
            'dd', 'if=' + self._nbd_device, 'of=' + out_file,
            'bs=' + block_size
        ]
        command += [
            'count=' + length, 'skip=' + offset, 'iflag=count_bytes,skip_bytes'
        ]
        if output_mode is OutputMode.OVERWRITE:
            command += ['seek=' + offset, 'oflag=seek_bytes']
        elif output_mode is OutputMode.APPEND:
            command += ['oflag=append']
        subprocess.check_output(command)

    def write_extents(self, extents, out_file, block_size, output_mode):
        """
        Write the given extents to the output file by calling dd for each
        extent.
        """
        for extent in extents:
            self._write_extent(
                extent=extent,
                out_file=out_file,
                block_size=block_size,
                output_mode=output_mode)


def _write_changed_blocks(bitmap, writer, out_file, output_mode, block_size, coalesce_extents):
    pass
