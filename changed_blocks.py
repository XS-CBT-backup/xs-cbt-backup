"""
Contains functions for downloading the given changed blocks and writing
them to a file.
"""

import subprocess
from enum import Enum, unique
from pathlib import Path


@unique
class OutputMode(Enum):
    """
    Defines how to write the data in the changed blocks to the output file.
    """
    OVERWRITE = 'r+b'
    APPEND = 'ab'


def _write_extent_python(client, extent, out, block_size):
    (offset, length) = extent
    for current_offset in range(offset, length, block_size):
        length = min(block_size, length - current_offset)
        data = client.read(offset=current_offset, length=length)
        out.write(data)


def _write_extents_python(client, extents, out_file, block_size, output_mode):
    with Path(out_file).open(output_mode) as out:
        for extent in extents:
            _write_extent_python(client=client,
                                 extent=extent,
                                 out=out,
                                 block_size=block_size)


def _write_extent_dd(device, extent, out_file, block_size, output_mode):
    (offset, length) = extent
    command = ['dd', 'if=' + device, 'of=' + out_file, 'bs=' + block_size]
    command += ['count=' + length, 'skip=' + offset,
                'iflag=count_bytes,skip_bytes']
    if output_mode is OutputMode.OVERWRITE:
        command += ['seek=' + offset, 'oflag=seek_bytes']
    elif output_mode is OutputMode.APPEND:
        command += ['oflag=append']
    subprocess.check_output(command)


def _write_extents_dd(device, extents, out_file, block_size, output_mode):
    for extent in extents:
        _write_extent_dd(device=device,
                         exten=extent,
                         out_file=out_file,
                         block_size=block_sizew,
                         output_mode=output_mode)
