
from enum import Enum, unique

from cbt_bitmap import CbtBitmap
from extent_writers import DdWriter, PythonWriter
from linux_nbd_client import LinuxNbdClient
from python_nbd_client import PythonNbdClient


@unique
class NbdClient(Enum):
    """
    Defines how to fetch the changed block data from the NBD server.
    """
    PYTHON = "python"
    LINUX_NBD_CLIENT = "nbd-client"


@unique
class ExtentWriter(Enum):
    """
    Defines how to write the changed extents to an output file.
    """
    PYTHON = "python"
    LINUX_DD = "dd"


class ChangedBlockDownloader(object):
    def __init__(self,
                 nbd_client,
                 extent_writer,
                 block_size,
                 merge_adjacent_extents,
                 use_tls,
                 nbd_client_block_size=None,
                 connections=None):
        self._nbd_client = nbd_client
        self._extent_writer = extent_writer
        if nbd_client == NbdClient.PYTHON and extent_writer == ExtentWriter.LINUX_DD:
            raise ValueError("Cannot use dd with the Python NBD client")
        self._block_size = block_size
        self._merge_adjacent_extents = merge_adjacent_extents
        self._use_tls = use_tls
        self._nbd_client_block_size = nbd_client_block_size
        self._connections = connections

    def download_changed_blocks(
            self,
            bitmap,
            vdi_nbd_server_info,
            out_file,
            output_mode):
        if self._nbd_client == NbdClient.PYTHON:
            client = PythonNbdClient(
                **vdi_nbd_server_info,
                use_tls=self._use_tls)
        elif self._nbd_client == NbdClient.LINUX_NBD_CLIENT:
            client = LinuxNbdClient(
                **vdi_nbd_server_info,
                use_tls=self._use_tls,
                nbd_device=None,
                block_size=self._nbd_client_block_size,
                timeout=10,
                use_socket_direct_protocol=False,
                connections=self._connections,
                persist=True)
        try:
            if self._extent_writer == ExtentWriter.PYTHON:
                writer = PythonWriter(client)
            elif self._extent_writer == ExtentWriter.LINUX_DD:
                writer = DdWriter(client.nbd_device)

            bitmap = CbtBitmap(bitmap)
            extents = bitmap.get_extents(
                merge_adjacent_extents=self._merge_adjacent_extents)
            writer.write_extents(
                extents=extents,
                out_file=out_file,
                block_size=self._block_size,
                output_mode=output_mode)
        finally:
            client.close()
