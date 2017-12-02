"""
Provides a way of downloading data from a network block device and
writing it into a file.
"""

from enum import Enum, unique

from cbt_bitmap import CbtBitmap
from extent_writers import DdWriter, PythonWriter, OutputMode
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


class BlockDownloader(object):
    """
    Combines an extent writer responsible for getting extents from an
    NBD client and writing them to a file with an NBD client
    implementation that is compatible with that extent writer, to form
    a block downloader object that is capable of connecting to a VDI via
    NBD and downloading either the changed blocks or the complete VDI to
    a file.
    """

    def __init__(self,
                 nbd_client,
                 extent_writer,
                 block_size,
                 merge_adjacent_extents,
                 use_tls,
                 nbd_client_block_size=None,
                 connections=None):
        """
        Creates a downloader that will use the given NBD client and extent
        writer to download blocks of a specified network block device and
        write them to an output file.
        """
        self._nbd_client = nbd_client
        self._extent_writer = extent_writer
        if nbd_client == NbdClient.PYTHON and \
                extent_writer == ExtentWriter.LINUX_DD:
            raise ValueError("Cannot use dd with the Python NBD client")
        self._block_size = block_size
        self._merge_adjacent_extents = merge_adjacent_extents
        self._use_tls = use_tls
        self._nbd_client_block_size = nbd_client_block_size
        self._connections = connections

    def _with_nbd_client(self, vdi_nbd_server_info, use_client):
        if self._nbd_client == NbdClient.PYTHON:
            with PythonNbdClient(**vdi_nbd_server_info,
                                 use_tls=self._use_tls) as client:
                use_client(client)
        elif self._nbd_client == NbdClient.LINUX_NBD_CLIENT:
            with LinuxNbdClient(**vdi_nbd_server_info,
                                use_tls=self._use_tls,
                                nbd_device=None,
                                block_size=self._nbd_client_block_size,
                                use_socket_direct_protocol=False,
                                connections=self._connections,
                                persist=True) as client:
                use_client(client)

    def _download_extents_from_client(
            self,
            extents,
            out_file,
            output_mode,
            client):
        if self._extent_writer == ExtentWriter.PYTHON:
            writer = PythonWriter(client)
        elif self._extent_writer == ExtentWriter.LINUX_DD:
            writer = DdWriter(client.nbd_device)
        writer.write_extents(
            extents=extents,
            out_file=out_file,
            block_size=self._block_size,
            output_mode=output_mode)

    def download_extents(
            self,
            extents,
            vdi_nbd_server_info,
            out_file,
            output_mode):
        """
        From the VDI specified by the given connection information,
        downloads the given extents, and writes these blocks to the
        output file.
        """
        self._with_nbd_client(
            vdi_nbd_server_info,
            lambda client: self._download_extents_from_client(
                extents, out_file, output_mode, client))

    def download_changed_blocks(
            self,
            bitmap,
            vdi_nbd_server_info,
            out_file,
            output_mode):
        """
        From the network block device specified by the given connection
        information, downloads the blocks that are marked as changed in
        the bitmap via NBD, and writes these blocks to the output file.
        """
        bitmap = CbtBitmap(bitmap)
        extents = bitmap.get_extents(
            merge_adjacent_extents=self._merge_adjacent_extents)
        self.download_extents(
            extents=extents,
            vdi_nbd_server_info=vdi_nbd_server_info,
            out_file=out_file,
            output_mode=output_mode)

    def _download_vdi_from_client(self, out_file, client):
        size = client.get_size()
        self._download_extents_from_client(
            extents=[(0, size)],
            out_file=out_file,
            output_mode=OutputMode.APPEND,
            client=client)

    def download_vdi(self, vdi_nbd_server_info, out_file):
        """
        Downloads the network block device specified by the given
        connection information, and writes these blocks to the output file.
        """
        self._with_nbd_client(
            vdi_nbd_server_info,
            lambda client: self._download_vdi_from_client(
                out_file, client))
