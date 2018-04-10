"""
Code for backing up VDIs.
"""

from enum import Enum, unique
from pathlib import Path
import shutil
import subprocess

from cbt_bitmap import CbtBitmap
from python_nbd_client import PythonNbdClient


def _copy(src, dst):
    try:
        subprocess.check_output(
            ["cp", "--reflink", str(src), str(dst)])
    except subprocess.CalledProcessError:
        shutil.copy(src=str(src), dst=str(dst))


def _get_nbd_info(session, vdi):
    return session.xenapi.VDI.get_nbd_info(vdi)[0]


class VdiDownloader(object):
    """
    Provides a way of backing up the data of a VDI incrementally to a file or
    downloading it completely.
    """
    # This class uses NBD both for full backup and incremental backup.
    # For secure NBD using TLS, no manual configuration is necessary, the
    # methods will use the configuration given by xapi. This means that the
    # communication with xapi must be secure, the server URL must use HTTPS,
    # Therefore this requires manual configuration; the CA certificate of the
    # server must be known by Python, see
    # https://github.com/xapi-project/xen-api/issues/2100#issuecomment-361930724

    def __init__(self, session, block_size, use_tls=True):
        self._session = session
        self._block_size = block_size
        self._use_tls = use_tls

    def _nbd_client(self, vdi_nbd_server_info):
        """
        Connect using the given NBD server details and return the NBD client.
        No manual configuration is needed for TLS, the client will
        automatically use the certificate and server hostname provided by the
        given vdi_nbd_server_info.
        """
        return PythonNbdClient(**vdi_nbd_server_info, use_tls=self._use_tls)

    @unique
    class _OutputMode(Enum):
        """
        Defines how to write the data in the changed blocks to the output file.
        """
        OVERWRITE = 'r+b'
        APPEND = 'ab'

    def _download_nbd_extents(
            self, nbd_client, extents, out_file, output_mode):
        """
        Write the given extents to the output file.
        """
        with Path(out_file).open(output_mode.value) as out:
            for extent in extents:
                (offset, length) = extent
                end = offset + length
                for current_offset in range(offset, end, self._block_size):
                    block_length = min(self._block_size, end - current_offset)
                    data = nbd_client.read(
                            offset=current_offset, length=block_length)
                    out.seek(current_offset)
                    out.write(data)

    def _download_changed_blocks(
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
        extents = bitmap.get_extents()
        with self._nbd_client(vdi_nbd_server_info) as nbd_client:
            self._download_nbd_extents(
                nbd_client=nbd_client,
                extents=extents,
                out_file=out_file,
                output_mode=output_mode)

    def _download_vdi(self, vdi_nbd_server_info, out_file):
        """
        Downloads the network block device specified by the given
        connection information, and writes these blocks to the output file.
        """
        with self._nbd_client(vdi_nbd_server_info) as nbd_client:
            size = nbd_client.get_size()
            self._download_nbd_extents(
                nbd_client=nbd_client,
                extents=[(0, size)],
                out_file=out_file,
                output_mode=self._OutputMode.APPEND)

    def incremental_vdi_backup(
            self,
            vdi,
            latest_backup,
            output_file):
        """
        Downloads the blocks that changed between this VDI and the base VDI
        and constructs a file containing this VDI's data.
        The latest_backup argument should be a tuple (base_vdi, base_vdi_data),
        where base_vdi_data is the file containing the data of base_vdi.
        A lightweight CoW copy of base_vdi_data is performed if possible to
        reconstruct the this VDI's data, otherwise a full copy is performed.
        """
        (vdi_from, vdi_from_backup) = latest_backup

        bitmap = self._session.xenapi.VDI.list_changed_blocks(vdi_from, vdi)

        nbd_info = _get_nbd_info(self._session, vdi)

        _copy(str(vdi_from_backup), str(output_file))

        self._download_changed_blocks(
            bitmap=bitmap,
            vdi_nbd_server_info=nbd_info,
            out_file=output_file,
            output_mode=self._OutputMode.OVERWRITE)

    def full_vdi_backup(self, vdi, output_file):
        """
        Downloads the data of the VDI to the give output file.
        """
        nbd_info = _get_nbd_info(self._session, vdi)
        self._download_vdi(
            vdi_nbd_server_info=nbd_info,
            out_file=output_file)
