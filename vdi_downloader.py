"""
Code for backing up VDIs.
"""

import shutil
import subprocess

from block_downloader import BlockDownloader, NbdClient, ExtentWriter
from extent_writers import OutputMode


class VdiDownloader(object):
    """
    Provides a way of backing up the data of a VDI incrementally to a file or
    downloading it completely.
    """
    def __init__(self, nbd_client=NbdClient.PYTHON, use_tls=True):
        extent_writer = \
            ExtentWriter.PYTHON if nbd_client == NbdClient.PYTHON else \
            ExtentWriter.LINUX_DD
        self._downloader = BlockDownloader(
            nbd_client=nbd_client,
            extent_writer=extent_writer,
            block_size=4 * 1024 * 1024,
            merge_adjacent_extents=True,
            use_tls=use_tls)

    def incremental_vdi_backup(
            self,
            session,
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

        try:
            subprocess.check_output(
                ["cp", "--reflink", str(vdi_from_backup), str(output_file)])
        except subprocess.CalledProcessError:
            shutil.copy(src=str(vdi_from_backup), dst=str(output_file))

        bitmap = session.xenapi.VDI.list_changed_blocks(vdi_from, vdi)
        vdi_info = session.xenapi.VDI.get_nbd_info(vdi)
        self._downloader.download_changed_blocks(
            bitmap=bitmap,
            vdi_nbd_server_info=vdi_info,
            out_file=output_file,
            output_mode=OutputMode.OVERWRITE)

    def full_vdi_backup(self, session, vdi, output_file):
        """
        Downloads the data of the VDI to the give output file.
        """
        vdi_info = session.xenapi.VDI.get_nbd_info(vdi)[0]
        self._downloader.download_vdi(
            vdi_nbd_server_info=vdi_info,
            out_file=output_file)
