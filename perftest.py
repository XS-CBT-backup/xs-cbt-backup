"""
Provides a CLI for measuring the performance of downloading VDI data via NBD.
"""

import os
import tempfile
import time

import XenAPI

import block_downloader
from extent_writers import OutputMode

MIB = 1024 * 1024

NBD_CLIENTS = [
    block_downloader.NbdClient.PYTHON,
    block_downloader.NbdClient.LINUX_NBD_CLIENT]

EXTENT_WRITERS = [
    block_downloader.ExtentWriter.PYTHON,
    block_downloader.ExtentWriter.LINUX_DD]

BLOCK_SIZES_TO_TEST = [1 * MIB, 4 * MIB, 40 * MIB]

TLS_MODES_TO_TEST = [False]


def _test_configs():
    for nbd_client in NBD_CLIENTS:
        for extent_writer in EXTENT_WRITERS:
            for block_size in BLOCK_SIZES_TO_TEST:
                for merge_adjacent_extents in [False, True]:
                    for use_tls in TLS_MODES_TO_TEST:
                        yield {"nbd_client": nbd_client,
                               "extent_writer": extent_writer,
                               "block_size": block_size,
                               "merge_adjacent_extents":
                                   merge_adjacent_extents,
                               "use_tls": use_tls}


def test_changed_blocks_download(bitmap, vdi_nbd_server_info):
    """
    Tests the performance of downloading the changed blocks in the given
    bitmap from this VDI.
    """
    for config in _test_configs():
        (_, out_file) = tempfile.mkstemp()
        try:
            start = time.monotonic()
            downloader = block_downloader.BlockDownloader(**config)
            downloader.download_changed_blocks(
                bitmap=bitmap,
                vdi_nbd_server_info=vdi_nbd_server_info,
                out_file=out_file,
                output_mode=OutputMode.APPEND)
            end = time.monotonic()
            print(config)
            print(start - end)
        except ValueError as error:
            print(error)
        finally:
            os.remove(out_file)


def test(pool_master, username, password, vdi_from_uuid, vdi_to_uuid):
    """
    Test the performance of various NBD configurations.
    """
    session = XenAPI.Session(pool_master)
    xenapi = session.xenapi
    xenapi.login_with_password(username, password, "perftest")
    vdi_from = xenapi.VDI.get_by_uuid(vdi_from_uuid)
    vdi_to = xenapi.VDI.get_by_uuid(vdi_to_uuid)
    bitmap = xenapi.VDI.list_changed_blocks(vdi_from, vdi_to)
    info = xenapi.VDI.get_nbd_info(vdi_from)[0]
    test_changed_blocks_download(bitmap=bitmap, vdi_nbd_server_info=info)


if __name__ == '__main__':
    import fire
    fire.Fire(test)
