import os
import tempfile
import time

import XenAPI

import changed_blocks
from extent_writers import OutputMode

MIB = 1024 * 1024

NBD_CLIENTS = [
    changed_blocks.NbdClient.PYTHON, changed_blocks.NbdClient.LINUX_NBD_CLIENT]

EXTENT_WRITERS = [
    changed_blocks.ExtentWriter.PYTHON, changed_blocks.ExtentWriter.LINUX_DD]

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
                               "merge_adjacent_extents": merge_adjacent_extents,
                               "use_tls": use_tls}


def test(pool_master, username, password, vdi_from_uuid, vdi_to_uuid):
    session = XenAPI.Session(pool_master)
    xenapi = session.xenapi
    xenapi.login_with_password(username, password, "perftest")
    vdi_from = xenapi.VDI.get_by_uuid(vdi_from_uuid)
    vdi_to = xenapi.VDI.get_by_uuid(vdi_to_uuid)
    bitmap = xenapi.VDI.list_changed_blocks(vdi_from, vdi_to)
    info = xenapi.VDI.get_nbd_info(vdi_from)[0]
    for config in _test_configs():
        (_, out_file) = tempfile.mkstemp()
        try:
            start = time.monotonic()
            changed_blocks.ChangedBlockDownloader(**config).download_changed_blocks(
                bitmap=bitmap,
                vdi_nbd_server_info=info,
                out_file=out_file,
                output_mode=OutputMode.APPEND)
            end = time.monotonic()
            print(config)
            print(start - end)
        except ValueError as error:
            print(error)
        finally:
            os.remove(out_file)


if __name__ == '__main__':
    import fire
    fire.Fire(test)
