#!/usr/bin/env python3

"""
CLI for testing changed block tracking and the NBD server in XenServer.
"""

import time

import XenAPI

import block_downloader
import cbt_tests
import nbd_networks


def _wait_after_nbd_disconnect():
    """
    Wait for a bit for the cleanup actions (unplugging and
    destroying the VBD) to finish after terminating the NBD
    session.
    There is a race condition where we can get
    XenAPI.Failure:
     ['VDI_IN_USE', 'OpaqueRef:<VDI ref>', 'destroy']
    if we immediately call VDI.destroy after closing the NBD
    session, because the VBD has not yet been cleaned up.
    """
    time.sleep(7)


TEMPORARY_TEST_VDI_NAME = "cbt_tests_test_vdi"


def _create_test_vdi(session, storage_repository):
    new_vdi_record = {
        "SR": storage_repository,
        "virtual_size": 4000000,
        "type": "user",
        "sharable": False,
        "read_only": False,
        "other_config": {},
        "name_label": (TEMPORARY_TEST_VDI_NAME)
    }
    return session.xenapi.VDI.create(new_vdi_record)


def _cleanup_test_vdis(session):
    temp_vdis = session.xenapi.VDI.get_by_name_label(
        TEMPORARY_TEST_VDI_NAME)
    if temp_vdis:
        _wait_after_nbd_disconnect()
    for vdi in temp_vdis:
        print("Destroying VDI {}".format(vdi))
        try:
            try:
                session.xenapi.VDI.destroy(vdi)
            except XenAPI.Failure as xenapi_error:
                print("Failed to destroy VDI {}: {}. Trying to "
                      "unplug VBDs first".
                      format(vdi, xenapi_error))
                for vbd in session.xenapi.VDI.get_VBDs(vdi):
                    print("Unplugging VBD {} of VDI {}".format(vbd, vdi))
                # Wait for a bit for the VBD unplug operations to finish
                time.sleep(4)
                session.xenapi.VDI.destroy(vdi)
        except XenAPI.Failure as xenapi_error:
            print("Failed to destroy VDI {}: {}".
                  format(vdi, xenapi_error))


def _get_block_downloader(use_tls):
    return block_downloader.BlockDownloader(
        nbd_client=block_downloader.NbdClient.PYTHON,
        extent_writer=block_downloader.ExtentWriter.PYTHON,
        block_size=4 * 1024 * 1024,
        merge_adjacent_extents=True,
        use_tls=use_tls)


def run_tests(pool_master, uname, pwd, skip_vlans=True):
    """
    Test various aspects of the changed block tracking and NBD server
    features of XenServer.
    """
    session = XenAPI.Session("http://" + pool_master)
    session.xenapi.login_with_password(uname, pwd, "1.0", "cbt_tests")

    for use_tls in [True, False]:
        nbd_networks.disable_nbd_on_all_networks(session)
        nbd_networks.auto_enable_nbd(session, use_tls=use_tls, skip_vlans=skip_vlans)
        nbd_networks.wait_after_nbd_network_changes()

        for storage_repository in session.xenapi.SR.get_all():
            vdi = _create_test_vdi(session, storage_repository)
            for nbd_info in session.xenapi.VDI.get_nbd_info(vdi):
                cbt_tests.loop_connect_disconnect(nbd_info, use_tls)
                cbt_tests.loop_connect_disconnect(nbd_info, use_tls, fail_connection=True)
                cbt_tests.download_whole_vdi_using_nbd(
                    _get_block_downloader(use_tls),
                    nbd_info)
                cbt_tests.parallel_nbd_connections(nbd_info, use_tls)
                cbt_tests.read_from_vdi_via_nbd(nbd_info, use_tls)
            cbt_tests.list_changed_blocks(session, vdi)

    _cleanup_test_vdis(session)
    session.xenapi.session.logout()


if __name__ == '__main__':
    import fire
    fire.Fire(run_tests)
