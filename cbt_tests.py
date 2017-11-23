#!/usr/bin/env python3

"""
CLI for testing changed block tracking and the NBD server in XenServer.
"""

import random
import socket
import subprocess
import time
from xmlrpc.client import ServerProxy

import XenAPI

import changed_blocks
from python_nbd_client import PythonNbdClient
import xapi_nbd_networks

PROGRAM_NAME = "cbt_tests.py"


def get_first_safely(iterable):
    """Gets the 'first' element of an iterable, if any, or None"""
    return next(iter(iterable), None)


def find_local_user_sr(session, host):
    """
    Get an SR that is only attached to this host (not shared), for
    testing local SRs
    """
    pbds = session.xenapi.host.get_PBDs(host)
    srs = [
        session.xenapi.PBD.get_SR(pbd) for pbd in pbds
        if session.xenapi.PBD.get_currently_attached(pbd) is True
    ]
    user_srs = [
        sr for sr in srs
        if sr is not None and
        session.xenapi.SR.get_content_type(sr) == "user" and
        session.xenapi.SR.get_shared(sr) is False
    ]
    return get_first_safely(user_srs)


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


class CBTTests(object):
    """
    A command-line program providing a set of commands that test various
    aspects of the changed block tracking and NBD server features in XenServer.
    """
    # 64K blocks
    _BLOCK_SIZE = 64 * 1024

    _TEMPORARY_TEST_VDI_NAME = "TMP_test_" + PROGRAM_NAME
    _TEST_VDI_NAME = "test"

    def __init__(self,
                 pool_master,
                 username,
                 password,
                 hostname=None,
                 sr_uuid=None,
                 use_tls=True,
                 skip_vlan_networks=True):

        self._pool_master_address = pool_master
        self._username = username
        self._password = password
        self._session = XenAPI.Session("http://" + self._pool_master_address)
        self._session.xenapi.login_with_password(
            username, password, "1.0", PROGRAM_NAME)
        if hostname is not None:
            [self._host] = self._session.xenapi.host.get_by_name_label(
                hostname)
        else:
            self._host = self._session.xenapi.session.get_this_host(
                self._session._session)
        if sr_uuid is not None:
            self._sr = self._session.xenapi.SR.get_by_uuid(sr_uuid)
        else:
            self._sr = find_local_user_sr(
                session=self._session,
                host=self._host)
        self._use_tls = use_tls
        self._skip_vlan_networks = skip_vlan_networks

    def create_test_session(self):
        """
        Create a session that won't be garbage-collected and maybe even logged
        out after we printed the session ref for the user
        """
        proxy = ServerProxy("http://" + self._pool_master_address)
        session = proxy.session.login_with_password(
            self._username, self._password)['Value']
        return session

    def __del__(self):
        self._cleanup_test_vdis()
        self._session.xenapi.session.logout()

    def get_certfile(self):
        """
        Returns the certificate of the given host or the pool master.
        """
        return self._session.xenapi.host.get_server_certificate(self._host)

    def _create_test_vdi(self, keep_after_exit=False):
        print("Creating a VDI")

        new_vdi_record = {
            "SR":
            self._sr,
            "virtual_size":
            40000000,
            "type":
            "user",
            "sharable":
            False,
            "read_only":
            False,
            "other_config": {},
            "name_label": (self._TEST_VDI_NAME if keep_after_exit else
                           self._TEMPORARY_TEST_VDI_NAME)
        }
        vdi = self._session.xenapi.VDI.create(new_vdi_record)
        return vdi

    def _auto_enable_nbd(self):
        xapi_nbd_networks.auto_enable_nbd(
            session=self._session,
            use_tls=self._use_tls,
            skip_vlan_networks=self._skip_vlan_networks)

    def _get_xapi_nbd_client(self, vdi=None, vdi_nbd_server_info=None):
        if vdi_nbd_server_info is None:
            vdi_nbd_server_info = self._session.xenapi.VDI.get_nbd_info(vdi)[0]
        return PythonNbdClient(**vdi_nbd_server_info, use_tls=self._use_tls)

    def _get_download_config(self):
        return {
            "nbd_client": changed_blocks.NbdClient.PYTHON,
            "extent_writer": changed_blocks.ExtentWriter.PYTHON,
            "block_size": 4 * 1024 * 1024,
            "merge_adjacent_extents": True,
            "use_tls": self._use_tls
        }

    def _read_from_vdi_via_nbd(self, vdi):
        client = self._get_xapi_nbd_client(vdi=vdi)
        try:
            # This usually gives us some interesting text for the ISO VDIs :)
            # If we read from position 0 that's boring, we get all zeros
            print(client.read(512 * 200, 512))
        finally:
            client.close()

    def read_from_vdi_via_nbd(self):
        """
        Creates a temporary VDI on the SR, and prints 512 bytes read
        from it at a 100 KiB offset using the NBD server.
        """
        vdi = self._create_test_vdi()
        self._read_from_vdi_via_nbd(vdi)

    def test_data_destroy(self):
        """
        Verifies that we can run data_destroy without errors on a VDI
        immediately after disconnecting from the NBD server serving that VDI.
        """
        vdi = self._create_test_vdi()
        vbds = self._session.xenapi.VDI.get_VBDs(vdi)
        assert not vbds
        self._session.xenapi.VDI.enable_cbt(vdi)
        snapshot = self._session.xenapi.VDI.snapshot(vdi)

        self._read_from_vdi_via_nbd(vdi=snapshot)
        self._session.xenapi.VDI.data_destroy(snapshot)
        vbds = self._session.xenapi.VDI.get_VBDs(vdi)
        # a cbt_metadata VDI should have no VBDs
        assert not vbds

    def repro_sm_bug(self):
        """
        Reproduces a race condition in SM between VDI.destroy on the
        snapshotted VDI, and VBD.unplug on the snapshot VDI when CBT is
        enabled. This has already been fixed.
        """
        vdi = self._create_test_vdi()
        print(self._session.xenapi.VDI.get_uuid(vdi))
        # Without this line, if we do not enable CBT, it works:
        self._session.xenapi.VDI.enable_cbt(vdi)
        snapshot = self._session.xenapi.VDI.snapshot(vdi)
        print(self._session.xenapi.VDI.get_uuid(snapshot))

        self._auto_enable_nbd()
        client = self._get_xapi_nbd_client(vdi=snapshot)
        print(client.read(512 * 200, 512))
        # If we run the VDI.destroy here it will work:
        # self._session.xenapi.VDI.destroy(vdi)
        client.close()

        # It also works if we first wait for the unplug to finish, so probably
        # this is a race between VBD.unplug on the snapshot and VDI.destroy on
        # the snapshotted VDI:
        # time.sleep(2)

        self._session.xenapi.VDI.destroy(vdi)

    def _test_nbd_server_cleans_up_vbds(self,
                                        terminate_while_connected,
                                        terminate_command):
        xapi_nbd_networks.disable_nbd_on_all_networks(self._session)

        vdi = self._create_test_vdi()
        xapi_nbd_networks.auto_enable_nbd(self._session)
        client = self._get_xapi_nbd_client(vdi=vdi)
        if not terminate_while_connected:
            client.close()
        else:
            vbds = self._session.xenapi.VDI.get_VBDs(vdi)
            assert len(vbds) == 1
        self._run_ssh_command(terminate_command)
        try:
            # wait for a while for the cleanup to finish
            time.sleep(6)
            assert not self._session.xenapi.VDI.get_VBDs(vdi)
        finally:
            self._control_xapi_nbd_service("start")

    def test_nbd_server_cleans_up_vbds(self):
        """
        Verifies that the NBD server has no leaked VBDs after it's restarted
        after abnormal termination.
        """
        self._test_nbd_server_cleans_up_vbds(
            False, ["service", "xapi-nbd", "stop"])
        self._test_nbd_server_cleans_up_vbds(
            True, ["service", "xapi-nbd", "restart"])
        # This is similar to a crash, as the program cannot handle this
        # signal
        self._test_nbd_server_cleans_up_vbds(
            True, ["pkill", "-9", "xapi-nbd"])

    def loop_connect_disconnect(self,
                                vdi=None,
                                iterations=1030,
                                random_delays=False,
                                fail_connection=False):
        """
        Keeps connecting to and disconnecting from the NBD server in a
        loop.
        """
        if vdi is None:
            vdi = self._create_test_vdi()

        if fail_connection:
            info = self._session.xenapi.VDI.get_nbd_info(vdi)[0]
            info["exportname"] = "invalid export name"

        for i in range(iterations):
            print("{}: connecting to {} on {}".format(i, vdi, self._host))
            if fail_connection:
                client = self._get_xapi_nbd_client(vdi_nbd_server_info=info)
            else:
                client = self._get_xapi_nbd_client(vdi=vdi)
            if random_delays:
                time.sleep(random.random())
            client.close()
            if random_delays:
                time.sleep(random.random())

    def parallel_nbd_connections(self, n_connections):
        """
        Create n_connections parallel connections to the NBD server.
        """
        # Stash the NBD clients here to avoid them being garbage collected
        # and disconnected immediately after creation :S.
        open_nbd_connections = []
        vdi = self._create_test_vdi()
        self._auto_enable_nbd()
        info = self._session.xenapi.VDI.get_nbd_info(vdi)[0]
        try:
            for i in range(n_connections):
                print("{}: connecting to {} on {}".format(i, vdi, self._host))
                client = self._get_xapi_nbd_client(vdi_nbd_server_info=info)
                open_nbd_connections += [client]
        finally:
            time.sleep(2)
            for client in open_nbd_connections:
                client.close()

    def _enable_nbd_on_network(self, network):
        print("Enabling secure NBD on network {}".format(network))
        nbd_purpose = "nbd" if self._use_tls else "insecure_nbd"
        self._session.xenapi.network.add_purpose(network, nbd_purpose)
        # wait for a bit for the changes to take effect
        # We do rate limiting with a 5s delay, so sometimes the update
        # takes at least 5 seconds
        time.sleep(7)

    def test_nbd_network_config(self):
        """
        Test that the NBD network configuration works correctly.
        The following are tested:
        - That VDI.get_nbd_info returns 0 records if NBD is disabled on
          all networks.
        - That when NBD is enabled on a given network, and disabled on
          the others, we can connect through that network if
          VDI.get_nbd_info returns entries for it.
        """
        vdi = self._create_test_vdi()
        # Test that if we disable NBD on all networks, we cannot connect
        xapi_nbd_networks.disable_nbd_on_all_networks(session=self._session)
        infos = self._session.xenapi.VDI.get_nbd_info(vdi)
        print(infos)
        assert infos == []
        # Test that if we enable NBD on all networks, we can connect
        self._get_xapi_nbd_client(vdi=vdi)
        # Now enable all the networks one by one and check that we can
        # connect through them.
        xapi_nbd_networks.disable_nbd_on_all_networks(session=self._session)
        for network in self._session.xenapi.network.get_all():
            if xapi_nbd_networks.has_vlan_pif(
                    session=self._session, network=network) \
                    and self._skip_vlan_networks:
                print("Skipping network {} because it has a"
                      " VLAN master PIF".format(network))
                continue
            xapi_nbd_networks.disable_nbd_on_all_networks(
                session=self._session)
            self._enable_nbd_on_network(network=network)
            infos = self._session.xenapi.VDI.get_nbd_info(vdi)
            if infos == []:
                print("Skipping network {} because VDI {} is not reachable"
                      " through it".format(network, vdi))
                continue
            for vdi_nbd_server_info in infos:
                self._get_xapi_nbd_client(
                    vdi=vdi,
                    vdi_nbd_server_info=vdi_nbd_server_info)

    def test_nbd_timeout(self, timeout):
        """
        Verifies that the NBD server closes the connection after the
        client has been inactive for a given timeout.
        """
        vdi = self._create_test_vdi()
        self._auto_enable_nbd()
        client = self._get_xapi_nbd_client(vdi=vdi)
        sleep = timeout + 2
        print("waiting for {} seconds".format(sleep))
        time.sleep(sleep)
        print("timeout over")
        try:
            client.read(0, 512)
            timed_out = False
        except socket.timeout as exc:
            print(exc)
            timed_out = True
        assert timed_out is True

    def _run_ssh_command(self, command):
        address = self._session.xenapi.host.get_address(self._host)
        return (subprocess.check_output([
            "sshpass", "-p", "xenroot", "ssh", "-o",
            "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no",
            "-l", "root", address
        ] + command))

    def _control_xapi_nbd_service(self, service_command):
        self._run_ssh_command(["service", "xapi-nbd", service_command])

    def verify_xapi_nbd_systemd_service(self, socket_activated=False):
        """
        Verify that the service is running & properly working.
        """
        vdi = self._create_test_vdi()
        # This will fail if the service isn't running
        self._control_xapi_nbd_service(service_command="status")
        self._read_from_vdi_via_nbd(vdi=vdi)
        self._control_xapi_nbd_service(service_command="stop")
        try:
            self._read_from_vdi_via_nbd(vdi=vdi)
            running = True
        except ConnectionRefusedError:
            running = False
        assert running == socket_activated
        self._control_xapi_nbd_service(service_command="restart")
        self._read_from_vdi_via_nbd(vdi=vdi)

    def print_cbt_bitmap(self):
        """
        Creates a temporary VDI, enables CBT on it, snapshots it, and
        then prints the changed blocks between the snapshot and the
        snapshotted VDI.
        """
        vdi_to = self._create_test_vdi()
        self._session.xenapi.VDI.enable_cbt(vdi_to)
        vdi_from = self._session.xenapi.VDI.snapshot(vdi_to)
        vdi_from_uuid = self._session.xenapi.VDI.get_uuid(vdi_from)
        vdi_to_uuid = self._session.xenapi.VDI.get_uuid(vdi_to)
        print("VDI.list_changed_blocks({}, {}):".format(
            vdi_from_uuid, vdi_to_uuid))
        print(self._session.xenapi.VDI.list_changed_blocks(vdi_from, vdi_to))

    def save_changed_blocks(self,
                            vdi_from=None,
                            vdi_to=None,
                            output_file=None,
                            overwrite_changed_blocks=True):
        blocks = self.download_changed_blocks(vdi_from=vdi_from, vdi_to=vdi_to)

        overwrite_changed_blocks = (output_file is
                                    not None) and overwrite_changed_blocks
        if overwrite_changed_blocks:
            self.overwrite_changed_blocks(blocks, output_file)
        else:
            return self.write_blocks_consecutively(blocks, output_file)

    def download_whole_vdi_using_nbd(self, vdi, path):
        """
        Downloads the VDI using the NBD server.
        """
        client = self._get_xapi_nbd_client(vdi=vdi)
        out = path.open('ab')
        # download 4MiB chunks
        chunk_size = 4 * 1024 * 1024
        for offset in range(0, client.size(), chunk_size):
            length = min(chunk_size, client.size() - offset)
            chunk = client.read(offset, length)
            print("Fetched chunk of length: {}".format(len(chunk)))
            out.seek(offset)
            out.write(chunk)
        out.close()
        client.close()
        return out.name

    def _cleanup_test_vdis(self):
        _wait_after_nbd_disconnect()
        for vdi in self._session.xenapi.VDI.get_by_name_label(
                self._TEMPORARY_TEST_VDI_NAME):
            print("Destroying VDI {}".format(vdi))
            try:
                try:
                    self._session.xenapi.VDI.destroy(vdi)
                except XenAPI.Failure as xenapi_error:
                    print("Failed to destroy VDI {}: {}. Trying to "
                          "unplug VBDs first".
                          format(vdi, xenapi_error))
                    for vbd in self._session.xenapi.VDI.get_VBDs(vdi):
                        print("Unplugging VBD {} of VDI {}".format(vbd, vdi))
                    # Wait for a bit for the VBD unplug operations to finish
                    time.sleep(4)
                    self._session.xenapi.VDI.destroy(vdi)
            except XenAPI.Failure as xenapi_error:
                print("Failed to destroy VDI {}: {}".
                      format(vdi, xenapi_error))


if __name__ == '__main__':
    import fire
    fire.Fire(CBTTests)
