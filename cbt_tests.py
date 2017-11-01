#!/usr/bin/env python3

import base64
import random
import subprocess
import tempfile
import time
from pathlib import Path
from xmlrpc.client import ServerProxy

from bitstring import BitArray

import XenAPI

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


def wait_after_nbd_disconnect():
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
        p = ServerProxy("http://" + self._pool_master_address)
        session = p.session.login_with_password(self._username,
                                                self._password)['Value']
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

    def _destroy_vdi_after_nbd_disconnect(self,
                                          vdi,
                                          destroy_op=None,
                                          wait_after_disconnect=False):
        if destroy_op is None:
            destroy_op = self._session.xenapi.VDI.destroy
            wait_after_disconnect = True

        if wait_after_disconnect:
            # Wait for a bit for the cleanup actions (unplugging and
            # destroying the VBD) to finish after terminating the NBD
            # session.
            # There is a race condition where we can get
            # XenAPI.Failure:
            #  ['VDI_IN_USE', 'OpaqueRef:<VDI ref>', 'destroy']
            # if we immediately call VDI.destroy after closing the NBD
            # session, because the VBD has not yet been cleaned up.
            time.sleep(7)

        destroy_op(vdi)

    def _read_from_vdi(self, vdi=None):
        if vdi is None:
            vdi = self.create_test_vdi()
            destroy_op = destroy_op or self._session.xenapi.VDI.destroy
        self._auto_enable_nbd()
        client = self._get_xapi_nbd_client(vdi=vdi)
        try:
            # This usually gives us some interesting text for the ISO VDIs :)
            # If we read from position 0 that's boring, we get all zeros
            print(c.read(512 * 200, 512))
        finally:
            client.close()

    def test_data_destroy(self, wait_after_disconnect=False):
        vdi = self.create_test_vdi()
        vbds = self._session.xenapi.VDI.get_VBDs(vdi)
        assert len(vbds) == 0
        self._session.xenapi.VDI.enable_cbt(vdi)
        snapshot = self._session.xenapi.VDI.snapshot(vdi)

        self._read_from_vdi(
            vdi=snapshot,
            destroy_op=self._session.xenapi.VDI.data_destroy,
            wait_after_disconnect=wait_after_disconnect)
        vbds = self._session.xenapi.VDI.get_VBDs(vdi)
        # a cbt_metadata VDI should have no VBDs
        assert len(vbds) == 0

    def repro_sm_bug(self):
        vdi = self.create_test_vdi()
        print(self._session.xenapi.VDI.get_uuid(vdi))
        # Without this line, if we do not enable CBT, it works:
        self._session.xenapi.VDI.enable_cbt(vdi)
        snapshot = self._session.xenapi.VDI.snapshot(vdi)
        print(self._session.xenapi.VDI.get_uuid(snapshot))

        self._auto_enable_nbd()
        c = self._get_xapi_nbd_client(vdi=snapshot)
        print(c.read(512 * 200, 512))
        # If we run the VDI.destroy here it will work:
        # self._session.xenapi.VDI.destroy(vdi)
        c.close()

        # It also works if we first wait for the unplug to finish, so probably
        # this is a race between VBD.unplug on the snapshot and VDI.destroy on
        # the snapshotted VDI:
        # time.sleep(2)

        self._session.xenapi.VDI.destroy(vdi)

    def _test_nbd_server_cleans_up_vbds(self,
                                        terminate_while_client_connected,
                                        terminate_command):
        self._disable_nbd_on_all_networks()

        vdi = self.create_test_vdi()
        xapi_nbd.auto_enable_nbd()
        vbds = self._session.xenapi.VDI.get_VBDs(vdi)
        assert len(vbds) == 0
        c = self._get_xapi_nbd_client(vdi=vdi)
        if not terminate_while_client_connected:
            c.close()
        vbds = self._session.xenapi.VDI.get_VBDs(vdi)
        assert len(vbds) == 1
        self.control_xapi_nbd_service(terminate_command)
        try:
            # wait for a while for the cleanup to finish
            time.sleep(8)
            vbds = self._session.xenapi.VDI.get_VBDs(vdi)
            assert len(vbds) == 0
        finally:
            self.control_xapi_nbd_service("start")

    def test_nbd_server_cleans_up_vbds(self):
        self._test_nbd_server_cleans_up_vbds(False, "stop")
        self._test_nbd_server_cleans_up_vbds(True, "restart")

    def loop_connect_disconnect(self,
                                vdi=None,
                                n=1030,
                                random_delays=False,
                                fail_connection=False):
        if vdi is None:
            vdi = self.create_test_vdi()

        if fail_connection:
            info = self._session.xenapi.VDI.get_nbd_info(vdi)[0]
            info["exportname"] = "invalid export name"

        for i in range(n):
            print("{}: connecting to {} on {}".format(i, vdi, self._host))
            if fail_connection:
                self._get_xapi_nbd_client(vdi_nbd_server_info=info)
            else:
                c = self._get_xapi_nbd_client(vdi=vdi)
            if random_delays:
                time.sleep(random.random())
            if not fail_connection:
                c.close()
            if random_delays:
                time.sleep(random.random())

    def parallel_nbd_connections(self, n):
        # Stash the NBD clients here to avoid them being garbage collected
        # and disconnected immediately after creation :S.
        open_nbd_connections = []
        vdi = self.create_test_vdi()
        self._auto_enable_nbd()
        info = self._session.xenapi.VDI.get_nbd_info(vdi)[0]
        try:
            for i in range(n):
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
        vdi = self.create_test_vdi()
        # test that if we disable NBD on all networks, we cannot connect
        self._disable_nbd_on_all_networks()
        infos = self._session.xenapi.VDI.get_nbd_info(vdi)
        print(infos)
        assert infos == []
        # test that if we enable NBD on all networks, we can connect
        self.get_xapi_nbd_client(vdi=vdi, auto_enable_nbd=True)
        # now enable all the networks one by one and check that we can
        # connect through them
        self._disable_nbd_on_all_networks()
        for network in self._session.xenapi.network.get_all():
            if xapi_nbd.has_vlan_pif(
                    self._session, network) and self._skip_vlan_networks:
                print("Skipping network {} because it has a"
                      " VLAN master PIF".format(network))
                continue
            self._disable_nbd_on_all_networks()
            self._enable_nbd_on_network(network=network)
            infos = self._session.xenapi.VDI.get_nbd_info(vdi)
            if infos == []:
                print("Skipping network {} because VDI {} is not reachable"
                      " through it".format(network, vdi))
                continue
            for vdi_nbd_server_info in infos:
                self.get_xapi_nbd_client(
                    vdi=vdi,
                    auto_enable_nbd=False,
                    vdi_nbd_server_info=vdi_nbd_server_info)

    def test_nbd_timeout(self, timeout):
        vdi = self.create_test_vdi()
        self._auto_enable_nbd()
        client = self._get_xapi_nbd_client(vdi=vdi)
        sleep = timeout * + 5
        print("waiting for {} seconds".format(sleep))
        time.sleep(sleep)
        print("timeout over")
        try:
            client.read(0, 512)
            success = True
        except Exception as e:
            print(e)
            success = False
        assert success is False

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
        # Verify that the service is running & properly working
        # This will fail if the service isn't running
        self._control_xapi_nbd_service(service_command="status")
        self.read_from_vdi()
        self._control_xapi_nbd_service(service_command="stop")
        try:
            self.read_from_vdi()
            running = True
        except ConnectionRefusedError:
            running = False
        assert running == socket_activated
        self._control_xapi_nbd_service(service_command="restart")
        self.read_from_vdi()

    def download_changed_blocks_in_bitmap_from_nbd(self, vdi, bitmap):

        bitmap = BitArray(base64.b64decode(bitmap))
        client = self._get_xapi_nbd_client(vdi=vdi)
        print("Size of network block device: %s" % client.size())
        for i in range(0, len(bitmap) - 1):
            if bitmap[i]:
                offset = i * self._BLOCK_SIZE
                print("Reading %d bytes from offset %d" % (self._BLOCK_SIZE,
                                                           offset))
                data = client.read(offset=offset, length=self._BLOCK_SIZE)
                yield (offset, data)
        client.close()

    def get_cbt_bitmap(self, vdi_from=None, vdi_to=None):
        if vdi_to is None:
            vdi_to = self.create_test_vdi()
            self._session.xenapi.VDI.enable_cbt(vdi_to)
        if vdi_from is None:
            vdi_from = self._session.xenapi.VDI.snapshot(vdi_to)

        vdi_from_uuid = self._session.xenapi.VDI.get_uuid(vdi_from)
        vdi_to_uuid = self._session.xenapi.VDI.get_uuid(vdi_to)
        print("self._session.xenapi.VDI.list_changed_blocks({}, {})".format(
            vdi_from_uuid, vdi_to_uuid))
        return self._session.xenapi.VDI.list_changed_blocks(vdi_from, vdi_to)

    def download_changed_blocks(self, vdi_from=None, vdi_to=None):
        if vdi_to is None:
            vdi_to = self.create_test_vdi()
            self._session.xenapi.VDI.enable_cbt(vdi_to)
        if vdi_from is None:
            vdi_from = self._session.xenapi.VDI.snapshot(vdi_to)

        bitmap = self.get_cbt_bitmap(vdi_from, vdi_to)
        return self.download_changed_blocks_in_bitmap_from_nbd(
            vdi=vdi_to, bitmap=bitmap)

    def write_blocks_consecutively(self, changed_blocks, output_file=None):
        if output_file is None:
            out = tempfile.NamedTemporaryFile('ab', delete=False)
        else:
            out = open(output_file, 'ab')
        for (o, b) in changed_blocks:
            out.write(b)
        out.close()
        return out.name

    def overwrite_changed_blocks(self, changed_blocks, output_file):
        with Path(output_file).open(mode='r+b') as out:
            for (offset, block) in changed_blocks:
                out.seek(offset)
                out.write(block)

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

    def download_whole_vdi_using_nbd(self, vdi, path=None):
        client = self._get_xapi_nbd_client(vdi=vdi)
        if path is None:
            out = tempfile.NamedTemporaryFile('ab', delete=False)
        else:
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
        c.close()
        return out.name

    def _cleanup_test_vdis(self):
        time.sleep(2)
        for vdi in self._session.xenapi.VDI.get_by_name_label(
                self._TEMPORARY_TEST_VDI_NAME):
            vdi_uuid = self._session.xenapi.VDI.get_uuid(vdi)
            for vbd in self._session.xenapi.VDI.get_VBDs(vdi):
                vbd_uuid = self._session.xenapi.VBD.get_uuid(vbd)
                print("Unplugging VBD {} of VDI {}".format(vbd_uuid, vdi_uuid))
            # Wait for a bit for the VBD unplug operations to finish
            time.sleep(4)
            print("Destroying VDI {}".format(vdi_uuid))
            try:
                self._session.xenapi.VDI.destroy(vdi)
            except XenAPI.Failure as xenapi_error:
                print("Failed to destroy VDI {}: {}".
                      format(vdi_uuid, xenapi_error))


if __name__ == '__main__':
    import fire
    fire.Fire(CBTTests)
