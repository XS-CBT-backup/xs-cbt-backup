#!/usr/bin/python3

import os

program_name = "cbt_tests.py"


def has_vlan_pif(session, network):
    pifs = session.xenapi.network.get_PIFs(network)
    vlan = False
    print("Checking PIFs of network {} to see if it is a VLAN master".format(
        network))
    for pif in pifs:
        print("Checking PIF {} of network {} to see if it is a VLAN master".
              format(pif, network))
        vlan_master_of = session.xenapi.PIF.get_VLAN_master_of(pif)
        print(vlan_master_of)
        if vlan_master_of != "OpaqueRef:NULL":
            print("Network {} has a VLAN PIF {}".format(network, pif))
            vlan = True
    return vlan


def enable_nbd_if_necessary(session, use_tls=True, skip_vlan_networks=True):
    import time
    has_nbd_network = False
    nbd_purpose = "nbd" if use_tls else "insecure_nbd"
    conflicting_nbd_purpose = "insecure_nbd" if use_tls else "nbd"

    for network in session.xenapi.network.get_all():
        purpose = session.xenapi.network.get_purpose(network)
        if nbd_purpose in purpose:
            print("Found network on which NBD ({}) is allowed: {}".format(
                purpose, network))
            has_nbd_network = True
        if conflicting_nbd_purpose in purpose:
            print("WARNING: Found conflicting NBD purpose {} ({}) on"
                  " network {}, removing it!!!!!!!!".format(
                      conflicting_nbd_purpose, purpose, network))
            session.xenapi.network.remove_purpose(network,
                                                  conflicting_nbd_purpose)
    if not has_nbd_network:
        print("WARNING: Found no network on which NBD is allowed, enabling "
              "secure NBD on ALL NETWORKS!!!!!!!")
        for network in session.xenapi.network.get_all():
            if has_vlan_pif(session, network) and skip_vlan_networks:
                print("Skipping network {} because it has a VLAN master PIF".
                      format(network))
                continue
            print("Enabling secure NBD on network {}".format(network))
            session.xenapi.network.add_purpose(network, nbd_purpose)
        # wait for a bit for the changes to take effect
        # We do rate limiting with a 5s delay, so sometimes the update
        # takes at least 5 seconds
        time.sleep(7)


def get_first_safely(iterable):
    """Gets the 'first' element of an iterable, if any, or None"""
    return next(iter(iterable), None)


class CBTTests(object):
    # 64K blocks
    BLOCK_SIZE = 64 * 1024

    TEMPORARY_TEST_VDI_NAME = "TMP_test_" + program_name
    TEST_VDI_NAME = "test"

    def __init__(self,
                 pool_master_address,
                 username,
                 password,
                 hostname=None,
                 use_tls=True,
                 skip_vlan_networks=True):
        self._pool_master_address = pool_master_address
        self._username = username
        self._password = password
        self._session = self.create_session()
        if hostname is not None:
            [self._host
             ] = self._session.xenapi.host.get_by_name_label(hostname)
        else:
            self._host = self._session.xenapi.session.get_this_host(
                self._session._session) or pool_master_address
        self._use_tls = use_tls
        self._skip_vlan_networks = skip_vlan_networks

    def __del__(self):
        self.cleanup_test_vdis()
        self._session.xenapi.session.logout()

    # Create a session that won't be garbage-collected and maybe even logged
    # out after we printed the session ref for the user
    def create_test_session(self):
        from xmlrpc.client import ServerProxy
        p = ServerProxy("http://" + self._pool_master_address)
        session = p.session.login_with_password(self._username,
                                                self._password)['Value']
        return session

    def get_certfile(self):
        return self._session.xenapi.host.get_server_certificate(self._host)

    def create_session(self):
        import XenAPI
        session = XenAPI.Session("http://" + self._pool_master_address)
        session.xenapi.login_with_password(self._username, self._password,
                                           "1.0", program_name)
        return session

    def create_test_vdi(self, sr=None, keep_after_exit=False):
        print("Creating a VDI")
        if sr is None:
            # Get an SR that is only attached to this host (not shared), for
            # testing local SRs
            pbds = self._session.xenapi.host.get_PBDs(self._host)
            srs = [
                self._session.xenapi.PBD.get_SR(pbd) for pbd in pbds
                if self._session.xenapi.PBD.get_currently_attached(pbd) is True
            ]
            user_srs = [
                sr for sr in srs
                if sr is not None
                and self._session.xenapi.SR.get_content_type(sr) == "user"
                and self._session.xenapi.SR.get_shared(sr) is False
            ]
            sr = get_first_safely(user_srs)

        new_vdi_record = {
            "SR":
            sr,
            "virtual_size":
            40000000,
            "type":
            "user",
            "sharable":
            False,
            "read_only":
            False,
            "other_config": {},
            "name_label": (self.TEST_VDI_NAME if keep_after_exit else
                           self.TEMPORARY_TEST_VDI_NAME)
        }
        vdi = self._session.xenapi.VDI.create(new_vdi_record)
        return vdi

    def get_xapi_nbd_client(self, vdi, auto_enable_nbd=True):
        from xapi_nbd_client import xapi_nbd_client
        return xapi_nbd_client(
            vdi=vdi,
            session=self._session,
            use_tls=self._use_tls,
            auto_enable_nbd=auto_enable_nbd,
            skip_vlan_networks=self._skip_vlan_networks)

    def _destroy_vdi_after_nbd_disconnect(self,
                                          vdi,
                                          destroy_op=None,
                                          wait_after_disconnect=False):
        import time

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
            time.sleep(5)

        destroy_op(vdi)

    def _read_from_vdi(self,
                       vdi=None,
                       destroy_op=None,
                       wait_after_disconnect=True,
                       auto_enable_nbd=True):
        if vdi is None:
            print("Creating a VDI")
            vdi = self.create_test_vdi()
            destroy_op = destroy_op or self._session.xenapi.VDI.destroy

        c = self.get_xapi_nbd_client(vdi=vdi, auto_enable_nbd=auto_enable_nbd)

        # This usually gives us some interesting text for the ISO VDIs :)
        # If we read from position 0 that's boring, we get all zeros
        print(c.read(512 * 200, 512))

        c.close()

        if destroy_op is not None:
            self._destroy_vdi_after_nbd_disconnect(
                vdi=vdi,
                destroy_op=destroy_op,
                wait_after_disconnect=wait_after_disconnect)

    def read_from_vdi(self,
                      vdi=None,
                      wait_after_disconnect=True,
                      auto_enable_nbd=True):
        self._read_from_vdi(
            vdi=vdi,
            wait_after_disconnect=wait_after_disconnect,
            auto_enable_nbd=auto_enable_nbd)

    def test_data_destroy(self, wait_after_disconnect=False):
        import time

        vdi = self.create_test_vdi()
        vbds = self._session.xenapi.VDI.get_VBDs(vdi)
        assert (len(vbds) == 0)
        try:
            self._session.xenapi.VDI.enable_cbt(vdi)
            snapshot = self._session.xenapi.VDI.snapshot(vdi)

            try:
                self._read_from_vdi(
                    vdi=snapshot,
                    destroy_op=self._session.xenapi.VDI.data_destroy,
                    wait_after_disconnect=wait_after_disconnect)
                vbds = self._session.xenapi.VDI.get_VBDs(vdi)
                # a cbt_metadata VDI should have no VBDs
                assert (len(vbds) == 0)
            finally:
                self._destroy_vdi_after_nbd_disconnect(vdi=snapshot)
        finally:
            # First wait for the unplug to finish, because there is a race
            # between VBD.unplug on the snapshot and VDI.destroy on the
            # snapshotted VDI:
            time.sleep(2)
            self._session.xenapi.VDI.destroy(vdi)

    def repro_sm_bug(self):
        # import time

        vdi = self.create_test_vdi()
        print(self._session.xenapi.VDI.get_uuid(vdi))
        # Without this line, if we do not enable CBT, it works:
        self._session.xenapi.VDI.enable_cbt(vdi)
        snapshot = self._session.xenapi.VDI.snapshot(vdi)
        print(self._session.xenapi.VDI.get_uuid(snapshot))

        c = self.get_xapi_nbd_client(vdi=snapshot)
        print(c.read(512 * 200, 512))
        # If we run the VDI.destroy here it will work:
        # self._session.xenapi.VDI.destroy(vdi)
        c.close()

        # It also works if we first wait for the unplug to finish, so probably
        # this is a race between VBD.unplug on the snapshot and VDI.destroy on
        # the snapshotted VDI:
        # time.sleep(2)

        self._session.xenapi.VDI.destroy(vdi)

    def _test_nbd_server_cleans_up_vbds(self, terminate_while_client_connected,
                                        terminate_command):
        import time

        vdi = self.create_test_vdi()
        try:
            vbds = self._session.xenapi.VDI.get_VBDs(vdi)
            assert (len(vbds) == 0)
            c = self.get_xapi_nbd_client(vdi=vdi)
            if not terminate_while_client_connected:
                c.close()
            vbds = self._session.xenapi.VDI.get_VBDs(vdi)
            assert (len(vbds) == 1)
            self.control_xapi_nbd_service(terminate_command)
            try:
                # wait for a while for the cleanup to finish
                time.sleep(8)
                vbds = self._session.xenapi.VDI.get_VBDs(vdi)
                assert (len(vbds) == 0)
            finally:
                self.control_xapi_nbd_service("start")
        finally:
            self._destroy_vdi_after_nbd_disconnect(vdi=vdi)

    def test_nbd_server_cleans_up_vbds(self):
        self._test_nbd_server_cleans_up_vbds(False, "stop")
        self._test_nbd_server_cleans_up_vbds(True, "restart")

    def loop_connect_disconnect(self, vdi=None, n=1030, random_delays=False):
        import time
        import random

        if vdi is None:
            vdi = self.create_test_vdi()
            delete_vdi = True

        try:
            for i in range(n):
                print("{}: connecting to {} on {}".format(i, vdi, self._host))
                c = self.get_xapi_nbd_client(vdi=vdi)
                if random_delays:
                    time.sleep(random.random())
                c.close()
                if random_delays:
                    time.sleep(random.random())
        finally:
            if delete_vdi:
                self._destroy_vdi_after_nbd_disconnect(vdi)

    def parallel_nbd_connections(self, same_vdi=True, n=100):
        import time

        # Stash the NBD clients here to avoid them being garbage collected
        # and disconnected immediately after creation :S.
        open_nbd_connections = []
        vdis_created = []

        if same_vdi:
            vdi = self.create_test_vdi()
            vdis_created += [vdi]

        try:
            for i in range(n):
                if not same_vdi:
                    vdi = self.create_test_vdi()
                    vdis_created += [vdi]
                print("{}: connecting to {} on {}".format(i, vdi, self._host))
                open_nbd_connections += [self.get_xapi_nbd_client(vdi=vdi)]
        finally:
            time.sleep(2)
            for c in open_nbd_connections:
                c.close()
            print("Destroying {} VDIs".format(len(vdis_created)))
            for vdi in vdis_created:
                print("Destroying VDI {}".format(vdi))
                self._session.xenapi.VDI.destroy(vdi)
                print("VDI destroyed")

    def _enable_nbd_on_network(self, network):
        import time
        print("Enabling secure NBD on network {}".format(network))
        nbd_purpose = "nbd" if self._use_tls else "insecure_nbd"
        self._session.xenapi.network.add_purpose(network, nbd_purpose)
        # wait for a bit for the changes to take effect
        # We do rate limiting with a 5s delay, so sometimes the update
        # takes at least 5 seconds
        time.sleep(7)

    def _disable_nbd_on_all_networks(self):
        import time
        for network in self._session.xenapi.network.get_all():
            self._session.xenapi.network.remove_purpose(network, "nbd")
            self._session.xenapi.network.remove_purpose(
                network, "insecure_nbd")
        # wait for a bit for the changes to take effect
        # We do rate limiting with a 5s delay, so sometimes the update
        # takes at least 5 seconds
        time.sleep(7)

    def test_nbd_network_config(self):
        import time
        vdi = self.create_test_vdi()
        try:
            # test that if we disable NBD on all networks, we cannot connect
            self._disable_nbd_on_all_networks()
            infos = self._session.xenapi.VDI.get_nbd_info(vdi)
            print(infos)
            assert (infos == [])
            # test that if we enable NBD on all networks, we can connect
            self.get_xapi_nbd_client(vdi=vdi, auto_enable_nbd=True)
            # now enable all the networks one by one and check that we can
            # connect through them
            self._disable_nbd_on_all_networks()
            for network in self._session.xenapi.network.get_all():
                if has_vlan_pif(self._session,
                                network) and self._skip_vlan_networks:
                    print("Skipping network {} because it has a"
                          " VLAN master PIF".format(network))
                    continue
                self._disable_nbd_on_all_networks()
                self._enable_nbd_on_network(network=network)
                infos = self._session.xenapi.VDI.get_nbd_info(vdi)
                if (infos == []):
                    print("Skipping network {} because VDI {} is not reachable"
                          " through it".format(network, vdi))
                    continue
                self.get_xapi_nbd_client(vdi=vdi, auto_enable_nbd=False)
            # wait for a bit for the changes to take effect
            # We do rate limiting with a 5s delay, so sometimes the update
            # takes at least 5 seconds
            time.sleep(7)
        finally:
            self._destroy_vdi_after_nbd_disconnect(vdi=vdi)

    def run_ssh_command(self, command):
        import subprocess
        return (subprocess.check_output([
            "sshpass", "-p", "xenroot", "ssh", "-o",
            "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no",
            "-l", "root", self._host
        ] + command))

    def control_xapi_nbd_service(self, service_command):
        self.run_ssh_command(["service", "xapi-nbd", service_command])

    def verify_xapi_nbd_systemd_service(self, socket_activated=False):
        # Verify that the service is running & properly working
        # This will fail if the service isn't running
        self.control_xapi_nbd_service(service_command="status")
        self.read_from_vdi()
        self.control_xapi_nbd_service(service_command="stop")
        try:
            self.read_from_vdi()
            running = True
        except ConnectionRefusedError:
            running = False
        assert (running == socket_activated)
        self.control_xapi_nbd_service(service_command="restart")
        self.read_from_vdi()

    def download_changed_blocks_in_bitmap_from_nbd(self, vdi, bitmap):
        import base64

        bitmap = base64.b64decode(bitmap)
        c = self.get_xapi_nbd_client(vdi=vdi)
        print("Size of network block device: %s" % c.size())
        for i in range(0, len(bitmap) - 1):
            if bitmap[i] == 1:
                offset = i * self.BLOCK_SIZE
                print("Reading %d bytes from offset %d" % (self.BLOCK_SIZE,
                                                           offset))
                data = c.read(offset=offset, length=self.BLOCK_SIZE)
                yield (offset, data)
        c.close()

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
        import tempfile
        if output_file is None:
            out = tempfile.NamedTemporaryFile('ab', delete=False)
        else:
            out = open(output_file, 'ab')
        for (o, b) in changed_blocks:
            out.write(b)
        out.close()
        return out.name

    def overwrite_changed_blocks(self, changed_blocks, output_file):
        from pathlib import Path
        with Path(output_file).open(mode='wb') as out:
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
        import tempfile

        c = self.get_xapi_nbd_client(vdi=vdi)
        if path is None:
            out = tempfile.NamedTemporaryFile('ab', delete=False)
        else:
            out = path.open('ab')
        # download 4MiB chunks
        chunk_size = 4 * 1024 * 1024
        for offset in range(0, c.size(), chunk_size):
            length = min(chunk_size, c.size() - offset)
            chunk = c.read(offset, length)
            print("Fetched chunk of length: {}".format(len(chunk)))
            out.seek(offset)
            out.write(chunk)
        out.close()
        c.close()
        return out.name

    def cleanup_test_vdis(self):
        import time
        time.sleep(2)
        for vdi in self._session.xenapi.VDI.get_by_name_label(
                self.TEMPORARY_TEST_VDI_NAME):
            vdi_uuid = self._session.xenapi.VDI.get_uuid(vdi)
            for vbd in self._session.xenapi.VDI.get_VBDs(vdi):
                vbd_uuid = self._session.xenapi.VBD.get_uuid(vbd)
                print("Unplugging VBD {} of VDI {}".format(vbd_uuid, vdi_uuid))
            print("Destroying VDI {}".format(vdi_uuid))
            try:
                self._session.xenapi.VDI.destroy(vdi)
            except:
                print("Failed to destroy VDI {}".format(vdi_uuid))


class CBTTestsCLI(object):
    def __init__(self,
                 pool_master,
                 hostname=None,
                 username=None,
                 password=None,
                 use_tls=True,
                 skip_vlan_networks=True):
        username = username or os.environ['XS_USERNAME']
        password = password or os.environ['XS_PASSWORD']
        self._cbt_tests = CBTTests(
            pool_master_address=pool_master,
            username=username,
            password=password,
            hostname=hostname,
            use_tls=use_tls,
            skip_vlan_networks=skip_vlan_networks)
        self._session = self._cbt_tests._session

    def create_test_session(self):
        session = self._cbt_tests.create_test_session()
        print(session)

    def create_test_vdi(self, sr=None):
        vdi = self._cbt_tests.create_test_vdi(sr=sr, keep_after_exit=True)
        print(vdi)
        print(self._session.xenapi.VDI.get_uuid(vdi))

    def read_from_vdi(self,
                      vdi=None,
                      wait_after_disconnect=True,
                      auto_enable_nbd=True):
        self._cbt_tests.read_from_vdi(
            vdi=vdi,
            wait_after_disconnect=wait_after_disconnect,
            auto_enable_nbd=auto_enable_nbd)

    def test_data_destroy(self, wait_after_disconnect=False):
        self._cbt_tests.test_data_destroy(
            wait_after_disconnect=wait_after_disconnect)

    def test_nbd_server_cleans_up_vbds(self):
        self._cbt_tests.test_nbd_server_cleans_up_vbds()

    def test_nbd_network_config(self):
        self._cbt_tests.test_nbd_network_config()

    def loop_connect_disconnect(self, vdi=None, n=1030, random_delays=False):
        self._cbt_tests.loop_connect_disconnect(
            vdi=vdi, n=n, random_delays=random_delays)

    def parallel_nbd_connections(self, same_vdi=True, n=100):
        self._cbt_tests.parallel_nbd_connections(same_vdi=same_vdi, n=n)

    def verify_xapi_nbd_systemd_service(self, socket_activated=False):
        self._cbt_tests.verify_xapi_nbd_systemd_service(
            socket_activated=socket_activated)

    def get_cbt_bitmap(self, vdi_from_uuid=None, vdi_to_uuid=None):
        if vdi_from_uuid is not None:
            vdi_from = self._session.xenapi.VDI.get_by_uuid(vdi_from_uuid)
        else:
            vdi_from = None
        if vdi_to_uuid is not None:
            vdi_to = self._session.xenapi.VDI.get_by_uuid(vdi_from_uuid)
        else:
            vdi_to = None
        print(self._cbt_tests.get_cbt_bitmap(vdi_from=vdi_from, vdi_to=vdi_to))

    def save_changed_blocks(self,
                            vdi_from_uuid=None,
                            vdi_to_uuid=None,
                            output_file=None,
                            overwrite_changed_blocks=True):
        if vdi_from_uuid is not None:
            vdi_from = self._session.xenapi.VDI.get_by_uuid(vdi_from_uuid)
        else:
            vdi_from = None
        if vdi_to_uuid is not None:
            vdi_to = self._session.xenapi.VDI.get_by_uuid(vdi_from_uuid)
        else:
            vdi_to = None
        return self._cbt_tests.save_changed_blocks(
            vdi_from=vdi_from,
            vdi_to=vdi_to,
            output_file=output_file,
            overwrite_changed_blocks=overwrite_changed_blocks)

    def download_whole_vdi_using_nbd(self, vdi, filename=None):
        from pathlib import Path
        vdi = self._session.xenapi.VDI.get_by_uuid(vdi)
        return self._cbt_tests.download_whole_vdi_using_nbd(
            vdi=vdi, path=Path(filename))

    def get_certfile(self):
        print(self._cbt_tests.get_certfile())

    def repro_sm_bug(self):
        self._cbt_tests.repro_sm_bug()

    def cleanup_test_vdis(self):
        self._cbt_tests.cleanup_test_vdis()


if __name__ == '__main__':
    import fire
    fire.Fire(CBTTestsCLI)
