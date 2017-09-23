#!/usr/bin/python3

import os

program_name = "cbt_tests.py"


def get_first_safely(iterable):
    """Gets the 'first' element of an iterable, if any, or None"""
    return next(iter(iterable), None)


class CBTTests(object):
    # 64K blocks
    BLOCK_SIZE = 64 * 1024

    def __init__(self,
                 pool_master,
                 username,
                 password,
                 host=None,
                 use_tls=True):
        self._pool_master = pool_master
        self._host = host or pool_master
        self._username = username
        self._password = password
        self._use_tls = use_tls
        self._session = self.create_session()

    def __del__(self):
        self._session.xenapi.session.logout()

    # Create a session that won't be garbage-collected and maybe even logged
    # out after we printed the session ref for the user
    def create_test_session(self):
        from xmlrpc.client import ServerProxy
        p = ServerProxy("http://" + self._pool_master)
        session = p.session.login_with_password(self._username,
                                                self._password)['Value']
        return session

    def get_certfile(self):
        return self._session.xenapi.host.get_server_certificate(self._host)

    def create_session(self):
        import XenAPI
        session = XenAPI.Session("http://" + self._pool_master)
        session.xenapi.login_with_password(self._username, self._password,
                                           "1.0", program_name)
        return session

    def create_test_vdi(self, sr=None):
        print("Creating a VDI")
        if sr is None:
            # Get an SR that is only attached to this host (not shared), for
            # testing local SRs
            hostname = (self._host).partition('.')[0]
            [host_ref] = self._session.xenapi.host.get_by_name_label(hostname)
            pbds = self._session.xenapi.host.get_PBDs(host_ref)
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
            "SR": sr,
            "virtual_size": 40000000,
            "type": "user",
            "sharable": False,
            "read_only": False,
            "other_config": {},
            "name_label": "test"
        }
        vdi = self._session.xenapi.VDI.create(new_vdi_record)
        return vdi

    def get_xapi_nbd_client(self, vdi):
        from xapi_nbd_client import xapi_nbd_client
        return xapi_nbd_client(
            vdi=vdi, session=self._session, use_tls=self._use_tls)

    def _read_from_vdi(self, vdi=None, destroy_op=None, wait_after_disconnect=True):
        import time

        if vdi is None:
            print("Creating a VDI")
            vdi = self.create_test_vdi()
            destroy_op = destroy_op or self._session.xenapi.VDI.destroy

        c = self.get_xapi_nbd_client(vdi=vdi)

        # This usually gives us some interesting text for the ISO VDIs :)
        # If we read from position 0 that's boring, we get all zeros
        print(c.read(512 * 200, 512))

        c.close()

        if destroy_op is not None:
            if wait_after_disconnect:
                # Wait for a bit for the cleanup actions (unplugging and destroying
                # the VBD) to finish after terminating the NBD session.
                # There is a race condition where we can get
                # XenAPI.Failure: ['VDI_IN_USE', 'OpaqueRef:<VDI ref>', 'destroy']
                # if we immediately call VDI.destroy after closing the NBD session,
                # because the VBD has not yet been cleaned up.
                time.sleep(2)

            destroy_op(vdi)

    def read_from_vdi(self, vdi=None, wait_after_disconnect=True):
        destroy_op = self._session.VDI.
        self.read_from_vdi(vdi=vdi, wait_after_disconnect=wait_after_disconnect)

    def test_data_destroy(self, wait_after_disconnect=False):
        vdi = self.create_test_vdi()
        try:
            self._session.xenapi.VDI.enable_cbt(vdi)
            snapshot = self._session.xenapi.VDI.snapshot(vdi)
            
            try:
                self.read_from_vdi(vdi=snapshot, destroy_op=self._session.xenapi.VDI.data_destroy, wait_after_disconnect=wait_after_disconnect)
            finally:
                # destroy the cbt_metadata VDI
                self._session.xenapi.VDI.destroy(snapshot)
        finally:
            self._session.xenapi.VDI.destroy(vdi)

    def loop_connect_disconnect(self, vdi=None, n=1000, random_delays=False):
        import time
        import random

        if vdi is None:
            vdi = self.create_test_vdi()
            delete_vdi = True

        try:
            for i in range(n):
                print("{}: connecting to {} on {}".format(
                    i, vdi, self._host))
                c = self.get_xapi_nbd_client(vdi=vdi)
                if random_delays:
                    time.sleep(random.random())
                c.close()
                if random_delays:
                    time.sleep(random.random())
        finally:
            if delete_vdi:
                self._session.xenapi.VDI.destroy(vdi)

    def parallel_nbd_connections(self, same_vdi=True, n=100):
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
                print("{}: connecting to {} on {}".format(
                    i, vdi, self._host))
                open_nbd_connections += [
                    self.get_xapi_nbd_client(vdi=vdi)
                ]
        finally:
            for c in open_nbd_connections:
                c.close()
            print("Destroying {} VDIs".format(len(vdis_created)))
            for vdi in vdis_created:
                print("Destroying VDI {}".format(vdi))
                self._session.xenapi.VDI.destroy(vdi)
                print("VDI destroyed")

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
                yield data
        c.close()

    def get_cbt_bitmap(self, vdi_from=None, vdi_to=None):
        if vdi_to is None:
            vdi_to = self.create_test_vdi()
            self._session.xenapi.VDI.enable_cbt(vdi_to)
        if vdi_from is None:
            vdi_from = self._session.xenapi.VDI.snapshot(vdi_to)

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
            out = tempfile.NamedTemporaryFile('ab')
        else:
            out = open(output_file, 'ab')
        for b in changed_blocks:
            out.write(b)
        out.close()
        return out.name

    def save_changed_blocks(self, vdi_from=None, vdi_to=None,
                            output_file=None):
        blocks = self.download_changed_blocks(vdi_from=vdi_from, vdi_to=vdi_to)
        return self.write_blocks_consecutively(blocks, output_file)


class CBTTestsCLI(object):
    def __init__(self,
                 pool_master,
                 host=None,
                 username=None,
                 password=None,
                 use_tls=True):
        username = username or os.environ['XS_USERNAME']
        password = password or os.environ['XS_PASSWORD']
        self._pool_master = pool_master
        self._host = host or pool_master
        self._username = username
        self._password = password
        self._cbt_tests = CBTTests(
            pool_master=pool_master,
            username=username,
            password=password,
            host=host,
            use_tls=use_tls)
        self._session = self._cbt_tests._session

    def create_test_session(self):
        session = self._cbt_tests.create_test_session()
        print(session)

    def create_test_vdi(self, sr=None):
        vdi = self._cbt_tests.create_test_vdi(sr=sr)
        print(self._session.xenapi.VDI.get_uuid(vdi))

    def read_from_vdi(self, vdi=None, wait_after_disconnect=True):
        self._cbt_tests.read_from_vdi(vdi=vdi, wait_after_disconnect=wait_after_disconnect)

    def test_data_destroy(self, wait_after_disconnect=False):
        self._cbt_tests.test_data_destroy(wait_after_disconnect=wait_after_disconnect)

    def loop_connect_disconnect(self, vdi=None, n=1000, random_delays=False):
        self._cbt_tests.loop_connect_disconnect(vdi=vdi, n=n, random_delays=random_delays)

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
                            output_file=None):
        if vdi_from_uuid is not None:
            vdi_from = self._session.xenapi.VDI.get_by_uuid(vdi_from_uuid)
        else:
            vdi_from = None
        if vdi_to_uuid is not None:
            vdi_to = self._session.xenapi.VDI.get_by_uuid(vdi_from_uuid)
        else:
            vdi_to = None
        return self._cbt_tests.save_changed_blocks(
            vdi_from=vdi_from, vdi_to=vdi_to, output_file=output_file)

    def get_certfile(self):
        print(self._cbt_tests.get_certfile())


if __name__ == '__main__':
    import fire
    fire.Fire(CBTTestsCLI)
