#!/usr/bin/python3

import cbt_test_lib
import os


class CBTTests(object):
    def __init__(self, pool_master, host=None, username=None, password=None):
        username = username or os.environ['XS_USERNAME']
        password = password or os.environ['XS_PASSWORD']
        self._pool_master = pool_master
        self._host = host or pool_master
        self._username = username
        self._password = password
        self._session = cbt_test_lib.create_session(
            pool_master=pool_master, username=username, password=password)

    def create_test_session(self):
        session = cbt_test_lib.create_test_session(
            pool_master=self._pool_master,
            username=self._username,
            password=self._password)
        print(session)

    def create_test_vdi(self, sr=None):
        vdi = cbt_test_lib.create_test_vdi(
            session=self._session, host=self._host, sr=sr)
        print(self._session.xenapi.VDI.get_uuid(vdi))

    def read_from_vdi(self, vdi=None):
        cbt_test_lib.read_from_vdi(
            session=self._session, host=self._host, vdi=vdi)

    def loop_connect_disconnect(self, vdi=None, n=1000):
        cbt_test_lib.loop_connect_disconnect(
            session=self._session, host=self._host, vdi=vdi, n=n)

    def parallel_nbd_connections(self, same_vdi=True, n=100):
        cbt_test_lib.parallel_nbd_connections(
            session=self._session, host=self._host, same_vdi=same_vdi, n=n)

    def verify_xapi_nbd_systemd_service(self, socket_activated=False):
        cbt_test_lib.verify_xapi_nbd_systemd_service(
            session=self._session, host=self._host, socket_activated=socket_activated)

    def get_cbt_bitmap(self, vdi_from_uuid=None, vdi_to_uuid=None):
        if vdi_from_uuid is not None:
            vdi_from = self._session.xenapi.VDI.get_by_uuid(vdi_from_uuid)
        else:
            vdi_from = None
        if vdi_to_uuid is not None:
            vdi_to = self._session.xenapi.VDI.get_by_uuid(vdi_from_uuid)
        else:
            vdi_to = None
        print(cbt_test_lib.get_cbt_bitmap(
            session=self._session, vdi_from=vdi_from, vdi_to=vdi_to))

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
        return cbt_test_lib.save_changed_blocks(
            session=self._session,
            vdi_from=vdi_from,
            vdi_to=vdi_to,
            output_file=output_file)


if __name__ == '__main__':
    import fire
    fire.Fire(CBTTests)
