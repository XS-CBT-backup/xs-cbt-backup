import XenAPI
from xapi_nbd_client import xapi_nbd_client
from create_test_vdi import create_test_vdi


def loop_connect_disconnect(host, vdi=None, n=1000):
    session = XenAPI.Session("http://" + host)
    session.xenapi.login_with_password("<username>", "<password>")

    if vdi is None:
        print("Creating a VDI")
        vdi_ref = create_test_vdi(host=host, session=session)
        vdi = session.xenapi.VDI.get_uuid(vdi_ref)
        delete_vdi = True

    try:
        try:
            for i in range(n):
                print("{}: connecting to {} on {}".format(i, vdi, host))
                xapi_nbd_client(host, vdi, session=session)
        finally:
            if delete_vdi:
                session.xenapi.VDI.destroy(vdi_ref)
    finally:
        session.xenapi.session.logout()

if __name__ == '__main__':
    import fire
    fire.Fire(loop_connect_disconnect)
