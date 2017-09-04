

def read_from_vdi(pool_master, host=None, vdi=None):
    from xapi_nbd_client import xapi_nbd_client
    from create_test_vdi import create_test_vdi
    import XenAPI

    session = XenAPI.Session("http://" + pool_master)
    session.xenapi.login_with_password("<username>", "<password>")

    if vdi is None:
        print("Creating a VDI")
        vdi_ref = create_test_vdi(pool_master=pool_master,
                                  host=host, session=session)
        vdi = session.xenapi.VDI.get_uuid(vdi_ref)
        delete_vdi = True
    else:
        delete_vdi = False

    c = xapi_nbd_client(host or pool_master, vdi, session=session)

    # This usually gives us some interesting text for the ISO VDIs :)
    # If we read from position 0 that's boring, we get all zeros
    print(c.read(512 * 200, 512))

    c.close()

    if delete_vdi:
        session.xenapi.VDI.destroy(vdi_ref)

if __name__ == '__main__':
    import fire
    fire.Fire(read_from_vdi)
