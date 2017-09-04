
def get_first_safely(iterable):
    """Gets the 'first' element of an iterable, if any, or None"""
    return next(iter(iterable), None)


def create_test_vdi(pool_master=None, sr=None, host=None, session=None):
    import XenAPI
    if session is None:
        session = XenAPI.Session("http://" + pool_master)
        session.xenapi.login_with_password("<username>", "<password>")

    if sr is None:
        hostname = (host or pool_master).partition('.')[0]
        [host] = session.xenapi.host.get_by_name_label(hostname)
        pbds = session.xenapi.host.get_PBDs(host)
        srs = [session.xenapi.PBD.get_SR(pbd) for pbd in pbds
               if session.xenapi.PBD.get_currently_attached(pbd) is True]
        user_srs = [sr
                    for sr in srs
                    if sr is not None and
                    session.xenapi.SR.get_content_type(sr) == "user" and
                    session.xenapi.SR.get_shared(sr) is False]
        sr = get_first_safely(user_srs)

    new_vdi_record = {
        "SR": sr,
        "virtual_size": 40000000,
        "type": "user",
        "sharable": True,
        "read_only": False,
        "other_config": {},
        "name_label": "test"
    }
    vdi = session.xenapi.VDI.create(new_vdi_record)
    return vdi

if __name__ == '__main__':
    import fire
    fire.Fire(create_test_vdi)
