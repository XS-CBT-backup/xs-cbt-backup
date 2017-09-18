import XenAPI

program_name = "cbt_tests.py"


def get_first_safely(iterable):
    """Gets the 'first' element of an iterable, if any, or None"""
    return next(iter(iterable), None)


# Create a session that won't be garbage-collected and maybe even logged
# out after we printed the session ref for the user
def create_test_session(pool_master, username, password):
    from xmlrpc.client import ServerProxy
    p = ServerProxy("http://" + pool_master)
    session = p.session.login_with_password(username, password)['Value']
    return session


def create_session(pool_master, username, password):
    session = XenAPI.Session("http://" + pool_master)
    session.xenapi.login_with_password(username, password, "1.0", program_name)
    return session


def create_test_vdi(session, host, sr=None):
    if sr is None:
        # Get an SR that is only attached to this host (not shared), for
        # testing local SRs
        hostname = (host).partition('.')[0]
        [host] = session.xenapi.host.get_by_name_label(hostname)
        pbds = session.xenapi.host.get_PBDs(host)
        srs = [
            session.xenapi.PBD.get_SR(pbd) for pbd in pbds
            if session.xenapi.PBD.get_currently_attached(pbd) is True
        ]
        user_srs = [
            sr for sr in srs
            if sr is not None and session.xenapi.SR.get_content_type(sr) ==
            "user" and session.xenapi.SR.get_shared(sr) is False
        ]
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


def read_from_vdi(session, host, vdi=None):
    from xapi_nbd_client import xapi_nbd_client
    import time

    if vdi is None:
        print("Creating a VDI")
        vdi_ref = create_test_vdi(session=session, host=host)
        vdi = session.xenapi.VDI.get_uuid(vdi_ref)
        delete_vdi = True
    else:
        delete_vdi = False

    c = xapi_nbd_client(hostname=host, vdi=vdi, session=session)

    # This usually gives us some interesting text for the ISO VDIs :)
    # If we read from position 0 that's boring, we get all zeros
    print(c.read(512 * 200, 512))

    c.close()

    if delete_vdi:
        # Wait for a bit for the cleanup actions (unplugging and destroying the
        # VBD) to finish after terminating the NBD session.
        # There is a race condition where we can get
        # XenAPI.Failure: ['VDI_IN_USE', 'OpaqueRef:<VDI ref>', 'destroy']
        # if we immediately call VDI.destroy after closing the NBD session,
        # because the VBD has not yet been cleaned up.
        time.sleep(2)

        session.xenapi.VDI.destroy(vdi_ref)


def loop_connect_disconnect(session, host, vdi=None, n=1000):
    from xapi_nbd_client import xapi_nbd_client
    if vdi is None:
        print("Creating a VDI")
        vdi_ref = create_test_vdi(session=session, host=host)
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


def run_ssh_command(host, command):
    import subprocess
    subprocess.check_output([
        "sshpass", "-p", "xenroot", "ssh", "-o",
        "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", "-l",
        "root", host
    ] + command)


def control_xapi_nbd_service(host, service_command):
    run_ssh_command(host, ["service", "xapi-nbd", service_command])


def verify_xapi_nbd_systemd_service(session, host):
    # Verify that the service is running & properly working
    # This will fail if the service isn't running
    control_xapi_nbd_service(host=host, service_command="status")
    read_from_vdi(session=session, host=host)
    control_xapi_nbd_service(host=host, service_command="stop")
    try:
        read_from_vdi(session=session, host=host)
        running = True
    except ConnectionRefusedError:
        running = False
    assert (running is False)
    control_xapi_nbd_service(host=host, service_command="restart")
    read_from_vdi(session=session, host=host)
