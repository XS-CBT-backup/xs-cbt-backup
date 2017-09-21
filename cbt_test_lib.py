import XenAPI

program_name = "cbt_tests.py"

# 64K blocks
BLOCK_SIZE = 64 * 1024


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


def create_test_vdi(session, host=None, sr=None):
    print("Creating a VDI")
    if sr is None:
        # Get an SR that is only attached to this host (not shared), for
        # testing local SRs
        if host is not None:
            hostname = (host).partition('.')[0]
            [host] = session.xenapi.host.get_by_name_label(hostname)
        else:
            host = get_first_safely(session.xenapi.host.get_all())
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
        "sharable": False,
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
        vdi = create_test_vdi(session=session, host=host)
        delete_vdi = True
    else:
        delete_vdi = False

    c = xapi_nbd_client(vdi=vdi, session=session)

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

        session.xenapi.VDI.destroy(vdi)


def loop_connect_disconnect(session, host, vdi=None, n=1000):
    from xapi_nbd_client import xapi_nbd_client
    if vdi is None:
        vdi = create_test_vdi(session=session, host=host)
        delete_vdi = True

    try:
        try:
            for i in range(n):
                print("{}: connecting to {} on {}".format(i, vdi, host))
                xapi_nbd_client(vdi=vdi, session=session)
        finally:
            if delete_vdi:
                session.xenapi.VDI.destroy(vdi)
    finally:
        session.xenapi.session.logout()


def parallel_nbd_connections(session, host, same_vdi=True, n=100):
    from xapi_nbd_client import xapi_nbd_client

    # Stash the NBD clients here to avoid them being garbage collected
    # and disconnected immediately after creation :S.
    open_nbd_connections = []
    vdis_created = []

    if same_vdi:
        vdi = create_test_vdi(session=session, host=host)
        vdis_created += [vdi]

    try:
        try:
            for i in range(n):
                if not same_vdi:
                    vdi = create_test_vdi(session=session, host=host)
                    vdis_created += [vdi]
                print("{}: connecting to {} on {}".format(i, vdi, host))
                open_nbd_connections += [
                    xapi_nbd_client(host=host, vdi=vdi, session=session)
                ]
        finally:
            for c in open_nbd_connections:
                c.close()
            print("Destroying {} VDIs".format(len(vdis_created)))
            for vdi in vdis_created:
                print("Destroying VDI {}".format(vdi))
                session.xenapi.VDI.destroy(vdi)
                print("VDI destroyed")
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


def download_changed_blocks_in_bitmap_from_nbd(session, vdi, bitmap):

    from xapi_nbd_client import xapi_nbd_client
    import base64

    bitmap = base64.b64decode(bitmap)
    c = xapi_nbd_client(session=session, vdi=vdi)
    print("Size of network block device: %s" % c.size())
    for i in range(0, len(bitmap) - 1):
        if bitmap[i] == 1:
            offset = i * BLOCK_SIZE
            print("Reading %d bytes from offset %d" % (BLOCK_SIZE, offset))
            data = c.read(offset=offset, length=BLOCK_SIZE)
            yield data
    c.close()


def get_cbt_bitmap(session, vdi_from=None, vdi_to=None):
    if vdi_to is None:
        vdi_to = create_test_vdi(session=session)
        session.xenapi.VDI.enable_cbt(vdi_to)
    if vdi_from is None:
        vdi_from = session.xenapi.VDI.snapshot(vdi_to)

    return session.xenapi.VDI.list_changed_blocks(vdi_from, vdi_to)


def download_changed_blocks(session, vdi_from=None, vdi_to=None):
    if vdi_to is None:
        vdi_to = create_test_vdi(session=session)
        session.xenapi.VDI.enable_cbt(vdi_to)
    if vdi_from is None:
        vdi_from = session.xenapi.VDI.snapshot(vdi_to)

    bitmap = get_cbt_bitmap(session, vdi_from, vdi_to)
    return download_changed_blocks_in_bitmap_from_nbd(
        session=session, vdi=vdi_to, bitmap=bitmap)


def write_blocks_consecutively(changed_blocks, output_file=None):
    import tempfile
    if output_file is None:
        out = tempfile.NamedTemporaryFile('ab')
    else:
        out = open(output_file, 'ab')
    for b in changed_blocks:
        out.write(b)
    out.close()
    return out.name


def save_changed_blocks(session, vdi_from=None, vdi_to=None, output_file=None):
    blocks = download_changed_blocks(
        session=session, vdi_from=vdi_from, vdi_to=vdi_to)
    return write_blocks_consecutively(blocks, output_file)
