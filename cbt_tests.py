"""
Functions for testing changed block tracking and the NBD server in XenServer.
"""

import os
import random
import socket
import subprocess
import tempfile
import time

from python_nbd_client import PythonNbdClient, NBDEOFError

PROGRAM_NAME = "cbt_tests.py"


class SSHControl(object):
    """
    Allows controlling the host and the xapi-nbd service on a given machine.
    """

    def __init__(self, address, uname, pwd):
        """
        The given username and password will be used to SSH to this address.
        """
        self._address = address
        self._uname = uname
        self._pwd = pwd

    def run_ssh_command(self, command):
        """Run an SSH command using sshpass to authenticate."""
        return (subprocess.check_output([
            "sshpass", "-p", self._pwd, "ssh", "-o",
            "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no",
            "-l", self._uname, self._address
        ] + command))

    def control_xapi_nbd_service(self, service_command):
        """
        Pass the service_command argument list to "service xapi-nbd",
        using sshpass to authenticate.
        """
        self.run_ssh_command(["service", "xapi-nbd", service_command])


def read_from_vdi_via_nbd(nbd_info, use_tls=True):
    with PythonNbdClient(**nbd_info, use_tls=use_tls) as client:
        client.read(512 * 200, 512)


def test_data_destroy(session, vdi, nbd_info):
    """
    Verifies that we can run data_destroy without errors on a VDI
    immediately after disconnecting from the NBD server serving that VDI.
    """
    session.xenapi.VDI.enable_cbt(vdi)
    snapshot = session.xenapi.VDI.snapshot(vdi)
    read_from_vdi_via_nbd(nbd_info=nbd_info)
    session.xenapi.VDI.data_destroy(snapshot)
    # a cbt_metadata VDI should have no VBDs
    assert not session.xenapi.VDI.get_VBDs(vdi)


def repro_sm_bug(session, vdi):
    """
    Reproduces a race condition in SM between VDI.destroy on the
    snapshotted VDI, and VBD.unplug on the snapshot VDI when CBT is
    enabled. This has already been fixed.
    """
    # Without this line, if we do not enable CBT, it works:
    session.xenapi.VDI.enable_cbt(vdi)
    snapshot = session.xenapi.VDI.snapshot(vdi)

    nbd_info = session.xenapi.VDI.get_nbd_info(snapshot)
    client = PythonNbdClient(**nbd_info)
    client.read(512 * 200, 512)
    # If we run the VDI.destroy here it will work:
    # self._session.xenapi.VDI.destroy(vdi)
    client.close()

    # It also works if we first wait for the unplug to finish, so probably
    # this is a race between VBD.unplug on the snapshot and VDI.destroy on
    # the snapshotted VDI:
    # time.sleep(2)

    session.xenapi.VDI.destroy(vdi)


def _interrupt_connection(
        ssh,
        nbd_info,
        terminate_while_connected,
        terminate_command):
    client = PythonNbdClient(**nbd_info)
    if not terminate_while_connected:
        client.close()
    ssh.run_ssh_command(terminate_command)
    ssh.control_xapi_nbd_service("start")
    # wait for a while for the cleanup to finish
    time.sleep(6)


def test_nbd_server_cleans_up_vbds(session, vdi, nbd_info, uname, pwd):
    """
    Verifies that the NBD server has no leaked VBDs after it's restarted
    after abnormal termination.
    """
    ssh = SSHControl(nbd_info["address"], uname, pwd)
    _interrupt_connection(
        ssh, nbd_info, False, ["service", "xapi-nbd", "stop"])
    assert not session.xenapi.VDI.get_VBDs(vdi)
    _interrupt_connection(
        ssh, nbd_info, True, ["service", "xapi-nbd", "restart"])
    assert not session.xenapi.VDI.get_VBDs(vdi)
    # This is similar to a crash, as the program cannot handle this
    # signal
    _interrupt_connection(
        ssh, nbd_info, True, ["pkill", "-9", "xapi-nbd"])
    assert not session.xenapi.VDI.get_VBDs(vdi)


def loop_connect_disconnect(
        nbd_info,
        use_tls,
        random_delays=False,
        fail_connection=False):
    if fail_connection:
        nbd_info["exportname"] = "invalid export name"
    for _ in range(1030):
        try:
            with PythonNbdClient(**nbd_info, use_tls=use_tls):
                if random_delays:
                    time.sleep(random.random())
            if random_delays:
                time.sleep(random.random())
        except NBDEOFError as exc:
            if not fail_connection:
                raise exc


def parallel_nbd_connections(nbd_info, use_tls):
    """
    Create n_connections parallel connections to the NBD server.
    """
    open_nbd_connections = []
    try:
        for _ in range(16):
            open_nbd_connections += [PythonNbdClient(**nbd_info, use_tls=use_tls)]
        try:
            open_nbd_connections += [PythonNbdClient(**nbd_info, use_tls=use_tls)]
            raise AssertionError
        except NBDEOFError:
            pass
        open_nbd_connections[0].close()
        open_nbd_connections += [PythonNbdClient(**nbd_info, use_tls=use_tls)]
    finally:
        time.sleep(2)
        for client in open_nbd_connections:
            client.close()


def _read_after_sleep(nbd_info, sleep):
    with PythonNbdClient(**nbd_info) as client:
        time.sleep(sleep)
        client.read(0, 512)


def test_xapi_nbd_timeout(nbd_info):
    timeout = 16
    _read_after_sleep(nbd_info, sleep=timeout - 2)
    try:
        _read_after_sleep(nbd_info, sleep=timeout + 2)
        raise AssertionError
    except socket.timeout:
        pass


def verify_xapi_nbd_systemd_service(
        nbd_info, uname, pwd, socket_activated=False):
    ssh = SSHControl(nbd_info["address"], uname, pwd)
    # This will fail if the service isn't running
    ssh.control_xapi_nbd_service(service_command="status")
    read_from_vdi_via_nbd(nbd_info)
    ssh.control_xapi_nbd_service(service_command="stop")
    try:
        read_from_vdi_via_nbd(nbd_info)
        assert socket_activated
    except ConnectionRefusedError:
        assert not socket_activated
    ssh.control_xapi_nbd_service(service_command="restart")
    read_from_vdi_via_nbd(nbd_info)


def download_whole_vdi_using_nbd(downloader, nbd_info):
    out_file = tempfile.mkstemp()
    try:
        downloader.download_vdi(
            vdi_nbd_server_info=nbd_info,
            out_file=out_file)
    finally:
        os.remove(out_file)


def list_changed_blocks(session, vdi):
    session.xenapi.VDI.enable_cbt(vdi)
    vdi_from = session.xenapi.VDI.snapshot(vdi)
    session.xenapi.VDI.list_changed_blocks(vdi_from, vdi)
