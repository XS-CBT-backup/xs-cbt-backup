"""
A Python wrapper around the nbd-client native Linux program.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path


def _write_cert_to_file(cert):
    certfile = tempfile.NamedTemporaryFile()
    certfile.write(cert)
    return certfile.name


def is_nbd_device_connected(nbd_device):
    """
    Checks whether the specified nbd device is connected according to
    nbd-client.
    """
    # First check if the file exists, because "nbd-client -c" returns
    # 1 for a non-existent file.
    if not Path(nbd_device).exists():
        raise FileNotFoundError
    cmd = ['nbd-client', '-c', nbd_device]
    returncode = subprocess.run(cmd).returncode
    if returncode == 0:
        return True
    if returncode == 1:
        return False
    raise subprocess.CalledProcessError(returncode=returncode, cmd=cmd)


def disconnect_nbd_device(nbd_device):
    """
    Disconnects the given device using nbd-client
    """
    subprocess.check_output(['nbd-client', '-d', nbd_device])


def disconnect_connected_devices():
    """
    Disconnects all the connected /dev/nbdX devices using nbd-client.
    """
    try:
        for device_no in range(0, sys.maxsize):
            nbd_device = "/dev/nbd{}".format(device_no)
            if is_nbd_device_connected(nbd_device=nbd_device):
                disconnect_nbd_device(nbd_device=nbd_device)
    except FileNotFoundError:
        pass


def find_unused_nbd_device():
    """
    Returns the path of the first /dev/nbdX device that is not
    connected according to nbd-client.
    """
    for device_no in range(0, sys.maxsize):
        nbd_device = "/dev/nbd{}".format(device_no)
        if not is_nbd_device_connected(nbd_device=nbd_device):
            return nbd_device


class LinuxNbdClient(object):
    """
    Python code wrapping an nbd-client connection.
    """
    def __init__(self,
                 address,
                 exportname="",
                 port=10809,
                 subject=None,
                 cert=None,
                 use_tls=True,
                 nbd_device=None,
                 block_size=None,
                 timeout=None,
                 use_socket_direct_protocol=False,
                 persist=True):

        if nbd_device is None:
            nbd_device = find_unused_nbd_device()

        command = ['nbd-client', '-name', exportname,
                   address, port, nbd_device]
        if block_size is not None:
            command += ['-block-size', str(block_size)]
        if timeout is not None:
            command += ['-timeout', str(timeout)]
        if use_socket_direct_protocol:
            command += ['-sdp']
        if persist:
            command += ['-persist']
        if use_tls:
            certfile = _write_cert_to_file(cert)
            command += ['-cacertfile', certfile]
            if subject is not None:
                command += ['-tlshostname', subject]
            command += ['-enable-tls']

        print(command)
        subprocess.check_output(command)

        self.nbd_device = nbd_device

    def __del__(self):
        self.close()

    def close(self):
        """
        Issues sync request, and then sends a disconnect request to the server.
        """
        self.flush()
        self._disconnect()

    def read(self, offset, length):
        """
        Returns length number of bytes read from the export, starting at
        the given offset.
        """
        with open(self.nbd_device, 'rb') as fin:
            fin.seek(offset)
            fin.read(length)

    def write(self, offset, length):
        """
        Returns length number of bytes read from the export, starting at
        the given offset.
        """
        with open(self.nbd_device, 'rb+') as fin:
            fin.seek(offset)
            fin.read(length)

    def _disconnect(self):
        subprocess.check_output(['nbd-client', '-d', self.nbd_device])

    def flush(self):
        """
        Issues a sync request.
        """
        os.sync()
